'''
Memory access must be routed through the bufferpool
Storage structure on disk follows this hierarchy:
DB Directory: Root Folder
    -> TableName: Subfolder
        -> PageRange_{page_range_index}: Subfolder
            -> Page_{record_column}_{page_index}.bin: Data File
'''


from lstore.config import MAX_NUM_FRAME, NUM_HIDDEN_COLUMNS
from lstore.page import Page
import os
import json
import threading
from queue import Queue
from typing import List, Union


class Frame:
    '''Container for a page in the bufferpool'''
    def __init__(self):
        self.pin:int = 0
        self.page:Page = None
        self.page_path = None
        self.dirty:bool = False
        '''Dirty flag indicates pending writes to disk before frame removal'''

        self._pin_lock = threading.Lock()
        self._write_lock = threading.Lock()

    def increment_pin(self):
        '''Increases reference counter'''
        with self._pin_lock:
            self.pin += 1

    def decrement_pin(self):
        '''Decreases reference counter'''
        with self._pin_lock:
            self.pin -= 1
            
    def get_page_capacity(self) -> bool:
        '''Checks if page can store additional records'''
        with self._write_lock:
            return self.page.has_capacity()

    def write_with_lock(self, value) -> int:
        '''Adds value to page with thread safety'''
        with self._write_lock:
            self.dirty = True
            return self.page.write(value)

    def write_precise_with_lock(self, slot_idx, value):
        '''Updates specific slot with thread safety'''
        with self._write_lock:
            self.dirty = True
            self.page.write_precise(slot_idx, value)

    def load_page(self, page_location:str):
        '''Retrieves page from storage or initializes new one if not found'''

        self._write_lock.acquire()

        self.page = Page()
        self.page_path = page_location
        if os.path.exists(page_location):
            with open(page_location, "r", encoding="utf-8") as page_file:
                page_data = json.load(page_file)
            self.page.deserialize(page_data)
        else:
            os.makedirs(os.path.dirname(page_location), exist_ok=True)
            self.dirty = True

        self._write_lock.release()

    def unload_page(self):
        if (self.pin > 0):
            raise MemoryError("Cannot unload a page currently in use")
        
        self._write_lock.acquire()

        if (self.dirty):
            with open(self.page_path, "w", encoding="utf-8") as page_file:
                page_content = self.page.serialize()
                json.dump(page_content, page_file)

        self.dirty = False
        self.page = None
        self.page_path = None

        self._write_lock.release()


class BufferPool:
    '''Mediates all page access operations'''
    def __init__(self, table_path, num_columns):
        self.frame_map = dict()
        '''Maps page locations to frame indices'''
        self.frame_count = MAX_NUM_FRAME * (num_columns + NUM_HIDDEN_COLUMNS)
        self.frames:List[Frame] = []
        self.free_frames = Queue(self.frame_count)
        self.used_frames = Queue(self.frame_count)
        
        self.table_path = table_path
        self.bufferpool_lock = threading.Lock()

        for idx in range(self.frame_count):
            self.frames.append(Frame())
            self.free_frames.put(idx)

    def get_page_path(self, page_range_index, record_column, page_index) -> str:
        '''Constructs file path for page storage'''
        return os.path.join(self.table_path, os.path.join(f"PageRange_{page_range_index}", f"Page_{record_column}_{page_index}.bin"))
    
    def mark_frame_used(self, frame_idx):
        '''Releases frame after usage'''
        self.frames[frame_idx].decrement_pin()

    def get_page_frame_num(self, page_range_index, record_column, page_index) -> Union[int, None]:
        '''Locates frame index for page if loaded in memory'''
        page_location = self.get_page_path(page_range_index, record_column, page_index)
        return self.frame_map.get(page_location, None)
    
    def get_page_frame(self, page_range_index, record_column, page_index) -> Union[Frame, None]:
        '''Retrieves frame containing requested page'''

        page_location = self.get_page_path(page_range_index, record_column, page_index)
        frame_idx = self.frame_map.get(page_location, None)

        if (frame_idx is None):
            # Return None if no frames available and eviction fails
            if (self.free_frames.empty() and not self.__replacement_policy()):
                return None
            
            target_frame:Frame = self.__load_new_frame(page_location)
            return target_frame
        
        target_frame:Frame = self.frames[frame_idx]
        target_frame.increment_pin()
        return target_frame
    
    def get_page_has_capacity(self, page_range_index, record_column, page_index) -> Union[bool, None]:
        '''Determines if page can accommodate more records'''
        page_location = self.get_page_path(page_range_index, record_column, page_index)

        with self.bufferpool_lock:
            frame_idx = self.frame_map.get(page_location, None)

            if (frame_idx is None):
                # Return None if no frames available and eviction fails
                if (self.free_frames.empty() and not self.__replacement_policy()):
                    return None
                
                target_frame:Frame = self.__load_new_frame(page_location)
                target_frame.decrement_pin()
                return target_frame.get_page_capacity()
            
        target_frame:Frame = self.frames[frame_idx]
        return target_frame.get_page_capacity()
    
    def read_page_slot(self, page_range_index, record_column, page_index, slot_index) -> Union[int, None]:
        '''Fetches value from specified slot in page'''
        page_location = self.get_page_path(page_range_index, record_column, page_index)
        with self.bufferpool_lock:
            frame_idx = self.frame_map.get(page_location, None)

            if (frame_idx is None):
                if (self.free_frames.empty() and not self.__replacement_policy()):
                    return None

                target_frame:Frame = self.__load_new_frame(page_location)
                return target_frame.page.get(slot_index)

        target_frame:Frame = self.frames[frame_idx]
        target_frame.increment_pin()
        return target_frame.page.get(slot_index)
    
    def write_page_slot(self, page_range_index, record_column, page_index, slot_index, value) -> bool:
        '''Updates specific slot with provided value'''
        page_location = self.get_page_path(page_range_index, record_column, page_index)

        with self.bufferpool_lock:
            frame_idx = self.frame_map.get(page_location, None)
            target_frame:Frame = None

            if (frame_idx is None):
                if (self.free_frames.empty() and not self.__replacement_policy()):
                    return False

                target_frame:Frame = self.__load_new_frame(page_location)
            else:
                target_frame:Frame = self.frames[frame_idx]
                target_frame.increment_pin()

        target_frame.write_precise_with_lock(slot_index, value)
        target_frame.decrement_pin()
        return True
    
    def write_page_next(self, page_range_index, record_column, page_index, value) -> Union[int, None]:
        '''Appends value to page and returns assigned slot position'''
        page_location = self.get_page_path(page_range_index, record_column, page_index)
        frame_idx = self.frame_map.get(page_location, None)
        target_frame:Frame = None

        if (frame_idx is None):
            if (self.free_frames.empty() and not self.__replacement_policy()):
                for i in range(MAX_NUM_FRAME):
                    print(f"Frame {i} has pin {self.frames[i].pin}")
                raise MemoryError("Frame allocation failed")

            target_frame:Frame = self.__load_new_frame(page_location)
        else:
            target_frame:Frame = self.frames[frame_idx]
            target_frame.increment_pin()

        slot_position = target_frame.write_with_lock(value)
        target_frame.decrement_pin()
        return slot_position
    
    def unload_all_frames(self):
        '''Flushes all frames to disk'''
        error_count = 0
        while (not self.used_frames.empty()):
            if (self.__replacement_policy() is False):
                error_count += 1
                if (error_count > MAX_NUM_FRAME):
                    raise MemoryError("Failed to unload all frames")

    def __load_new_frame(self, page_location:str) -> Frame:
        '''Allocates new frame for page'''
        
        # Note: Queue.get can block transactions until frame becomes available (for Milestone 3)
        frame_idx = self.free_frames.get()
        target_frame:Frame = self.frames[frame_idx]
        target_frame.increment_pin()

        target_frame.load_page(page_location)
        self.frame_map[page_location] = frame_idx
        #print(f"Loading frame {frame_idx} with {page_location}")

        self.used_frames.put(frame_idx)

        return target_frame
        
    def __replacement_policy(self) -> bool:
        '''
        Implements LRU eviction strategy
        Returns success status of frame allocation attempt
        '''
        frames_to_check = self.used_frames.qsize()

        for _ in range(frames_to_check):
            frame_idx = self.used_frames.get()
            target_frame:Frame = self.frames[frame_idx]

            if (target_frame.pin == 0):
                #print(f"removing {target_frame.page_path} from frame_map")
                # Evict unused frame
                del self.frame_map[target_frame.page_path]
                target_frame.unload_page()
                self.free_frames.put(frame_idx)
                return True
            else:
                # Return frame to queue if still in use
                self.used_frames.put(frame_idx)

        return False

    # Implementation in progress
    # def __frame_acquisition_strategy(self, page_location) -> Frame:
    #     '''Obtains frame for given page location, returns False if unsuccessful'''
    #     frame_idx = self.frame_map.get(page_location, None)
    #     target_frame:Frame = None

    #     if (frame_idx is None):
    #         if (self.free_frames.empty() and not self.__replacement_policy()):
    #             # Return False if no frames available and eviction fails
    #             return False

    #         target_frame:Frame = self.__load_new_frame(page_location)
    #     else:
    #         target_frame:Frame = self.frames[frame_idx]
    #         target_frame.increment_pin()

    #     return target_frame
