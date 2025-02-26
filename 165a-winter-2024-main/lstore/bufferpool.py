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
from typing import List, Union, Dict, Tuple


class BufferPool:
    '''Mediates all page access operations'''
    def __init__(self, table_path, num_columns):
        self.frame_map = dict()
        '''Maps page locations to frame indices'''
        self.frame_count = MAX_NUM_FRAME * (num_columns + NUM_HIDDEN_COLUMNS)
        
        # Direct frame management without Frame class
        self.pages = [None] * self.frame_count  # Stores Page objects
        self.page_paths = [None] * self.frame_count  # Stores file paths
        self.pin_counts = [0] * self.frame_count  # Reference counters
        self.dirty_flags = [False] * self.frame_count  # Dirty flags
        
        # Locks for thread safety
        self.pin_locks = [threading.Lock() for _ in range(self.frame_count)]
        self.write_locks = [threading.Lock() for _ in range(self.frame_count)]
        
        self.free_frames = Queue(self.frame_count)
        self.used_frames = Queue(self.frame_count)
        
        self.table_path = table_path
        self.bufferpool_lock = threading.Lock()

        # Initialize free frames
        for idx in range(self.frame_count):
            self.free_frames.put(idx)

    def get_page_path(self, page_range_index, record_column, page_index) -> str:
        '''Constructs file path for page storage'''
        return os.path.join(self.table_path, os.path.join(f"PageRange_{page_range_index}", f"Page_{record_column}_{page_index}.bin"))
    
    def increment_pin(self, frame_idx):
        '''Increases reference counter for a frame'''
        with self.pin_locks[frame_idx]:
            self.pin_counts[frame_idx] += 1
    
    def decrement_pin(self, frame_idx):
        '''Decreases reference counter for a frame'''
        with self.pin_locks[frame_idx]:
            self.pin_counts[frame_idx] -= 1
    
    def mark_frame_used(self, frame_idx):
        '''Releases frame after usage'''
        self.decrement_pin(frame_idx)

    def get_page_capacity(self, frame_idx) -> bool:
        '''Checks if page can store additional records'''
        with self.write_locks[frame_idx]:
            return self.pages[frame_idx].has_capacity()

    def write_with_lock(self, frame_idx, value) -> int:
        '''Adds value to page with thread safety'''
        with self.write_locks[frame_idx]:
            self.dirty_flags[frame_idx] = True
            return self.pages[frame_idx].write(value)

    def write_precise_with_lock(self, frame_idx, slot_idx, value):
        '''Updates specific slot with thread safety'''
        with self.write_locks[frame_idx]:
            self.dirty_flags[frame_idx] = True
            self.pages[frame_idx].write_precise(slot_idx, value)

    def load_page(self, frame_idx, page_location):
        '''Retrieves page from storage or initializes new one if not found'''
        with self.write_locks[frame_idx]:
            self.pages[frame_idx] = Page()
            self.page_paths[frame_idx] = page_location
            
            if os.path.exists(page_location):
                with open(page_location, "r", encoding="utf-8") as page_file:
                    page_data = json.load(page_file)
                self.pages[frame_idx].deserialize(page_data)
            else:
                os.makedirs(os.path.dirname(page_location), exist_ok=True)
                self.dirty_flags[frame_idx] = True

    def unload_page(self, frame_idx):
        '''Writes page to disk if dirty and releases the frame'''
        if self.pin_counts[frame_idx] > 0:
            raise MemoryError("Cannot unload a page currently in use")
        
        with self.write_locks[frame_idx]:
            if self.dirty_flags[frame_idx]:
                with open(self.page_paths[frame_idx], "w", encoding="utf-8") as page_file:
                    page_content = self.pages[frame_idx].serialize()
                    json.dump(page_content, page_file)

            self.dirty_flags[frame_idx] = False
            self.pages[frame_idx] = None
            self.page_paths[frame_idx] = None

    def get_page_frame_num(self, page_range_index, record_column, page_index) -> Union[int, None]:
        '''Locates frame index for page if loaded in memory'''
        page_location = self.get_page_path(page_range_index, record_column, page_index)
        return self.frame_map.get(page_location, None)
    
    def get_page_frame(self, page_range_index, record_column, page_index) -> Union[int, None]:
        '''Retrieves frame index containing requested page'''
        page_location = self.get_page_path(page_range_index, record_column, page_index)
        frame_idx = self.frame_map.get(page_location, None)

        if frame_idx is None:
            # Return None if no frames available and eviction fails
            if self.free_frames.empty() and not self.__replacement_policy():
                return None
            
            frame_idx = self.__load_new_frame(page_location)
            return frame_idx
        
        self.increment_pin(frame_idx)
        return frame_idx
    
    def get_page_has_capacity(self, page_range_index, record_column, page_index) -> Union[bool, None]:
        '''Determines if page can accommodate more records'''
        page_location = self.get_page_path(page_range_index, record_column, page_index)

        with self.bufferpool_lock:
            frame_idx = self.frame_map.get(page_location, None)

            if frame_idx is None:
                # Return None if no frames available and eviction fails
                if self.free_frames.empty() and not self.__replacement_policy():
                    return None
                
                frame_idx = self.__load_new_frame(page_location)
                self.decrement_pin(frame_idx)
                return self.get_page_capacity(frame_idx)
            
        return self.get_page_capacity(frame_idx)
    
    def read_page_slot(self, page_range_index, record_column, page_index, slot_index) -> Union[int, None]:
        '''Fetches value from specified slot in page'''
        page_location = self.get_page_path(page_range_index, record_column, page_index)
        
        with self.bufferpool_lock:
            frame_idx = self.frame_map.get(page_location, None)

            if frame_idx is None:
                if self.free_frames.empty() and not self.__replacement_policy():
                    return None

                frame_idx = self.__load_new_frame(page_location)
                value = self.pages[frame_idx].get(slot_index)
                self.decrement_pin(frame_idx)
                return value

        self.increment_pin(frame_idx)
        value = self.pages[frame_idx].get(slot_index)
        self.decrement_pin(frame_idx)
        return value
    
    def write_page_slot(self, page_range_index, record_column, page_index, slot_index, value) -> bool:
        '''Updates specific slot with provided value'''
        page_location = self.get_page_path(page_range_index, record_column, page_index)

        with self.bufferpool_lock:
            frame_idx = self.frame_map.get(page_location, None)

            if frame_idx is None:
                if self.free_frames.empty() and not self.__replacement_policy():
                    return False

                frame_idx = self.__load_new_frame(page_location)
            else:
                self.increment_pin(frame_idx)

        self.write_precise_with_lock(frame_idx, slot_index, value)
        self.decrement_pin(frame_idx)
        return True
    
    def write_page_next(self, page_range_index, record_column, page_index, value) -> Union[int, None]:
        '''Appends value to page and returns assigned slot position'''
        page_location = self.get_page_path(page_range_index, record_column, page_index)
        
        with self.bufferpool_lock:
            frame_idx = self.frame_map.get(page_location, None)

            if frame_idx is None:
                if self.free_frames.empty() and not self.__replacement_policy():
                    for i in range(MAX_NUM_FRAME):
                        print(f"Frame {i} has pin {self.pin_counts[i]}")
                    raise MemoryError("Frame allocation failed")

                frame_idx = self.__load_new_frame(page_location)
            else:
                self.increment_pin(frame_idx)

        slot_position = self.write_with_lock(frame_idx, value)
        self.decrement_pin(frame_idx)
        return slot_position
    
    def unload_all_frames(self):
        '''Flushes all frames to disk'''
        error_count = 0
        while not self.used_frames.empty():
            if self.__replacement_policy() is False:
                error_count += 1
                if error_count > MAX_NUM_FRAME:
                    raise MemoryError("Failed to unload all frames")

    def __load_new_frame(self, page_location:str) -> int:
        '''Allocates new frame for page'''
        
        # Note: Queue.get can block transactions until frame becomes available (for Milestone 3)
        frame_idx = self.free_frames.get()
        self.increment_pin(frame_idx)

        self.load_page(frame_idx, page_location)
        self.frame_map[page_location] = frame_idx
        #print(f"Loading frame {frame_idx} with {page_location}")

        self.used_frames.put(frame_idx)

        return frame_idx
        
    def __replacement_policy(self) -> bool:
        '''
        Implements LRU eviction strategy
        Returns success status of frame allocation attempt
        '''
        frames_to_check = self.used_frames.qsize()

        for _ in range(frames_to_check):
            frame_idx = self.used_frames.get()

            if self.pin_counts[frame_idx] == 0:
                #print(f"removing {self.page_paths[frame_idx]} from frame_map")
                # Evict unused frame
                del self.frame_map[self.page_paths[frame_idx]]
                self.unload_page(frame_idx)
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
