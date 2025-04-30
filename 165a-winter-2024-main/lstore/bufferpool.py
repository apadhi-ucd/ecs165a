from lstore.config import MAX_NUM_FRAME, NUM_HIDDEN_COLUMNS
from lstore.page import Page
from lstore.lock import Latch
import os
import threading
from queue import Queue
from typing import List, Union


def _parse_string(s, idx):
    """Parse a JSON string starting at index idx"""
    start_idx = idx
    idx += 1  # Skip opening quote
    result = ""
    while idx < len(s) and s[idx] != '"':
        if s[idx] == '\\' and idx + 1 < len(s):
            idx += 1
            if s[idx] == 'n': result += '\n'
            elif s[idx] == 'r': result += '\r'
            elif s[idx] == 't': result += '\t'
            elif s[idx] == 'b': result += '\b'
            elif s[idx] == 'f': result += '\f'
            else: result += s[idx]
        else:
            result += s[idx]
        idx += 1
    return result, idx + 1  # Skip closing quote

def _parse_number(s, idx):
    """Parse a JSON number starting at index idx"""
    start_idx = idx
    while idx < len(s) and (s[idx].isdigit() or s[idx] in '.-+eE'):
        idx += 1
    num_str = s[start_idx:idx]
    if '.' in num_str or 'e' in num_str.lower():
        return float(num_str), idx
    else:
        return int(num_str), idx

def _parse_value(s, idx):
    """Parse a JSON value starting at index idx"""
    while idx < len(s) and s[idx].isspace():
        idx += 1
    
    if idx >= len(s):
        raise ValueError("Unexpected end of JSON data")
    
    if s[idx] == '{':
        return _parse_object(s, idx)
    elif s[idx] == '[':
        return _parse_array(s, idx)
    elif s[idx] == '"':
        return _parse_string(s, idx)
    elif s[idx].isdigit() or s[idx] == '-':
        return _parse_number(s, idx)
    elif s[idx:idx+4] == 'true':
        return True, idx + 4
    elif s[idx:idx+5] == 'false':
        return False, idx + 5
    elif s[idx:idx+4] == 'null':
        return None, idx + 4
    else:
        raise ValueError(f"Invalid JSON at position {idx}: {s[idx:idx+10]}")

def _parse_object(s, idx):
    """Parse a JSON object starting at index idx"""
    result = {}
    idx += 1  # Skip opening brace
    
    # Skip whitespace
    while idx < len(s) and s[idx].isspace():
        idx += 1
    
    if idx < len(s) and s[idx] == '}':
        return result, idx + 1
    
    while idx < len(s):
        # Parse key
        while idx < len(s) and s[idx].isspace():
            idx += 1
        
        if s[idx] != '"':
            raise ValueError(f"Expected string key at position {idx}")
        
        key, idx = _parse_string(s, idx)
        
        # Skip whitespace and colon
        while idx < len(s) and s[idx].isspace():
            idx += 1
        
        if idx >= len(s) or s[idx] != ':':
            raise ValueError(f"Expected ':' at position {idx}")
        
        idx += 1  # Skip colon
        
        # Parse value
        value, idx = _parse_value(s, idx)
        result[key] = value
        
        # Skip whitespace
        while idx < len(s) and s[idx].isspace():
            idx += 1
        
        if idx >= len(s):
            raise ValueError("Unexpected end of JSON data")
        
        if s[idx] == '}':
            return result, idx + 1
        
        if s[idx] != ',':
            raise ValueError(f"Expected ',' or '}}' at position {idx}")
        
        idx += 1  # Skip comma
    
    raise ValueError("Unexpected end of JSON data")

def _parse_array(s, idx):
    """Parse a JSON array starting at index idx"""
    result = []
    idx += 1  # Skip opening bracket
    
    # Skip whitespace
    while idx < len(s) and s[idx].isspace():
        idx += 1
    
    if idx < len(s) and s[idx] == ']':
        return result, idx + 1
    
    while idx < len(s):
        # Parse value
        value, idx = _parse_value(s, idx)
        result.append(value)
        
        # Skip whitespace
        while idx < len(s) and s[idx].isspace():
            idx += 1
        
        if idx >= len(s):
            raise ValueError("Unexpected end of JSON data")
        
        if s[idx] == ']':
            return result, idx + 1
        
        if s[idx] != ',':
            raise ValueError(f"Expected ',' or ']' at position {idx}")
        
        idx += 1  # Skip comma
    
    raise ValueError("Unexpected end of JSON data")

def json_load(file):
    """Load JSON data from a file-like object"""
    content = file.read()
    value, _ = _parse_value(content, 0)
    return value

def _stringify_value(value, indent=None, level=0):
    """Convert a Python value to a JSON string"""
    if value is None:
        return "null"
    elif value is True:
        return "true"
    elif value is False:
        return "false"
    elif isinstance(value, (int, float)):
        return str(value)
    elif isinstance(value, str):
        # Escape special characters
        escaped = value.replace('\\', '\\\\').replace('"', '\\"')
        escaped = escaped.replace('\n', '\\n').replace('\r', '\\r')
        escaped = escaped.replace('\t', '\\t').replace('\b', '\\b').replace('\f', '\\f')
        return f'"{escaped}"'
    elif isinstance(value, list):
        if not value:
            return "[]"
        
        if indent is None:
            items = [_stringify_value(item, indent) for item in value]
            return f"[{','.join(items)}]"
        else:
            next_level = level + 1
            items = [f"{' ' * (next_level * indent)}{_stringify_value(item, indent, next_level)}" for item in value]
            return "[" + "\n" + ",\n".join(items) + "\n" + (' ' * (level * indent)) + "]"
    elif isinstance(value, dict):
        if not value:
            return "{}"
        
        if indent is None:
            items = [f"{_stringify_value(k, indent)}:{_stringify_value(v, indent)}" for k, v in value.items()]
            return f"{{{','.join(items)}}}"
        else:
            next_level = level + 1
            items = [f"{' ' * (next_level * indent)}{_stringify_value(k, indent)}:{' '}{_stringify_value(v, indent, next_level)}" for k, v in value.items()]
            return "{" + "\n" + ",\n".join(items) + "\n" + (' ' * (level * indent)) + "}"
    else:
        raise TypeError(f"Object of type {type(value)} is not JSON serializable")

def json_dump(obj, file, indent=None):
    """Serialize obj as a JSON formatted stream to file"""
    json_str = _stringify_value(obj, indent)
    file.write(json_str)


class BufferPool:
    def __init__(self, table_path, num_columns):
        self.frame_directory = dict()  # Maps page paths to frame numbers
        self.num_frames = MAX_NUM_FRAME * (num_columns + NUM_HIDDEN_COLUMNS)
        
        self.frame_pins = [Latch() for _ in range(self.num_frames)]  # Pin counts for each frame
        self.frame_pages = [None] * self.num_frames  # Page objects for each frame
        self.frame_page_paths = [None] * self.num_frames  # Page paths for each frame
        self.frame_dirty_bits = [False] * self.num_frames  # Dirty flags for each frame
        
        # Locks for each frame
        self.frame_write_locks = [threading.Lock() for _ in range(self.num_frames)]
        
        self.available_frames_queue = Queue(self.num_frames)
        self.unavailable_frames_queue = Queue(self.num_frames)
        
        self.table_path = table_path
        self.bufferpool_lock = threading.Lock()

        # Initialize available frames
        for i in range(self.num_frames):
            self.available_frames_queue.put(i)
    
    def _load_page_to_frame(self, frame_num, page_path):
        '''Loads a page from disk to the specified frame'''
        
        with self.frame_write_locks[frame_num]:
            self.frame_pages[frame_num] = Page()
            self.frame_page_paths[frame_num] = page_path
            
            if os.path.exists(page_path):
                with open(page_path, "r", encoding="utf-8") as page_file:
                    page_json_data = json_load(page_file)
                self.frame_pages[frame_num].deserialize(page_json_data)
            else:
                os.makedirs(os.path.dirname(page_path), exist_ok=True)
                self.frame_dirty_bits[frame_num] = True
    
    def _unload_page_from_frame(self, frame_num):
        '''Unloads a page from the specified frame'''
        if self.frame_pins[frame_num].count > 0:
            raise MemoryError("Cannot unload a page that's being used by processes")
        
        with self.frame_write_locks[frame_num]:
            if self.frame_dirty_bits[frame_num]:
                with open(self.frame_page_paths[frame_num], "w", encoding="utf-8") as page_json_file:
                    page_data = self.frame_pages[frame_num].serialize()
                    json_dump(page_data, page_json_file)
            
            self.frame_dirty_bits[frame_num] = False
            self.frame_pages[frame_num] = None
            self.frame_page_paths[frame_num] = None
    
    def _write_precise_with_lock(self, frame_num, slot_index, value):
        '''Writes a value to a page slot with a lock'''
        with self.frame_write_locks[frame_num]:
            self.frame_dirty_bits[frame_num] = True
            self.frame_pages[frame_num].write_precise(slot_index, value)
    
    def _write_with_lock(self, frame_num, value) -> int:
        '''Writes a value to a page with a lock'''
        with self.frame_write_locks[frame_num]:
            self.frame_dirty_bits[frame_num] = True
            return self.frame_pages[frame_num].write(value)
    
    def _get_page_capacity(self, frame_num) -> bool:
        '''Returns True if the page has capacity for more records'''
        with self.frame_write_locks[frame_num]:
            return self.frame_pages[frame_num].has_capacity()
    
    def _increment_pin(self, frame_num):
        '''Increments the pin count of a frame'''
        self.frame_pins[frame_num].count_up()
    
    def _decrement_pin(self, frame_num):
        '''Decrements the pin count of a frame'''
        self.frame_pins[frame_num].count_down()

    def get_page_frame(self, page_range_index, record_column, page_index) -> Union[int, None]:
        '''Returns the frame number of a page if the page can be grabbed from disk, 
        otherwise returns None'''

        page_disk_path = self.get_page_path(page_range_index, record_column, page_index)
        page_frame_num = self.frame_directory.get(page_disk_path, None)

        if (page_frame_num is None):
            if (self.available_frames_queue.empty() and not self.__replacement_policy()):
                return None
            
            page_frame_num = self.__load_new_frame(page_disk_path)
        else:
            self._increment_pin(page_frame_num)
        
        return page_frame_num
    
    def get_page_has_capacity(self, page_range_index, record_column, page_index) -> Union[bool, None]:
        '''Returns True if the page has capacity for more records'''
        page_disk_path = self.get_page_path(page_range_index, record_column, page_index)

        with self.bufferpool_lock:
            page_frame_num = self.frame_directory.get(page_disk_path, None)

            if (page_frame_num is None):
                if (self.available_frames_queue.empty() and not self.__replacement_policy()):
                    return None
                
                page_frame_num = self.__load_new_frame(page_disk_path)
                self._decrement_pin(page_frame_num)
                return self._get_page_capacity(page_frame_num)
            
        return self._get_page_capacity(page_frame_num)
    
    def read_page_slot(self, page_range_index, record_column, page_index, slot_index) -> Union[int, None]:
        '''Returns the value within a page if the page can be grabbed from disk,
        otherwise returns None'''
        page_disk_path = self.get_page_path(page_range_index, record_column, page_index)
        with self.bufferpool_lock:
            page_frame_num = self.frame_directory.get(page_disk_path, None)

            if (page_frame_num is None):
                if (self.available_frames_queue.empty() and not self.__replacement_policy()):
                    return None

                page_frame_num = self.__load_new_frame(page_disk_path)
                return self.frame_pages[page_frame_num].get(slot_index)

        self._increment_pin(page_frame_num)
        return self.frame_pages[page_frame_num].get(slot_index)
    
    def write_page_slot(self, page_range_index, record_column, page_index, slot_index, value) -> bool:
        '''Writes a value to a page slot'''
        page_disk_path = self.get_page_path(page_range_index, record_column, page_index)

        with self.bufferpool_lock:
            page_frame_num = self.frame_directory.get(page_disk_path, None)

            if (page_frame_num is None):
                if (self.available_frames_queue.empty() and not self.__replacement_policy()):
                    return False

                page_frame_num = self.__load_new_frame(page_disk_path)
            else:
                self._increment_pin(page_frame_num)

        self._write_precise_with_lock(page_frame_num, slot_index, value)
        self._decrement_pin(page_frame_num)
        return True
    
    def write_page_next(self, page_range_index, record_column, page_index, value) -> Union[int, None]:
        '''Write a value to page and returns the slot it was written to, returns None if unable to locate frame'''
        page_disk_path = self.get_page_path(page_range_index, record_column, page_index)
        page_frame_num = self.frame_directory.get(page_disk_path, None)

        if (page_frame_num is None):
            if (self.available_frames_queue.empty() and not self.__replacement_policy()):
                for i in range(MAX_NUM_FRAME):
                    print(f"Frame {i} has pin {self.frame_pins[i].count}")
                raise MemoryError("Unable to allocate new frame")

            page_frame_num = self.__load_new_frame(page_disk_path)
        else:
            self._increment_pin(page_frame_num)

        slot_index = self._write_with_lock(page_frame_num, value)
        self._decrement_pin(page_frame_num)
        return slot_index
    
    def get_page_frame_num(self, page_range_index, record_column, page_index) -> Union[int, None]:
        '''Returns the frame number of the page if the page is in memory, otherwise returns None'''
        page_disk_path = self.get_page_path(page_range_index, record_column, page_index)
        return self.frame_directory.get(page_disk_path, None)

    def get_page_path(self, page_range_index, record_column, page_index) -> str:
        '''Returns the path of the page'''
        return os.path.join(self.table_path, os.path.join(f"PageRange_{page_range_index}", f"Page_{record_column}_{page_index}.bin"))
    
    def mark_frame_used(self, frame_num):
        '''Use this to close a frame once a page has been used'''
        self._decrement_pin(frame_num)

    def unload_all_frames(self):
        '''Unloads all frames in the bufferpool'''
        fail_count = 0
        while (not self.unavailable_frames_queue.empty()):
            if (self.__replacement_policy() is False):
                fail_count += 1
                if (fail_count > MAX_NUM_FRAME):
                    raise MemoryError("Unable to unload all frames")

    def __load_new_frame(self, page_path:str) -> int:
        '''Loads a new frame into the bufferpool and returns the frame number'''
        
        frame_num = self.available_frames_queue.get()
        self._increment_pin(frame_num)

        self._load_page_to_frame(frame_num, page_path)
        self.frame_directory[page_path] = frame_num

        self.unavailable_frames_queue.put(frame_num)

        return frame_num
        
    def __replacement_policy(self) -> bool:
        '''
        Using LRU Policy
        Returns true if we were properly able to allocate new space for a frame
        '''
        num_used_frames = self.unavailable_frames_queue.qsize()

        for _ in range(num_used_frames):
            frame_num = self.unavailable_frames_queue.get()

            if (self.frame_pins[frame_num].count == 0):

                del self.frame_directory[self.frame_page_paths[frame_num]]
                self._unload_page_from_frame(frame_num)
                self.available_frames_queue.put(frame_num)
                return True
            else:
                self.unavailable_frames_queue.put(frame_num)

        return False
