# Buffer Pool Class: 
# - Notes: Least frequently used out first
# - Instance variabels: dirty page count (might need page count, or might just consistently have the same # of pages loaded)
# - evict(): checks if page is clean or dirty, writes to disk if clean, otherwise discards
# - pull(): pull full page from disk based off of needed data
# - read(): should return wanted data and update the dirty/clean and page visits variables in the page

import msgpack
from lstore.page import Page
from typing import Optional


# POLICY: Least used first out
class BufferPool:
    def __init__(self, pool_size: int):
        self.pool_size = pool_size
        self.pages = {}  # Dictionary acting as an LRU cache
        self.pin_counts = {}  # Track pinned pages
        self.dirty_pages = {}  # Track dirty pages
    
    def get_page(self, path: str, page_num: int) -> Optional["Page"]:
        """
        Gets a page from the buffer pool. If not in pool, loads from msgpack storage.
        Automatically pins the page.
        """
        page_id = (path, page_num)

        # If page is in pool, mark as recently used and pin it
        if page_id in self.pages:
            self.pages.move_to_end(page_id)  # Move to end to mark as recently used
            self.pin_counts[page_id] += 1
            return self.pages[page_id]

        # If pool is full, evict least recently used pages
        while len(self.pages) >= self.pool_size:
            self.evict()

        # Load page from msgpack storage
        page = self._load_page_from_msgpack(path, page_num)

        # Add to pool and pin it
        self.pages[page_id] = page
        self.pin_counts[page_id] = 1

        return page
    
    def _load_page_from_msgpack(self, path: str, page_num: int) -> "Page":
        """
        Loads a page from msgpack storage.
        """
        file_name = path + f"_{page_num}.msgpack"

        try:
            with open(file_name, "rb") as file:
                data = msgpack.unpackb(file.read(), raw=False)
                return Page(data["path"], data["page_num"], data["content"])
        except FileNotFoundError:
            return Page(path, page_num)  # Return an empty page if not found

    def evict(self) -> bool:
        # check if page is dirty or clean
        # if clean, discard
        # if dirty, write to memeory then discard
        for page_id in list(self.pages.keys()):
            if self.pin_counts[page_id] == 0:  # Only evict unpinned pages

                # if dirty, write to memory then discard
                if page_id in self.dirty_pages:

                    # Write page content to disk
                    self.flush_page(page_id[0], page_id[1])
                    self.dirty_pages.remove(page_id)

                # Discard page
                evicted_page = self.pages.pop(page_id)
                self._save_page_to_msgpack(evicted_page)
                del self.pin_counts[page_id]
                return True
            
        # If no unpinned pages, return False    
        return False

    def _save_page_to_msgpack(self, page: "Page"):
        """
        Saves a page to msgpack storage.
        """
        file_name = page.path + f"_{page.page_num}.msgpack"
        with open(file_name, "wb") as file:
            file.write(msgpack.packb({"path": page.path, "page_num": page.page_num, "content": page.content}, use_bin_type=True))

    def pin_page(self, path: str, page_num: int) -> None:
        """
        Pins a page in memory
        """
        page_id = (path, page_num)
        if page_id in self.pin_counts:
            self.pin_counts[page_id] += 1
            
    def unpin_page(self, path: str, page_num: int) -> None:
        """
        Unpins a page in memory
        """
        page_id = (path, page_num)
        if page_id in self.pin_counts and self.pin_counts[page_id] > 0:
            self.pin_counts[page_id] -= 1
            
    def mark_dirty(self, path: str, page_num: int) -> None:
        """
        Marks a page as dirty
        """
        self.dirty_pages.add((path, page_num))

    def flush_page(self, path: str, page_num: int) -> None:
        """
        Writes a page back to disk
        """
        page_id = (path, page_num)
        if page_id in self.pages:
            page = self.pages[page_id]
            page.flush_to_disk()  # Call the new flush function
        

    def flush_all(self) -> None:
        """
        Writes all dirty pages back to disk
        """
        for page_id in self.dirty_pages.copy():
            self.flush_page(page_id[0], page_id[1])
            self.dirty_pages.remove(page_id)
            
    def clear(self) -> None:
        """
        Clears the bufferpool after flushing dirty pages
        """
        self.flush_all()
        self.pages.clear()
        self.pin_counts.clear()
        self.dirty_pages.clear() 

  #  def pull(self, desired_rid): # parameter(s) may be incorrect here
        # search disk to find desired page within table
        # determine least used page in the pool
        # call evict() on least used page
        # then pull desired page into pool
        pass

   # def read(self, desired_rid):
        # check if desired data is already in Buffer Pool
        # if not, call pull()
        # read data
        # return data
        pass

