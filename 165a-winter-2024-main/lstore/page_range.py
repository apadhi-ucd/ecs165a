from lstore.page import Page

NUM_DATA_COLUMNS = 4

class PageRange:
    BASE_PAGES_PER_RANGE = 16  # 16 base pages per range
    
    def __init__(self, num_columns):
        self.num_columns = num_columns  # Number of columns in the table for each page
        self.base_pages = [[] for _ in range(num_columns + NUM_DATA_COLUMNS)] # Base page storage
        self.tail_pages = [[] for _ in range(num_columns + NUM_DATA_COLUMNS)] # Tail page storage
        self.num_tail_records = 0 
        self.num_base_records = 0

        for column in self.base_pages:
            column.append(Page())
        for column in self.tail_pages:
            column.append(Page())
    """
    Inserts a record into the first available base page.
    """
    def insert_record(self, record, page_type):
        if page_type == 'base':
            if len(record) != self.num_columns + NUM_DATA_COLUMNS:
                return None
        
            if not self.has_base_page_capacity():
                for column in self.base_pages:
                    column.append(Page())
        
            page_index = len(self.base_pages[0]) - 1
            slot = self.base_pages[0][page_index].num_records
        
            for i, value in enumerate(record):
                self.base_pages[i][page_index].write(value)
        
            self.num_base_records += 1
            return page_index, slot
        else:
            if len(record) != self.num_columns + NUM_DATA_COLUMNS:
                return None
        
            if not self.has_tail_page_capacity():
                for column in self.tail_pages:
                    column.append(Page())
        
            page_index = len(self.tail_pages[0]) - 1
            slot = self.tail_pages[0][page_index].num_records
        
            for i, value in enumerate(record):
                self.tail_pages[i][page_index].write(value)
        
            self.num_tail_records += 1
            return page_index, slot
  
    

    """
    Updates a record by writing to a column.
    """
    def update(self, page_type, page_index, index, column, value):
        pages = self.base_pages if page_type == 'base' else self.tail_pages
        if page_index >= len(pages[0]) or index >= pages[0][page_index].num_records:
            raise IndexError("Invalid page_index or index")
        
        if column < 0 or column >= self.num_columns + NUM_DATA_COLUMNS:
            raise ValueError("Invalid column index")
        
        pages[column][page_index].update(index, value)

    """
    Flags a base record as deleted.
    :param page_index: int  # Index of the page containing the record
    :param index: int  # index number within the page
    """
    def delete(self, page_index, index):
        if page_index >= len(self.base_pages[0]) or index >= self.base_pages[0][page_index].num_records:
            raise IndexError("Invalid delete")
        
        self.base_pages[0][page_index].invalidate(index)

    

