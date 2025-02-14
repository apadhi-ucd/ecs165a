from lstore.page import Page    
import lstore.config as config

class PageRange:

    def __init__(self, num_columns):
        self.num_columns = num_columns
        self.base_pages = []                # List of base pages for each column
        self.tail_pages = []                # List of tail pages for each column
        self.num_tail_records = 0           # Number of tail records
        self.num_base_records = 0           # Number of base records
        
        # Initialize pages for each column
        for _ in range(num_columns + config.METADATA_COLUMNS):
            self.base_pages.append([Page()])
            self.tail_pages.append([Page()])

    """
    Return either base or tail pages based on flag
    :param is_base: bool - True for base pages, False for tail pages
    """
    def _get_pages(self, is_base):
        return self.base_pages if is_base else self.tail_pages

    """
    Returns true if the page range can store another record
    :param is_base: bool - True for base pages, False for tail pages
    """
    def has_page_capacity(self, is_base):
        pages = self._get_pages(is_base)
        return pages[0][-1].has_capacity()

    """
    Adds a new page for each column when current ones are full
    :param is_base: bool - True for base pages, False for tail pages
    """
    def add_page(self, is_base):
        pages = self._get_pages(is_base)
        for column in pages:
            column.append(Page())

    """
    Write a record to the page 
    :param is_base: bool - True for base pages, False for tail pages
    """
    def write_record(self, record, is_base):
        if len(record) != self.num_columns + config.METADATA_COLUMNS:
            print("Unexpected value in write_record")
            return
        
        # Write record to page
        pages = self._get_pages(is_base)
        if not self.has_page_capacity(is_base):
            self.add_page(is_base)
            
        page_index = len(pages[0]) - 1
        slot = pages[0][page_index].num_records
        
        # Write metadata
        for i, value in enumerate(record):
            pages[i][page_index].write(value)
            
        # Update metadata
        if is_base:
            self.num_base_records += 1
        else:
            self.num_tail_records += 1
        return (page_index, slot)

    """
    Read a record from the page 
    :param is_base: bool - True for base pages, False for tail pages
    """
    def read_record(self, page_index, slot, projected_columns_index, is_base):
        pages = self._get_pages(is_base)
        # Check if the index is valid
        if page_index >= len(pages[0]) or slot >= pages[0][page_index].num_records:
            print("Invalid index")
            return
            
        record = []

        # Read metadata
        for i in range(config.METADATA_COLUMNS):
            record.append(pages[i][page_index].read(slot))
            
        # Read data
        for i in range(self.num_columns):
            if projected_columns_index[i]:
                value = pages[i + config.METADATA_COLUMNS][page_index].read(slot)
                record.append(value)
            else:
                record.append(None)
                
        return record
    """
    Update a record in the page
    :param is_base: bool - True for base pages, False for tail pages
    """
    def update_record_column(self, page_index, slot, column, value, is_base):
        pages = self._get_pages(is_base)
        # Check if the index is valid
        if page_index >= len(pages[0]) or slot >= pages[0][page_index].num_records:
            print("Invalid index")
            return
        
        # Update metadata
        if column < 0 or column >= self.num_columns + config.METADATA_COLUMNS:
            print("Invalid column")
            return
            
        # Update data
        page = pages[column][page_index]
        old_value = page.read(slot)
        if old_value is not None:
            page.data[slot * 8:(slot + 1) * 8] = value.to_bytes(8, byteorder='big', signed=True)

    
