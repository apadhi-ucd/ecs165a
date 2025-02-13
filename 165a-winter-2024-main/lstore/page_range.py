from lstore.page import Page    

class PageRange:
    BASE_PAGES_PER_RANGE = 16

    def __init__(self, num_columns):
        self.num_columns = num_columns
        self.base_pages = [[] for _ in range(num_columns)]
        self.tail_pages = [[] for _ in range(num_columns)]
        self.num_base_records = 0
        self.num_tail_records = 0

        # Initialize with one page per column
        for col in range(num_columns):
            self.base_pages[col].append(Page())
            self.tail_pages[col].append(Page())

    """
    Returns true if the page range has capacity to store another record

    :param is_base: bool    # True if checking base pages, False if checking tail pages
    """
    def has_page_capacity(self, is_base=True):
        pages = self.base_pages if is_base else self.tail_pages
        return pages[0][-1].has_capacity()

    """
    Adds a new page to the page range

    :param is_base: bool    # True if adding to base pages, False if adding to tail pages
    """
    def add_page(self, is_base=True):
        pages = self.base_pages if is_base else self.tail_pages
        for column in pages:
            column.append(Page())


    """
    Writes a record to the page range

    :param is_base: bool    # True if adding to base pages, False if adding to tail pages
    """
    def write(self, record, is_base=True):
        if len(record) != self.num_columns:
            raise ValueError(f"Expected {self.num_columns} columns, got {len(record)}")

        if not self.has_page_capacity(is_base):
            self.add_page(is_base)

        pages = self.base_pages if is_base else self.tail_pages
        page_index = len(pages[0]) - 1
        slot = pages[0][page_index].num_records  # Slot is determined by the first column

        for i, value in enumerate(record):
            pages[i][page_index].write(value)

        if is_base:
            self.num_base_records += 1
        else:
            self.num_tail_records += 1

        return page_index, slot

    """
    Reads a record from the page range

    :param page_index: int  # Index of the page to read from
    :param slot: int        # Slot of the record to read
    :param projected_columns: list of bool    # True if the column should be projected, False if not
    :param is_base: bool    # True if adding to base pages, False if adding to tail pages
    """
    def read(self, page_index, slot, projected_columns, is_base=True):
        pages = self.base_pages if is_base else self.tail_pages
        if page_index >= len(pages[0]) or slot >= pages[0][page_index].num_records:
            return None

        return [pages[i][page_index].read(slot) if projected_columns[i] else None for i in range(self.num_columns)]
    



    """
    Updates a single column's value in a record.
    :param page_index: int     # Index of the page containing the record
    :param slot: int           # Slot number within the page
    :param column: int         # Column index to update
    :param value: int          # New value to write
    :param is_base: bool       # True if adding to base pages, False if adding to tail pages
    """
    def update(self, page_index, slot, column, value, is_base = True):
        """
        Updates a single column's value in a record.
        :param is_base: bool       # True for base pages, False for tail pages
        :param page_index: int     # Index of the page containing the record
        :param slot: int           # Slot number within the page
        :param column: int         # Column index to update
        :param value: int          # New value to write
        """
        pages = self.base_pages if is_base else self.tail_pages

        if page_index >= len(pages[0]) or slot >= pages[0][page_index].num_records:
            return False
        
        if column < 0 or column >= self.num_columns:
            return False

        # Overwrite the old value with the new one
        page = pages[column][page_index]
        old_value = page.read(slot)
        if old_value is not None:
            page.data[slot * Page.RECORD_SIZE:(slot + 1) * Page.RECORD_SIZE] = value.to_bytes(Page.RECORD_SIZE, byteorder='big', signed=True)
            return True
        return False
