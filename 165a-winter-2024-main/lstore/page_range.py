from lstore.page import Page

class PageRange:
    BASE_PAGES_PER_RANGE = 16  # 16 base pages per range
    
    def __init__(self, num_columns):
        self.num_columns = num_columns  # Number of columns in the table for each page
        self.base_pages = [Page(num_columns, 'base') for _ in range(self.BASE_PAGES_PER_RANGE)] # Base page storage
        self.tail_pages = [Page(num_columns, 'tail')] # Tail page storage
    

    """
    Inserts a record into the first available base page.
    """
    def insert(self, record):
        schema_encoding = 0
        for page in self.base_pages:
            if page.write(record, schema_encoding):
                return True
        return False
    

    """
    Updates a record by writing to a tail page.
    """
    def update(self, original_record, updated_columns, version):
        schema_encoding = int(''.join(str(int(col is not None)) for col in updated_columns), 2)  # Bit vector
        new_record = [original_record[i] if updated_columns[i] is None else updated_columns[i] for i in range(len(original_record))]
        tail_page = self.tail_pages[-1]
        if not tail_page.has_capacity():
            new_tail_page = Page(self.num_columns, 'tail')
            self.tail_pages.append(new_tail_page)
            tail_page = new_tail_page
        tail_page.write(new_record, schema_encoding, version)

    """
    Flags a record to delete by setting its value to DELETE_FLAG.
        
    :param record_id: The index of the record to delete.
    """
    def delete(self, record_id):
        for page in self.base_pages:
            if record_id < page.num_records:
                page.invalidate(record_id)
                return True
        return False
    

