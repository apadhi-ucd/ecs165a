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
        for page in self.base_pages:
            if page.write(record):
                return True
        return False
    

    """
    Updates a record by writing to a tail page.
    """
    def update(self, record):
        tail_page = self.tail_pages[-1]
        if not tail_page.has_capacity():
            new_tail_page = Page(self.num_columns, 'tail')
            self.tail_pages.append(new_tail_page)
            tail_page = new_tail_page
        tail_page.write(record)
