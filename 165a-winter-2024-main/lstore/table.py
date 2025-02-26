from lstore.index import Index
from lstore.page_range import PageRange
import msgpack

class Record:

    def __init__(self, indirection, rid, timestamp, schema_encoding, key, columns):
        self.rid = rid                  
        self.key = key                  
        self.columns = columns         
        self.indirection = indirection  
        self.timestamp = timestamp     
        self.schema_encoding = schema_encoding 


    def __getitem__(self, column):
        return self.columns[column]

    def __str__(self):

        return f"Record(rid={self.rid}, key={self.key}, columns={self.columns})"

class Table:

    """
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """
    def __init__(self, name, num_columns, key):
        self.name = name
        self.key = key
        self.num_columns = num_columns
        self.page_directory = {}
        self.index = Index(self)
        self.rid = 1                                                     
        self.page_range = [PageRange(num_columns)]  
        self.page_range_index = 0                   # Current page range index
        self.page_directory = {}                     # Maps RID to record location
        # Add new flag to track first select call.
        self.first_select_called = False

    def reset(self):
        # Reset in-memory record state so that stale persistent records are cleared.
        from lstore.page_range import PageRange
        self.page_range = [PageRange(self.num_columns)]
        self.page_range_index = 0
        self.page_directory = {}
        from lstore.index import Index
        self.index = Index(self)

    """
    # Inserts a record into the table
    """
    def new_rid(self):
        self.rid += 1
        return self.rid - 1

    """
    # Creates a page range 
    """
    def create_page_range(self):
        if self.page_range[-1].has_page_capacity(True): return;
    
        self.page_range.append(PageRange(self.num_columns))
        self.page_range_index += 1

    def __merge(self):
        print("merge is happening")
        pass

    @staticmethod
    def delete_data(db_path, name):
        """Deletes the table's data file by writing an empty file."""
        data_file = db_path + f"_{name}.msgpack"
        try:
            with open(data_file, "wb") as file:
                file.write(msgpack.packb([], use_bin_type=True))  # Overwrite with empty list
        except FileNotFoundError:
            pass  # If file doesn't exist, do nothing

    def persist_records(self, db_path):
        """
        Persist record data (page ranges and page_directory) to disk.
        """
        data_file = db_path + f"_{self.name}.msgpack"
        def persist_page(page):
            return {
                "num_records": page.num_records,
                # Convert bytearray to list of ints
                "data": list(page.data)
            }
        persisted_page_ranges = []
        for pr in self.page_range:
            pr_data = {
                "base_pages": [],
                "tail_pages": [],
                "num_base_records": pr.num_base_records,
                "num_tail_records": pr.num_tail_records
            }
            for column_pages in pr.base_pages:
                serialized = [persist_page(page) for page in column_pages]
                pr_data["base_pages"].append(serialized)
            for column_pages in pr.tail_pages:
                serialized = [persist_page(page) for page in column_pages]
                pr_data["tail_pages"].append(serialized)
            persisted_page_ranges.append(pr_data)
        # Convert page_directory keys to strings
        persisted_page_directory = { str(k): v for k, v in self.page_directory.items() }
        persist_dict = {
            "page_ranges": persisted_page_ranges,
            "page_directory": persisted_page_directory
        }
        with open(data_file, "wb") as file:
            file.write(msgpack.packb(persist_dict, use_bin_type=True))
    
    def load_records(self, db_path):
        """
        Load record data from disk and rebuild page_range, page_directory, and index by scanning all base pages.
        """
        data_file = db_path + f"_{self.name}.msgpack"
        try:
            with open(data_file, "rb") as file:
                persist_dict = msgpack.unpackb(file.read(), raw=False, strict_map_key=False)
            def load_page(page_data):
                from lstore.page import Page
                page = Page()  # create a new empty page
                page.num_records = page_data["num_records"]
                page.data = bytearray(page_data["data"])
                return page
            loaded_page_ranges = []
            for pr_data in persist_dict.get("page_ranges", []):
                from lstore.page_range import PageRange
                pr = PageRange(self.num_columns)
                # Rebuild base pages
                pr.base_pages = []
                for col_data in pr_data.get("base_pages", []):
                    pages = [load_page(page_data) for page_data in col_data]
                    pr.base_pages.append(pages)
                # Rebuild tail pages
                pr.tail_pages = []
                for col_data in pr_data.get("tail_pages", []):
                    pages = [load_page(page_data) for page_data in col_data]
                    pr.tail_pages.append(pages)
                pr.num_base_records = pr_data.get("num_base_records", 0)
                pr.num_tail_records = pr_data.get("num_tail_records", 0)
                loaded_page_ranges.append(pr)
            if loaded_page_ranges:
                self.page_range = loaded_page_ranges
            # Rebuild page_directory and index by scanning all base records.
            from lstore.config import METADATA_COLUMNS, RID_COLUMN
            self.page_directory = {}
            from lstore.index import Index
            self.index = Index(self)
            for pr_index, pr in enumerate(self.page_range):
                # Base pages of every column should have the same page structure.
                num_pages = len(pr.base_pages[0])
                for page_index in range(num_pages):
                    num_slots = pr.base_pages[0][page_index].num_records
                    for slot in range(num_slots):
                        record = pr.read_record(page_index, slot, [1] * self.num_columns, True)
                        if record is None:
                            continue
                        rid = record[RID_COLUMN]
                        self.page_directory[rid] = (pr_index, page_index, slot)
                        self.index.insert(record)
        except FileNotFoundError:
            pass

    def consolidate_index(self):
        from lstore.config import METADATA_COLUMNS, RID_COLUMN
        from lstore.index import Index
        self.index = Index(self)
        for rid, loc in self.page_directory.items():
            if loc is None:
                continue
            pr_index, bp_index, slot = loc
            record = self.page_range[pr_index].read_record(bp_index, slot, [1] * self.num_columns, True)
            if record is not None:
                self.index.insert(record)