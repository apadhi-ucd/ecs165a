from BTrees.OOBTree import OOBTree
import lstore.config as config

class Index:

    def __init__(self, table):
        # One index for each table. All our empty initially.
        self.indices = [None] * table.num_columns  # One index per table 
        self.key = table.key  
        self.indices[self.key] = OOBTree()  # Initialize key index
                
    """
    # returns the location of all records with the given value on column "column"
    """
    def locate(self, column, value):

        # check if column is valid
        bTree = self.indices[column]
        return bTree[value] if value in bTree else None


    """
    # Returns the RIDs of all records with values in column "column" between "begin" and "end"
    """
    def locate_range(self, begin, end, column):

        # check if column is valid
        bTree = self.indices[column] # 
        rids = set()

        # iterate over keys in range
        for key in bTree.keys(begin, end):
            rids.update(bTree[key])
        return rids if rids else None

    """
    # optional: Create index on specific column
    """
    def create_index(self, column_number: int) -> True:

        if column_number >= len(self.indices):
            # Extend indices list if needed
            self.indices.extend([None] * (column_number - len(self.indices) + 1))
            
        if self.indices[column_number] is None:
            self.indices[column_number] = OOBTree()
            self.restart_index_by_col(column_number)
        return True
    
    """
    # optional: Drop index of specific column
    """
    def drop_index(self, column_number: int) -> True:

        if column_number < len(self.indices):
            self.indices[column_number] = None
        return True

    """
    # Insert a record into the index
    :param record: Record to insert
    """
    def insert(self, record):
        # get rid
        rid = record[config.RID_COLUMN]

        # iterate over columns
        for i, column in enumerate(self.indices): 
            if column is not None: 
                # get key
                key = record[i + config.METADATA_COLUMNS]
                # insert
                if key not in column:
                    column[key] = set()
                column[key].add(rid)
                
    """
    # Delete a record into the index
    :param record: Record to delete
    """
    def delete(self, record):
        rid = record[config.RID_COLUMN]

        # iterate over columns
        for i, column in enumerate(self.indices):
            if column is not None:
                key = record[i + config.METADATA_COLUMNS]
                # delete
                if key in column:
                    column[key].remove(rid)
                    if not column[key]:
                        del column[key]
                else:
                    return None

# EDIT THESE !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
    def get_value_in_col_by_rid(self, column_number: int, rid: int) -> int:
        """
        Get value using RID from BTree
        """
        if column_number < len(self.indices) and self.indices[column_number] is not None and rid in self.indices[column_number]:
            return self.indices[column_number][rid]
        return None

    def get_rid_in_col_by_value(self, column_number: int, value: int) -> list:
        """
        Get RIDs by value from BTree
        BTrees maintain sorted order, but we still need to scan for matching values
        """
        if column_number >= len(self.indices) or self.indices[column_number] is None:
            return []
        return [k for k, v in self.indices[column_number].items() if v == value]
    
    def restart_index(self):
        """
        Rebuild all BTree indices
        """
        # Initialize indices for total columns (metadata + data)
        self.indices = [None] * self.table.total_columns
        for i in range(self.table.total_columns):
            if len(self.table.page_directory[i]) > 0:
                self.indices[i] = OOBTree()
                for k, v in self.table.page_directory[i].items():
                    self.indices[i][k] = self.table.read_page(v[0], v[1])
            
    def restart_index_by_col(self, col):
        """
        Rebuild BTree index for a specific column
        """
        if col >= len(self.indices):
            # Extend indices list if needed
            self.indices.extend([None] * (col - len(self.indices) + 1))
            
        self.indices[col] = OOBTree()
        for k, v in self.table.page_directory[col].items():
            self.indices[col][k] = self.table.read_page(v[0], v[1])

    def add_or_move_record_by_col(self, column_number: int, rid: int, value: int):
        """
        Add or update a record in the BTree index
        """
        if column_number >= len(self.indices):
            # Extend indices list if needed
            self.indices.extend([None] * (column_number - len(self.indices) + 1))
            
        if self.indices[column_number] is None:
            self.create_index(column_number)
        self.indices[column_number][rid] = value
