"""

Rather than implementing a B Tree we used a PyPi Library

"""

from BTrees.OOBTree import OOBTree 

class Index:
    """
    A class that manages database indices using B-trees for efficient record lookup
    """
    
    def __init__(self, table):
        """
        Initialize the index structure
        - Creates array of None values for each column
        - Stores the primary key column number
        - Creates B-tree index for the primary key column
        """
        self.indices = [None] * table.num_columns  # Array to store index for each column
        self.key = table.key                       # Store primary key column number
        self.indices[self.key] = OOBTree()         # Create B-tree index for primary key
        
    def add(self, record):
        """
        Add a record to all existing indices
        - Gets record ID from the record
        - For each indexed column, adds the record ID to the corresponding B-tree
        """
        rid = record[config.RID_COLUMN]
        for i, column_index in enumerate(self.indices):
            if column_index is not None:  # If this column has an index
                key = record[i + config.METADATA_COLUMNS]
                if key not in column_index:
                    column_index[key] = set()  # Create new set if key doesn't exist
                column_index[key].add(rid)     # Add record ID to the set
                
    def delete(self, record):
        """
        Remove a record from all indices
        - Gets record ID from the record
        - Removes the record ID from all indexed columns
        - Removes empty sets from the B-trees
        """
        rid = record[config.RID_COLUMN]
        for i, column_index in enumerate(self.indices):
            if column_index is not None:
                key = record[i + config.METADATA_COLUMNS]
                if key in column_index:
                    column_index[key].remove(rid)  # Remove record ID from set
                    if not column_index[key]:      # If set is empty
                        del column_index[key]      # Remove the key entirely
                else:
                    raise Exception("Key not found in index")
                    
    def locate(self, column, value):
        """
        Find all record IDs that match a specific value in a column
        Returns None if no matches found
        """
        tree = self.indices[column]
        if value not in tree:
            return None
        return tree[value]
        
    def locate_range(self, begin, end, column):
        """
        Find all record IDs for values between begin and end in a column
        Returns None if no matches found
        """
        tree = self.indices[column]
        matching_rids = set()
        for key in tree.keys(begin, end):  # Use B-tree's range search
            matching_rids.update(tree[key])
        return matching_rids if matching_rids else None
        
    def create_index(self, column_number):
        """
        Create a new B-tree index for a column if it doesn't already exist
        """
        if self.indices[column_number] is not None:
            return
        self.indices[column_number] = OOBTree()
        
    def drop_index(self, column_number):
        """
        Remove an index from a column (except primary key column)
        """
        if column_number != self.key:  # Don't allow dropping primary key index
            self.indices[column_number] = None
