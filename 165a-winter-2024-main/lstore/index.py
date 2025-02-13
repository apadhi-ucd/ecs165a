"""

Rather than implementing a B Tree we used a PyPi Library

"""

from BTrees.OOBTree import OOBTree 

RID_COLUMN = 1
METADATA_COLUMNS = 4

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
        record_id = record[self.RID_COLUMN]

        for row, col in enumerate(self.indices):
            if col is not None:
                key = record[row + self.METADATA_COLUMNS]
                if key not in col:
                    col[key] = set()
                col[key].add(record_id)
                
    def delete(self, record):
        """
        Remove a record from all indices
        - Gets record ID from the record
        - Removes the record ID from all indexed columns
        - Removes empty sets from the B-trees
        """
        record_id = record[self.RID_COLUMN]
        for col, tree in enumerate(self.indices):
            if tree is not None:
                value = record[col + self.METADATA_COLUMNS]
                # If the value exists in the b-tree, attempt to remove the record id.
                if value in tree:
                    tree[value].discard(record_id)
                    # If the set is empty after deletion, remove the key from the tree.
                    if not tree[value]:
                        del tree[value]
                    
                    
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
