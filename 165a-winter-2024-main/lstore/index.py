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

        bTree = self.indices[column]
        return bTree[value] if value in bTree else None


    """
    # Returns the RIDs of all records with values in column "column" between "begin" and "end"
    """
    def locate_range(self, begin, end, column):


        bTree = self.indices[column] # 
        rids = set()
        for key in bTree.keys(begin, end):
            rids.update(bTree[key])
        return rids if rids else None

    """
    # optional: Create index on specific column
    """
    def create_index(self, column_number):
        pass

    """
    # optional: Drop index of specific column
    """
    def drop_index(self, column_number):
        pass

    """
    # Insert a record into the index
    :param record: Record to insert
    """
    def insert(self, record):
        rid = record[config.RID_COLUMN]
        for i, column in enumerate(self.indices): 
            if column is not None: 
                key = record[i + config.METADATA_COLUMNS]
                if key not in column:
                    column[key] = set()
                column[key].add(rid)
                
    """
    # Delete a record into the index
    :param record: Record to delete
    """
    def delete(self, record):
        rid = record[config.RID_COLUMN]
        for i, column in enumerate(self.indices):
            if column is not None:
                key = record[i + config.METADATA_COLUMNS]
                if key in column:
                    column[key].remove(rid)
                    if not column[key]:
                        del column[key]
                else:
                    return None