from lstore.table import Table, Record
from lstore.index import Index


class Query:
    """
    # Creates a Query object that can perform different queries on the specified table 
    Queries that fail must return False
    Queries that succeed should return the result or True
    Any query that crashes (due to exceptions) should return False
    """
    def __init__(self, table):
        self.table = table
        pass

    
    """
    # internal Method
    # Read a record with specified RID
    # Returns True upon succesful deletion
    # Return False if record doesn't exist or is locked due to 2PL
    """
    def delete(self, primary_key):
        return self.table.delete_record(primary_key)
    
    
    """
    # Insert a record with specified columns
    # Return True upon succesful insertion
    # Returns False if insert fails for whatever reason
    """
    def insert(self, *columns):
        # Validate the number of columns provided matches the table's schema
        if len(columns) != self.table.num_columns:
            return False

        # Create the initial schema encoding (all zeros).
        # This encoding is often used to mark the status of columns (0 => unmodified).
        schema_encoding = '0' * self.table.num_columns

        # Assume that the primary key is the first column.
        primary_key = columns[0]

        # Check if the primary key already exists in the page directory.
        if primary_key in self.table.page_directory:
            return False

        try:
            # Generate a new record ID (RID) for the record.
            rid = self.table.new_base_rid()

            # Create a new record object (even if not used beyond writing, this is common practice).
            new_record = Record(rid, primary_key, columns, schema_encoding)

            # Write the record data into the base pages of the table.
            self.table.base_write(rid, columns)

            # Update the page directory: map the primary key to its record ID.
            self.table.page_directory[primary_key] = rid

            # Insert the new record into the index using the primary key.
            self.table.index.insert(primary_key, rid)

            return True

        except Exception:
            # If there is any error during insertion, return False.
            return False

    
    """
    # Read matching record with specified search key
    # :param search_key: the value you want to search based on
    # :param search_key_index: the column index you want to search based on
    # :param projected_columns_index: what columns to return. array of 1 or 0 values.
    # Returns a list of Record objects upon success
    # Returns False if record locked by TPL
    # Assume that select will never be called on a key that doesn't exist
    """
    def select(self, search_key, search_key_index, projected_columns_index):
        records = self.table.select_record(search_key, search_key_index)
        if records:
            return [Record(r.rid, r.key, [r.columns[i] if projected_columns_index[i] else None for i in range(len(r.columns))]) for r in records]
        else:
            return []

    
    """
    # Read matching record with specified search key
    # :param search_key: the value you want to search based on
    # :param search_key_index: the column index you want to search based on
    # :param projected_columns_index: what columns to return. array of 1 or 0 values.
    # :param relative_version: the relative version of the record you need to retreive.
    # Returns a list of Record objects upon success
    # Returns Empty List if record locked by TPL
    # Assume that select will never be called on a key that doesn't exist
    """
    def select_version(self, search_key, search_key_index, projected_columns_index, relative_version):
        try:
        # If searching by primary key (optimization)
        if search_key_index == 0:
            if search_key not in self.table.page_directory:
                return []
            
            rid = self.table.page_directory[search_key]
            # Read specific version of the record
            columns = self.table.read_version(rid, relative_version)
            
            if columns:
                # Project only the requested columns
                projected_columns = [
                    columns[i] if projected_columns_index[i] else None 
                    for i in range(len(columns))
                ]
                return [Record(rid, search_key, projected_columns)]
            return []
            
        # If searching by another column
        matching_records = []
        for primary_key, rid in self.table.page_directory.items():
            columns = self.table.read_version(rid, relative_version)
            
            if columns and columns[search_key_index] == search_key:
                # Project only the requested columns
                projected_columns = [
                    columns[i] if projected_columns_index[i] else None 
                    for i in range(len(columns))
                ]
                matching_records.append(Record(rid, primary_key, projected_columns))
                
        return matching_records
        
    except Exception:
        return []    
    
    """
    # Update a record with specified key and columns
    # Returns True if update is succesful
    # Returns False if no records exist with given key or if the target record cannot be accessed due to 2PL locking
    """
    def update(self, primary_key, *columns):
        return self.table.update_record(primary_key, columns)

    
    """
    :param start_range: int         # Start of the key range to aggregate 
    :param end_range: int           # End of the key range to aggregate 
    :param aggregate_columns: int  # Index of desired column to aggregate
    # this function is only called on the primary key.
    # Returns the summation of the given range upon success
    # Returns False if no record exists in the given range
    """
    def sum(self, start_range, end_range, aggregate_column_index):
        return self.table.sum_records(start_range, end_range, aggregate_column_index)

    
    """
    :param start_range: int         # Start of the key range to aggregate 
    :param end_range: int           # End of the key range to aggregate 
    :param aggregate_columns: int  # Index of desired column to aggregate
    :param relative_version: the relative version of the record you need to retreive.
    # this function is only called on the primary key.
    # Returns the summation of the given range upon success
    # Returns False if no record exists in the given range
    """
    def sum_version(self, start_range, end_range, aggregate_column_index, relative_version):
        try:
        sum_result = 0
        records_found = False
        
        # Iterate through page directory in sorted order for primary keys
        for primary_key in sorted(self.table.page_directory.keys()):
            # Check if key is within range (inclusive)
            if start_range <= primary_key <= end_range:
                records_found = True
                rid = self.table.page_directory[primary_key]
                
                # Use read_version instead of read_record to get specific version
                columns = self.table.read_version(rid, relative_version)
                
                if columns:
                    # Add the value from the specified column
                    value = columns[aggregate_column_index]
                    if value is not None:  # Handle NULL values
                        sum_result += value
        
        return sum_result if records_found else False
        
    except Exception:
        return False
    
    """
    incremenets one column of the record
    this implementation should work if your select and update queries already work
    :param key: the primary of key of the record to increment
    :param column: the column to increment
    # Returns True is increment is successful
    # Returns False if no record matches key or if target record is locked by 2PL.
    """
    def increment(self, key, column):
        r = self.select(key, self.table.key, [1] * self.table.num_columns)[0]
        if r is not False:
            updated_columns = [None] * self.table.num_columns
            updated_columns[column] = r[column] + 1
            u = self.update(key, *updated_columns)
            return u
        return False
