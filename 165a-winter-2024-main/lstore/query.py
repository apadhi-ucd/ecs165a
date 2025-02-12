from lstore.table import Table, Record
from lstore.index import Index
import time


class Query:
    """
    # Creates a Query object that can perform different queries on the specified table 
    Queries that fail must return False
    Queries that succeed should return the result or True
    Any query that crashes (due to exceptions) should return False
    """
    def __init__(self, table):
        self.table = table

    
    """
    # internal Method
    # Read a record with specified RID
    # Returns True upon succesful deletion
    # Return False if record doesn't exist or is locked due to 2PL
    """
    def delete(self, primary_key):
        return self.table.delete(primary_key)
    
    
    """
    # Insert a record with specified columns
    # Return True upon succesful insertion
    # Returns False if insert fails for whatever reason
    """
    def insert(self, *columns):
        if len(columns) != self.table.num_columns:
            return False
        return self.table.insert(*columns)


    
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
        record = self.table.select(search_key, search_key_index, projected_columns_index)
        if record:
            return record
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
            return self.table.select_version(search_key, search_key_index, projected_columns_index, relative_version)        
        except Exception:
            return []    
    
    """
    # Update a record with specified key and columns
    # Returns True if update is succesful
    # Returns False if no records exist with given key or if the target record cannot be accessed due to 2PL locking
    """
    def update(self, primary_key, *columns):
        return self.table.update(primary_key, *columns)

    
    """
    :param start_range: int         # Start of the key range to aggregate 
    :param end_range: int           # End of the key range to aggregate 
    :param aggregate_columns: int  # Index of desired column to aggregate
    # this function is only called on the primary key.
    # Returns the summation of the given range upon success
    # Returns False if no record exists in the given range
    """
    def sum(self, start_range, end_range, aggregate_column_index):
        return self.table.sum_version(start_range, end_range, aggregate_column_index)

    
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
            return self.table.sum_version(start_range, end_range, aggregate_column_index, relative_version)
        
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


class UpdateOperations:
    def __init__(self, table):
        self.table = table

    def create_schema_encoding(self, columns, existing_schema=None):
        if existing_schema is None:
            schema = [1 if col is not None else 0 for col in columns]
        else:
            schema = existing_schema.copy()
            for i, col in enumerate(columns):
                if col is not None:
                    schema[i] = 1
        return schema, int("".join(map(str, schema)), 2)

    def create_tail_record(self, tail_rid, indirection_rid, columns, schema_num, prev_record=None):
        tail_record = [None] * 4  # Assuming METADATA_COLUMNS is 4
        tail_record[0] = indirection_rid  # INDIRECTION_COLUMN
        tail_record[1] = tail_rid  # RID_COLUMN
        tail_record[2] = int(time.time())  # TIMESTAMP_COLUMN
        tail_record[3] = schema_num  # SCHEMA_ENCODING_COLUMN

        if prev_record is None:
            tail_record.extend(col if col is not None else 0 for col in columns)
        else:
            for i, col in enumerate(columns):
                if col is not None:
                    tail_record.append(col)
                elif prev_record[i + 4]:  # Assuming METADATA_COLUMNS is 4
                    tail_record.append(prev_record[i + 4])
                else:
                    tail_record.append(0)
        return tail_record

    def write_tail_record(self, page_range, page_range_index, tail_record):
        tail_index, tail_slot = page_range.write_tail_record(tail_record)
        self.table.page_directory[tail_record[1]] = (page_range_index, tail_index, tail_slot)
        return tail_index, tail_slot

    def update_base_record_metadata(self, page_range, base_page_index, base_slot, tail_rid, schema_num):
        page_range.update_base_record_column(base_page_index, base_slot, 0, tail_rid)
        page_range.update_base_record_column(base_page_index, base_slot, 3, schema_num)

    def update(self, primary_key, *columns):
        try:
            rids = self.table.index.locate(self.table.key, primary_key)
            if not rids:
                return False

            for rid in rids:
                page_range_index, base_page_index, base_slot = self.table.page_directory[rid]
                page_range = self.table.page_ranges[page_range_index]
                base_record = page_range.read_base_record(base_page_index, base_slot, [0] * self.table.num_columns)

                if base_record[0] == 0:
                    tail_rid = self.table.new_rid()
                    schema, schema_num = self.create_schema_encoding(columns)
                    tail_record = self.create_tail_record(tail_rid, base_record[1], columns, schema_num)
                    self.write_tail_record(page_range, page_range_index, tail_record)
                    self.update_base_record_metadata(page_range, base_page_index, base_slot, tail_rid, schema_num)
                else:
                    existing_schema = [int(bit) for bit in f"{base_record[3]:0{self.table.num_columns}b}"]
                    latest_tail_rid = base_record[0]
                    page_range_index, latest_tail_index, latest_tail_slot = self.table.page_directory[latest_tail_rid]
                    latest_tail_record = page_range.read_tail_record(latest_tail_index, latest_tail_slot, existing_schema)
                    new_tail_rid = self.table.new_rid()
                    schema, schema_num = self.create_schema_encoding(columns, existing_schema)
                    new_tail_record = self.create_tail_record(new_tail_rid, latest_tail_rid, columns, schema_num, latest_tail_record)
                    self.write_tail_record(page_range, page_range_index, new_tail_record)
                    self.update_base_record_metadata(page_range, base_page_index, base_slot, new_tail_rid, schema_num)

            return True
        except:
            return False

    def delete(self, primary_key):
        return self.table.delete(primary_key)

    def sum(self, start_range, end_range, aggregate_column_index):
        return self.table.sum_version(start_range, end_range, aggregate_column_index, 0)

    def sum_version(self, start_range, end_range, aggregate_column_index, relative_version):
        try:
            return self.table.sum_version(start_range, end_range, aggregate_column_index, relative_version)
        except:
            return False

    def increment(self, key, column):
        records = self.table.select(key, self.table.key, [1] * self.table.num_columns)
        if not records or records is False:
            return False

        record = records[0]
        updated_columns = [None] * self.table.num_columns
        updated_columns[column] = record[column] + 1
        return self.update(key, *updated_columns)
