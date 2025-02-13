"""
Database query operations.
Handles database operations including insert, update, delete, and select.
Returns False on failure, True or requested data on success.
"""
import time
from lstore.table import Table, Record
from lstore.index import Index

SCHEMA_ENCODING_COLUMN = 3
INDIRECTION_COLUMN = 0
METADATA_COLUMNS = 4
RID_COLUMN = 1

class Query:
    """
    Handles all database table operations
    """

    def __init__(self, table):
        """
        Initialize with target table
        """
        self.table = table

    def create_record(self, raw_data, primary_key):
        """
        Create Record object from raw data
        """
        schema_value = raw_data[SCHEMA_ENCODING_COLUMN]
        column_count = self.table.num_columns
        schema_encoding = [int(bit) for bit in f"{schema_value:0{column_count}b}"]
        data_values = raw_data[METADATA_COLUMNS:]
        
        return Record(
            indirection=raw_data[INDIRECTION_COLUMN],
            rid=raw_data[RID_COLUMN],
            schema_encoding=schema_encoding,
            key=primary_key,
            columns=data_values
        )

    def get_base_records(self, key_value, key_column_index, column_filter):
        """
        Get base records matching the key value
        """
        matching_records = []
        matching_rids = self.table.index.locate(key_column_index, key_value)
        
        if matching_rids is None:
            return []

        for record_id in matching_rids:
            page_range_idx, base_page_idx, slot_idx = self.table.page_directory[record_id]
            raw_record_data = self.table.page_ranges[page_range_idx].read_base_record(
                base_page_idx, 
                slot_idx, 
                column_filter
            )
            record_object = self.create_record(raw_record_data, key_value)
            matching_records.append(record_object)

        return matching_records

    def insert(self, *columns):
        """
        Insert new record into table
        """
        try:
            if len(columns) != self.table.num_columns:
                return False

            rid = self.table.new_rid()
            page_range = self.table.page_ranges[self.table.page_ranges_index]

            new_record = [None] * METADATA_COLUMNS
            new_record[INDIRECTION_COLUMN] = 0
            new_record[RID_COLUMN] = rid
            new_record[SCHEMA_ENCODING_COLUMN] = 0
            new_record.extend(columns)

            index, slot = page_range.write_base_record(new_record)
            self.table.page_directory[rid] = (self.table.page_ranges_index, index, slot)
            self.table.index.add(new_record)

            self.table.add_new_page_range()
            return True
        except:
            return False

    def select(self, search_key, search_key_index, projected_columns_index):
        """
        Select records matching search criteria
        """
        return self.select_version(search_key, search_key_index, projected_columns_index, 0)

def select_version(self, search_key, search_key_index, projected_columns_index, relative_version):
        """
        Retrieves a specific version of records matching the search criteria
        :param search_key: int             #Value to search for
        :param search_key_index: int       #Column to search in
        :param projected_columns_index: list #Columns to return (1s and 0s)
        :param relative_version: int        #Version to retrieve (negative for older versions)
        :return: list                      #List of matching Record objects
        """
        try:
            # Get base records
            base_records = self.get_base_records(search_key, search_key_index, projected_columns_index)
            versioned_records = []

            # Process each base record
            for base_record in base_records:
                # Handle records with no updates
                if base_record.indirection == 0:
                    versioned_records.append(base_record)
                    continue

                # Initialize version traversal
                current_version = 0
                current_tail_rid = base_record.indirection
                should_return_base = False

                # Get initial tail record
                page_range_index, tail_page_index, tail_slot = self.table.page_directory[current_tail_rid]
                current_tail_record = self.table.page_ranges[page_range_index].read_tail_record(
                    tail_page_index, 
                    tail_slot, 
                    projected_columns_index
                )

                # Traverse the version chain
                while current_version > relative_version:
                    if current_tail_record[config.INDIRECTION_COLUMN] == base_record.rid:
                        should_return_base = True
                        break

                    # Move to next version
                    current_tail_rid = current_tail_record[config.INDIRECTION_COLUMN]
                    page_range_index, tail_page_index, tail_slot = self.table.page_directory[current_tail_rid]
                    current_tail_record = self.table.page_ranges[page_range_index].read_tail_record(
                        tail_page_index, 
                        tail_slot,
                        projected_columns_index
                    )
                    current_version -= 1

                # Determine which record to return
                if should_return_base or relative_version < current_version:
                    versioned_records.append(base_record)
                else:
                    # Update base record with tail record values
                    schema_value = current_tail_record[config.SCHEMA_ENCODING_COLUMN]
                    updated_columns_bitmap = [int(bit) for bit in f"{schema_value:0{self.table.num_columns}b}"]
                    
                    for col_idx, is_updated in enumerate(updated_columns_bitmap):
                        if is_updated == 1:
                            base_record.columns[col_idx] = current_tail_record[config.METADATA_COLUMNS + col_idx]
                    versioned_records.append(base_record)

            return versioned_records
        except:
            return False

    def createSchemaEncoding(self, columns, existing_schema=None):
        """
        Create or update schema encoding for record updates
        """
        if existing_schema is None:
            schema = [1 if col is not None else 0 for col in columns]
        else:
            schema = existing_schema.copy()
            for i, col in enumerate(columns):
                if col is not None:
                    schema[i] = 1
        return schema, int("".join(map(str, schema)), 2)

    def createTailRecord(self, tail_rid, indirection_rid, columns, schema_num, prev_record=None):
        """
        Create new tail record with metadata
        """
        tail_record = [None] * METADATA_COLUMNS
        tail_record[INDIRECTION_COLUMN] = indirection_rid
        tail_record[RID_COLUMN] = tail_rid
        tail_record[SCHEMA_ENCODING_COLUMN] = schema_num

        if prev_record is None:
            tail_record.extend(col if col is not None else 0 for col in columns)
        else:
            for i, col in enumerate(columns):
                if col is not None:
                    tail_record.append(col)
                elif prev_record[i + METADATA_COLUMNS]:
                    tail_record.append(prev_record[i + METADATA_COLUMNS])
                else:
                    tail_record.append(0)

        return tail_record

    def writeTailRecord(self, page_range, page_range_index, tail_record):
        """
        Write tail record and update directory
        """
        tail_index, tail_slot = page_range.write_tail_record(tail_record)
        self.table.page_directory[tail_record[RID_COLUMN]] = (page_range_index, tail_index, tail_slot)
        return tail_index, tail_slot

    def updateBaseRecordMetadata(self, page_range, base_page_index, base_slot, tail_rid, schema_num):
        """
        Update base record metadata after tail record creation
        """
        page_range.update_base_record_column(base_page_index, base_slot, INDIRECTION_COLUMN, tail_rid)
        page_range.update_base_record_column(base_page_index, base_slot, SCHEMA_ENCODING_COLUMN, schema_num)

    def handleFirstUpdate(self, page_range, base_record, page_range_index, base_page_index, base_slot, columns):
        """
        Handle first update to a record
        """
        tail_rid = self.table.new_rid()
        schema, schema_num = self.createSchemaEncoding(columns)
        
        tail_record = self.createTailRecord(
            tail_rid,
            base_record[RID_COLUMN],
            columns,
            schema_num
        )
        self.writeTailRecord(page_range, page_range_index, tail_record)
        self.updateBaseRecordMetadata(page_range, base_page_index, base_slot, tail_rid, schema_num)

    def handleSubsequentUpdate(self, page_range, base_record, page_range_index, base_page_index, base_slot, columns):
        """
        Handle updates to previously updated record
        """
        existing_schema = [int(bit) for bit in f"{base_record[SCHEMA_ENCODING_COLUMN]:0{self.table.num_columns}b}"]
        latest_tail_rid = base_record[INDIRECTION_COLUMN]
        
        page_range_index, latest_tail_index, latest_tail_slot = self.table.page_directory[latest_tail_rid]
        latest_tail_record = page_range.read_tail_record(latest_tail_index, latest_tail_slot, existing_schema)
        
        new_tail_rid = self.table.new_rid()
        schema, schema_num = self.createSchemaEncoding(columns, existing_schema)
        
        new_tail_record = self.createTailRecord(
            new_tail_rid,
            latest_tail_rid,
            columns,
            schema_num,
            latest_tail_record
        )
        self.writeTailRecord(page_range, page_range_index, new_tail_record)
        self.updateBaseRecordMetadata(page_range, base_page_index, base_slot, new_tail_rid, schema_num)

    def update(self, primary_key, *columns):
        """
        Updates a record in the table
        :param primary_key: int   #Primary key of record to update
        :param columns: *args     #New values for each column (None for unchanged)
        :return: bool           #True if successful, False otherwise
        """
        try:
            # Locate records to update
            rids = self.table.index.locate(self.table.key, primary_key)
            if rids is None:
                return False

            # Update each matching record
            for rid in rids:
                # Get record location
                page_range_index, base_page_index, base_slot = self.table.page_directory[rid]
                page_range = self.table.page_ranges[page_range_index]
                
                # Read base record
                base_record = page_range.read_base_record(
                    base_page_index, 
                    base_slot, 
                    [0] * self.table.num_columns
                )

                # Handle update based on record state
                if base_record[config.INDIRECTION_COLUMN] == 0:
                    # First update to this record
                    self.handleFirstUpdate(
                        page_range, 
                        base_record, 
                        page_range_index, 
                        base_page_index, 
                        base_slot, 
                        columns
                    )
                else:
                    # Record has previous updates
                    self.handleSubsequentUpdate(
                        page_range, 
                        base_record, 
                        page_range_index, 
                        base_page_index, 
                        base_slot, 
                        columns
                    )

            return True
        except:
            return False

    def delete(self, primary_key):
        """
        Deletes record(s) from the table
        :param primary_key: int   #Primary key of record(s) to delete
        :return: bool           #True if successful, False otherwise
        """
        try:
            # Locate records to delete
            rids = self.table.index.locate(self.table.key, primary_key)
            if not rids:
                return False

            # Delete each matching record
            for rid in rids:
                # Get record location and data
                page_range_index, base_page_index, base_slot = self.table.page_directory[rid]
                page_range = self.table.page_ranges[page_range_index]
                
                # Read base record
                base_record = page_range.read_base_record(
                    base_page_index, 
                    base_slot, 
                    [1] * self.table.num_columns
                )

                if page_range_index is None:
                    return False

                # Mark base record as deleted
                base_rid = rid
                page_range.update_base_record_column(base_page_index, base_slot, config.RID_COLUMN, 0)

                # Delete associated tail records
                current_rid = base_record[config.INDIRECTION_COLUMN]
                while current_rid and current_rid != base_rid:
                    # Get tail record location
                    page_range_index, tail_index, tail_slot = self.table.page_directory[current_rid]
                    if page_range_index is None:
                        break

                    # Read and mark tail record as deleted
                    tail_record = page_range.read_tail_record(
                        tail_index, 
                        tail_slot,
                        [0] * self.table.num_columns
                    )
                    page_range.update_tail_record_column(tail_index, tail_slot, config.RID_COLUMN, 0)
                    
                    # Move to next tail record
                    current_rid = tail_record[config.INDIRECTION_COLUMN]

                # Clean up index and directory entries
                self.table.index.delete(base_record)
                self.table.page_directory[rid] = None

            return True
        except:
            return False

    def sum(self, start_range, end_range, aggregate_column_index):
        """
        Computes the sum of values in a column over a key range
        :param start_range: int           #Start of key range (inclusive)
        :param end_range: int             #End of key range (inclusive)
        :param aggregate_column_index: int #Column to sum
        :return: int                     #Sum of values, or False if no records found
        """
        # Use sum_version with default version (0)
        return self.sum_version(start_range, end_range, aggregate_column_index, 0)

    def sum_version(self, start_range, end_range, aggregate_column_index, relative_version):
        """
        Computes the sum of values in a column over a key range for a specific version
        :param start_range: int           #Start of key range (inclusive)
        :param end_range: int             #End of key range (inclusive)
        :param aggregate_column_index: int #Column to sum
        :param relative_version: int       #Version to use for sum
        :return: int                     #Sum of values, or False if no records found
        """
        try:
            # Initialize sum variables
            total_sum = 0
            found_records = False
            
            # Create projection for target column
            projection = [1 if i == aggregate_column_index else 0 for i in range(self.table.num_columns)]

            # Sum values over range
            for key in range(start_range, end_range + 1):
                # Get records for current key
                records = self.select_version(key, self.table.key, projection, relative_version)
                
                if records and records is not False:
                    found_records = True
                    value = records[0].columns[aggregate_column_index]
                    total_sum += value if value is not None else 0

            return total_sum if found_records else False
        except:
            return False
    
    def increment(self, key, column):
        """
        Increment value in record
        """
        try:
            records = self.select(key, self.table.key, [1] * self.table.num_columns)
            if not records or records is False:
                return False

            record = records[0]
            updated_columns = [None] * self.table.num_columns
            updated_columns[column] = record[column] + 1
            
            return self.update(key, *updated_columns)
        except:
            return False


