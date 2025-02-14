import time
from lstore.table import Record
import lstore.config as config
from datetime import datetime

class Query:
    
    def __init__(self, table):
        self.table = table

    """
    # Deletes all records with the given primary key
    :param primary_key: int - Primary key value
    """
    def delete(self, primary_key):
        try:
            rids = self.table.index.locate(self.table.key, primary_key)
            if not rids:
                return False
            for rid in rids:
                page_range_index, base_index, slot_index = self.table.page_directory[rid]
                page_range = self.table.page_range[page_range_index]
                base_rec = page_range.read_record(base_index, slot_index, [1] * self.table.num_columns, True)
                if page_range_index is None:
                    return False
                page_range.update_record_column(base_index, slot_index, config.RID_COLUMN, 0, True)
                current_rid = base_rec[config.INDIRECTION_COLUMN]
                while current_rid and current_rid != rid:
                    page_range_index, tail_index, tail_slot = self.table.page_directory[current_rid]
                    if page_range_index is None:
                        break
                    tail_rec = page_range.read_record(tail_index, tail_slot, [0] * self.table.num_columns, False)
                    page_range.update_record_column(tail_index, tail_slot, config.RID_COLUMN, 0, False)
                    current_rid = tail_rec[config.INDIRECTION_COLUMN]
                self.table.index.delete(base_rec)
                self.table.page_directory[rid] = None
            return True
        except:
            return False
        
    """
    # Inserts a new record with the given values
    :param values: list - List of values for the new record
    """
    def insert(self, *values):
        try:
            if len(values) != self.table.num_columns:
                return False
            
            new_rid = self.table.new_rid()
            active_page_range = self.table.page_range[self.table.page_range_index]
            new_entry = [None] * config.METADATA_COLUMNS
            new_entry[config.INDIRECTION_COLUMN] = 0
            new_entry[config.RID_COLUMN] = new_rid
            new_entry[config.TIMESTAMP_COLUMN] = int(time.time())
            new_entry[config.SCHEMA_ENCODING_COLUMN] = 0
            new_entry.extend(values)
            
            page_index, slot_index = active_page_range.write_record(new_entry, True)
            self.table.page_directory[new_rid] = (self.table.page_range_index, page_index, slot_index)
            self.table.index.insert(new_entry)
            self.table.create_page_range()
            
            return True
        except:
            return False

    """
    # Selects all records with the given key
    :param search_key: int -  Key value
    :param search_key_index: int - Index of the key column
    :param projected_columns_index: list - List of 1s and 0s to indicate which columns to return
    """
    def select(self, search_key, search_key_index, projected_columns_index):
        return self.select_version(search_key, search_key_index, projected_columns_index, 0)
    
    """
    # Selects all records with the given key and version
    :param search_key: int -  Key value
    :param search_key_index: int - Index of the key column
    :param projected_columns_index: list - List of 1s and 0s to indicate which columns to return
    :param relative_version: int - Version number to retrieve
    """
    def select_version(self, search_key, search_key_index, projected_columns_index, relative_version):
        try:
            base_records = self.get_records(search_key, search_key_index, projected_columns_index)
            retrieved_records = []
            
            for base_rec in base_records:
                if base_rec.indirection == 0:
                    retrieved_records.append(base_rec)
                    continue
                
                current_version = 0
                tail_rid = base_rec.indirection
                return_base = False
                pr_index, tp_index, slot = self.table.page_directory[tail_rid]
                tail_record = self.table.page_range[pr_index].read_record(tp_index, slot, projected_columns_index, False)
                
                while current_version > relative_version:
                    if tail_record[config.INDIRECTION_COLUMN] == base_rec.rid:
                        return_base = True
                        break
                    tail_rid = tail_record[config.INDIRECTION_COLUMN]
                    pr_index, tp_index, slot = self.table.page_directory[tail_rid]
                    tail_record = self.table.page_range[pr_index].read_record(tp_index, slot, projected_columns_index, False)
                    current_version -= 1
                
                if return_base or relative_version < current_version:
                    retrieved_records.append(base_rec)
                else:
                    schema_value = tail_record[config.SCHEMA_ENCODING_COLUMN]
                    updated_columns = [int(bit) for bit in f"{schema_value:0{self.table.num_columns}b}"]
                    for index, updated in enumerate(updated_columns):
                        if updated:
                            base_rec.columns[index] = tail_record[config.METADATA_COLUMNS + index]
                    retrieved_records.append(base_rec)
            
            return retrieved_records
        except:
            return False

    """
    # Updates all records with the given key
    :param primary_key: int - Primary key value
    :param columns: list - List of new values for the updated record
    """    
    def update(self, primary_key, *columns):
        try:
            rids = self.table.index.locate(self.table.key, primary_key)
            if not rids:
                return False
            
            # Update all records with the given key
            for rid in rids:
                page_range_index, base_index, slot_index = self.table.page_directory[rid]
                page_range = self.table.page_range[page_range_index]
                base_rec = page_range.read_record(base_index, slot_index, [0] * self.table.num_columns, True)

                # If the record is being updated for the first time, create a new tail record
                if base_rec[config.INDIRECTION_COLUMN] == 0:
                    tail_rid = self.table.new_rid()
                    schema, schema_num = self.createSchemaEncoding(columns)
                    tail_rec = self.createTailRecord(tail_rid, base_rec[config.RID_COLUMN], columns, schema_num)
                    self.writeTailRecord(page_range, page_range_index, tail_rec)
                    page_range.update_record_column(base_index, slot_index, config.INDIRECTION_COLUMN, tail_rid, True)
                    page_range.update_record_column(base_index, slot_index, config.SCHEMA_ENCODING_COLUMN, schema_num, True)

                # If the record has been updated before, create a new tail record based on the latest tail record
                else:
                    existing_schema = [int(bit) for bit in f"{base_rec[config.SCHEMA_ENCODING_COLUMN]:0{self.table.num_columns}b}"]
                    latest_tail_rid = base_rec[config.INDIRECTION_COLUMN]
                    page_range_index, latest_tail_index, latest_tail_slot = self.table.page_directory[latest_tail_rid]
                    latest_tail_rec = page_range.read_record(latest_tail_index, latest_tail_slot, existing_schema, False)
                    new_tail_rid = self.table.new_rid()
                    schema, schema_num = self.createSchemaEncoding(columns, existing_schema)
                    new_tail_rec = self.createTailRecord(new_tail_rid, latest_tail_rid, columns, schema_num, latest_tail_rec)
                    self.writeTailRecord(page_range, page_range_index, new_tail_rec)
                    page_range.update_record_column(base_index, slot_index, config.INDIRECTION_COLUMN, new_tail_rid, True)
                    page_range.update_record_column(base_index, slot_index, config.SCHEMA_ENCODING_COLUMN, schema_num, True)
            return True
        except:
            return False

    """
    # Sums all records with the given key
    :param start_range: int - Start of the key range
    :param end_range: int - End of the key range
    :param agg_col_index: int - Index of the column to aggregate
    """
    def sum(self, start_range, end_range, agg_col_index):
        return self.sum_version(start_range, end_range, agg_col_index, 0)

    """
    # Sums all records with the given key and version
    :param start_range: int - Start of the key range
    :param end_range: int - End of the key range
    :param agg_col_index: int - Index of the column to aggregate
    :param relative_version: int - Version number to retrieve
    """
    def sum_version(self, start_range, end_range, agg_col_index, relative_version):
        try:
            total_sum, found_records = 0, False
            projection = [1 if i == agg_col_index else 0 for i in range(self.table.num_columns)]
            for key in range(start_range, end_range + 1):
                records = self.select_version(key, self.table.key, projection, relative_version)
                if records:
                    found_records = True
                    total_sum += records[0].columns[agg_col_index] or 0
            return total_sum if found_records else False
        except:
            return False

    """
    # Increments the value of a column in all records with the given key
    :param key: int - Primary key value
    :param column: int - Index of the column to increment
    """
    def increment(self, key, column):
        try:
            records = self.select(key, self.table.key, [1] * self.table.num_columns)
            if not records:
                return False
            record = records[0]
            updated_columns = [None] * self.table.num_columns
            updated_columns[column] = record[column] + 1
            return self.update(key, *updated_columns)
        except:
            return False

    """
    # Creates a new record with the passed values
    :param values: list - List of values for the new record
    :param primary_key: int - Primary key value for the new record
    """
    def create_record(self, values, primary_key):
        timestamp = datetime.fromtimestamp(float(values[config.TIMESTAMP_COLUMN]))
        schema_encoding_value = values[config.SCHEMA_ENCODING_COLUMN]
        num_columns = self.table.num_columns
        schema_bits = [int(bit) for bit in f"{schema_encoding_value:0{num_columns}b}"]
        column_data = values[config.METADATA_COLUMNS:]
        
        return Record(indirection=values[config.INDIRECTION_COLUMN], rid=values[config.RID_COLUMN], timestamp=timestamp, schema_encoding=schema_bits, key=primary_key, columns=column_data)

    """
    # Returns all base records with the given key
    :param key: int - Primary key value
    :param key_index: int - Index of the key column
    :param column_mask: list - List of 1s and 0s to indicate which columns to return
    """
    def get_records(self, key, key_index, column_mask):
        results = []
        matching_rids = self.table.index.locate(key_index, key)
        if not matching_rids:
            return []
        
        for rid in matching_rids:
            pr_index, bp_index, slot = self.table.page_directory[rid]
            raw_record = self.table.page_range[pr_index].read_record(bp_index, slot, column_mask, True)
            record_obj = self.create_record(raw_record, key)
            results.append(record_obj)
        return results
    
    """
    # Creates a schema encoding for a new record
    :param columns: list - List of column values
    :param existing_schema: list - List of existing schema bits, if any
    """
    def createSchemaEncoding(self, columns, existing_schema=None):
        if existing_schema is None:
            schema_bits = [1 if col is not None else 0 for col in columns]
        else:
            schema_bits = existing_schema[:]
            for index, col in enumerate(columns):
                if col is not None:
                    schema_bits[index] = 1
        
        return schema_bits, int("".join(map(str, schema_bits)), 2)

    """
    # Creates a new tail record for an update
    :param tail_rid: int - RID of the new tail record
    :param indirection_rid: int - RID of the base record
    :param column_values: list - List of new column values
    :param schema_encoding: int - Schema encoding for the new record
    :param prev_record: list - List of values from the previous tail record
    """
    def createTailRecord(self, tail_rid, indirection_rid, column_values, schema_encoding, prev_record=None):
        tail_entry = [None] * config.METADATA_COLUMNS
        tail_entry[config.INDIRECTION_COLUMN] = indirection_rid
        tail_entry[config.RID_COLUMN] = tail_rid
        tail_entry[config.TIMESTAMP_COLUMN] = int(time.time())
        tail_entry[config.SCHEMA_ENCODING_COLUMN] = schema_encoding
        
        if prev_record is None:
            tail_entry.extend(col if col is not None else 0 for col in column_values)
        else:
            for i, col in enumerate(column_values):
                if col is not None:
                    tail_entry.append(col)
                elif prev_record[i + config.METADATA_COLUMNS]:
                    tail_entry.append(prev_record[i + config.METADATA_COLUMNS])
                else:
                    tail_entry.append(0)
        
        return tail_entry

    """
    # Writes a new tail record to the page range
    :param page_range: PageRange - Page range to write the record
    :param page_range_index: int - Index of the page range
    :param tail_rec: list - List of values for the new tail
    """
    def writeTailRecord(self, page_range, page_range_index, tail_rec):
        tail_index, tail_slot = page_range.write_record(tail_rec, False)
        self.table.page_directory[tail_rec[config.RID_COLUMN]] = (page_range_index, tail_index, tail_slot)
        return tail_index, tail_slot
