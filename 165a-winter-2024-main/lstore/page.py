class Page:
    """
    A page that stores table records in a columnar format with support for base and tail pages.
    """
    PAGE_SIZE = 4096 * 16  # 64KB
    RECORDS_PER_PAGE = 4000  # Each page holds 4k records
    DELETE_FLAG = -1  # Special value to indicate deletion

    def __init__(self, num_columns, page_type="base"):
        """
        Initialize a new page with specified number of columns
        
        Args:
            num_columns (int): Number of data columns in the table
            page_type (str): Type of page ('base' or 'tail')
        """
        self.num_columns = num_columns + 1  # Add one extra column for schema encoding
        self.page_type = page_type
        self.pages = [bytearray(self.PAGE_SIZE) for _ in range(self.num_columns)]
        self.num_records = 0
        self.offsets = [0] * self.num_columns
        self.invalid_records = set()
        self.version_metadata = [] if page_type == 'tail' else None

    def has_capacity(self):
        """
        Returns true if the page has capacity to store another record
        """
        return self.num_records < self.RECORDS_PER_PAGE

    def write(self, record, schema_encoding, version=None):
        """
        Writes a record and its schema encoding to the page
        
        Args:
            record (list): List of values to write
            schema_encoding (str): Schema encoding for the record
            version (any, optional): Version information for tail pages
            
        Returns:
            bool: True if write successful, False if page is full
        """
        if not self.has_capacity():
            return False

        # Write schema encoding to first column
        schema_bytes = str(schema_encoding).encode('utf-8')
        self.pages[0][self.offsets[0]:self.offsets[0] + len(schema_bytes)] = schema_bytes
        self.offsets[0] += len(schema_bytes)

        # Write each column value
        for i, value in enumerate(record, start=1):  # Start from 1 since column 0 is schema
            value_bytes = str(value).encode('utf-8')
            value_size = len(value_bytes)

            # Check if we have enough space in this column
            if self.offsets[i] + value_size > self.PAGE_SIZE:
                return False

            # Write the value
            self.pages[i][self.offsets[i]:self.offsets[i] + value_size] = value_bytes
            self.offsets[i] += value_size

        # Add version metadata for tail pages
        if self.page_type == 'tail':
            self.version_metadata.append(version)

        self.num_records += 1
        return True

    def read(self, record_index, projected_columns):
        """
        Reads a record at the given index, considering only projected columns
        
        Args:
            record_index (int): Index of the record to read
            projected_columns (list): Boolean list indicating which columns to read
            
        Returns:
            list: Record values for projected columns, or None if record is invalid
        """
        if record_index >= self.num_records or record_index in self.invalid_records:
            return None

        record = []
        for i in range(self.num_columns):
            if projected_columns[i]:
                # Calculate offset for this record in this column
                # Note: This is simplified and would need proper offset tracking
                start_offset = self.offsets[i] * (record_index // self.num_records)
                end_offset = start_offset + self.offsets[i]
                value_bytes = self.pages[i][start_offset:end_offset]
                try:
                    value = value_bytes.decode('utf-8')
                    record.append(value)
                except:
                    record.append(None)
        return record

    def invalidate(self, record_id):
        """
        Marks a record as invalid/deleted
        
        Args:
            record_id (int): Index of the record to invalidate
        """
        self.invalid_records.add(record_id)
