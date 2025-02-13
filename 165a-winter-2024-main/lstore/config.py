# Metadata column indices
INDIRECTION_COLUMN = 0          # Points to the most recent version of the record
RID_COLUMN = 1                  # Record ID
TIMESTAMP_COLUMN = 2            # Tracks the timestamp of the record
SCHEMA_ENCODING_COLUMN = 3      # Tracks which columns were modified

# Storage configuration
PAGE_SIZE = 4096                        # Size of each page in bytes
RECORD_SIZE = 8                         # Size of each record in bytes (64-bit integers)
RECORDS_PER_PAGE = PAGE_SIZE // RECORD_SIZE  # Number of records per page
BASE_PAGES_PER_RANGE = 16                # Number of base pages per range

# Page and record configuration
PAGE_SIZE = 4096           # Size of each page in bytes
RECORD_SIZE = 8           # Size of each record value in bytes (64-bit integers)
RECORDS_PER_PAGE = PAGE_SIZE // RECORD_SIZE  # Number of records that fit in one page

# Column configuration
METADATA_COLUMNS = 4      # Number of metadata columns (indirection, RID, timestamp, schema)

# Page range configuration
MAX_BASE_PAGES = 16       # Maximum number of base pages per page range

# Merge configuration
MERGE_TRIGGER_COUNT = 2000  # Number of updates before triggering merge
MERGE_THRESHOLD = 0.2     # Percentage of records that need to be updated to trigger merge 
