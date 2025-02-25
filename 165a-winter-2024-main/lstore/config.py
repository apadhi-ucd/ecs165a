# Description: Configuration file for the LStore database system
METADATA_COLUMNS = 4            # Number of metadata columns in each record
INDIRECTION_COLUMN = 0          # Indirection column number
RID_COLUMN = 1                  # RID column number
TIMESTAMP_COLUMN = 2            # Timestamp column number
SCHEMA_ENCODING_COLUMN = 3      # Schema encoding column number
PAGE_SIZE = 4096                # Size of each page in bytes
RECORD_SIZE = 8                 # Size of each record in bytes (64-bit integers)
PAGE_CAPACITY = PAGE_SIZE // RECORD_SIZE  # Number of records per page
BASE_PAGES_PER_RANGE = 16          # 16 base pages per range
BUFFER_POOL_SIZE = 20           # UNSURE WHAT TRUE SIZE SHOULD BE
DIRTY_PAGE_THRESHOLD = 15       # Threshold of dirty pages before automatic merge
MERGE_THRESHOLD = 1000            # Number of updates before merge



