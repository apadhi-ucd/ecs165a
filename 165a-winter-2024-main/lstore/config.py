# Description: Configuration file for the LStore database system
INTEGER_BYTE_SIZE = 4

# Metadata columns
METADATA_COLUMNS = 4            # Number of metadata columns in each record
INDIRECTION_COLUMN = 0          # Indirection column number
RID_COLUMN = 1                  # RID column number
TIMESTAMP_COLUMN = 2            # Timestamp column number
SCHEMA_ENCODING_COLUMN = 3      # Schema encoding column number
UPDATE_TIMESTAMP_COLUMN = 3     # Update timestamp column (same as schema encoding for now)

# Page Constants
PAGE_SIZE = 4096                # Size of each page in bytes
RECORD_SIZE = 8                 # Size of each record in bytes (64-bit integers)
PAGE_CAPACITY = PAGE_SIZE // RECORD_SIZE  # Number of records per page
MAX_RECORD_PER_PAGE = PAGE_SIZE // INTEGER_BYTE_SIZE

# Page Range Constants
BASE_PAGES_PER_RANGE = 16       # 16 base pages per range
MAX_PAGE_RANGE = 32             # How many base pages are in a Page Range
MAX_RECORD_PER_PAGE_RANGE = MAX_RECORD_PER_PAGE * MAX_PAGE_RANGE
MAX_TAIL_PAGES_BEFORE_MERGING = MAX_PAGE_RANGE

# Record Constants
NUM_HIDDEN_COLUMNS = 5
RECORD_DELETION_FLAG = -1
RECORD_NONE_VALUE = -2

# Buffer Pool Constants
BUFFER_POOL_SIZE = 20           # Size of the buffer pool
DIRTY_PAGE_THRESHOLD = 15       # Threshold of dirty pages before automatic merge
MERGE_THRESHOLD = 1000          # Number of updates before merge
MAX_NUM_FRAME = MAX_PAGE_RANGE * 2
MERGE_FRAME_ALLOCATION = 8



