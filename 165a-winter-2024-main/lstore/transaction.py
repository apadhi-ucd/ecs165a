from lstore.table import Table, Record
from lstore.index import Index
from lstore.lock import LockManager
from lstore.config import *

class Transaction:
    """
    Transaction class that handles ACID properties for database operations.
    Provides functionality for grouping multiple queries into an atomic unit
    with proper locking, logging, and rollback capabilities.
    """
    
    def __init__(self):
        """
        Initialize a new transaction with empty query list and log.
        """
        self.queries = []  # List of (query_function, table, args) tuples
        self.log = []      # List of log entries for rollback operations
    
    """
    Purpose: Generates a unique ID for each query to be used in the locking

    Functionality:
        - Takes a query function, table, and arguments as input
        - Analyzes the query type (insert, delete, update, select, etc.)
        - Returns a tuple containing primary key and table key for the record
        - For different query types, extracts the relevant identifiers from different argument positions
    """
    def __query_unique_identifier(self, query, table, args):
        if query.__name__ in ["delete", "update"]:
            return (args[0], table.key)
        elif query.__name__ == "insert":
            return (args[table.key], table.key)
        elif query.__name__ in ["select", "select_version"]:
            return (args[0], args[1])
        elif query.__name__ in ["sum", "sum_version"]:
            return args[3]
        else:
            raise ValueError(f"Query {query.__name__} not supported")
    '''
    Purpose: Adds a query operation to the transaction

    Functionality:
        - Takes a query function, target table, and query arguments as input
        - Stores these as a tuple in the transaction's queries list
        - Doesn't execute the query immediately, just queues it for later execution
        - Allows building up a multi-operation transaction before committing
    '''
    def add_query(self, query, table, *args):
        self.queries.append((query, table, args))
    
    '''
    Purpose: Executes all queries in the transaction while ensuring ACID properties
    
    Functionality:
    Phase 1 - Lock acquisition:
        - Generates a unique transaction ID
        - Iterates through all queries to acquire appropriate locks
        - Acquires shared (S) locks for read operations and intention shares (IS) locks on tables
        - Acquires exclusive (X) locks for write operations and intention exclusive (IX) locks on tables
        - Upgrades locks if needed (from shared to exclusive)
        - Aborts transaction if any lock acquisition fails

    Phase 2 - Query execution:
        - Executes each query with appropriate parameters
        - Creates log entries for each operation for potential rollback
        - Aborts if any query fails
        - Adds successful operations to the transaction log

    Phase 3 - Commit:
        - Calls the commit method to finalize the transaction and release all locks
    '''
    def run(self):
        transaction_id = id(self)
        locked_records = {}
        locked_tables = {}

        # Phase 1: Acquire all necessary locks
        for query, table, args in self.queries:
            record_identifier = self.__query_unique_identifier(query, table, args)

            if record_identifier not in locked_records:
                # For read operations (select, sum)
                if query.__name__ in ["select", "select_version", "sum", "sum_version"]:
                    locked_records[record_identifier] = "S"  # Shared lock
                    locked_tables[record_identifier] = "IS"  # Intention Shared lock
                    try:
                        table.lock_manager.acquire_lock(transaction_id, record_identifier, "S")
                        table.lock_manager.acquire_lock(transaction_id, table.name, "IS")
                    except Exception as e:
                        print("Failed to acquire shared lock: ", e)
                        return self.abort()
                
                # For write operations (insert, update, delete)
                else:
                    locked_records[record_identifier] = "X"  # Exclusive lock
                    locked_tables[record_identifier] = "IX"  # Intention Exclusive lock
                    try:
                        table.lock_manager.acquire_lock(transaction_id, record_identifier, "X")
                        table.lock_manager.acquire_lock(transaction_id, table.name, "IX")
                    except Exception as e:
                        print("Failed to acquire exclusive lock: ", e)
                        return self.abort()
            
            # Upgrade lock if needed (read → write)
            elif locked_records[record_identifier] == "S" and query.__name__ in ["update", "delete", "insert"]:
                locked_records[record_identifier] = "X"
                locked_tables[record_identifier] = "IX"
                try:
                    table.lock_manager.upgrade_lock(transaction_id, record_identifier, "S", "X")
                    table.lock_manager.upgrade_lock(transaction_id, table.name, "IS", "IX")
                except Exception as e:
                    print("Failed to upgrade lock: ", e)
                    return self.abort()

        # Phase 2: Execute queries and log changes
        for query, table, args in self.queries:
            # Create log entry for potential rollback
            log_entry = {"query": query.__name__, "table": table, "args": args, "changes": []}

            # Execute query with appropriate parameters
            if query.__name__ in ["insert", "update", "delete"]:
                result = query(*args, log_entry=log_entry)
            else:  # select or sum operations
                result = query(*args) 
            
            # Abort if query failed
            if result == False:
                return self.abort()
            
            # Log successful operation
            self.log.append(log_entry)

        # Phase 3: Commit the transaction
        return self.commit()
    
    '''
    Purpose: Finalizes a successful transaction by releasing all locks and clearing the log

    Functionality:
        - Releases all locks acquired during the transaction
        - Clears the transaction log as changes are now permanent
        - Returns True to indicate successful commit
    '''
    def commit(self):
        transaction_id = id(self)

        # Release all locks (lock_manager is shared globally)
        if self.queries:
            self.queries[0][1].lock_manager.release_all_locks(transaction_id)
        
        # Clear the log after successful commit
        self.log.clear()
        return True
    
    '''
    Purpose: Rolls back all changes made during a failed transaction

    Functionality:
        - Processes the log entries in reverse order (newest to oldest)
        - For insert operations:
            - Soft-deletes the inserted record
            - Removes entries from indices
        - For delete operations:
            - Recreates and reinserts the deleted record
            - Clears logged column info
        - For update operations:
            - Removes updated entries from indices
            - Clears logged column information
        - Returns False to indicate transaction abort
    '''
    def abort(self):
        print("Aborting transaction")

        # Process logs in reverse order (newest first)
        for entry in reversed(self.log):
            query_type = entry["query"]
            table = entry["table"]
            args = entry["args"]
            changes = entry["changes"]
            primary_key = args[0]

            # Undo insert operations
            if query_type == "insert":
                rid = changes["rid"]
                # Soft delete the record
                table.deallocation_base_rid_queue.put(rid)
                table.index.delete_from_all_indices(primary_key)
                table.index.delete_logged_columns()
            
            # Undo delete operations
            elif query_type == "delete":
                prev_columns = changes["prev_columns"]
                rid = changes["rid"]
                # Recreate and reinsert the deleted record
                record = Record(rid, primary_key, prev_columns)
                table.insert_record(record)
                table.index.clear_logged_columns()
            
            # Undo update operations
            elif query_type == "update":
                prev_columns = changes["prev_columns"]
                table.index.delete_from_all_indices(prev_columns[table.key], prev_columns)
                table.index.delete_logged_columns()
        
        # Release all locks
        transaction_id = id(self)
        if self.queries and self.queries[0][1].lock_manager.transaction_states.get(transaction_id):
            self.queries[0][1].lock_manager.release_all_locks(transaction_id)
        
        return False