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
    def __query_unique_identifier(self, query_func, target_table, query_args):
        if query_func.__name__ in ["delete", "update"]:
            return (query_args[0], target_table.key)
        elif query_func.__name__ == "insert":
            return (query_args[target_table.key], target_table.key)
        elif query_func.__name__ in ["select", "select_version"]:
            return (query_args[0], query_args[1])
        elif query_func.__name__ in ["sum", "sum_version"]:
            return query_args[3]
        else:
            raise ValueError(f"Query {query_func.__name__} not supported")
    '''
    Purpose: Adds a query operation to the transaction

    Functionality:
        - Takes a query function, target table, and query arguments as input
        - Stores these as a tuple in the transaction's queries list
        - Doesn't execute the query immediately, just queues it for later execution
        - Allows building up a multi-operation transaction before committing
    '''
    def add_query(self, query_func, target_table, *query_args):
        self.queries.append((query_func, target_table, query_args))
    
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
        tx_id = id(self)
        record_locks = {}
        table_locks = {}

        # Phase 1: Acquire all necessary locks
        for query_func, target_table, query_args in self.queries:
            record_id = self.__query_unique_identifier(query_func, target_table, query_args)

            if record_id not in record_locks:
                # For read operations (select, sum)
                if query_func.__name__ in ["select", "select_version", "sum", "sum_version"]:
                    record_locks[record_id] = "S"  # Shared lock
                    table_locks[record_id] = "IS"  # Intention Shared lock
                    try:
                        target_table.lock_manager.acquire_lock(tx_id, record_id, "S")
                        target_table.lock_manager.acquire_lock(tx_id, target_table.name, "IS")
                    except Exception as error:
                        print("Failed to acquire shared lock: ", error)
                        return self.abort()
                
                # For write operations (insert, update, delete)
                else:
                    record_locks[record_id] = "X"  # Exclusive lock
                    table_locks[record_id] = "IX"  # Intention Exclusive lock
                    try:
                        target_table.lock_manager.acquire_lock(tx_id, record_id, "X")
                        target_table.lock_manager.acquire_lock(tx_id, target_table.name, "IX")
                    except Exception as error:
                        print("Failed to acquire exclusive lock: ", error)
                        return self.abort()
            
            # Upgrade lock if needed (read → write)
            elif record_locks[record_id] == "S" and query_func.__name__ in ["update", "delete", "insert"]:
                record_locks[record_id] = "X"
                table_locks[record_id] = "IX"
                try:
                    target_table.lock_manager.upgrade_lock(tx_id, record_id, "S", "X")
                    target_table.lock_manager.upgrade_lock(tx_id, target_table.name, "IS", "IX")
                except Exception as error:
                    print("Failed to upgrade lock: ", error)
                    return self.abort()

        # Phase 2: Execute queries and log changes
        for query_func, target_table, query_args in self.queries:
            # Create log entry for potential rollback
            operation_log = {"query": query_func.__name__, "table": target_table, "args": query_args, "changes": []}

            # Execute query with appropriate parameters
            if query_func.__name__ in ["insert", "update", "delete"]:
                query_result = query_func(*query_args, log_entry=operation_log)
            else:  # select or sum operations
                query_result = query_func(*query_args) 
            
            # Abort if query failed
            if query_result == False:
                return self.abort()
            
            # Log successful operation
            self.log.append(operation_log)

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
        tx_id = id(self)

        # Release all locks (lock_manager is shared globally)
        if self.queries:
            self.queries[0][1].lock_manager.release_all_locks(tx_id)
        
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
        for log_entry in reversed(self.log):
            operation_type = log_entry["query"]
            target_table = log_entry["table"]
            operation_args = log_entry["args"]
            operation_changes = log_entry["changes"]
            key_value = operation_args[0]

            # Undo insert operations
            if operation_type == "insert":
                record_id = operation_changes["rid"]
                # Soft delete the record
                target_table.deallocation_base_rid_queue.put(record_id)
                target_table.index.delete_from_all_indices(key_value)
                target_table.index.delete_logged_columns()
            
            # Undo delete operations
            elif operation_type == "delete":
                original_columns = operation_changes["prev_columns"]
                record_id = operation_changes["rid"]
                # Recreate and reinsert the deleted record
                restored_record = Record(record_id, key_value, original_columns)
                target_table.insert_record(restored_record)
                target_table.index.clear_logged_columns()
            
            # Undo update operations
            elif operation_type == "update":
                original_columns = operation_changes["prev_columns"]
                target_table.index.delete_from_all_indices(original_columns[target_table.key], original_columns)
                target_table.index.delete_logged_columns()
        
        # Release all locks
        tx_id = id(self)
        if self.queries and self.queries[0][1].lock_manager.transaction_states.get(tx_id):
            self.queries[0][1].lock_manager.release_all_locks(tx_id)
        
        return False