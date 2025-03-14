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
    
    '''
    Purpose: Adds a query operation to the transaction

    Functionality:
        - Takes a query function, target table, and query arguments as input
        - Stores these as a tuple in the transaction's queries list
        - Doesn't execute the query immediately, just queues it for later execution
        - Allows building up a multi-operation transaction before committing
    '''
    def add_query(self, operation, data_table, *operation_params):
        self.queries.append((operation, data_table, operation_params))
    
    '''
    Purpose: Finalizes a successful transaction by releasing all locks and clearing the log

    Functionality:
        - Releases all locks acquired during the transaction
        - Clears the transaction log as changes are now permanent
        - Returns True to indicate successful commit
    '''
    def commit(self):
        transaction_identifier = id(self)

        # Release all locks (lock_manager is shared globally)
        if self.queries:
            self.queries[0][1].lock_manager.release_all_locks(transaction_identifier)
        
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
        for journal_entry in reversed(self.log):
            query_name = journal_entry["query"]
            db_table = journal_entry["table"]
            cmd_args = journal_entry["args"]
            modifications = journal_entry["changes"]
            primary_key = cmd_args[0]

            # Undo insert operations
            if query_name == "insert":
                row_id = modifications["rid"]
                # Soft delete the record
                db_table.deallocation_base_rid_queue.put(row_id)
                db_table.index.delete_from_all_indices(primary_key)
                db_table.index.delete_logged_columns()
            
            # Undo delete operations
            elif query_name == "delete":
                previous_data = modifications["prev_columns"]
                row_id = modifications["rid"]
                # Recreate and reinsert the deleted record
                new_record = Record(row_id, primary_key, previous_data)
                db_table.insert_record(new_record)
                db_table.index.clear_logged_columns()
            
            # Undo update operations
            elif query_name == "update":
                previous_data = modifications["prev_columns"]
                db_table.index.delete_from_all_indices(previous_data[db_table.key], previous_data)
                db_table.index.delete_logged_columns()
        
        # Release all locks
        transaction_identifier = id(self)
        if self.queries and self.queries[0][1].lock_manager.transaction_states.get(transaction_identifier):
            self.queries[0][1].lock_manager.release_all_locks(transaction_identifier)
        
        return False
    
    """
    Purpose: Generates a unique ID for each query to be used in the locking

    Functionality:
        - Takes a query function, table, and arguments as input
        - Analyzes the query type (insert, delete, update, select, etc.)
        - Returns a tuple containing primary key and table key for the record
        - For different query types, extracts the relevant identifiers from different argument positions
    """
    def __query_unique_identifier(self, operation, data_table, operation_params):
        if operation.__name__ in ["delete", "update"]:
            return (operation_params[0], data_table.key)
        elif operation.__name__ == "insert":
            return (operation_params[data_table.key], data_table.key)
        elif operation.__name__ in ["select", "select_version"]:
            return (operation_params[0], operation_params[1])
        elif operation.__name__ in ["sum", "sum_version"]:
            return operation_params[3]
        else:
            raise ValueError(f"Query {operation.__name__} not supported")
    
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
        transaction_identifier = id(self)
        lock_registry = {}
        table_registry = {}

        # Phase 1: Acquire all necessary locks
        for operation, data_table, operation_params in self.queries:
            resource_id = self.__query_unique_identifier(operation, data_table, operation_params)

            if resource_id not in lock_registry:
                # For read operations (select, sum)
                if operation.__name__ in ["select", "select_version", "sum", "sum_version"]:
                    lock_registry[resource_id] = "S"  # Shared lock
                    table_registry[resource_id] = "IS"  # Intention Shared lock
                    try:
                        data_table.lock_manager.acquire_lock(transaction_identifier, resource_id, "S")
                        data_table.lock_manager.acquire_lock(transaction_identifier, data_table.name, "IS")
                    except Exception as lock_error:
                        print("Failed to acquire shared lock: ", lock_error)
                        return self.abort()
                
                # For write operations (insert, update, delete)
                else:
                    lock_registry[resource_id] = "X"  # Exclusive lock
                    table_registry[resource_id] = "IX"  # Intention Exclusive lock
                    try:
                        data_table.lock_manager.acquire_lock(transaction_identifier, resource_id, "X")
                        data_table.lock_manager.acquire_lock(transaction_identifier, data_table.name, "IX")
                    except Exception as lock_error:
                        print("Failed to acquire exclusive lock: ", lock_error)
                        return self.abort()
            
            # Upgrade lock if needed (read → write)
            elif lock_registry[resource_id] == "S" and operation.__name__ in ["update", "delete", "insert"]:
                lock_registry[resource_id] = "X"
                table_registry[resource_id] = "IX"
                try:
                    data_table.lock_manager.upgrade_lock(transaction_identifier, resource_id, "S", "X")
                    data_table.lock_manager.upgrade_lock(transaction_identifier, data_table.name, "IS", "IX")
                except Exception as lock_error:
                    print("Failed to upgrade lock: ", lock_error)
                    return self.abort()

        # Phase 2: Execute queries and log changes
        for operation, data_table, operation_params in self.queries:
            # Create log entry for potential rollback
            journal_entry = {"query": operation.__name__, "table": data_table, "args": operation_params, "changes": []}

            # Execute query with appropriate parameters
            if operation.__name__ in ["insert", "update", "delete"]:
                execution_result = operation(*operation_params, log_entry=journal_entry)
            else:  # select or sum operations
                execution_result = operation(*operation_params) 
            
            # Abort if query failed
            if execution_result == False:
                return self.abort()
            
            # Log successful operation
            self.log.append(journal_entry)

        # Phase 3: Commit the transaction
        return self.commit()