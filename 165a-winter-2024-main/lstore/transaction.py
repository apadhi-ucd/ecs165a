from lstore.table import Table, Record
from lstore.index import Index
from lstore.lock import LockManager
from lstore.config import *
from lstore.query import Query

from datetime import datetime 

class Transaction:
    """
    Transaction class that handles ACID properties for database operations.
    Provides functionality for grouping multiple queries into an atomic unit
    with proper locking, logging, and rollback capabilities.
    """
    
    def __init__(self, lock_manager=None):
        """
        Initialize a new transaction with empty query list and log.
        """
        self.queries = []  # List of (query_function, table, args) tuples
        self.log = []      # List of log entries for rollback operations
        self.log_manager = TransactionLog()
        self.undo_log = []
    
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
    Purpose: Finalizes a successful transaction by releasing all locks and clearing the log

    Functionality:
        - Releases all locks acquired during the transaction
        - Clears the transaction log as changes are now permanent
        - Returns True to indicate successful commit
    '''
    def commit(self):
        tx_id = id(self)

        # release_all_locks called on one table since lock_manager is shared globally
        self.queries[0][1].lock_manager.release_all_locks(tx_id)
        self.undo_log.clear()
        # TBD, persist the log to disk here.

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
        '''Rolls back the transaction and releases all locks'''

        # reverse logs to process latest first
        for log_item in reversed(self.undo_log):

            operation_type = log_item["query"]
            target_table = log_item["table"]
            operation_args = log_item["args"]
            operation_changes = log_item["changes"]
            
            key_value = operation_args[0]


            if operation_type == "insert":
                # undo insert - remove inserted record from storage and indexes
                try:
                    # For an insert, rollback by deleting the inserted record.
                    record_id = operation_changes["rid"]
                    # soft delete
                    target_table.deallocation_base_rid_queue.put(record_id)
                    target_table.index.delete_from_all_indices(key_value)
                    target_table.index.delete_logged_columns()
                except Exception as error:
                    print(f"Error rolling back insert for RID {record_id}: {error}")
 
            elif operation_type == "delete":
                # undo delete - insert values into indexes

                try:
                    # For a delete, rollback by re-inserting the deleted record.                    
                    original_columns = operation_changes["prev_columns"]
                    record_id = operation_changes["rid"]

                    # create new record object based on logged values
                    new_record = Record(record_id, key_value, original_columns)

                    # insert record and restore indexes
                    target_table.insert_record(new_record)
                    #target_table.index.clear_logged_columns()
                    target_table.index.insert_in_all_indices(new_record.columns)
                except Exception as error:
                    print(f"Error rolling back delete for RID {record_id}: {error}")
                
            elif operation_type == "update":
                # undo update - restore previous column values
                try:
                    # For an update, rollback by restoring the previous state.
                    current_columns = operation_changes["prev_columns"]  #new columns now when reversed
                    previous_columns = operation_changes["columns"]  #prev columns now
                    target_table.index.delete_from_all_indices(current_columns[target_table.key], current_columns)
                    target_table.index.delete_logged_columns()
                    #target_table.index.update_all_indices(current_columns[target_table.key], current_columns, previous_columns)
                    target_table.update_record(record_id, current_columns)
                    target_table.index.update_all_indices(key_value, current_columns, previous_columns)
                except Exception as error:
                    print(f"Error rolling back update for RID {record_id}: {error}")
        
        tx_id = id(self)
        if self.queries and self.queries[0][1].lock_manager.transaction_states.get(tx_id):
            self.queries[0][1].lock_manager.release_all_locks(tx_id)
        return False
    
    """
    Purpose: Generates a unique ID for each query to be used in the locking

    Functionality:
        - Takes a query function, table, and arguments as input
        - Analyzes the query type (insert, delete, update, select, etc.)
        - Returns a tuple containing primary key and table key for the record
        - For different query types, extracts the relevant identifiers from different argument positions
    """
    def __query_unique_identifier(self, query_func, target_table, query_args):
        if (query_func.__name__ in ["delete", "update"]):
            return (query_args[0], target_table.key)
        elif (query_func.__name__ == "insert"):
            return (query_args[target_table.key], target_table.key)
        elif (query_func.__name__ in ["select", "select_version"]):
            return (query_args[0], query_args[1])
        elif (query_func.__name__ in ["sum", "sum_version"]):
            return query_args[3]
        else:
            raise ValueError(f"Query {query_func.__name__} not supported")
    
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
        '''Acquires intention/record locks and handles logging'''
        tx_id = id(self)
        record_locks = {}
        table_locks = {}

        for query_func, target_table, query_args in self.queries:
            # set table and record lock types
            record_key = self.__query_unique_identifier(query_func, target_table, query_args)

            if record_key not in record_locks:
                if (query_func.__name__ in ["select", "select_version", "sum", "sum_version"]):
                    record_locks[record_key] = "S"
                    table_locks[record_key] = "IS"
                    try:
                        target_table.lock_manager.acquire_lock(tx_id, record_key, "S")
                        target_table.lock_manager.acquire_lock(tx_id, target_table.name, "IS")
                    except Exception as error:
                        print("Failed to aquire shared lock: ", error)
                        return self.abort()
                    
                else:
                    record_locks[record_key] = "X"
                    table_locks[record_key] = "IX"
                    try:
                        target_table.lock_manager.acquire_lock(tx_id, record_key, "X")
                        target_table.lock_manager.acquire_lock(tx_id, target_table.name, "IX")
                    except Exception as error:
                        print("Failed to aquire exclusive lock: ", error)
                        return self.abort()
                    
            elif (record_locks[record_key] == "S" and query_func.__name__ in ["update", "delete", "insert"]):
                record_locks[record_key] = "X"
                table_locks[record_key] = "IX"
                try:
                    target_table.lock_manager.upgrade_lock(tx_id, record_key, "S", "X")
                    target_table.lock_manager.upgrade_lock(tx_id, target_table.name, "IS", "IX")
                except Exception as error:
                    print("Failed to upgrade lock: ", error)
                    return self.abort()

        # execute queries and handle logging
        for query_func, target_table, query_args in self.queries:

            # create log dictionary to store changes made during a given transaction
            operation_log = {"query": query_func.__name__, "table": target_table, "args": query_args, "changes": []}

            # pass operation_log into query only if insert, update, or delete
            if query_func.__name__ in ["insert", "update", "delete"]:
                query_result = query_func(*query_args, log_entry=operation_log)

            elif query_func.__name__ in ["select", "select_version", "sum", "sum_version"]:
                query_result = query_func(*query_args) 
            
            # If the query has failed the transaction should abort
            if query_result == False:
                return self.abort()
            
            # Record the log entry locally for potential rollback and in the log manager 
            self.undo_log.append(operation_log)
            
            # Record the log entry locally and in the persistent log manager for recovery
            self.undo_log.append(operation_log)
            operation_type = operation_log.get("query")
            record_id = operation_log.get("rid")
            previous_state = operation_log.get("prev_columns")
            new_state = operation_log.get("columns")
            self.log_manager.log_operation(tx_id, operation_type, record_id, previous_state, new_state)
            

        return self.commit()


class TransactionLog:
    """
    Manages transaction logging for recovery and rollback operations.
    Stores transaction history in memory and provides methods for
    adding, retrieving, and removing transaction logs.
    """
    
    def __init__(self, persistence_file_path="transaction_log.pkl"):
        """Initialize a new transaction log with an empty history dictionary."""
        self.persistence_file_path = persistence_file_path
        self.transaction_history = {}
        
    def get_transaction_log(self, tx_id):
        """
        Retrieve the log entries for a specific transaction.
        
        Args:
            tx_id: The transaction identifier
            
        Returns:
            A list of log entries or an empty list if transaction not found
        """
        return self.transaction_history.get(tx_id, [])
        
    def remove_transaction(self, tx_id):
        """
        Remove a transaction's log entries from history.
        
        Args:
            tx_id: The transaction identifier to remove
        """
        if tx_id in self.transaction_history:
            del self.transaction_history[tx_id]
            
    def _create_transaction_entry(self, tx_id, table_identifier, operation_type, 
                                 record_identifier, previous_state, new_state):
        """
        Create a new transaction log entry.
        
        Args:
            tx_id: Transaction identifier
            table_identifier: Table being modified
            operation_type: Type of operation (insert, update, delete)
            record_identifier: Record ID being affected
            previous_state: State before operation
            new_state: State after operation
            
        Returns:
            A dictionary containing the transaction entry
        """
        return {
            "timestamp": datetime.now(),
            "transaction_id": tx_id,
            "table": table_identifier,
            "operation": operation_type,
            "record_id": record_identifier,
            "pre_args": previous_state,
            "post_args": new_state
        }
    
    def _ensure_transaction_exists(self, tx_id):
        """
        Ensure a transaction entry exists in the history.
        
        Args:
            tx_id: Transaction identifier
        """
        if tx_id not in self.transaction_history:
            self.transaction_history[tx_id] = []
            
    def log_operation(self, tx_id, table_identifier, operation_type, 
                     record_identifier, previous_state=None, new_state=None):
        """
        Log a transaction operation.
        
        Args:
            tx_id: Transaction identifier
            table_identifier: Table being modified
            operation_type: Type of operation (insert, update, delete)
            record_identifier: Record ID being affected
            previous_state: State before operation (optional)
            new_state: State after operation (optional)
        """
        # Create the transaction entry
        transaction_entry = self._create_transaction_entry(
            tx_id, table_identifier, operation_type, 
            record_identifier, previous_state, new_state
        )
        
        # Ensure transaction exists in history
        self._ensure_transaction_exists(tx_id)
        
        # Add entry to transaction history
        self.transaction_history[tx_id].append(transaction_entry)
