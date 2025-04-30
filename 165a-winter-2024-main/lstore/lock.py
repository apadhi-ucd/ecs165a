import threading
from collections import defaultdict
import time

class Latch:
    def __init__(self, count=0):
        self.count = count
        self.lock = threading.Condition()
        self.writing = False

    def count_down(self):
        with self.lock:
            while self.writing:
                self.lock.wait()
            self.count -= 1
            if self.count <= 0:
                self.lock.notify_all()

    def count_up(self):
        with self.lock:
            while self.writing:
                self.lock.wait()
            self.count += 1

    def acquire_write_lock(self):
        with self.lock:
            while self.writing or self.count > 0:
                self.lock.wait()
            self.writing = True
    
    def release_write_lock(self):
        with self.lock:
            self.writing = False
            self.lock.notify_all()

    def await_latch(self):
        with self.lock:
            while self.count > 0:
                self.lock.wait()
                
class WaitForGraph:
    def __init__(self):
        self.graph = defaultdict(lambda: set())
        self.lock = threading.Lock()

    def add_edge(self, transaction_id1, transaction_id2):
        '''Returns False if a deadlock is detected after adding the edge'''
        with self.lock:
            #print(f"Adding edge from {transaction_id1} to {transaction_id2}")
            self.graph[transaction_id1].add(transaction_id2)
            if (self.detect_cycle()):
                return False

        return True
    
    def remove_edge(self, transaction_id1, transaction_id2):
        with self.lock:
            #print(f"Removing edge from {transaction_id1} to {transaction_id2}")
            if transaction_id2 in self.graph[transaction_id1]:
                self.graph[transaction_id1].remove(transaction_id2)
            if not self.graph[transaction_id1]:
                del self.graph[transaction_id1]

    def remove_transaction(self, transaction_id):
        with self.lock:
            if transaction_id in self.graph:
                del self.graph[transaction_id]

            for transaction in self.graph.values():
                # print("Removing transaction", transaction, "from", transaction_id)
                transaction.discard(transaction_id)

    def detect_cycle(self):
        '''Returns True if there is a Cycle for the given transaction_id'''
        visited = set()
        stack = set()
        transactions = list(self.graph.keys())
        for transaction in transactions:
            if self._detect_cycle(transaction, visited, stack):
                return True
        return False
                    
    # doing a simple dfs to detect cycle
    def _detect_cycle(self, transaction, visited:set, stack:set):
        if transaction not in visited:
            visited.add(transaction)
            stack.add(transaction)
            for neighbor in self.graph[transaction]:
                if neighbor not in visited and self._detect_cycle(neighbor, visited, stack):
                    return True
                elif neighbor in stack:
                    return True
            stack.remove(transaction)
        return False

class LockManager:
    def __init__(self):
        self.lock_table = defaultdict(lambda: {'S': set(), 'X': set(), 'IS': Latch(), 'IX': Latch()})
        self.condition = threading.Condition()
        self.wait_for_graph = WaitForGraph()

        self.transaction_states = defaultdict(lambda: 'growing')

    def acquire_lock(self, transaction_id, record_id, lock_type):
        """
        Acquires a lock on the given record for the specified transaction.
        Lock types: 'S' for shared, 'X' for exclusive, 'IS' for intention to read, 'IX' for intention to write.
        """
        with self.condition:
            # Raises exception if the transaction is locked by 2PL
            if self.transaction_states[transaction_id] == 'shrinking':
                raise Exception(f"Transaction {transaction_id} is in the shrinking phase and cannot acquire more locks.")

            if lock_type == 'X':
                # print("Acquiring exclusive lock", transaction_id, record_id)
                # Wait until no other transaction holds a lock on the record
                while self.lock_table[record_id]['S'] or self.lock_table[record_id]['X'] or self.lock_table[record_id]['IS'].count > 0 or self.lock_table[record_id]['IX'].count > 0:
                    # Since its a set we can keep adding the edge even if it already exists
                    other_transaction = next(iter(self.lock_table[record_id]['S'] | self.lock_table[record_id]['X']))
                    if self.wait_for_graph.add_edge(transaction_id, other_transaction):
                        self.condition.wait()
                    else:
                        self.release_all_locks(transaction_id)
                        raise Exception(f"Deadlock detected grabbing exclusive lock - Transaction {transaction_id} is waiting for {other_transaction}")
                self.lock_table[record_id]['X'].add(transaction_id)

            elif lock_type == 'S':
                # print("Acquiring exclusive lock", transaction_id, record_id)
                # Wait until no other transaction holds an exclusive lock on the record
                while self.lock_table[record_id]['X'] or self.lock_table[record_id]['IX'].count > 0:
                    other_transaction = next(iter(self.lock_table[record_id]['X']))
                    if self.wait_for_graph.add_edge(transaction_id, other_transaction):
                        self.condition.wait()
                    else:
                        self.release_all_locks(transaction_id)
                        raise Exception(f"Deadlock detected grabbing shared lock - Transaction {transaction_id} is waiting for {other_transaction}")
                self.lock_table[record_id]['S'].add(transaction_id)

            elif lock_type == 'IS':
                # Wait until no other transaction holds an exclusive lock on the record
                while self.lock_table[record_id]['X']:
                    self.wait_for_graph.add_edge(transaction_id, record_id)
                    self.condition.wait()
                self.lock_table[record_id]['IS'].count_up()

            elif lock_type == 'IX':
                # Wait until no other transaction holds an exclusive lock on the record
                while self.lock_table[record_id]['X']:
                    self.wait_for_graph.add_edge(transaction_id, record_id)
                    self.condition.wait()
                self.lock_table[record_id]['IX'].count_up()

            else:
                raise ValueError("Invalid lock type")

            self.condition.notify_all()

    def release_lock(self, transaction_id, record_id, lock_type):
        """
        Releases the lock held by the specified transaction on the given record.
        """
        with self.condition:
            if lock_type in ['IS', 'IX']:
                self.lock_table[record_id][lock_type].count_down()
            else:
                if transaction_id in self.lock_table[record_id][lock_type]:
                    self.lock_table[record_id][lock_type].remove(transaction_id)
                    if not any(self.lock_table[record_id].values()):
                        del self.lock_table[record_id]
            self.condition.notify_all()

            # Since we are following strict 2PL we ensure this transaction will not grab locks in the future
            # so we can remove all edges associated with this trasaction id
            self.wait_for_graph.remove_transaction(transaction_id)
            self.transaction_states[transaction_id] = 'shrinking' 

    def release_all_locks(self, transaction_id):
        """
        Releases all locks held by the specified transaction.
        """
        with self.condition:
            record_ids = list(self.lock_table.keys())
            for record_id in record_ids:
                lock_types = list(self.lock_table[record_id].keys())
                for lock_type in lock_types:
                    if lock_type in ['IS', 'IX']:
                        self.lock_table[record_id][lock_type].count_down()
                    elif transaction_id in self.lock_table[record_id][lock_type]:
                        self.lock_table[record_id][lock_type].remove(transaction_id)
                        if not any(self.lock_table[record_id].values()):
                            del self.lock_table[record_id]
            self.condition.notify_all()

            self.wait_for_graph.remove_transaction(transaction_id)
            del self.transaction_states[transaction_id]

    def upgrade_lock(self, transaction_id, record_id, current_lock_type, new_lock_type):
        """
        Upgrades a IS or IX lock to its respective S or X lock or S lock to an X lock.
        """
        with self.condition:
            if current_lock_type == 'IX' and new_lock_type == 'X':
                # Wait until no other transaction holds a conflicting lock
                while self.lock_table[record_id]['S'] or self.lock_table[record_id]['X'] or self.lock_table[record_id]['IS'].count > 0:
                    self.condition.wait()

                if not (transaction_id in self.lock_table[record_id]['IX']):
                    raise ValueError("Transaction does not hold an IX lock")
                    
                self.lock_table[record_id]['IX'].count_down()
                self.lock_table[record_id]['X'].add(transaction_id)

            elif current_lock_type == 'IS' and new_lock_type == 'S':
                # Wait until no other transaction holds an exclusive lock
                while self.lock_table[record_id]['X']:
                    self.condition.wait()

                if not (transaction_id in self.lock_table[record_id]['IS']):
                    raise ValueError("Transaction does not hold an IS lock")
                
                self.lock_table[record_id]['IS'].count_down()
                self.lock_table[record_id]['S'].add(transaction_id)

            elif current_lock_type == 'IS' and new_lock_type == 'IX':
                # Wait until no other transaction holds a conflicting lock
                while self.lock_table[record_id]['X'] or self.lock_table[record_id]['IS'].count > 1:
                    other_transaction = next(iter(self.lock_table[record_id]['X']))
                    if self.wait_for_graph.add_edge(transaction_id, other_transaction):
                        self.condition.wait()
                    else:
                        self.release_all_locks(transaction_id)
                        raise Exception(f"Deadlock detected grabbing intention to write lock - Transaction {transaction_id} is waiting for {other_transaction}")
                
                self.lock_table[record_id]['IS'].count_down()
                self.lock_table[record_id]['IX'].count_up()
            
            elif current_lock_type == 'S' and new_lock_type == 'X':
                # Wait until no other transaction holds a conflicting lock
                while self.lock_table[record_id]['X'] or self.lock_table[record_id]['IS'].count > 0:
                    other_transaction = next(iter(self.lock_table[record_id]['X']))
                    if self.wait_for_graph.add_edge(transaction_id, other_transaction):
                        self.condition.wait()
                    else:
                        self.release_all_locks(transaction_id)
                        raise Exception(f"Deadlock detected grabbing exclusive lock - Transaction {transaction_id} is waiting for {other_transaction}")

                if not (transaction_id in self.lock_table[record_id]['S']):
                    raise ValueError("Transaction does not hold an S lock")
                
                self.lock_table[record_id]['S'].remove(transaction_id)
                self.lock_table[record_id]['X'].add(transaction_id)
            else:
                raise ValueError("Invalid lock upgrade", current_lock_type, new_lock_type)
            self.condition.notify_all()
