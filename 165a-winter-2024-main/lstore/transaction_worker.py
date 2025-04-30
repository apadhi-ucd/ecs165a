from lstore.table import Table, Record
from lstore.index import Index
from lstore.lock import LockManager
import threading
import logging

class TransactionWorker:
    
    """
    # Creates a transaction worker object.
    """
    def __init__(self, transactions = None):
        self.stats = []
        self.transactions = transactions if transactions is not None else []
        self.result = 0
        self.worker_thread = None
        self.lock = threading.Lock()
        self.transaction_errors = {} 


    """
    Appends t to transactions
    """
    def add_transaction(self, t):
        self.transactions.append(t)
    
    """
    Runs all transaction as a thread
    """
    def run(self):
        self.worker_thread = threading.Thread(target = self.__run)
        self.worker_thread.start()
    
    """
    Waits for the worker to finish
    """
    def join(self):
        if self.worker_thread is not None:
            self.worker_thread.join()

        return self.result
    
    def __run(self):

        pending_transactions = list(self.transactions) if len(self.transactions) > 0 else []
        
        for current_transaction in pending_transactions:
            transaction_result = None
            
            transaction_result = current_transaction.run()
            self.stats.append(transaction_result)

        self.result = len(list(filter(lambda x: x, self.stats)))