from lstore.table import Table, Record
from lstore.index import Index
from lstore.lock import LockManager
import threading
import logging

class TransactionWorker:

    """
    # Creates a transaction worker object.
    """
    def __init__(self, transactions = []):
        self.stats = []
        self.transactions = transactions
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
        self.worker_thread = threading.Thread(target=self.__run)
        self.worker_thread.start()
    

    """
    Waits for the worker to finish
    """
    def join(self):
        if self.worker_thread is not None:
            self.worker_thread.join()

        return self.result


    def __run(self):
        transactions_list = list(self.transactions if len(self.transactions) > 0 else [None])

        for transaction in transactions_list:
            result = None
            result = transaction.run()
            self.stats.append(result)
        # stores the number of transactions that committed
        self.result = len(list(filter(lambda x: x, self.stats)))

