from datetime import datetime

class TransactionLog:
    def __init__(self, log_file="transaction_log.pkl"):
        self.log_file = log_file
        self.transactions = {}

    def log_operation(self, transaction_id, table_name, operation, record_id, pre_args=None, post_args=None):
        log_entry = {
            "timestamp": datetime.now(),
            "transaction_id": transaction_id,
            "table":table_name,
            "operation": operation,
            "record_id": record_id,
            "pre_args": pre_args,
            "post_args": post_args
        }

        if transaction_id not in self.transactions:
            self.transactions[transaction_id] = []
        
        self.transactions[transaction_id].append(log_entry)


    def get_transaction_log(self, transaction_id):
        
        return self.transactions.get(transaction_id, [])

    def remove_transaction(self, transaction_id):
        if transaction_id in self.transactions:
            del self.transactions[transaction_id]
 