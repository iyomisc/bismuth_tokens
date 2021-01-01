import sqlite3
import os.path

WORKING_DIR = os.path.dirname(os.path.abspath(__file__)) + "/"

PROTOCOL_CHANGE_HEIGHT = 637385


class Tokens:
    def __init__(self):
        self.ledger = None
        self.db = sqlite3.connect(WORKING_DIR + "data/tokens.db", check_same_thread=False)
        
        cursor = self.db.cursor()
        cursor.execute("CREATE TABLE IF NOT EXISTS transactions(token TEXT, block_height INTEGER, timestamp NUMERIC, address TEXT, recipient TEXT, amount INTEGER, signature TEXT PRIMARY KEY)") # address will be "" if the tx is a creation tx
        cursor.execute("CREATE TABLE IF NOT EXISTS balances(token TEXT, address TEXT, balance INTEGER, PRIMARY KEY (token, address))")
        
        self.db.commit()
    
    def insert_tx(self, token, block_height, timestamp, address, recipient, amount, signature, update_balance=True):
        cursor = self.db.cursor()
        cursor.execute("INSERT INTO transactions(token, block_height, timestamp, address, recipient, amount, signature) VALUES(?, ?, ?, ?, ?, ?, ?)", (token, block_height, timestamp, address, recipient, amount, signature))
        self.db.commit()
        if update_balance:
            cursor = self.db.cursor()
            if address:
                cursor.execute("REPLACE INTO balances(token, address, balance) VALUES(?, ?, COALESCE((SELECT balance FROM balances WHERE address=? AND token=?), 0) - ?)", (token, address, address, token, amount))
            cursor.execute("REPLACE INTO balances(token, address, balance) VALUES(?, ?, COALESCE((SELECT balance FROM balances WHERE address=? AND token=?), 0) + ?)", (token, recipient, recipient, token, amount))
            self.db.commit()
            
    def remove_tx(self, signature, update_balance=True):
        if update_balance:
            cursor = self.db.cursor()
            cursor.execute("SELECT token, address, recipient, amount FROM transactions WHERE signature=?", (signature, ))
            result = cursor.fetchall()
            if len(result) and len(result[0]):
                token, address, recipient, amount = result[0]
                cursor = self.db.cursor()
                
                if address:
                    cursor.execute("REPLACE INTO balances(token, address, balance) VALUES(?, ?, COALESCE((SELECT balance FROM balances WHERE address=? AND token=?), 0) + ?)", (token, address, address, token, amount))
                cursor.execute("REPLACE INTO balances(token, address, balance) VALUES(?, ?, COALESCE((SELECT balance FROM balances WHERE address=? AND token=?), 0) - ?)", (token, recipient, recipient, token, amount))
                self.db.commit()
                
        cursor = self.db.cursor()
        cursor.execute("DELETE FROM transactions WHERE signature=?", (signature, ))
        self.db.commit()
    
    def remove_txs_since(self, height):
        cursor = self.db.cursor()
        cursor.execute("SELECT signature FROM transactions WHERE block_height >= ?", (height, ))
        for signature in cursor.fetchall():
            self.remove_tx(signature[0])
    
    def new_tx(self, transaction):
        if transaction[2] <= PROTOCOL_CHANGE_HEIGHT and "token:" in transaction[1]:
            transaction = list(transaction)
            if transaction[1].count(":") >= 3:
                transaction[0] = ":".join(transaction[1].split(":")[:2])
                transaction[1] = ":".join(transaction[1].split(":")[2:4])
            transaction = tuple(transaction)
                
        if ":" not in transaction[1]:
            # Invalid tx
            return
        
        token, amount = transaction[1].split(":")[:2]
        
        if not token or not amount:
            # Invalid tx
            return

        if not amount.isdigit():
            # amount is negative or is not int
            return

        token = token.lower()
        amount = int(amount)
        
        if transaction[0] == "token:issue":
            cursor = self.db.cursor()
            cursor.execute("SELECT count(*) FROM balances WHERE token = ?", (token, ))
            result = cursor.fetchall()
            if result[0][0]:
                # The token already exists
                return
            self.insert_tx(token, transaction[2], transaction[3], "", transaction[4], amount, transaction[6])
            
        elif transaction[0] == "token:transfer":
            
            if not self.can_send(transaction[4], token, amount):
                # Balance too low or not created token
                return
            self.insert_tx(token, transaction[2], transaction[3], transaction[4], transaction[5], amount, transaction[6])
    
    def load_from_ledger(self, ledger_path=""):
        if self.ledger is None:
            if not ledger_path:
                raise ValueError("No ledger path given")
            self.ledger = sqlite3.connect(ledger_path, check_same_thread=False)

        cursor = self.ledger.cursor()
        cursor.execute("SELECT operation, openfield, block_height, timestamp, address, recipient, signature FROM transactions WHERE block_height > ? AND (operation like 'token:%' OR (block_height <= ? AND openfield like 'token:%')) ORDER BY block_height ASC, timestamp ASC", (self.get_last_transaction_height(), PROTOCOL_CHANGE_HEIGHT))
        transactions = cursor.fetchall()
        
        for transaction in transactions:
            self.new_tx(transaction)

    def get_balance(self, address, token):
        cursor = self.db.cursor()
        cursor.execute("SELECT balance FROM balances WHERE address=? AND token=?", (address, token))
        result = cursor.fetchall()
        if len(result) and len(result[0]):
            return result[0][0]
        return 0
        
    def can_send(self, address, token, amount):
        return self.get_balance(address, token) >= amount
    
    def get_last_transaction_height(self):
        cursor = self.db.cursor()
        cursor.execute("SELECT block_height FROM transactions ORDER BY block_height DESC LIMIT 1")
        result = cursor.fetchall()
        if len(result) and len(result[0]):
            return result[0][0]
        return -1

    def get_last_transactions(self):
        cursor = self.db.cursor()
        cursor.execute("SELECT token, block_height, timestamp, address, recipient, amount FROM transactions ORDER BY block_height DESC, timestamp DESC LIMIT 100")
        return  cursor.fetchall()
        
    def get_address_transactions(self, address):
        cursor = self.db.cursor()
        cursor.execute("SELECT token, block_height, timestamp, address, recipient, amount FROM transactions WHERE address=? or recipient=? ORDER BY block_height DESC, timestamp DESC", (address, address))
        return cursor.fetchall()
        
    def get_token_transactions(self, token):
        cursor = self.db.cursor()
        cursor.execute("SELECT token, block_height, timestamp, address, recipient, amount FROM transactions WHERE token=? ORDER BY block_height DESC, timestamp DESC", (token, ))
        return cursor.fetchall()
        
    def get_address_balances(self, address):
        cursor = self.db.cursor()
        cursor.execute("SELECT token, balance FROM balances WHERE address=? ORDER BY token ASC", (address, ))
        return cursor.fetchall()
        
    def get_token_balances(self, token):
        cursor = self.db.cursor()
        cursor.execute("SELECT address, balance FROM balances WHERE token=? ORDER BY balance DESC", (token, ))
        return cursor.fetchall()
    
    def get_token_info(self, token):
        cursor = self.db.cursor()
        cursor.execute("SELECT recipient, amount, timestamp FROM transactions WHERE token=? AND address=''", (token, ))
        return cursor.fetchall()
    
    def get_all_token_info(self):
        cursor = self.db.cursor()
        cursor.execute("SELECT token, recipient, amount, timestamp FROM transactions WHERE address='' ORDER BY block_height DESC, timestamp DESC")
        return cursor.fetchall()


tokens = Tokens()

