import sys
import os.path

sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from tokens import tokens

MANAGER = None


def action_init(params):
    global MANAGER
    try:
        MANAGER = params['manager']
        tokens.load_from_ledger(ledger_path=MANAGER.config.ledger_path)
    except Exception as e:
        print(e)
        

def action_fullblock(full_block):
    for tx in full_block['transactions']:
        if tx[10].startswith("token:"):
            tokens.new_bismuth_tx((tx[10], tx[11], int(tx[0]), float(tx[1]), tx[2], tx[3], tx[5])) #operation, openfield, block_height, timestamp, address, recipient, signature


def action_rollback(info):
    tokens.remove_token_txs_since(int(info["height"]))
