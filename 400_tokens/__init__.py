import json
import os.path
PLUGIN_DIR = os.path.dirname(os.path.abspath(__file__)) + "/"

with open(PLUGIN_DIR + "config.json") as f:
    config = json.load(f)


import importlib.util
spec = importlib.util.spec_from_file_location("tokens", config["tokens_module_path"])
mod = importlib.util.module_from_spec(spec)
spec.loader.exec_module(mod)
tokens = mod.tokens

MANAGER = None


def action_init(params):
    global MANAGER
    try:
        MANAGER = params['manager']
        tokens.load_from_ledger()
    except:
        pass
        

    
def action_fullblock(full_block):
    for tx in full_block['transactions']:
        if "token:" in tx[10]:
            tokens.new_tx((tx[10], tx[11], int(tx[0]), float(tx[1]), tx[2], tx[3], tx[5])) #operation, openfield, block_height, timestamp, address, recipient, signature
        
def action_rollback(info):
    tokens.remove_txs_since(int(info["height"]))
    
