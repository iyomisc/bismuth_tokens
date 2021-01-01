# bismuth tokens

A tool to keep track of bismuth tokens transactions and balances.

- 400_tokens is a node plugin that updates a database when a new token transaction is made.
- tokens.py is a module that handles the database.

To use it, put `400_tokens` in the plugins folder of a node and restart it.
A database is now created and filled up. The data can be accessed directly, or using the `tokens.py` module.

https://bismuth.today uses this tool for its token explorer.
