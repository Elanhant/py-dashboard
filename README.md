py-dashboard
============

Module for calculating dashboard data.

This module does not change any of your existing collections and tables, it only collects necessary data and saves results of processing in its own collections.

Dependencies
------------
  * Python 2.7.x
  * pymongo
  * psycopg2

Use also need enough free space on your hard drive. This module creates three persistent Mongo collections and several temporary collections in order to calculate necessary data. Persistent collections (`DASHBOARD_CHEQUES_COLLECTION`, `DASHBOARD_COLLECTION`, `UPDATER_COLLECTION` in `config.py` file) will take about 1/6 of the size of `CHEQUES_COLLECTION` collection.

Usage
-----

  1. Create your own config file – `config.py`, – using `config.py.sample`<br>
    **This file must be in the same directory as `main.py`!**
  2. Make sure you have the permisson to execute `main.py`
  3. Run the program by typing `python main.py` or `./main.py`
  4. Execution may take quite a long time. If an exception is raised, an email message that contains traceback and log will be sent to all mailboxes defined in `MAILER_TO` option in `config.py` file
