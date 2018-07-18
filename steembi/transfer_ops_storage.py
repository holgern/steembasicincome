# This Python file uses the following encoding: utf-8
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals
from builtins import bytes
from builtins import object
from beemgraphenebase.py23 import py23_bytes, bytes_types
import shutil
import time
import os
import sqlite3
from appdirs import user_data_dir
from datetime import datetime, timedelta
from beem.utils import formatTimeString, addTzInfo
import logging
from binascii import hexlify
import random
import hashlib
import dataset
log = logging.getLogger(__name__)
log.setLevel(logging.DEBUG)
log.addHandler(logging.StreamHandler())

timeformat = "%Y%m%d-%H%M%S"


class DataDir(object):
    """ This class ensures that the user's data is stored in its OS
        preotected user directory:

        **OSX:**

         * `~/Library/Application Support/<AppName>`

        **Windows:**

         * `C:\\Documents and Settings\\<User>\\Application Data\\Local Settings\\<AppAuthor>\\<AppName>`
         * `C:\\Documents and Settings\\<User>\\Application Data\\<AppAuthor>\\<AppName>`

        **Linux:**

         * `~/.local/share/<AppName>`

         Furthermore, it offers an interface to generated backups
         in the `backups/` directory every now and then.
    """

    def __init__(self, data_dir, storageDatabase):
        #: Storage
        self.data_dir = data_dir
        self.storageDatabase = storageDatabase
        self.sqlDataBaseFile = os.path.join(data_dir, storageDatabase)
        self.url = "sqlite:///"
        self.mkdir_p()

    def mkdir_p(self):
        """ Ensure that the directory in which the data is stored
            exists
        """
        if os.path.isdir(self.data_dir):
            return
        else:
            try:
                os.makedirs(self.data_dir)
            except FileExistsError:
                self.sqlDataBaseFile = ":memory:"
                return
            except OSError:
                self.sqlDataBaseFile = ":memory:"
                return

    def sqlite3_backup(self, backupdir):
        """ Create timestamped database copy
        """
        if self.sqlDataBaseFile == ":memory:":
            return
        if not os.path.isdir(backupdir):
            os.mkdir(backupdir)
        backup_file = os.path.join(
            backupdir,
            os.path.basename(self.storageDatabase) +
            datetime.utcnow().strftime("-" + timeformat))
        self.sqlite3_copy(self.sqlDataBaseFile, backup_file)
        configStorage["lastBackup"] = datetime.utcnow().strftime(timeformat)

    def sqlite3_copy(self, src, dst):
        """Copy sql file from src to dst"""
        if self.sqlDataBaseFile == ":memory:":
            return
        if not os.path.isfile(src):
            return
        connection = sqlite3.connect(self.sqlDataBaseFile)
        cursor = connection.cursor()
        # Lock database before making a backup
        cursor.execute('begin immediate')
        # Make new backup file
        shutil.copyfile(src, dst)
        log.info("Creating {}...".format(dst))
        # Unlock database
        connection.rollback()

    def recover_with_latest_backup(self, backupdir="backups"):
        """ Replace database with latest backup"""
        file_date = 0
        if self.sqlDataBaseFile == ":memory:":
            return
        if not os.path.isdir(backupdir):
            backupdir = os.path.join(self.data_dir, backupdir)
        if not os.path.isdir(backupdir):
            return
        newest_backup_file = None
        for filename in os.listdir(backupdir):
            backup_file = os.path.join(backupdir, filename)
            if os.stat(backup_file).st_ctime > file_date:
                if os.path.isfile(backup_file):
                    file_date = os.stat(backup_file).st_ctime
                    newest_backup_file = backup_file
        if newest_backup_file is not None:
            self.sqlite3_copy(newest_backup_file, self.sqlDataBaseFile)

    def clean_data(self):
        """ Delete files older than 70 days
        """
        if self.sqlDataBaseFile == ":memory:":
            return
        log.info("Cleaning up old backups")
        for filename in os.listdir(self.data_dir):
            backup_file = os.path.join(self.data_dir, filename)
            if os.stat(backup_file).st_ctime < (time.time() - 70 * 86400):
                if os.path.isfile(backup_file):
                    os.remove(backup_file)
                    log.info("Deleting {}...".format(backup_file))

    def refreshBackup(self):
        """ Make a new backup
        """
        backupdir = os.path.join(self.data_dir, "backups")
        self.sqlite3_backup(backupdir)
        self.clean_data()


class TransferTrx(DataDir):
    """ This is the trx storage class
    """
    __tablename__ = 'transfers'

    def __init__(self, data_dir, storageDatabase):
        super(TransferTrx, self).__init__(data_dir, storageDatabase)

    def exists_table(self):
        """ Check if the database table exists
        """

        connection = dataset.connect(self.url + self.sqlDataBaseFile)
        if len(connection.tables) == 0:
            return False
        if connection.tables in self.__tablename__:
            return True
        else:
            return False
 

    def create_table(self):
        """ Create the new table in the SQLite database
        """
        query = ("CREATE TABLE {0} ("
                 "id INTEGER PRIMARY KEY AUTOINCREMENT,"
                 "block int NOT NULL,"
                 "op_acc_index int NOT NULL,"
                 "op_acc_name varchar(50) NOT NULL,"
                 "trx_in_block smallint NOT NULL,"
                 "op_in_trx smallint NOT NULL,"
                 "timestamp datetime DEFAULT NULL,"
                 "from_account varchar(50) DEFAULT NULL,"
                 "to_account varchar(50) DEFAULT NULL,"
                 "amount decimal(15,6) DEFAULT NULL,"
                 "amount_symbol varchar(5) DEFAULT NULL,"
                 "memo varchar(2048) DEFAULT NULL,"
                 "op_type varchar(50) NOT NULL)".format(self.__tablename__))
        connection = dataset.connect(self.url + self.sqlDataBaseFile)
        connection.query(query)
        # connection = sqlite3.connect(self.sqlDataBaseFile)
        # cursor = connection.cursor()
        # cursor.execute(query)
        connection.commit()

    def get_all_data(self):
        """ Returns the public keys stored in the database
        """
        query = ("SELECT block, op_acc_index, op_acc_name, trx_in_block, op_in_trx, timestamp, from_account, to_account,"
                 "amount, amount_symbol, memo, op_type from {0} ".format(self.__tablename__))
        connection = sqlite3.connect(self.sqlDataBaseFile)
        cursor = connection.cursor()
        try:
            cursor.execute(query)
            results = cursor.fetchall()
            return [{"block": ret[0], "op_acc_index": ret[1], "op_acc_name": ret[2], "trx_in_block": ret[3], "op_in_trx": ret[4],  "timestamp": ret[5], "from": ret[6], "to": ret[7],
                    "amount": ret[8], "amount_symbol": ret[9], "memo": ret[10], "op_type": ret[11]} for ret in results]
        except sqlite3.OperationalError:
            return []

    def get_all_index(self, ):
        """ Returns all ids
        """
        query = ("SELECT op_acc_index, op_acc_name from {0}".format(self.__tablename__))
        connection = sqlite3.connect(self.sqlDataBaseFile)
        cursor = connection.cursor()
        try:
            cursor.execute(query)
            results = cursor.fetchall()
            return [x for x in results]
        except sqlite3.OperationalError:
            return []

    def get_all_op_index(self, op_acc_name):
        """ Returns all ids
        """
        query = ("SELECT op_acc_index from {0} where op_acc_name=?".format(self.__tablename__), (op_acc_name,))
        connection = sqlite3.connect(self.sqlDataBaseFile)
        cursor = connection.cursor()
        try:
            cursor.execute(*query)
            results = cursor.fetchall()
            return [x[0] for x in results]
        except sqlite3.OperationalError:
            return []

    def get_latest_index(self, op_acc_name):
        """ Returns all ids
        """
        connection = dataset.connect(self.url + self.sqlDataBaseFile)
        table = connection[self.__tablename__]
        query = ("SELECT op_acc_index from {0} ORDER BY op_acc_index DESC where op_acc_name=?".format(self.__tablename__), (op_acc_name,))
        connection = sqlite3.connect(self.sqlDataBaseFile)
        cursor = connection.cursor()
        try:
            cursor.execute(*query)
            result = cursor.fetchone()
            return result
        except sqlite3.OperationalError:
            return []

    def get_account_from_op(self, op_acc_name, _from):
        """ Returns all entries for given value

        """
        query = ("SELECT block, op_acc_index, op_acc_name, trx_in_block, op_in_trx, timestamp, from_account, to_account,"
                 "amount, amount_symbol, memo, op_type from {0} WHERE op_acc_name=? and from=?".format(self.__tablename__), (op_acc_name, _from,))
        connection = sqlite3.connect(self.sqlDataBaseFile)
        cursor = connection.cursor()
        cursor.execute(*query)
        results = cursor.fetchall()
        if results:
            return [{"block": ret[0], "op_acc_index": ret[1], "op_acc_name": ret[2], "trx_in_block": ret[3], "op_in_trx": ret[4],  "timestamp": ret[5], "from": ret[6], "to": ret[7],
                    "amount": ret[8], "amount_symbol": ret[9], "memo": ret[10], "op_type": ret[11]} for ret in results]
        else:
            return None

    def get_account_to_op(self, op_acc_name, to):
        """ Returns all entries for given value

        """
        query = ("SELECT block, op_acc_index, op_acc_name, trx_in_block, op_in_trx, timestamp, from_account, to_account,"
                 "amount, amount_symbol, memo, op_type from {0} WHERE op_acc_name=? and to=?".format(self.__tablename__), (op_acc_name, to,))
        connection = sqlite3.connect(self.sqlDataBaseFile)
        cursor = connection.cursor()
        cursor.execute(*query)
        results = cursor.fetchall()
        if results:
            return [{"block": ret[0], "op_acc_index": ret[1], "op_acc_name": ret[2], "trx_in_block": ret[3], "op_in_trx": ret[4],  "timestamp": ret[5], "from": ret[6], "to": ret[7],
                    "amount": ret[8], "amount_symbol": ret[9], "memo": ret[10], "op_type": ret[11]} for ret in results]
        else:
            return None

    def get(self, value, where="id"):
        """ Returns all entries for given value

        """
        query = ("SELECT block, op_acc_index, op_acc_name, trx_in_block, op_in_trx, timestamp, from_account, to_account,"
                 "amount, amount_symbol, memo, op_type from {0} WHERE {1}=?".format(self.__tablename__, where), (value,))
        connection = sqlite3.connect(self.sqlDataBaseFile)
        cursor = connection.cursor()
        cursor.execute(*query)
        results = cursor.fetchall()
        if results:
            return [{"block": ret[0], "op_acc_index": ret[1], "op_acc_name": ret[2], "trx_in_block": ret[3], "op_in_trx": ret[4],  "timestamp": ret[5], "from": ret[6], "to": ret[7],
                    "amount": ret[8], "amount_symbol": ret[9], "memo": ret[10], "op_type": ret[11]} for ret in results]
        else:
            return None

    def add(self, block, op_acc_index, op_acc_name, trx_in_block, op_in_trx, timestamp, _from, to, amount, amount_symbol, memo, op_type):
        """ Add a new data set

        """
        query = ("INSERT INTO {0} (block, op_acc_index, op_acc_name, trx_in_block, op_in_trx, timestamp, from_account, to_account,"
                 "amount, amount_symbol, memo, op_type) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
                 "".format(self.__tablename__), (block, op_acc_index, op_acc_name, trx_in_block, op_in_trx, timestamp,
                                                 _from, to, amount, amount_symbol, memo, op_type))
        connection = sqlite3.connect(self.sqlDataBaseFile)
        cursor = connection.cursor()
        cursor.execute(*query)
        connection.commit()

    def add_batch(self, data):
        """ Add a new data set

        """
        connection = dataset.connect(self.url + self.sqlDataBaseFile)
        table = connection[self.__tablename__]
        connection.begin()
        for d in data:
            table.insert(d)
            
        connection.commit()

    def delete(self, ID):
        """ Delete a data set

           :param int ID: database id
        """
        query = ("DELETE FROM {0} WHERE id=?".format(self.__tablename__), (ID,))
        connection = sqlite3.connect(self.sqlDataBaseFile)
        cursor = connection.cursor()
        cursor.execute(*query)
        connection.commit()

    def wipe(self, sure=False):
        """Purge the entire database. No data set will survive this!"""
        if not sure:
            log.error(
                "You need to confirm that you are sure "
                "and understand the implications of "
                "wiping your wallet!"
            )
            return
        else:
            query = ("DELETE FROM {0} ".format(self.__tablename__))
            connection = sqlite3.connect(self.sqlDataBaseFile)
            cursor = connection.cursor()
            cursor.execute(query)
            connection.commit()


