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
from sqlalchemy import and_
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

    def __init__(self, data_dir, storageDatabase, databaseConnector=None):
        #: Storage
        self.data_dir = data_dir
        self.storageDatabase = storageDatabase
        self.sqlDataBaseFile = os.path.join(data_dir, storageDatabase)
        if databaseConnector is None:
            self.databaseConnector = "sqlite:///" + self.sqlDataBaseFile
        else:
            self.databaseConnector = databaseConnector
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


class AccountTrx(DataDir):
    """ This is the trx storage class
    """
    __tablename__ = 'sbi_ops'

    def __init__(self, data_dir, storageDatabase):
        super(AccountTrx, self).__init__(data_dir, storageDatabase)

    def exists_table(self):
        """ Check if the database table exists
        """

        db = dataset.connect(self.databaseConnector)
        if len(db.tables) == 0:
            return False
        if self.__tablename__ in db.tables:
            return True
        else:
            return False
 

    def create_table(self):
        """ Create the new table in the SQLite database
        """
        query = ("CREATE TABLE {0} ("
                 "id INTEGER PRIMARY KEY AUTOINCREMENT,"
                 "op_acc_index int NOT NULL,"
                 "op_acc_name varchar(50) NOT NULL,"
                 "block int NOT NULL,"
                 "trx_in_block smallint NOT NULL,"
                 "op_in_trx smallint NOT NULL,"
                 "timestamp datetime DEFAULT NULL,"                 
                 "op_dict text NOT NULL)".format(self.__tablename__))
        db = dataset.connect(self.databaseConnector)
        db.query(query)
        db.commit()

    def add(self, data):
        """ Add a new data set

        """
        db = dataset.connect(self.databaseConnector)
        table = db[self.__tablename__]
        table.insert(data)    
        db.commit()

    def add_batch(self, data):
        """ Add a new data set

        """
        db = dataset.connect(self.databaseConnector)
        table = db[self.__tablename__]
        db.begin()
        for d in data:
            table.insert(d)
            
        db.commit()

    def get_latest_index(self, account_name):
        table = self.db[self.__tablename__]
        return table.find_one(op_acc_name=account_name, order_by='-op_acc_index')

    def delete(self, ID):
        """ Delete a data set

           :param int ID: database id
        """
        db = dataset.connect(self.databaseConnector)
        table = db[self.__tablename__]
        table.delete(id=ID)

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
            db = dataset.connect(self.databaseConnector)
            table = db[self.__tablename__]
            table.drop



class TransferTrx(DataDir):
    """ This is the trx storage class
    """
    __tablename__ = 'transfers'

    def __init__(self, data_dir, storageDatabase):
        super(TransferTrx, self).__init__(data_dir, storageDatabase)

    def exists_table(self):
        """ Check if the database table exists
        """

        db = dataset.connect(self.databaseConnector)
        if len(db.tables) == 0:
            return False
        if self.__tablename__ in db.tables:
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
                 "'from' varchar(50) DEFAULT NULL,"
                 "'to' varchar(50) DEFAULT NULL,"
                 "amount decimal(15,6) DEFAULT NULL,"
                 "amount_symbol varchar(5) DEFAULT NULL,"
                 "memo varchar(2048) DEFAULT NULL,"
                 "op_type varchar(50) NOT NULL)".format(self.__tablename__))
        db = dataset.connect(self.databaseConnector)
        db.query(query)
        db.commit()

    def find(self, memo, to):
        db = dataset.connect(self.databaseConnector)
        table = db[self.__tablename__].table
        statement = table.select(and_(table.c.memo.like("%" + memo + "%"), table.c.to == to))
        result = db.query(statement)
        ret = []
        for r in result:
            ret.append(r)
        return ret

    def add(self, data):
        """ Add a new data set

        """
        db = dataset.connect(self.databaseConnector)
        table = db[self.__tablename__]
        table.insert(data)    
        db.commit()

    def add_batch(self, data):
        """ Add a new data set

        """
        db = dataset.connect(self.databaseConnector)
        table = db[self.__tablename__]
        db.begin()
        for d in data:
            table.insert(d)
            
        db.commit()

    def get_latest_index(self, account_name):
        table = self.db[self.__tablename__]
        return table.find_one(op_acc_name=account_name, order_by='-op_acc_index')

    def delete(self, ID):
        """ Delete a data set

           :param int ID: database id
        """
        db = dataset.connect(self.databaseConnector)
        table = db[self.__tablename__]
        table.delete(id=ID)

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
            db = dataset.connect(self.databaseConnector)
            table = db[self.__tablename__]
            table.drop


