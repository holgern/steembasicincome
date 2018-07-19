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
    appname = "steembasicincome"
    appauthor = "steembasicincome"
    storageDatabase = "sbi.sqlite"

    data_dir = user_data_dir(appname, appauthor)
    sqlDataBaseFile = os.path.join(data_dir, storageDatabase)
    databaseConnector = "sqlite:///" + sqlDataBaseFile
    db = dataset.connect(databaseConnector)

    def __init__(self):
        #: Storage
        if self.databaseConnector[:6] == "sqlite":
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


class Trx(DataDir):
    """ This is the trx storage class
    """
    __tablename__ = 'trx'

    def __init__(self):
        super(Trx, self).__init__()

    def exists_table(self):
        """ Check if the database table exists
        """
        if len(self.db.tables) == 0:
            return False
        if self.__tablename__ in self.db.tables:
            return True
        else:
            return False

    def create_table(self):
        """ Create the new table in the SQLite database
        """
        query = ("CREATE TABLE {0} ("
                 "id INTEGER PRIMARY KEY AUTOINCREMENT,"
                 "'index' int,"
                 "source varchar(50) DEFAULT NULL,"
                 "memo text,"
                 "account varchar(50) DEFAULT NULL,"
                 "sponsor varchar(50) DEFAULT NULL,"
                 "sponsee text,"
                 "shares int,"
                 "vests decimal(15,6) DEFAULT NULL,"
                 "timestamp datetime DEFAULT NULL,"
                 "share_age int,"
                 "status varchar(50) DEFAULT NULL,"
                 "share_type varchar(50) DEFAULT NULL)".format(self.__tablename__))
        self.db.query(query)
        self.db.commit()

    def get_all_data(self):
        """ Returns the public keys stored in the database
        """
        return self.db[self.__tablename__].all()

    def get_all_ids(self):
        """ Returns all ids
        """
        table = self.db[self.__tablename__]
        id_list = []
        for trx in table:
            id_list.append(trx["id"])
        return id_list

    def get_all_op_index(self, source):
        """ Returns all ids
        """
        table = self.db[self.__tablename__]
        id_list = []
        for trx in table.find(source=source):
            id_list.append(trx["id"])
        return id_list

    def get_account(self, account, share_type="standard"):
        """ Returns all entries for given value

        """
        table = self.db[self.__tablename__]
        id_list = []
        for trx in table.find(account=account, share_type=share_type):
            id_list.append(trx)
        return id_list        

    def get(self, ID):
        """ Returns all entries for given value

        """
        table = self.db[self.__tablename__]
        return table.find_one(id=ID)

    def get_SBD_transfer(self, account, shares, timestamp):
        """ Returns all entries for given value

        """
        table = self.db[self.__tablename__]
        found_trx = None
        for trx in table.find(account=account, shares=-shares, share_type="SBD"):
            if addTzInfo(trx["timestamp"]) < addTzInfo(timestamp):
                found_trx = trx
        return found_trx

    def update_share_age(self):
        """ Change share_age depending on timestamp

        """
        table = self.db[self.__tablename__]        
        id_list = self.get_all_ids()
        for ID in id_list:
            data = self.get(ID)
            if data["status"].lower() == "refunded":
                return
            age = (datetime.utcnow()) - (data["timestamp"])
            share_age = int(age.total_seconds() / 60 / 60 / 24)
            data = dict(id=ID, share_age=share_age)
            table.update(data, ['id'])

    def update_delegation_shares(self, source, account, shares):
        """ Change share_age depending on timestamp

        """
        table = self.db[self.__tablename__]
        found_trx = None
        for trx in table.find(source=source, account=account, status="Valid", share_type="Delegation"):
            found_trx = trx
        data = dict(id=found_trx["id"], shares=shares)
        table.update(data, ['id'])

    def update_delegation_state(self, source, account, share_type_old, share_type_new):
        """ Change share_age depending on timestamp

        """
        table = self.db[self.__tablename__]
        found_trx = None
        for trx in table.find(source=source, account=account, share_type=share_type_old):
            found_trx = trx
        data = dict(id=found_trx["id"], share_type=share_type_new)
        table.update(data, ['id'])

    def add(self, data):
        """ Add a new data set

        """
        table = self.db[self.__tablename__]
        table.insert(data)    
        self.db.commit()

    def delete(self, ID):
        """ Delete a data set

           :param int ID: database id
        """
        table = self.db[self.__tablename__]
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
            table = self.db[self.__tablename__]
            table.drop


class Member(DataDir):
    """ This is the trx storage class
    """
    __tablename__ = 'member'

    def __init__(self):
        super(Member, self).__init__()

    def exists_table(self):
        """ Check if the database table exists
        """
        if len(self.db.tables) == 0:
            return False
        if self.__tablename__ in self.db.tables:
            return True
        else:
            return False

    def create_table(self):
        """ Create the new table in the SQLite database
        """
        query = ("CREATE TABLE {0} ("
                 "account varchar(50) PRIMARY KEY,"
                 "note text DEFAULT NULL,"
                 "shares int,"
                 "total_share_days int,"
                 "avg_share_age float,"
                 "last_comment datetime DEFAULT NULL,"
                 "last_post datetime DEFAULT NULL,"
                 "original_enrollment datetime DEFAULT NULL,"
                 "latest_enrollment datetime DEFAULT NULL,"
                 "flags text DEFAULT NULL,"
                 "earned_rshares int DEFAULT NULL,"
                 "rewarded_rshares int DEFAULT NULL,"
                 "balance_rshares int DEFAULT NULL,"
                 "upvote_delay float DEFAULT NULL,"
                 "comment_upvote bool DEFAULT NULL)".format(self.__tablename__))
        self.db.query(query)
        self.db.commit()

    def get_all_data(self):
        """ Returns the public keys stored in the database
        """
        return self.db[self.__tablename__].all()
    
    def get_all_accounts(self):
        """ Returns all ids
        """
        table = self.db[self.__tablename__]
        id_list = []
        for trx in table:
            id_list.append(trx["account"])
        return id_list
    
    def add(self, data):
        """ Add a new data set
    
        """
        table = self.db[self.__tablename__]
        table.insert(data)
        self.db.commit()


    def add_batch(self, data):
        """ Add a new data set

        """
        table = self.db[self.__tablename__]
        self.db.begin()
        for d in data:
            table.insert(d)
     
        self.db.commit()
    
    def get(self, account):
        """ Change share_age depending on timestamp
    
        """
        table = self.db[self.__tablename__]
        return table.find_one(account=account)
        
    def update_shares(self, account, add_shares, datetime):
        """ Change share_age depending on timestamp
    
        """
        table = self.db[self.__tablename__]
        member = table.find_one(account=account)
        shares = member["shares"] + add_shares
        data = dict(account=account, shares=shares, latest_enrollment=datetime)
        table.update(data, ['account'])
    
    def delete(self, account):
        """ Delete a data set
    
           :param int ID: database id
        """
        table = self.db[self.__tablename__]
        table.delete(account=account)
    
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
            table = self.db[self.__tablename__]
            table.drop


class Configuration(DataDir):
    """ This is the configuration storage that stores key/value
        pairs in the `config` table of the SQLite3 database.
    """
    __tablename__ = "config"

    #: Default configuration

    def __init__(self):
        super(Configuration, self).__init__()

    def exists_table(self):
        """ Check if the database table exists
        """
        query = ("SELECT name FROM sqlite_master "
                 "WHERE type='table' AND name=?", (self.__tablename__,))
        try:
            connection = sqlite3.connect(self.sqlDataBaseFile)
            cursor = connection.cursor()
            cursor.execute(*query)
            return True if cursor.fetchone() else False
        except sqlite3.OperationalError:
            self.sqlDataBaseFile = ":memory:"
            log.warning("Could not read(database: %s)" % (self.sqlDataBaseFile))
            return True

    def create_table(self):
        """ Create the new table in the SQLite database
        """
        query = ("CREATE TABLE {0} ("
                 "id INTEGER PRIMARY KEY AUTOINCREMENT,"
                 "key STRING(256),"
                 "value STRING(256))".format(self.__tablename__))
        connection = sqlite3.connect(self.sqlDataBaseFile)
        cursor = connection.cursor()
        try:
            cursor.execute(query)
            connection.commit()
        except sqlite3.OperationalError:
            log.error("Could not write to database: %s" % (self.__tablename__))
            raise NoWriteAccess("Could not write to database: %s" % (self.__tablename__))

    def checkBackup(self):
        """ Backup the SQL database every 7 days
        """
        if ("lastBackup" not in configStorage or
                configStorage["lastBackup"] == ""):
            print("No backup has been created yet!")
            self.refreshBackup()
        try:
            if (
                datetime.utcnow() -
                datetime.strptime(configStorage["lastBackup"],
                                  timeformat)
            ).days > 7:
                print("Backups older than 7 days!")
                self.refreshBackup()
        except:
            self.refreshBackup()

    def _haveKey(self, key):
        """ Is the key `key` available int he configuration?
        """
        query = ("SELECT value FROM {0} WHERE key=?".format(self.__tablename__), (key,))
        connection = sqlite3.connect(self.sqlDataBaseFile)
        cursor = connection.cursor()
        try:
            cursor.execute(*query)
            return True if cursor.fetchone() else False
        except sqlite3.OperationalError:
            log.warning("Could not read %s (database: %s)" % (str(key), self.__tablename__))
            return False

    def __getitem__(self, key):
        """ This method behaves differently from regular `dict` in that
            it returns `None` if a key is not found!
        """
        query = ("SELECT value FROM {0} WHERE key=?".format(self.__tablename__), (key,))
        connection = sqlite3.connect(self.sqlDataBaseFile)
        cursor = connection.cursor()
        try:
            cursor.execute(*query)
            result = cursor.fetchone()
            if result:
                return result[0]
            else:
                if key in self.config_defaults:
                    return self.config_defaults[key]
                else:
                    return None
        except sqlite3.OperationalError:
            log.warning("Could not read %s (database: %s)" % (str(key), self.__tablename__))
            if key in self.config_defaults:
                return self.config_defaults[key]
            else:
                return None

    def get(self, key, default=None):
        """ Return the key if exists or a default value
        """
        if key in self:
            return self.__getitem__(key)
        else:
            return default

    def __contains__(self, key):
        if self._haveKey(key) or key in self.config_defaults:
            return True
        else:
            return False

    def __setitem__(self, key, value):
        if self._haveKey(key):
            query = ("UPDATE {0} SET value=? WHERE key=?".format(self.__tablename__), (value, key))
        else:
            query = ("INSERT INTO {0} (key, value) VALUES (?, ?)".format(self.__tablename__), (key, value))
        connection = sqlite3.connect(self.sqlDataBaseFile)
        cursor = connection.cursor()
        try:
            cursor.execute(*query)
            connection.commit()
        except sqlite3.OperationalError:
            log.error("Could not write to %s (database: %s)" % (str(key), self.__tablename__))
            raise NoWriteAccess("Could not write to %s (database: %s)" % (str(key), self.__tablename__))

    def delete(self, key):
        """ Delete a key from the configuration store
        """
        query = ("DELETE FROM {0} WHERE key=?".format(self.__tablename__), (key,))
        connection = sqlite3.connect(self.sqlDataBaseFile)
        cursor = connection.cursor()
        try:
            cursor.execute(*query)
            connection.commit()
        except sqlite3.OperationalError:
            log.error("Could not write to %s (database: %s)" % (str(key), self.__tablename__))
            raise NoWriteAccess("Could not write to %s (database: %s)" % (str(key), self.__tablename__))

    def __iter__(self):
        return iter(list(self.items()))

    def items(self):
        query = ("SELECT key, value from {0} ".format(self.__tablename__))
        connection = sqlite3.connect(self.sqlDataBaseFile)
        cursor = connection.cursor()
        cursor.execute(query)
        r = {}
        for key, value in cursor.fetchall():
            r[key] = value
        return r

    def __len__(self):
        query = ("SELECT id from {0} ".format(self.__tablename__))
        connection = sqlite3.connect(self.sqlDataBaseFile)
        cursor = connection.cursor()
        cursor.execute(query)
        return len(cursor.fetchall())




# Create keyStorage
trxStorage = Trx()
memberStorage = Member()
configStorage = Configuration()

# Create Tables if database is brand new
if not configStorage.exists_table():
    configStorage.create_table()

newTrxStorage = False
if not trxStorage.exists_table():
    newTrxStorage = True
    trxStorage.create_table()

newMemberStorage = False
if not memberStorage.exists_table():
    newMemberStorage = True
    memberStorage.create_table()
