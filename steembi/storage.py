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

    def __init__(self):
        #: Storage
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
        query = ("SELECT name FROM sqlite_master "
                 "WHERE type='table' AND name=?", (self.__tablename__, ))
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
                 "op_index int,"
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
        connection = sqlite3.connect(self.sqlDataBaseFile)
        cursor = connection.cursor()
        cursor.execute(query)
        connection.commit()

    def get_all_data(self):
        """ Returns the public keys stored in the database
        """
        query = ("SELECT id, op_index, source, account, sponsor, sponsee, shares, vests,"
                 "timestamp, share_age, status, share_type from {0} ".format(self.__tablename__))
        connection = sqlite3.connect(self.sqlDataBaseFile)
        cursor = connection.cursor()
        try:
            cursor.execute(query)
            results = cursor.fetchall()
            return [{"ID": ret[0], "op_index": ret[1], "source": ret[2],  "account": ret[3], "sponsor": ret[4], "sponsee": ret[5],
                    "shares": ret[6], "vests": ret[7], "timestamp": ret[8], "share_age": ret[9], "status": ret[10], "share_type": ret[11]} for ret in results]
        except sqlite3.OperationalError:
            return []

    def get_all_ids(self):
        """ Returns all ids
        """
        query = ("SELECT ID from {0} ".format(self.__tablename__))
        connection = sqlite3.connect(self.sqlDataBaseFile)
        cursor = connection.cursor()
        try:
            cursor.execute(query)
            results = cursor.fetchall()
            return [x[0] for x in results]
        except sqlite3.OperationalError:
            return []

    def get_account(self, account, share_type="standard"):
        """ Returns all entries for given value

        """
        query = ("SELECT id, op_index, source, account, sponsor, sponsee, shares, vests,"
                 "timestamp, share_age, status, share_type from {0} WHERE account=? and share_type=?".format(self.__tablename__), (account, share_type,))
        connection = sqlite3.connect(self.sqlDataBaseFile)
        cursor = connection.cursor()
        cursor.execute(*query)
        results = cursor.fetchall()
        if results:
            return [{"ID": ret[0], "op_index": ret[1], "source": ret[2],  "account": ret[3], "sponsor": ret[4], "sponsee": ret[5],
                    "shares": ret[6], "vests": ret[7], "timestamp": ret[8], "share_age": ret[9], "status": ret[10], "share_type": ret[11]} for ret in results]
        else:
            return None

    def get(self, value, where="id"):
        """ Returns all entries for given value

        """
        query = ("SELECT id, op_index, source, account, sponsor, sponsee, shares, vests,"
                 "timestamp, share_age, status, share_type from {0} WHERE {1}=?".format(self.__tablename__, where), (value,))
        connection = sqlite3.connect(self.sqlDataBaseFile)
        cursor = connection.cursor()
        cursor.execute(*query)
        results = cursor.fetchall()
        if results:
            return [{"ID": ret[0], "op_index": ret[1], "source": ret[2],  "account": ret[3], "sponsor": ret[4], "sponsee": ret[5],
                    "shares": ret[6], "vests": ret[7], "timestamp": ret[8], "share_age": ret[9], "status": ret[10], "share_type": ret[11]} for ret in results]
        else:
            return None

    def update_share_age(self):
        """ Change share_age depending on timestamp

        """
        id_list = self.get_all_ids()
        if ID in id_list:
            data = self.get(ID)
            if data["status"].lower() == "refunded":
                return
            age = addTzInfo(datetime.utcnow()) - formatTimeString(data["timestamp"])
            share_age = int(age.total_seconds() / 60 / 60 / 24)
            query = ("UPDATE {0} SET share_age=? WHERE id=?".format(self.__tablename__), (share_age, ID))
            connection = sqlite3.connect(self.sqlDataBaseFile)
            cursor = connection.cursor()
            cursor.execute(*query)
            connection.commit()

    def add(self, index, source, memo, account, sponsor, sponsee, shares, vests, timestamp, share_age, status, share_type):
        """ Add a new data set

        """
        query = ("INSERT INTO {0} (op_index, source, memo, account, sponsor, sponsee, shares, vests,"
                 "timestamp, share_age, status, share_type) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
                 "".format(self.__tablename__), (index, source, memo, account, sponsor, sponsee, shares, vests,
                                                 timestamp, share_age, status, share_type))
        connection = sqlite3.connect(self.sqlDataBaseFile)
        cursor = connection.cursor()
        cursor.execute(*query)
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
configStorage = Configuration()

# Create Tables if database is brand new
if not configStorage.exists_table():
    configStorage.create_table()

newTrxStorage = False
if not trxStorage.exists_table():
    newTrxStorage = True
    trxStorage.create_table()
