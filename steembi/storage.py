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


class TrxDB(object):
    """ This is the trx storage class
    """
    __tablename__ = 'trx'

    def __init__(self, db):
        self.db = db

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
        query = ("CREATE TABLE `sbi`.`trx` ( `index` INT, `source` VARCHAR(50) NOT NULL, `memo` text, `account` VARCHAR(50) DEFAULT NULL, `sponsor` VARCHAR(50) DEFAULT NULL ,  `sponsee` text, `shares` INT, `vests` decimal(15,6) DEFAULT NULL, `timestamp` DATETIME NOT NULL ,  `status` VARCHAR(50) NOT NULL, `share_type` varchar(50) NOT NULL, `id` INTEGER NOT NULL AUTO_INCREMENT, PRIMARY KEY (`id`)) ENGINE = InnoDB;")
        self.db.query(query)
        self.db.commit()

    def get_all_data(self):
        """ Returns the public keys stored in the database
        """
        return self.db[self.__tablename__].all()

    def get_all_op_index(self, source):
        """ Returns all ids
        """
        table = self.db[self.__tablename__]
        id_list = []
        for trx in table.find(source=source):
            id_list.append(trx["index"])
        return id_list

    def get_account(self, account, share_type="standard"):
        """ Returns all entries for given value

        """
        table = self.db[self.__tablename__]
        id_list = []
        for trx in table.find(account=account, share_type=share_type):
            id_list.append(trx)
        return id_list        

    def get(self, index, source):
        """ Returns all entries for given value

        """
        table = self.db[self.__tablename__]
        return table.find_one(index=index, source=source)

    def get_share_type(self, share_type):
        """ Returns all ids
        """
        table = self.db[self.__tablename__]
        return table.find(share_type=share_type)

    def get_lastest_share_type(self, share_type):
        table = self.db[self.__tablename__]
        return table.find_one(order_by='-index', share_type=share_type)    

    def get_SBD_transfer(self, account, shares, timestamp):
        """ Returns all entries for given value

        """
        table = self.db[self.__tablename__]
        found_trx = None
        for trx in table.find(account=account, shares=-shares, share_type="SBD"):
            if addTzInfo(trx["timestamp"]) < addTzInfo(timestamp):
                found_trx = trx
        return found_trx

    def update_delegation_shares(self, source, account, shares):
        """ Change share_age depending on timestamp

        """
        table = self.db[self.__tablename__]
        found_trx = None
        for trx in table.find(source=source, account=account, status="Valid", share_type="Delegation"):
            found_trx = trx
        data = dict(index=found_trx["index"], source=source, shares=shares)
        table.update(data, ['index', 'source'])

    def update_delegation_state(self, source, account, share_type_old, share_type_new):
        """ Change share_age depending on timestamp

        """
        table = self.db[self.__tablename__]
        found_trx = None
        for trx in table.find(source=source, account=account, share_type=share_type_old):
            found_trx = trx
        data = dict(index=found_trx["index"], source=source, share_type=share_type_new)
        table.update(data, ['index', 'source'])

    def add(self, data):
        """ Add a new data set

        """
        table = self.db[self.__tablename__]
        table.insert(data)    
        self.db.commit()

    def delete(self, index, source):
        """ Delete a data set

           :param int ID: database id
        """
        table = self.db[self.__tablename__]
        table.delete(index=index, source=source)

    def delete_all(self, source):
        """ Delete a data set

           :param int ID: database id
        """
        table = self.db[self.__tablename__]
        table.delete(source=source)

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


class MemberDB(object):
    """ This is the trx storage class
    """
    __tablename__ = 'member'

    def __init__(self, db):
        self.db = db

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
        query = ("CREATE TABLE `sbi`.`member` (`account` VARCHAR(50) NOT NULL, `note` text, `shares` INT NOT NULL, `bonus_shares` INT NOT NULL, `total_share_days` INT,  `avg_share_age` float,  `last_comment` DATETIME, `last_post` DATETIME,  `original_enrollment` DATETIME, `latest_enrollment` DATETIME, `flags` text, `earned_rshares` INT, `rewarded_rshares` INT, `balance_rshares` INT,  `upvote_delay` float, `comment_upvote` bool, PRIMARY KEY (`account`)) ENGINE = InnoDB;")
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
        table.upsert(data, ["account"])
        self.db.commit()


    def add_batch(self, data):
        """ Add a new data set

        """
        table = self.db[self.__tablename__]
        self.db.begin()
        for d in data:
            table.upsert(d, ["account"])
     
        self.db.commit()
    
    def get(self, account):
        """ Change share_age depending on timestamp
    
        """
        table = self.db[self.__tablename__]
        return table.find_one(account=account)

    def get_highest_avg_share_age(self):
        table = self.db[self.__tablename__]
        return table.find_one(order_by='avg_share_age')

    def get_last_updated_member(self):
        table = self.db[self.__tablename__]
        return table.find_one(order_by='-update_at')    
        
    def update_shares(self, account, add_shares, datetime):
        """ Change share_age depending on timestamp
    
        """
        table = self.db[self.__tablename__]
        member = table.find_one(account=account)
        shares = member["shares"] + add_shares
        data = dict(account=account, shares=shares, latest_enrollment=datetime)
        table.update(data, ['account'])

    def update(self, data):
        """ Change share_age depending on timestamp
    
        """
        table = self.db[self.__tablename__]
        table.upsert(data, ['account'])
    
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



class ConfigurationDB(object):
    """ This is the trx storage class
    """
    __tablename__ = 'configuration'

    def __init__(self, db):
        self.db = db

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
        query = ("CREATE TABLE `sbi`.`configuration` (`id` enum('1') NOT NULL, `share_cycle_min` float NOT NULL, `sp_share_ratio` float NOT NULL, `rshares_per_cycle` INT,  `comment_vote_divider` float,  `comment_vote_timeout_h` float, `last_cycle` DATETIME,  PRIMARY KEY (`id`)) ENGINE = InnoDB;")
        self.db.query(query)
        self.db.commit()

    def get(self):
        """ Returns the public keys stored in the database
        """
        table = self.db[self.__tablename__]
        return table.find_one(id=1)
    
    def set(self, data):
        """ Add a new data set
    
        """
        data["id"]= 1
        table = self.db[self.__tablename__]
        table.upsert(data, ["id"])
        self.db.commit()


        """ Change share_age depending on timestamp
    
        """
        table = self.db[self.__tablename__]
        return table.find_one(account=account)

    def update(self, data):
        """ Change share_age depending on timestamp
    
        """
        data["id"]= 1
        table = self.db[self.__tablename__]
        table.update(data, ['id'])
    
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


class KeysDB(object):
    """ This is the trx storage class
    """
    __tablename__ = 'steem_keys'

    def __init__(self, db):
        self.db = db

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
        query = ("CREATE TABLE `sbi`.`keys` (`key_type` VARCHAR(50) NOT NULL, `account` VARCHAR(50) NOT NULL, `wif` VARCHAR(50) NOT NULL,  PRIMARY KEY (`account`, `key_type`)) ENGINE = InnoDB;")
        self.db.query(query)
        self.db.commit()

    def get(self, account, key_type):
        """ Returns the public keys stored in the database
        """
        table = self.db[self.__tablename__]
        return table.find_one(account=account, key_type=key_type)
    
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


class TransactionMemoDB(object):
    """ This is the trx storage class
    """
    __tablename__ = 'transaction_memo'

    def __init__(self, db):
        self.db = db

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
        query = ("CREATE TABLE `sbi`.`transaction_memo` ( `index` INT, `sender` VARCHAR(16) NOT NULL, `to` VARCHAR(16) DEFAULT NULL, `memo` text, `encrypted` BOOLEAN DEFAULT FALSE, `referenced_accounts` text, `amount` decimal(15,6) DEFAULT NULL, `amount_symbol` varchar(5) DEFAULT NULL, `timestamp` DATETIME NOT NULL ,  `id` INTEGER NOT NULL AUTO_INCREMENT, PRIMARY KEY (`id`)) ENGINE = InnoDB;")
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

    def get_sender(self, sender):
        """ Returns all entries for given value

        """
        table = self.db[self.__tablename__]
        id_list = []
        for trx in table.find(sender=sender):
            id_list.append(trx)
        return id_list        

    def get(self, ID):
        """ Returns all entries for given value

        """
        table = self.db[self.__tablename__]
        return table.find_one(id=ID)

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


class TransactionOutDB(object):
    """ This is the trx storage class
    """
    __tablename__ = 'transaction_out'

    def __init__(self, db):
        self.db = db

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
        query = ("CREATE TABLE `sbi`.`transaction_memo` ( `index` INT, `sender` VARCHAR(16) NOT NULL, `to` VARCHAR(16) DEFAULT NULL, `memo` text, `encrypted` BOOLEAN DEFAULT FALSE, `referenced_accounts` text, `amount` decimal(15,6) DEFAULT NULL, `amount_symbol` varchar(5) DEFAULT NULL, `timestamp` DATETIME NOT NULL ,  `id` INTEGER NOT NULL AUTO_INCREMENT, PRIMARY KEY (`id`)) ENGINE = InnoDB;")
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

    def get_sender(self, sender):
        """ Returns all entries for given value

        """
        table = self.db[self.__tablename__]
        id_list = []
        for trx in table.find(sender=sender):
            id_list.append(trx)
        return id_list        

    def get(self, ID):
        """ Returns all entries for given value

        """
        table = self.db[self.__tablename__]
        return table.find_one(id=ID)

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


class PendingRefundDB(object):
    """ This is the trx storage class
    """
    __tablename__ = 'pending_refunds'

    def __init__(self, db):
        self.db = db

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
        query = ("CREATE TABLE `sbi`.`transaction_memo` ( `index` INT, `sender` VARCHAR(16) NOT NULL, `to` VARCHAR(16) DEFAULT NULL, `memo` text, `encrypted` BOOLEAN DEFAULT FALSE, `referenced_accounts` text, `amount` decimal(15,6) DEFAULT NULL, `amount_symbol` varchar(5) DEFAULT NULL, `timestamp` DATETIME NOT NULL ,  `id` INTEGER NOT NULL AUTO_INCREMENT, PRIMARY KEY (`id`)) ENGINE = InnoDB;")
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

    def get_sender(self, sender):
        """ Returns all entries for given value

        """
        table = self.db[self.__tablename__]
        id_list = []
        for trx in table.find(sender=sender):
            id_list.append(trx)
        return id_list        

    def get(self, ID):
        """ Returns all entries for given value

        """
        table = self.db[self.__tablename__]
        return table.find_one(id=ID)

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
    
