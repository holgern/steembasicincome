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


class Trx(object):
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


class Member(object):
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

    def get_highest_avg_share_age(self):
        table = self.db[self.__tablename__]
        return table.find_one(order_by='avg_share_age')
        
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

