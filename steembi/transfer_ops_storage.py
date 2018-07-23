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


class AccountTrx(object):
    """ This is the trx storage class
    """
    __tablename__ = 'sbi_ops'

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
        query = ("CREATE TABLE `sbi_steem_ops`.`sbi_ops` ( `virtual_op` INT NOT NULL , `op_acc_index` INT NOT NULL , `op_acc_name` VARCHAR(50) NOT NULL , `block` INT NOT NULL , `trx_in_block` INT NOT NULL , `op_in_trx` INT NOT NULL , `timestamp` DATETIME NOT NULL , `op_dict` TEXT NOT NULL , PRIMARY KEY (`op_acc_index`, `op_acc_name`)) ENGINE = InnoDB;")
        self.db.query(query)
        self.db.commit()

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

    def get_latest_index(self, account_name):
        table = self.db[self.__tablename__]
        return table.find_one(op_acc_name=account_name, order_by='-op_acc_index')

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



class TransferTrx(object):
    """ This is the trx storage class
    """
    __tablename__ = 'transfers'

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
        query = ("CREATE TABLE `sbi_steem_ops`.`transfers` ( `virtual_op` INT NOT NULL , `op_acc_index` INT NOT NULL , `op_acc_name` VARCHAR(50) NOT NULL , `block` INT NOT NULL , `trx_in_block` INT NOT NULL , `op_in_trx` INT NOT NULL , `timestamp` DATETIME NOT NULL , `from` VARCHAR(50) NOT NULL, `to` VARCHAR(50) NOT NULL, `amount` decimal(15,6) DEFAULT NULL, `amount_symbol`varchar(5) DEFAULT NULL,  `memo` varchar(2048) DEFAULT NULL, `op_type` varchar(50) NOT NULL, PRIMARY KEY (`op_acc_index`, `op_acc_name`)) ENGINE = InnoDB;")

        self.db.query(query)
        self.db.commit()

    def find(self, memo, to):
        table = self.db[self.__tablename__].table
        statement = table.select(and_(table.c.memo.like("%" + memo + "%"), table.c.to == to))
        result = self.db.query(statement)
        ret = []
        for r in result:
            ret.append(r)
        return ret

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

    def get_latest_index(self, account_name):
        table = self.db[self.__tablename__]
        return table.find_one(op_acc_name=account_name, order_by='-op_acc_index')

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


