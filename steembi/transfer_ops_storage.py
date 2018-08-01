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
    

    def __init__(self, db, account):
        self.db = db
        self.__tablename__ = "%s_ops" % account

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
        query = ("CREATE TABLE `sbi_steem_ops`.`%s` ( `virtual_op` INT NOT NULL , `op_acc_index` INT NOT NULL , `op_acc_name` VARCHAR(50) NOT NULL , `block` INT NOT NULL , `trx_in_block` INT NOT NULL , `op_in_trx` INT NOT NULL , `timestamp` DATETIME NOT NULL , `type` VARCHAR(50) NOT NULL, `op_dict` TEXT NOT NULL , PRIMARY KEY (`op_acc_index`)) ENGINE = InnoDB;" % self.__tablename__)
        self.db.query(query)
        self.db.commit()

    def add(self, data):
        """ Add a new data set

        """
        table = self.db[self.__tablename__]
        table.insert(data)    
        self.db.commit()

    def get_all(self, op_types = []):
        ops = []
        table = self.db[self.__tablename__]
        for op in table.find():
            if op["type"] in op_types or len(op_types) == 0:
                ops.append(op)
        return ops

    def add_batch(self, data):
        """ Add a new data set

        """
        table = self.db[self.__tablename__]
        self.db.begin()
        for d in data:
            table.insert(d)
            
        self.db.commit()

    def get_latest_index(self):
        table = self.db[self.__tablename__]
        return table.find_one(order_by='-op_acc_index')

    def delete(self, ID):
        """ Delete a data set

           :param int ID: database id
        """
        table = self.db[self.__tablename__]
        table.delete(op_acc_index=ID)

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


class MemberHistDB(object):
    """ This is the trx storage class
    """
    

    def __init__(self, db):
        self.db = db
        self.__tablename__ = "member_hist"

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
        query = ("CREATE TABLE `sbi_steem_ops`.`member_hist` ( `block_num` INT NOT NULL , `block_id`  varchar(40) NOT NULL,  `trx_id`  varchar(40) NOT NULL ,  `trx_num`  INT NOT NULL, `op_num` INT NOT NULL, `timestamp` DATETIME NOT NULL, `type` varchar(30) NOT NULL, `author` varchar(16) DEFAULT NULL, `permlink` varchar(50) DEFAULT NULL, `parent_author` varchar(16) DEFAULT NULL, `parent_permlink` varchar(50) DEFAULT NULL,  PRIMARY KEY (`block_num`, `trx_id`, `op_num`)) ENGINE = InnoDB; ")
        self.db.query(query)
        self.db.commit()

    def add(self, data):
        """ Add a new data set

        """
        table = self.db[self.__tablename__]
        table.insert(data)    
        self.db.commit()

    def add_batch(self, data, chunk_size=1000):
        """ Add a new data set

        """
        table = self.db[self.__tablename__]
        table.insert_many(data, chunk_size=chunk_size)

    def get_latest_block_num(self):
        table = self.db[self.__tablename__]
        op = table.find_one(order_by='-block_num')
        if op is None:
            return None
        return op["block_num"]

    def get_latest_timestamp(self):
        table = self.db[self.__tablename__]
        op = table.find_one(order_by='-timestamp')
        if op is None:
            return None
        return op["timestamp"]

    def get_block(self, block_num):
        ret = []
        table = self.db[self.__tablename__]
        for op in table.find(block_num=block_num):
            ret.append(op)
        return ret

    def get_block_trx_id(self, block_num):
        ret = []
        table = self.db[self.__tablename__]
        for op in table.find(block_num=block_num):
            ret.append(op["trx_id"])
        return ret

    def get_ops(self, op_type):
        table = self.db[self.__tablename__]
        return table.find(type=op_type)

    def get_comments(self, author):
        table = self.db[self.__tablename__]
        return table.find(type="comment", author=author)

    def get_votes(self, voter):
        table = self.db[self.__tablename__]
        return table.find(type="vote", voter=voter)

    def get_ordered_block_num(self, limit=1000):
        table = self.db[self.__tablename__]
        return table.find(order_by='block_num', _limit=limit)

    def get_ordered_block_num_reverse(self, limit=1000):
        table = self.db[self.__tablename__]
        return table.find(order_by='-block_num', _limit=limit)

    def delete(self, block_num, trx_id, op_num):
        """ Delete a data set

           :param int ID: database id
        """
        table = self.db[self.__tablename__]
        table.delete(block_num=block_num, trx_id=trx_id, op_num=op_num)

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
            table.drop()