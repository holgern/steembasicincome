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

    def get_all_data(self):
        """ Returns the public keys stored in the database
        """
        return self.db[self.__tablename__].all()

    def get_all_data_sorted(self):
        """ Returns the public keys stored in the database
        """
        return self.db[self.__tablename__].find(order_by='index')    

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

    def get_SBD_transfer(self, account, shares, timestamp, SBD_symbol="SBD"):
        """ Returns all entries for given value

        """
        table = self.db[self.__tablename__]
        found_trx = None
        for trx in table.find(account=account, shares=-shares, share_type=SBD_symbol):
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

    def update_memo(self, source, account, memo_old, memo_new):
        """ Change share_age depending on timestamp

        """
        table = self.db[self.__tablename__]
        found_trx = None
        for trx in table.find(source=source, account=account, memo=memo_old):
            found_trx = trx
        data = dict(index=found_trx["index"], source=source, memo=memo_new)
        table.update(data, ['index', 'source'])

    def update_sponsee(self, source, account, memo, sponsee, status):
        """ Change share_age depending on timestamp

        """
        table = self.db[self.__tablename__]
        found_trx = None
        for trx in table.find(source=source, account=account, memo=memo):
            found_trx = trx
        data = dict(index=found_trx["index"], source=source, sponsee=sponsee, status=status)
        table.update(data, ['index', 'source'])


    def update_sponsee(self, source, account, memo, sponsee, status):
        """ Change share_age depending on timestamp

        """
        table = self.db[self.__tablename__]
        found_trx = None
        for trx in table.find(source=source, account=account, memo=memo):
            found_trx = trx
        data = dict(index=found_trx["index"], source=source, sponsee=sponsee, status=status)
        table.update(data, ['index', 'source'])

    def update_sponsee_index(self, index, source, sponsee, status):
        """ Change share_age depending on timestamp

        """
        table = self.db[self.__tablename__]
        data = dict(index=index, source=source, sponsee=sponsee, status=status)
        table.update(data, ['index', 'source'])

    def update_sponsor_index(self, index, source, sponsor, status):
        """ Change share_age depending on timestamp

        """
        table = self.db[self.__tablename__]
        data = dict(index=index, source=source, sponsor=sponsor, status=status)
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

    def update_avg_share_age(self, account, avg_share_age):
        """ Change share_age depending on timestamp
    
        """
        table = self.db[self.__tablename__]
        data = dict(account=account, avg_share_age=avg_share_age)
        table.update(data, ['account'])

    def update_last_vote(self, account, last_received_vote):
        """ Change share_age depending on timestamp
    
        """
        table = self.db[self.__tablename__]
        data = dict(account=account, last_received_vote=last_received_vote)
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


class BlacklistDB(object):
    """ This is the trx storage class
    """
    __tablename__ = 'blacklist'

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


class AccountsDB(object):
    """ This is the accounts storage class
    """
    __tablename__ = 'accounts'

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

    def get(self):
        """ Returns the accounts stored in the database
        """
        table = self.db[self.__tablename__]
        accounts = []
        for a in table.all():
            if a["voting"] == 1:
                accounts.append(a["name"])
        return accounts

    def get_data(self):
        table = self.db[self.__tablename__]
        accounts = {}
        for acc in table.all():
            accounts[acc["name"]] = acc
        return accounts
        
    def get_transfer(self):
        """ Returns the accounts stored in the database
        """
        table = self.db[self.__tablename__]
        accounts = []
        for a in table.all():
            if a["transfer"] == 1:
                accounts.append(a["name"])
        return accounts

    def get_upvote_reward_rshares(self):
        """ Returns the accounts stored in the database
        """
        table = self.db[self.__tablename__]
        accounts = []
        for a in table.all():
            if a["upvote_reward_rshares"] == 1:
                accounts.append(a["name"])
        return accounts

    def get_transfer_memo_sender(self):
        """ Returns the accounts stored in the database
        """
        table = self.db[self.__tablename__]
        accounts = []
        for a in table.all():
            if a["transfer_memo_sender"] == 1:
                accounts.append(a["name"])
        return accounts

    def set(self, data):
        """ Add a new data set
    
        """
        table = self.db[self.__tablename__]
        table.upsert(data, ["name"])
        self.db.commit()

    def update(self, data):
        """ Change share_age depending on timestamp
    
        """
        table = self.db[self.__tablename__]
        table.update(data, ['name'])
    
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


class TransferMemoDB(object):
    """ This is the trx storage class
    """
    __tablename__ = 'transfer_memos'

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

    def get(self, memo_type):
        """ Returns the public keys stored in the database
        """
        table = self.db[self.__tablename__]
        return table.find_one(memo_type=memo_type)

    def get_all_data(self):
        """ Returns the public keys stored in the database
        """
        return self.db[self.__tablename__].all()


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

    def get_all(self):
        """ Returns all entries for given value

        """
        table = self.db[self.__tablename__]
        for d in table:
            yield d

    def update_memo(self, sender, to, memo_old, memo_new, encrypted):
        """ Change share_age depending on timestamp

        """
        table = self.db[self.__tablename__]
        found_trx = None
        for trx in table.find(sender=sender, to=to, memo=memo_old):
            found_trx = trx
        data = dict(index=found_trx["id"], memo=memo_new, encrypted=encrypted)
        table.update(data, ['id'])

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

    def delete_sender(self, sender):
        """ Delete a data set

           :param int ID: database id
        """
        table = self.db[self.__tablename__]
        table.delete(sender=sender)

    def delete_to(self, to):
        """ Delete a data set

           :param int ID: database id
        """
        table = self.db[self.__tablename__]
        table.delete(to=to)

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
    

