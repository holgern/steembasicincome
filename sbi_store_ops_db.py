from beem.account import Account
from beem.amount import Amount
from beem import Steem
from beem.instance import set_shared_steem_instance
from beem.nodelist import NodeList
from beem.utils import formatTimeString
import re
import os
from sqlitedict import SqliteDict
from contextlib import closing

def db_store(path, database, key, data):
    with closing(SqliteDict(path + database, autocommit=True)) as db:
        db[key] = data

def db_load(path, database, key):
    with closing(SqliteDict(path + database, autocommit=True)) as db:
        return db[key]

def db_append(path, database, key, new_data):
    with closing(SqliteDict(path + database, autocommit=True)) as db:
        data = db[key]
        data.append(new_data)
        db[key] = data

def db_extend(path, database, key, new_data):
    with closing(SqliteDict(path + database, autocommit=True)) as db:
        data = db[key]
        data.extend(new_data)
        db[key] = data

def db_has_database(path, database):
    if not os.path.isfile(path + database):
        return False
    else:
        return True

def db_has_key(path, database, key):
    if not os.path.isfile(path + database):
        return False
    with closing(SqliteDict(path + database, autocommit=True)) as db:
        if key in db:
            return True
        else:
            return False

if __name__ == "__main__":
    accounts = ["steembasicincome", "sbi2", "sbi3", "sbi4", "sbi5", "sbi6", "sbi7", "sbi8"]
    path = "E:\\curation_data\\"
    database = "sbi.sqlite"
    # Update current node list from @fullnodeupdate
    nodes = NodeList()
    nodes.update_nodes(weights={"hist": 1})
    stm = Steem(node=nodes.get_nodes(appbase=False, https=False))
    print(str(stm))
    set_shared_steem_instance(stm)
    
    for account in accounts:
        account = Account(account)
        print("account %s" % account["name"])
        # Go trough all transfer ops
        cnt = 0
        if not db_has_key(path, database, account["name"]):
            ops = []
            for op in account.history():
                ops.append(op)
                if cnt % 1000 == 0:
                    print(op["timestamp"])
                cnt += 1

            db_store(path, database, account["name"], ops)        
        # append new ops
        else:
            ops = db_load(path, database, account["name"])
            print("account %s - %d ops  %s - %s" %(account["name"], len(ops), ops[0]["timestamp"], ops[-1]["timestamp"]))
            start_index = ops[-1]["index"] + 1
            for op in account.history(start=start_index, use_block_num=False):
                ops.append(op)
                if cnt % 1000 == 0:
                    print(op["timestamp"])
                cnt += 1
            db_store(path, database, account["name"], ops)
        ops = db_load(path, database, account["name"])
        print("account %s - %d ops  %s - %s" %(account["name"], len(ops), ops[0]["timestamp"], ops[-1]["timestamp"]))
        
        last_op = {}
        last_op["index"] = -1
        last_op["timestamp"] = '2000-12-29T10:07:45'
        for op in ops:
            if (op["index"] - last_op["index"]) != 1:
                print("error %s %d %d" % (account, op["index"], last_op["index"]))
            if (formatTimeString(op["timestamp"]) < formatTimeString(last_op["timestamp"])):
                print("error %s %s %s" % (account, op["timestamp"], last_op["timestamp"]))
            last_op = op