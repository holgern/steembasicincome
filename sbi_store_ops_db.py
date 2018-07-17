from beem.account import Account
from beem.amount import Amount
from beem import Steem
from beem.instance import set_shared_steem_instance
from beem.nodelist import NodeList
from beem.utils import formatTimeString
import re
import os
from steembi.sqlite_dict import db_store, db_load, db_append, db_extend, db_has_database, db_has_key


if __name__ == "__main__":
    accounts = ["steembasicincome", "sbi2", "sbi3", "sbi4", "sbi5", "sbi6", "sbi7", "sbi8"]
    path = "E:\\sbi\\"
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
        first_error_index = None
        index = 0
        for op in ops:
            if (op["index"] - last_op["index"]) != 1:
                print("error %s %d %d" % (account["name"], op["index"], last_op["index"]))
                if first_error_index is None:
                    first_error_index = index
            if (formatTimeString(op["timestamp"]) < formatTimeString(last_op["timestamp"])):
                print("error %s %s %s" % (account["name"], op["timestamp"], last_op["timestamp"]))
                if first_error_index is None:
                    first_error_index = index                
            last_op = op
            index += 1
        if first_error_index is not None:
            db_store(path, database, account["name"], ops[:first_error_index - 1])