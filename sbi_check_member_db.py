from beem.account import Account
from beem.amount import Amount
from beem import Steem
from beem.instance import set_shared_steem_instance
from beem.nodelist import NodeList
from beem.utils import formatTimeString
import re
import json
import os
from time import sleep
from steembi.sqlite_dict import db_store, db_load, db_append, db_extend, db_has_database, db_has_key
from steembi.parse_hist_op import ParseAccountHist
    

if __name__ == "__main__":
    accounts = ["steembasicincome", "sbi2", "sbi3", "sbi4", "sbi5", "sbi6", "sbi7", "sbi8"]
    database_ops = "sbi_ops.sqlite"
    path = ""
    path = "E:\\sbi\\"
    load_ops_from_database = True
    # Update current node list from @fullnodeupdate

    from steembi.storage import (trxStorage, memberStorage)

    # Update current node list from @fullnodeupdate
    print("build member database")
    # memberStorage.wipe(True)
    member_accounts = memberStorage.get_all_accounts()
    data = trxStorage.get_all_data()
    
    member_data = {}
    for m in member_accounts:
        member_data[m] = memberStorage.get(m)
    
    for op in data:
        if op["status"].lower() == "valid":
            share_type = op["share_type"]
            if share_type.lower() in ["delegation"]:
                if op["shares"] > 0 and op["sponsor"] in member_data:
                    print("del. bonus_shares: %s - %d" % (op["sponsor"], op["shares"]))
                    member_data[op["sponsor"]]["bonus_shares"] += op["shares"]
            elif share_type.lower() in ["mgmt", "mgmttransfer"]:
                if op["shares"] > 0 and op["sponsor"] in member_data:
                    member_data[op["sponsor"]]["bonus_shares"] += op["shares"]
                    print("mngt bonus_shares: %s - %d" % (op["sponsor"], op["shares"]))

    shares = 0
    bonus_shares = 0
    for m in member_data:
        shares += member_data[m]["shares"]
        bonus_shares += member_data[m]["bonus_shares"]
    print("shares: %d" % shares)
    print("bonus shares: %d" % bonus_shares)
    print("total shares: %d" % (shares + bonus_shares))
    
