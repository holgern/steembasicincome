from beem.account import Account
from beem.amount import Amount
from beem import Steem
from beem.instance import set_shared_steem_instance
from beem.nodelist import NodeList
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
    # nodes = NodeList()
    # nodes.update_nodes()
    # stm = Steem(node=nodes.get_nodes())
    data = trxStorage.get_all_data()
    status = {}
    share_type = {}
    n_records = 0
    member_data = {}
    for op in data:
        if op["status"] == "Valid":
            share_type = op["share_type"]
            if share_type in ["RemovedDelegation", "Delegation", "DelegationLeased"]:
                continue
            sponsor = op["sponsor"]
            sponsee = json.loads(op["sponsee"])
            shares = op["shares"]
            if shares == 0:
                continue
            if memberStorage.get(sponsor) is None:
                member = {"account": sponsor, "shares": shares, "total_share_days": 0, "avg_share_age": float(0),
                          "original_enrollment": op["timestamp"], "latest_enrollment": op["timestamp"], "earned_rshares": 0, "rewarded_rshares": 0,
                          "balance_rshares": 0, "comment_upvote": False}
                memberStorage.add(member)
            else:
                memberStorage.update_shares(sponsor, shares, op["timestamp"])
            if len(sponsee) == 0:
                continue
            for s in sponsee:
                shares = sponsee[s]
                if memberStorage.get(s) is None:
                    member = {"account": s, "shares": shares, "total_share_days": 0, "avg_share_age": float(0),
                              "original_enrollment": op["timestamp"], "latest_enrollment": op["timestamp"], "earned_rshares": 0, "rewarded_rshares": 0,
                              "balance_rshares": 0, "comment_upvote": False}
                    memberStorage.add(member)
                else:
                    memberStorage.update_shares(s, shares, op["timestamp"])
                
                



