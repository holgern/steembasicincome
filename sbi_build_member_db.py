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
    accs = memberStorage.get_all_accounts()
    for a in accs:
        memberStorage.delete(a)
    # nodes = NodeList()
    # nodes.update_nodes()
    # stm = Steem(node=nodes.get_nodes())
    data = trxStorage.get_all_data()
    status = {}
    share_type = {}
    n_records = 0
    member_data = {}
    share_age_member = {}
    for op in data:
        if op["status"] == "Valid":
            share_type = op["share_type"]
            if share_type in ["RemovedDelegation", "Delegation", "DelegationLeased", "Mgmt", "MgmtTransfer"]:
                continue
            sponsor = op["sponsor"]
            sponsee = json.loads(op["sponsee"])
            shares = op["shares"]
            share_age = op["share_age"]
            if isinstance(op["timestamp"], str):
                timestamp = formatTimeString(op["timestamp"])
            else:
                timestamp = op["timestamp"]
            if shares == 0:
                continue
            if sponsor not in member_data:
                
                member = {"account": sponsor, "shares": shares, "bonus_shares": 0, "total_share_days": 0, "avg_share_age": float(0),
                          "original_enrollment": timestamp, "latest_enrollment": timestamp, "earned_rshares": 0, "rewarded_rshares": 0,
                          "balance_rshares": 0, "comment_upvote": False}
                member_data[sponsor] = member
                share_age_member[sponsor] = [share_age]
            else:
                member_data[sponsor]["latest_enrollment"] = timestamp
                member_data[sponsor]["shares"] += shares
                share_age_member[sponsor].append(share_age)
            if len(sponsee) == 0:
                continue
            for s in sponsee:
                shares = sponsee[s]
                if s not in member_data:
                    member = {"account": s, "shares": shares, "bonus_shares": 0, "total_share_days": 0, "avg_share_age": float(0),
                              "original_enrollment": timestamp, "latest_enrollment": timestamp, "earned_rshares": 0, "rewarded_rshares": 0,
                              "balance_rshares": 0, "comment_upvote": False}
                    member_data[s] = member
                    share_age_member[s] = [share_age]
                else:
                    member_data[s]["latest_enrollment"] = timestamp
                    member_data[s]["shares"] += shares
                    share_age_member[s].append(share_age)

    empty_shares = []       
    for m in member_data:
        if member_data[m]["shares"] <= 0:
            empty_shares.append(m)
    
    for del_acc in empty_shares:
        del member_data[del_acc]
    

    for m in share_age_member:
        if m in member_data:
            member_data[m]["total_share_days"] = sum(share_age_member[m])
            member_data[m]["avg_share_age"] = sum(share_age_member[m]) / len(share_age_member[m])

    shares = 0
    bonus_shares = 0
    for m in member_data:
        shares += member_data[m]["shares"]
        bonus_shares += member_data[m]["bonus_shares"]
    print("shares: %d" % shares)
    print("bonus shares: %d" % bonus_shares)
    print("total shares: %d" % (shares + bonus_shares))
    
    member_list = []
    for m in member_data:
        member_list.append(member_data[m])
    memberStorage.add_batch(member_list)