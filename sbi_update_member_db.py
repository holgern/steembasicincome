from beem.account import Account
from beem.amount import Amount
from beem import Steem
from beem.instance import set_shared_steem_instance
from beem.nodelist import NodeList
from datetime import datetime, timedelta
import re
import json
import os
from time import sleep
import dataset
from steembi.parse_hist_op import ParseAccountHist
from steembi.storage import Trx, Member
    

if __name__ == "__main__":
    config_file = 'config.json'
    if not os.path.isfile(config_file):
        accounts = ["steembasicincome", "sbi2", "sbi3", "sbi4", "sbi5", "sbi6", "sbi7", "sbi8"]
        path = "E:\\sbi\\"
        database = "sbi_ops.sqlite"
        database_transfer = "sbi_transfer.sqlite"
        databaseConnector = None
        other_accounts = ["minnowbooster"]
        mgnt_shares = {"josephsavage": 3, "earthnation-bot": 1, "holger80": 1}
    else:
        with open(config_file) as json_data_file:
            config_data = json.load(json_data_file)
        print(config_data)
        accounts = config_data["accounts"]
        path = config_data["path"]
        database = config_data["database"]
        database_transfer = config_data["database_transfer"]
        databaseConnector = config_data["databaseConnector"]
        databaseConnector2 = config_data["databaseConnector2"]
        other_accounts = config_data["other_accounts"]
        mgnt_shares = config_data["mgnt_shares"]

    db2 = dataset.connect(databaseConnector2)
    # Create keyStorage
    trxStorage = Trx(db2)
    memberStorage = Member(db2)
    print("update member database")
    # memberStorage.wipe(True)
    member_accounts = memberStorage.get_all_accounts()
    data = trxStorage.get_all_data()
    
    member_data = {}
    n_records = 0
    share_age_member = {}    
    for m in member_accounts:
        member_data[m] = memberStorage.get(m)
        
    # clear shares
    for m in member_data:
        member_data[m]["shares"] = 0
        member_data[m]["bonus_shares"] = 0
        share_age_member[m] = []
    
    last_mgmt_op = {}
        
    for op in data:
        if op["status"] == "Valid":
            share_type = op["share_type"]
            if share_type in ["RemovedDelegation", "DelegationLeased"]:
                continue
            if share_type.lower() in ["delegation"]:
                if op["shares"] > 0 and op["sponsor"] in member_data:
                    # print("del. bonus_shares: %s - %d" % (op["sponsor"], op["shares"]))
                    member_data[op["sponsor"]]["bonus_shares"] += op["shares"]
            elif share_type.lower() in ["mgmt", "mgmttransfer"]:
                if op["shares"] > 0 and op["sponsor"] in member_data:
                    member_data[op["sponsor"]]["bonus_shares"] += op["shares"]
                    # print("mngt bonus_shares: %s - %d" % (op["sponsor"], op["shares"]))
            else:
                sponsor = op["sponsor"]
                sponsee = json.loads(op["sponsee"])
                shares = op["shares"]
                share_age = 0
                if isinstance(op["timestamp"], str):
                    timestamp = formatTimeString(op["timestamp"])
                else:
                    timestamp = op["timestamp"]
    
                age = (datetime.utcnow()) - (timestamp)
                share_age = int(age.total_seconds() / 60 / 60 / 24)  
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
    
                    
    print("update share age")
    for m in share_age_member:
        if m in member_data:
            member_data[m]["total_share_days"] = sum(share_age_member[m])
            member_data[m]["avg_share_age"] = sum(share_age_member[m]) / len(share_age_member[m])
    if False:
        print("update last post and comment date")
        for m in member_data:
            acc = Account(m)
            latest_post = acc.get_blog(limit=1)
            latest_comment = None
            for r in acc.comment_history(limit=1):
                latest_comment = r
            if len(latest_post) > 0:
                member_data[m]["last_post"] = latest_post[0]["created"]
            if latest_comment is not None:
                member_data[m]["last_comment"] = latest_comment["created"]

    print("write member database")
    memberStorage.db = dataset.connect(databaseConnector2)
    for m in member_data:
        data = member_data[m]
        memberStorage.update(data)
