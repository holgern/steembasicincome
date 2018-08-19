from beem.account import Account
from beem.comment import Comment
from beem.vote import ActiveVotes
from beem.amount import Amount
from beem import Steem
from beem.instance import set_shared_steem_instance
from beem.nodelist import NodeList
from beem.utils import addTzInfo, resolve_authorperm, formatTimeString, construct_authorperm
from datetime import datetime, timedelta
import re
import json
import os
from time import sleep
import dataset
from steembi.parse_hist_op import ParseAccountHist
from steembi.storage import TrxDB, MemberDB, ConfigurationDB
from steembi.transfer_ops_storage import TransferTrx, AccountTrx, MemberHistDB
from steembi.member import Member

    

if __name__ == "__main__":
    config_file = 'config.json'
    if not os.path.isfile(config_file):
        accounts = ["steembasicincome", "sbi2", "sbi3", "sbi4", "sbi5", "sbi6", "sbi7", "sbi8", "sbi9"]
        path = "E:\\sbi\\"
        database = "sbi_ops.sqlite"
        database_transfer = "sbi_transfer.sqlite"
        databaseConnector = None
        other_accounts = ["minnowbooster"]
        mgnt_shares = {"josephsavage": 3, "earthnation-bot": 1, "holger80": 1}
    else:
        with open(config_file) as json_data_file:
            config_data = json.load(json_data_file)
        # print(config_data)
        accounts = config_data["accounts"]
        path = config_data["path"]
        database = config_data["database"]
        database_transfer = config_data["database_transfer"]
        databaseConnector = config_data["databaseConnector"]
        databaseConnector2 = config_data["databaseConnector2"]
        other_accounts = config_data["other_accounts"]
        mgnt_shares = config_data["mgnt_shares"]
        
        
    db2 = dataset.connect(databaseConnector2)
    db = dataset.connect(databaseConnector)
    transferStorage = TransferTrx(db)    
    # Create keyStorage
    trxStorage = TrxDB(db2)
    memberStorage = MemberDB(db2)
    accountStorage = MemberHistDB(db)
    confStorage = ConfigurationDB(db2)
    
    conf_setup = confStorage.get()
    
    last_cycle = conf_setup["last_cycle"]
    share_cycle_min = conf_setup["share_cycle_min"]
    sp_share_ratio = conf_setup["sp_share_ratio"]
    rshares_per_cycle = conf_setup["rshares_per_cycle"]
    upvote_multiplier = conf_setup["upvote_multiplier"]
    last_paid_post = conf_setup["last_paid_post"]
    
    
    print("last_cycle: %s - %.2f min" % (formatTimeString(last_cycle), (datetime.utcnow() - last_cycle).total_seconds() / 60))
    if last_cycle is None:
        last_cycle = datetime.utcnow() - timedelta(seconds = 60 * 145)
        confStorage.update({"last_cycle": last_cycle})
    elif (datetime.utcnow() - last_cycle).total_seconds() > 60 * share_cycle_min:
        current_cycle = last_cycle + timedelta(seconds=60 * share_cycle_min)
        confStorage.update({"last_cycle": last_cycle + timedelta(seconds=60 * share_cycle_min)})
        
        print("update member database")
        # memberStorage.wipe(True)
        member_accounts = memberStorage.get_all_accounts()
        data = trxStorage.get_all_data()
        
        data = sorted(data, key=lambda x: (datetime.utcnow() - x["timestamp"]).total_seconds(), reverse=True)
        
        # Update current node list from @fullnodeupdate
        nodes = NodeList()
        # nodes.update_nodes()
        #stm = Steem(node=nodes.get_nodes())    
        stm = Steem()
        member_data = {}
        n_records = 0
        share_age_member = {}    
        for m in member_accounts:
            member_data[m] = Member(memberStorage.get(m))
            
        
        # del management shares
        if False:
            print("Delete all mgmt trx data")
            trxStorage.delete_all("mgmt")
            shares_sum = 0
            mngt_shares_sum = 0
            for op in data:
                if op["status"] == "Valid":
                    share_type = op["share_type"]            
                    if share_type.lower() not in ["delegation", "RemovedDelegation", "DelegationLeased", "mgmt", "mgmttransfer"]:
                        shares = op["shares"]
                        shares_sum += shares
                        mngt_shares_sum += shares
                        if mngt_shares_sum >= 100:
                            mngt_shares_sum -= 100
                            timestamp = op["timestamp"]
                            sponsee = {}
                            memo = ""
                            latest_share = trxStorage.get_lastest_share_type("Mgmt")
                            if latest_share is not None:
                                start_index = latest_share["index"] + 1
                            else:
                                start_index = 0
                            for account in mgnt_shares:
                                shares = mgnt_shares[account]
                                sponsor = account
                                mngtData = {"index": start_index, "source": "mgmt", "memo": "", "account": account, "sponsor": sponsor, "sponsee": sponsee, "shares": shares, "vests": float(0), "timestamp": formatTimeString(timestamp),
                                         "status": "Valid", "share_type": "Mgmt"}
                                start_index += 1
                                trxStorage.add(mngtData)                            
        
        # clear shares
        for m in member_data:
            member_data[m]["shares"] = 0
            member_data[m]["bonus_shares"] = 0
            member_data[m].reset_share_age_list()
        
        shares_sum = 0
        latest_share = trxStorage.get_lastest_share_type("Mgmt")
        mngt_shares_sum = (latest_share["index"] + 1) / len(mgnt_shares) * 100
        print("mngt_shares sum %d" % mngt_shares_sum)
        latest_data_timestamp = None
        for op in data:
            if op["status"] == "Valid":
                share_type = op["share_type"]
                if latest_data_timestamp is None:
                    latest_data_timestamp = formatTimeString(op["timestamp"])
                elif latest_data_timestamp < formatTimeString(op["timestamp"]):
                    latest_data_timestamp = formatTimeString(op["timestamp"])
                if share_type in ["RemovedDelegation", "DelegationLeased"]:
                    continue
                if share_type.lower() in ["delegation"]:
                    if op["shares"] > 0 and op["sponsor"] in member_data:
                        # print("del. bonus_shares: %s - %d" % (op["sponsor"], op["shares"]))
                        member_data[op["sponsor"]]["bonus_shares"] += op["shares"]
                    elif op["vests"] > 0 and op["sponsor"] in member_data:
                        sp = stm.vests_to_sp(float(op["vests"]))
                        member_data[op["sponsor"]]["bonus_shares"] += int(sp / sp_share_ratio)
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
        
                    if shares == 0:
                        continue
                    shares_sum += shares
                    if (shares_sum - mngt_shares_sum) >= 100:
                        mngt_shares_sum += 100
                        print("add mngt shares")
                        latest_share = trxStorage.get_lastest_share_type("Mgmt")
                        if latest_share is not None:
                            start_index = latest_share["index"] + 1
                        else:
                            start_index = 0
                        for account in mgnt_shares:
                            shares = mgnt_shares[account]
                            mgmt_data = {"index": start_index, "source": "mgmt", "memo": "", "account": account, "sponsor": account, "sponsee": {}, "shares": shares, "vests": float(0), "timestamp": formatTimeString(timestamp),
                                     "status": "Valid", "share_type": "Mgmt"}
                            start_index += 1
                            trxStorage.add(mgmt_data)                          
                    if sponsor not in member_data:
                        member = Member(sponsor, shares, timestamp)
                        member.append_share_age(timestamp)
                        member_data[sponsor] = member
                    else:
                        member_data[sponsor]["latest_enrollment"] = timestamp
                        member_data[sponsor]["shares"] += shares
                        member_data[sponsor].append_share_age(timestamp)
                    if len(sponsee) == 0:
                        continue
                    for s in sponsee:
                        shares = sponsee[s]
                        if s not in member_data:
                            member = Member(s, shares, timestamp)
                            member.append_share_age(timestamp)
                            member_data[s] = member
                        else:
                            member_data[s]["latest_enrollment"] = timestamp
                            member_data[s]["shares"] += shares
                            member_data[sponsor].append_share_age(timestamp)
    
        empty_shares = []
        latest_enrollment = None
        for m in member_data:
            if member_data[m]["shares"] <= 0:
                empty_shares.append(m)
            if latest_enrollment is None:
                latest_enrollment = member_data[m]["latest_enrollment"]
            elif latest_enrollment < member_data[m]["latest_enrollment"]:
                latest_enrollment = member_data[m]["latest_enrollment"]
        
        print("latest data timestamp: %s - latest member enrollment %s" % (str(latest_data_timestamp), str(latest_enrollment)))
          
    
        for del_acc in empty_shares:
            del member_data[del_acc]
    
        # date_now = datetime.utcnow()
        date_now = latest_enrollment
        date_7_before = addTzInfo(date_now - timedelta(seconds=7 * 24 * 60 * 60))
        date_28_before = addTzInfo(date_now - timedelta(seconds=28 * 24 * 60 * 60))
    
        print("update share age")
        for m in member_data:
            if "first_cycle_at" not  in member_data[m]:
                member_data[m]["first_cycle_at"] = current_cycle
            elif member_data[m]["first_cycle_at"] < datetime(2000, 1 , 1, 0, 0, 0):
                member_data[m]["first_cycle_at"] = current_cycle
            member_data[m]["balance_rshares"] += (member_data[m]["shares"] + member_data[m]["bonus_shares"]) * rshares_per_cycle
            member_data[m]["earned_rshares"] += (member_data[m]["shares"] + member_data[m]["bonus_shares"]) * rshares_per_cycle
            member_data[m].calc_share_age()
        
        print("reward voted steembasicincome post")
        account = Account("steembasicincome")
        blog = account.get_blog(limit=10)[::-1]
        if last_paid_post is None:
            post = blog[-2]
            last_paid_post = post["created"]
        for post in blog:
            if post["created"] < last_paid_post:
                continue
            last_paid_post = post["created"]
            all_votes = ActiveVotes(post["authorperm"])
            for vote in all_votes:
                if vote["voter"] in member_data:
                    rshares = vote["rshares"] * upvote_multiplier
                    if rshares < rshares_per_cycle:
                        rshares = rshares_per_cycle
                    member_data[vote["voter"]]["earned_rshares"] += rshares
                    member_data[vote["voter"]]["balance_rshares"] += rshares
        confStorage.update({"last_paid_post": last_paid_post})
    
        print("write member database")
        memberStorage.db = dataset.connect(databaseConnector2)
        member_data_list = []
        for m in member_data:
            member_data_list.append(member_data[m])
        memberStorage.add_batch(member_data_list)
        member_data_list = []