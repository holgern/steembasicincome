from beem.account import Account
from beem.comment import Comment
from beem.vote import ActiveVotes
from beem.amount import Amount
from beem import Steem
from beem.instance import set_shared_steem_instance
from beem.nodelist import NodeList
from beem.memo import Memo
from beem.utils import addTzInfo, resolve_authorperm, formatTimeString, construct_authorperm
from datetime import datetime, timedelta
import requests
import re
import json
import os
from time import sleep
import dataset
from steembi.parse_hist_op import ParseAccountHist
from steembi.storage import TrxDB, MemberDB, ConfigurationDB, KeysDB, TransactionMemoDB
from steembi.transfer_ops_storage import TransferTrx, AccountTrx, MemberHistDB
from steembi.memo_parser import MemoParser
from steembi.member import Member


def memo_sp_delegation(new_shares, sp_per_share):
    memo = "Thank you for your SP delegation! Your shares have increased by %d (%d SP = +1 bonus share)" % (new_shares, sp_per_share)
    return memo

def memo_sp_adjustment(shares, sp_per_share):
    memo = "@steembasicincome has adjusted your shares according to your recalled delegation."
    memo += "If you decide to delegate again, %dSP = +1 bonus share. You still have %d shares and will continue to receive upvotes" % (sp_per_share, shares)
    return memo

def memo_welcome():
    memo = "Your enrollment to Steem Basic Income has been processed."
    return memo

def memo_sponsoring(sponsor):
    memo = "Congratulations! thanks to @%s you have been enrolled in Steem Basic Income." % (sponsor)
    memo += "Learn more at https://steemit.com/basicincome/@steembasicincome/steem-basic-income-a-complete-overview"

def memo_update_shares(shares):
    memo = "Your Steem Basic Income has been increased. You now have %d shares!" % shares
    return memo

def memo_sponsoring_update_shares(sponsor, shares):
    memo = "Congratulations! thanks to @%s your Steem Basic Income has been increased. You now have " % sponsor
    memo += "%d shares! Learn more at https://steemit.com/basicincome/@steembasicincome/steem-basic-income-a-complete-overview" % shares
    return memo
    

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
    keyStorage = KeysDB(db2)
    memberStorage = MemberDB(db2)
    # accountStorage = MemberHistDB(db)
    confStorage = ConfigurationDB(db2)
    transactionStorage = TransactionMemoDB(db2)
    
    conf_setup = confStorage.get()
    
    last_cycle = conf_setup["last_cycle"]
    share_cycle_min = conf_setup["share_cycle_min"]
    sp_share_ratio = conf_setup["sp_share_ratio"]
    rshares_per_cycle = conf_setup["rshares_per_cycle"]
    upvote_multiplier = conf_setup["upvote_multiplier"]
    last_paid_post = conf_setup["last_paid_post"]
    last_paid_comment = conf_setup["last_paid_comment"]
    

    
    
    print("last_cycle: %s - %.2f min" % (formatTimeString(last_cycle), (datetime.utcnow() - last_cycle).total_seconds() / 60))
    if last_cycle is None:
        last_cycle = datetime.utcnow() - timedelta(seconds = 60 * 145)
        confStorage.update({"last_cycle": last_cycle})
    elif True: # doing same maintanence
        data = trxStorage.get_all_data()
        data = sorted(data, key=lambda x: (datetime.utcnow() - x["timestamp"]).total_seconds(), reverse=True)
        # data = sorted(data, key=lambda x: (datetime.utcnow() - x["timestamp"]).total_seconds(), reverse=True)
        key_list = []
        key = keyStorage.get("steembasicincome", "memo")
        if key is not None:
            key_list.append(key["wif"])
        #print(key_list)
        nodes = NodeList()
        try:
            nodes.update_nodes()
        except:
            print("could not update nodes")        
        stm = Steem(keys=key_list, node=nodes.get_nodes())        
        if True: # check if member are blacklisted
            member_accounts = memberStorage.get_all_accounts()
            member_data = {}
            n_records = 0
            share_age_member = {}    
            for m in member_accounts:
                member_data[m] = Member(memberStorage.get(m))
            cnt = 0
            for m in member_data:
                cnt += 1
                if cnt % 100 == 0:
                    print("%d/%d" % (cnt, len(member_data)))
                    
                    
                response = ""
                cnt2 = 0
                while str(response) != '<Response [200]>' and cnt2 < 10:
                    response = requests.get("http://blacklist.usesteem.com/user/%s" % m)
                    cnt2 += 1
                    
                
                if "blacklisted" in response.json():
                    if "steemcleaners" in response.json()["blacklisted"]:
                        member_data[m]["steemcleaners"] = True
                    else:
                        member_data[m]["steemcleaners"] = False
                    if "buildawhale" in response.json()["blacklisted"]:
                        member_data[m]["buildawhale"] = True
                    else:
                        member_data[m]["buildawhale"] = False
                
            
            print("write member database")
            member_data_list = []
            for m in member_data:
                member_data_list.append(member_data[m])
            memberStorage.add_batch(member_data_list)
            member_data_list = []