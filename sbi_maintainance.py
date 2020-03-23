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
import time
from time import sleep
import dataset
from steembi.parse_hist_op import ParseAccountHist
from steembi.storage import TrxDB, MemberDB, ConfigurationDB, KeysDB, TransactionMemoDB, AccountsDB
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
        raise Exception("config.json is missing!")
    else:
        with open(config_file) as json_data_file:
            config_data = json.load(json_data_file)
        # print(config_data)
        accounts = config_data["accounts"]
        databaseConnector = config_data["databaseConnector"]
        databaseConnector2 = config_data["databaseConnector2"]
        mgnt_shares = config_data["mgnt_shares"]
        hive_blockchain = config_data["hive_blockchain"]
        
    start_prep_time = time.time()
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

    accountStorage = AccountsDB(db2)
    accounts = accountStorage.get()
    other_accounts = accountStorage.get_transfer()     
    
    conf_setup = confStorage.get()
    
    last_cycle = conf_setup["last_cycle"]
    share_cycle_min = conf_setup["share_cycle_min"]
    sp_share_ratio = conf_setup["sp_share_ratio"]
    rshares_per_cycle = conf_setup["rshares_per_cycle"]
    upvote_multiplier = conf_setup["upvote_multiplier"]
    last_paid_post = conf_setup["last_paid_post"]
    last_paid_comment = conf_setup["last_paid_comment"]
    last_delegation_check = conf_setup["last_delegation_check"]
    minimum_vote_threshold = conf_setup["minimum_vote_threshold"]
    upvote_multiplier_adjusted = conf_setup["upvote_multiplier_adjusted"]
    
    accountTrx = {}
    for account in accounts:
        if account == "steembasicincome":
            accountTrx["sbi"] = AccountTrx(db, "sbi")
        else:
            accountTrx[account] = AccountTrx(db, account)    

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
    stm = Steem(keys=key_list, node=nodes.get_nodes(hive=hive_blockchain))

    if False: # check if member are blacklisted
        member_accounts = memberStorage.get_all_accounts()
        member_data = {}
        n_records = 0
        share_age_member = {}    
        for m in member_accounts:
            member_data[m] = Member(memberStorage.get(m))
        
        for m in member_data:
            response = requests.get("http://blacklist.usesteem.com/user/%s" % m)
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
    if False: # LessOrNoSponsee
        memo_parser = MemoParser(steem_instance=stm)            
        for op in data:
            if op["status"] != "LessOrNoSponsee":
                continue
            processed_memo = ascii(op["memo"]).replace('\n', '').replace('\\n', '').replace('\\', '')
            print(processed_memo)                
            if processed_memo[1] == '@':
                processed_memo = processed_memo[1:-1]
                
            if processed_memo[2] == '@':
                processed_memo = processed_memo[2:-2]
            [sponsor, sponsee, not_parsed_words, account_error] = memo_parser.parse_memo(processed_memo, op["shares"], op["account"])
            sponsee_amount = 0
            for a in sponsee:
                sponsee_amount += sponsee[a]
            if account_error:
                continue
            if sponsee_amount != op["shares"]:
                continue
            for m in member_data:
                member_data[m].calc_share_age_until(op["timestamp"])
                
            max_avg_share_age = 0
            sponsee_name = None
            for m in member_data:  
                if max_avg_share_age < member_data[m]["avg_share_age"]:
                    max_avg_share_age = member_data[m]["avg_share_age"]
                    sponsee_name = m
            if sponsee_amount == 0 and sponsee_name is not None:
                sponsee = {sponsee_name: shares}
                sponsee_dict = json.dumps(sponsee)
                print(sponsee_dict)
                trxStorage.update_sponsee_index(op["index"], op["source"], sponsee_dict, "Valid")       
               
    if False: # deal with encrypted memos
        print("check for encrypted memos")

        set_shared_steem_instance(stm)
        for op in data:
            if op["status"] != "LessOrNoSponsee":
                continue
            processed_memo = ascii(op["memo"]).replace('\n', '')
            processed_memo = ascii(op["memo"]).replace('\n', '')
            if processed_memo[1] == '#':
                processed_memo = processed_memo[1:-1]
            if processed_memo[2] == '#':
                processed_memo = processed_memo[2:-2]
            print("processed_memo: %s, source: %s" %(processed_memo, op["source"]))
            if len(processed_memo) > 1 and (processed_memo[0] == '#' or processed_memo[1] == '#') and  op["source"] == "steembasicincome":

                print("found: %s" % processed_memo)
                memo = Memo(op["source"], op["account"], steem_instance=stm)
                processed_memo = ascii(memo.decrypt(processed_memo)).replace('\n', '')
                print("decrypt memo %s" % processed_memo)
                trxStorage.update_memo(op["source"], op["account"], op["memo"], processed_memo)
    if False: # deal with encrypted memos
        print("check for encrypted memos")

        for op in transactionStorage.get_all():

            processed_memo = ascii(op["memo"]).replace('\n', '')
            processed_memo = ascii(op["memo"]).replace('\n', '')
            if processed_memo[1] == '#':
                processed_memo = processed_memo[1:-1]
            if processed_memo[2] == '#':
                processed_memo = processed_memo[2:-2]
            # print("processed_memo: %s, to: %s" %(processed_memo, op["to"]))
            if len(processed_memo) > 1 and (processed_memo[0] == '#' or processed_memo[1] == '#') and  op["to"] == "steembasicincome":

                print("found: %s" % processed_memo)
                memo = Memo(op["to"], op["sender"], steem_instance=stm)
                dec_memo = memo.decrypt(processed_memo)
                processed_memo = ascii(dec_memo).replace('\n', '')
                print("decrypt memo %s" % processed_memo)
                transactionStorage.update_memo(op["sender"], op["to"], op["memo"], processed_memo, True)

    if False:  #check when sponsor is different from account
        print('check sponsor accounts')
        memo_parser = MemoParser(steem_instance=stm)
        for op in data:
            if op["status"] != "Valid":
                continue
            if op["memo"] is None:
                continue
            if ":@" not in op["memo"]:
                continue
            processed_memo = ascii(op["memo"]).replace('\n', '').replace('\\n', '').replace('\\', '')
            print(processed_memo)                
            if processed_memo[1] == '@':
                processed_memo = processed_memo[1:-1]
                
            if processed_memo[2] == '@':
                processed_memo = processed_memo[2:-2]
            [sponsor, sponsee, not_parsed_words, account_error] = memo_parser.parse_memo(processed_memo, op["shares"], op["account"])
            sponsee_amount = 0
            for a in sponsee:
                sponsee_amount += sponsee[a]
            if account_error:
                continue
            if sponsee_amount != op["shares"]:
                continue
            if sponsor == op["sponsor"]:
                continue
            try:

                trxStorage.update_sponsor_index(op["index"], op["source"], sponsor, "Valid")
            except:
                print("error: %s" % processed_memo)  

    if False:  #check accountDoesNotExists
        print('check not existing accounts')

        memo_parser = MemoParser(steem_instance=stm)
        for op in data:
            if op["status"] != "AccountDoesNotExist":
                continue
            processed_memo = ascii(op["memo"]).replace('\n', '').replace('\\n', '').replace('\\', '')
            print(processed_memo)                
            if processed_memo[1] == '@':
                processed_memo = processed_memo[1:-1]
                
            if processed_memo[2] == '@':
                processed_memo = processed_memo[2:-2]
            [sponsor, sponsee, not_parsed_words, account_error] = memo_parser.parse_memo(processed_memo, op["shares"], op["account"])
            sponsee_amount = 0
            for a in sponsee:
                sponsee_amount += sponsee[a]
            if account_error:
                continue
            if sponsee_amount != op["shares"]:
                continue
            try:
                # sponsee = Account(processed_memo[1:], steem_instance=stm)
                # sponsee_dict = json.dumps({sponsee["name"]: op["shares"]})
                sponsee_dict = json.dumps(sponsee)
                print(sponsee_dict)
                trxStorage.update_sponsee_index(op["index"], op["source"], sponsee_dict, "Valid")
            except:
                print("error: %s" % processed_memo)   
    if False: # fix memos with \n\n
        print("check for memos with \\n")
        for op in data:
            if op["status"] != "AccountDoesNotExist":
                continue
            if len(op["memo"]) < 4:
                continue
            processed_memo = ascii(op["memo"]).replace('\n', '').replace('\\n', '').replace('\\', '')
            if processed_memo[1] == '@':
                processed_memo = processed_memo[1:-1]
            if processed_memo[2] == '@':
                processed_memo = processed_memo[2:-2]                
            if processed_memo[0] != "@":
                continue
            try:
                sponsee = Account(processed_memo[1:])
                sponsee_dict = json.dumps({sponsee["name"]: op["shares"]})
                print(sponsee_dict)
                trxStorage.update_sponsee_index(op["index"], op["source"], sponsee_dict, "Valid")
            except:
                print("error: %s" % processed_memo)

    if False: # fix memos with \n\n
        print('check not existing accounts')
        memo_parser = MemoParser(steem_instance=stm)
        for op in data:
            if op["status"] != "Valid":
                continue
            if op["sponsee"].find("karthikdtrading1") == -1:
                continue                

            processed_memo = ascii(op["memo"]).replace('\n', '').replace('\\n', '').replace('\\', '')
            print(processed_memo)                
            if processed_memo[1] == '@':
                processed_memo = processed_memo[1:-1]
                
            if processed_memo[2] == '@':
                processed_memo = processed_memo[2:-2]
            [sponsor, sponsee, not_parsed_words, account_error] = memo_parser.parse_memo(processed_memo, op["shares"], op["account"])
            sponsee_amount = 0
            for a in sponsee:
                sponsee_amount += sponsee[a]
            if account_error:
                continue
            if sponsee_amount != op["shares"]:
                continue
            try:
                # sponsee = Account(processed_memo[1:], steem_instance=stm)
                # sponsee_dict = json.dumps({sponsee["name"]: op["shares"]})
                sponsee_dict = json.dumps(sponsee)
                print(sponsee_dict)
                trxStorage.update_sponsee_index(op["index"], op["source"], sponsee_dict, "Valid")
            except:
                print("error: %s" % processed_memo)           

    if False: #check all trx datasets
        print("check trx dataset")
        for op in data:
            if op["status"] == "Valid":
                try:
                    share_type = op["share_type"]
                    sponsor = op["sponsor"]
                    sponsee = json.loads(op["sponsee"])
                    shares = op["shares"]
                    share_age = 0
                    if isinstance(op["timestamp"], str):
                        timestamp = formatTimeString(op["timestamp"])
                    else:
                        timestamp = op["timestamp"]
                except:
                    print("error at: %s" % str(op))
    if False: #reset last cycle
        print("reset last cycle")
        confStorage.update({"last_cycle": last_cycle - timedelta(seconds=60 * share_cycle_min)})
    if False: # reset management shares
        print("Reset all mgmt trx data")
        trxStorage.delete_all("mgmt")
        shares_sum = 0
        total_mgnt_shares_sum = 0
        mngt_shares_sum = 0
        share_mngt_round = 0
        for account in mgnt_shares:
            share_mngt_round += mgnt_shares[account]
        start_index = 0
        for op in data:
            if op["status"] == "Valid":
                share_type = op["share_type"]            
                if share_type.lower() not in ["delegation", "removeddelegation", "delegationleased", "mgmt", "mgmttransfer", "sharetransfer"]:
                    shares = op["shares"]
                    sponsee = json.loads(op["sponsee"])
                    shares_sum += shares
                    mngt_shares_sum += shares
                    for s in sponsee:
                        shares_sum += sponsee[s]
                        mngt_shares_sum += sponsee[s]
                    if mngt_shares_sum >= (100):
                        mngt_shares_sum -= (100)
                        timestamp = op["timestamp"]
                        memo = ""
                        for account in mgnt_shares:
                            mngt_shares = mgnt_shares[account]
                            total_mgnt_shares_sum += mngt_shares
                            sponsor = account
                            mngtData = {"index": start_index, "source": "mgmt", "memo": "", "account": account, "sponsor": sponsor, "sponsee": {}, "shares": mngt_shares, "vests": float(0), "timestamp": formatTimeString(timestamp),
                                     "status": "Valid", "share_type": "Mgmt"}
                            start_index += 1
                            trxStorage.add(mngtData)
        print("total_mgnt_shares_sum: %d - shares %d" % (total_mgnt_shares_sum, shares_sum))
    # check delegation
    if False:
        delegation = {}
        sum_sp = {}
        sum_sp_shares = {}
        sum_sp_leased = {}
        account = "steembasicincome"
        excluded_accounts = ["blocktrades"]
        delegation = {}
        delegation_shares = {}
        sum_sp = 0
        sum_sp_leased = 0
        sum_sp_shares = 0
        
        print("load delegation")
        for d in trxStorage.get_share_type(share_type="Delegation"):
            if d["share_type"] == "Delegation":
                delegation[d["account"]] = stm.vests_to_sp(float(d["vests"]))
            delegation_shares[d["account"]] = d["shares"]
        for d in trxStorage.get_share_type(share_type="RemovedDelegation"):
            if d["share_type"] == "RemovedDelegation":
                delegation[d["account"]] = 0
            delegation_shares[d["account"]] = 0
        
        delegation_leased = {}
        delegation_shares = {}
        print("update delegation")
        delegation_account = delegation
        for acc in delegation_account:
            if delegation_account[acc] == 0:
                continue
            # if acc in delegation_shares and delegation_shares[acc] > 0:
            #    continue
            print(acc)
            leased = transferStorage.find(acc, account)
            if len(leased) == 0:
                delegation_shares[acc] = delegation_account[acc]
                shares = int(delegation_account[acc] / sp_share_ratio)
                trxStorage.update_delegation_shares(account, acc, shares)
                continue
            delegation_leased[acc] = delegation_account[acc]
            trxStorage.update_delegation_state(account, acc, "Delegation", 
                                              "DelegationLeased")