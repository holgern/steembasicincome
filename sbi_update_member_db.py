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
    
    accountTrx = {}
    for account in accounts:
        if account == "steembasicincome":
            accountTrx["sbi"] = AccountTrx(db, "sbi")
        else:
            accountTrx[account] = AccountTrx(db, account)    

    
    
    print("sbi_update_member_db: last_cycle: %s - %.2f min" % (formatTimeString(last_cycle), (datetime.utcnow() - last_cycle).total_seconds() / 60))
    if last_cycle is None:
        last_cycle = datetime.utcnow() - timedelta(seconds = 60 * 145)
        confStorage.update({"last_cycle": last_cycle})
    elif False: # doing same maintanence
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
                try:
                    # sponsee = Account(processed_memo[1:], steem_instance=stm)
                    # sponsee_dict = json.dumps({sponsee["name"]: op["shares"]})
                    sponsee_dict = json.dumps(sponsee)
                    print(sponsee_dict)
                    trxStorage.update_sponsee_index(op["index"], op["source"], sponsee_dict, "Valid")
                except:
                    print("error: %s" % processed_memo)                
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
                            
    elif (datetime.utcnow() - last_cycle).total_seconds() > 60 * share_cycle_min:
        
        
        new_cycle = (datetime.utcnow() - last_cycle).total_seconds() > 60 * share_cycle_min
        current_cycle = last_cycle + timedelta(seconds=60 * share_cycle_min)
        
        
        print("Update member database, new cycle: %s" % str(new_cycle))
        # memberStorage.wipe(True)
        member_accounts = memberStorage.get_all_accounts()
        data = trxStorage.get_all_data()
        
        data = sorted(data, key=lambda x: (datetime.utcnow() - x["timestamp"]).total_seconds(), reverse=True)
        
        # Update current node list from @fullnodeupdate
        key_list = []
        key = keyStorage.get("steembasicincome", "memo")
        if key is not None:
            key_list.append(key["wif"])
        #print(key_list)
        nodes = NodeList()
        nodes.update_nodes()
        stm = Steem(keys=key_list, node=nodes.get_nodes())

        member_data = {}
        n_records = 0
        share_age_member = {}    
        for m in member_accounts:
            member_data[m] = Member(memberStorage.get(m))


        mngt_shares = 0
        delegation = {}
        delegation_timestamp = {}
        # clear shares
        for m in member_data:
            member_data[m]["shares"] = 0
            member_data[m]["bonus_shares"] = 0
            delegation[m] = 0
            delegation_timestamp[m] = None
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
                if share_type in ["DelegationLeased"]:
                    continue
                if isinstance(op["timestamp"], str):
                    timestamp = formatTimeString(op["timestamp"])
                else:
                    timestamp = op["timestamp"]                
                if share_type.lower() in ["sharetransfer"]:
                    if op["shares"] > 0 and op["sponsor"] in member_data and op["account"] in member_data:
                        if op["shares"] > member_data[op["account"]]["shares"]:
                            continue
                        member_data[op["account"]]["shares"] -= op["shares"]
                        member_data[op["sponsor"]]["shares"] += op["shares"]

                        member_data[op["sponsor"]]["latest_enrollment"] = timestamp
                        member_data[op["sponsor"]].append_share_age(timestamp, op["shares"])
                elif share_type.lower() in ["delegation"]:
                    if op["shares"] > 0 and op["sponsor"] in member_data:
                        # print("del. bonus_shares: %s - %d" % (op["sponsor"], op["shares"]))
                        delegation[op["sponsor"]] = op["shares"]
                    elif op["vests"] > 0 and op["sponsor"] in member_data:
                        sp = stm.vests_to_sp(float(op["vests"]))
                        delegation[op["sponsor"]] = int(sp / sp_share_ratio)
                    delegation_timestamp[op["sponsor"]] = timestamp
                elif share_type.lower() in ["removeddelegation"]:
                    delegation[op["sponsor"]] = 0
                    delegation_timestamp[op["sponsor"]] = None
                    
                elif share_type.lower() in ["mgmttransfer"]:
                    if op["shares"] > 0 and op["sponsor"] in member_data and op["account"] in member_data:
                        if op["shares"] > member_data[op["account"]]["bonus_shares"]:
                            continue
                        member_data[op["account"]]["bonus_shares"] -= op["shares"]
                        member_data[op["sponsor"]]["bonus_shares"] += op["shares"]

                        member_data[op["sponsor"]]["latest_enrollment"] = timestamp
                        member_data[op["sponsor"]].append_share_age(timestamp, op["shares"])                    
                    
                elif share_type.lower() in ["mgmt"]:
             
                    if op["shares"] > 0 and op["sponsor"] in member_data:
                        member_data[op["sponsor"]]["bonus_shares"] += op["shares"]
                        member_data[op["sponsor"]].append_share_age(timestamp, op["shares"])
                        mngt_shares += op["shares"]
                    else:
                        member = Member(op["sponsor"], op["shares"], timestamp)
                        member.append_share_age(timestamp, op["shares"])
                        member_data[op["sponsor"]] = member                        
                        print("mngt bonus_shares: %s - %d" % (op["sponsor"], op["shares"]))
                else:
                    sponsor = op["sponsor"]
                    sponsee = json.loads(op["sponsee"].replace('""', '"'))
                    shares = op["shares"]
                    share_age = 0
                    if isinstance(op["timestamp"], str):
                        timestamp = formatTimeString(op["timestamp"])
                    else:
                        timestamp = op["timestamp"]
        
                    if shares == 0:
                        continue
                    shares_sum += shares
                    for s in sponsee:
                        shares_sum += sponsee[s]
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
                        memo_text = memo_welcome()
                        # print("send memo %s with %s" % (sponsor, memo_text))
                        member = Member(sponsor, shares, timestamp)
                        member.append_share_age(timestamp, shares)
                        member_data[sponsor] = member
                    else:
                        
                        member_data[sponsor]["latest_enrollment"] = timestamp
                        member_data[sponsor]["shares"] += shares
                        memo_text = memo_update_shares(member_data[sponsor]["shares"])
                        # print("send memo %s with %s" % (sponsor, memo_text))
                        member_data[sponsor].append_share_age(timestamp, shares)

                    if len(sponsee) == 0:
                        continue
                    for s in sponsee:
                        shares = sponsee[s]
                        if s not in member_data:
                            memo_text = memo_sponsoring(sponsor)
                            # print("send memo %s with %s" % (s, memo_text))
                            member = Member(s, shares, timestamp)
                            member.append_share_age(timestamp, shares)
                            member_data[s] = member
                        else:
                            member_data[s]["latest_enrollment"] = timestamp
                            member_data[s]["shares"] += shares
                            memo_text = memo_sponsoring_update_shares(sponsor, member_data[s]["shares"])
                            # print("send memo %s with %s" % (s, memo_text))                            
                            member_data[s].append_share_age(timestamp, shares)

        # add bonus_shares from active delegation
        for m in member_data:
            if m in delegation:
                member_data[m]["bonus_shares"] += delegation[m]
            if m in delegation_timestamp and delegation_timestamp[m] is not None and m in delegation:
                member_data[m].append_share_age(delegation_timestamp[m], delegation[m])
        
        print("update share age")
        
        empty_shares = []
        latest_enrollment = None
        for m in member_data:
            if member_data[m]["shares"] <= 0:
                empty_shares.append(m)
                member_data[m]["total_share_days"] = 0
                member_data[m]["avg_share_age"] = 0
                continue
            member_data[m].calc_share_age()
            if latest_enrollment is None:
                latest_enrollment = member_data[m]["latest_enrollment"]
            elif latest_enrollment < member_data[m]["latest_enrollment"]:
                latest_enrollment = member_data[m]["latest_enrollment"]
        
        print("latest data timestamp: %s - latest member enrollment %s" % (str(latest_data_timestamp), str(latest_enrollment)))
        
        if False: # LessOrNoSponsee
            print("check for LessOrNoSponsee")
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
                    
        
        # for del_acc in empty_shares:
        #    del member_data[del_acc]
    
        # date_now = datetime.utcnow()
        date_now = latest_enrollment
        date_7_before = addTzInfo(date_now - timedelta(seconds=7 * 24 * 60 * 60))
        date_28_before = addTzInfo(date_now - timedelta(seconds=28 * 24 * 60 * 60))
        if new_cycle:
            
            for m in member_data:
                if member_data[m]["shares"] <= 0:
                    continue
                if "first_cycle_at" not  in member_data[m]:
                    member_data[m]["first_cycle_at"] = current_cycle
                elif member_data[m]["first_cycle_at"] < datetime(2000, 1 , 1, 0, 0, 0):
                    member_data[m]["first_cycle_at"] = current_cycle
                member_data[m]["balance_rshares"] += (member_data[m]["shares"] + member_data[m]["bonus_shares"]) * rshares_per_cycle
                member_data[m]["earned_rshares"] += (member_data[m]["shares"] + member_data[m]["bonus_shares"]) * rshares_per_cycle
                member_data[m]["subscribed_rshares"] += (member_data[m]["shares"]) * rshares_per_cycle
                member_data[m]["delegation_rshares"] += (member_data[m]["bonus_shares"]) * rshares_per_cycle
            
            print("reward voted steembasicincome post")
            # account = Account("steembasicincome", steem_instance=stm)
            
            if last_paid_post is None:
                last_paid_post = datetime(2018, 8, 9, 3, 36, 48)
            new_paid_post = last_paid_post
            for account in accounts:
                account = Account(account, steem_instance=stm)

                if account["name"] == "steembasicincome":
                    ops = accountTrx["sbi"].get_newest(op_types=["comment"], limit=500)
                else:
                    ops = accountTrx[account["name"]].get_newest(op_types=["comment"], limit=500)
                blog = []
                for op in ops[::-1]:
                    comment = (json.loads(op["op_dict"]))    
                    if comment["parent_author"] == "" and comment["author"] == account["name"] and formatTimeString(comment["timestamp"]) > addTzInfo(last_paid_post):
                        try:
                            c = Comment(comment, steem_instance=stm)
                            c.refresh()
                            blog.append(c)
                        except:
                            continue
                
                for post in blog:
                    if post["created"] <= addTzInfo(last_paid_post):
                        continue
                    if post.is_pending():
                        continue
                    if post.is_comment():
                        continue
                    if post["author"] != account["name"]:
                        continue
                    if post["created"] > addTzInfo(new_paid_post):
                        new_paid_post = post["created"]            
                    last_paid_post = post["created"]
                    all_votes = ActiveVotes(post["authorperm"])
                    for vote in all_votes:
                        if vote["voter"] in member_data:
                            if member_data[vote["voter"]]["shares"] <= 0:
                                continue
                            if account["name"] == "steembasicincome":
                                rshares = vote["rshares"] * upvote_multiplier
                                if rshares < rshares_per_cycle:
                                    rshares = rshares_per_cycle
                            else:
                                rshares = vote["rshares"] * upvote_multiplier
                            member_data[vote["voter"]]["earned_rshares"] += rshares
                            member_data[vote["voter"]]["curation_rshares"] += rshares
                            member_data[vote["voter"]]["balance_rshares"] += rshares
            confStorage.update({"last_paid_post": last_paid_post})


        print("reward voted steembasicincome comment")
        if last_paid_comment is None:
            last_paid_comment = datetime(2018, 8, 9, 3, 36, 48)
        new_paid_comment = last_paid_comment
        for account in accounts:
            account = Account(account, steem_instance=stm)

            if account["name"] == "steembasicincome":
                ops = accountTrx["sbi"].get_newest(op_types=["comment"], limit=500)
            else:
                ops = accountTrx[account["name"]].get_newest(op_types=["comment"], limit=500)
            posts = []
            for op in ops[::-1]:
                comment = (json.loads(op["op_dict"]))    
                if comment["parent_author"] != "" and comment["author"] == account["name"] and formatTimeString(comment["timestamp"]) > addTzInfo(last_paid_comment):
                    try:
                        c = Comment(comment, steem_instance=stm)
                        c.refresh()
                        posts.append(c)
                    except:
                        continue

            for post in posts:
                if post["created"] <= addTzInfo(last_paid_comment):
                    break
                if post.is_pending():
                    continue
                if not post.is_comment():
                    continue
                if post["author"] != account["name"]:
                    continue
                if post["created"] > addTzInfo(new_paid_comment):
                    new_paid_comment = post["created"]
                all_votes = ActiveVotes(post["authorperm"])
                for vote in all_votes:
                    if vote["voter"] in member_data:
                        if member_data[vote["voter"]]["shares"] <= 0:
                            continue                    
                        rshares = vote["rshares"]
                        if rshares < 50000000:
                            continue
                        rshares = rshares * upvote_multiplier
                        member_data[vote["voter"]]["earned_rshares"] += rshares
                        member_data[vote["voter"]]["curation_rshares"] += rshares
                        member_data[vote["voter"]]["balance_rshares"] += rshares
        confStorage.update({"last_paid_comment": new_paid_comment})


    
        print("write member database")
        memberStorage.db = dataset.connect(databaseConnector2)
        member_data_list = []
        member_data_json = []
        for m in member_data:
            member_data_list.append(member_data[m])
            member_json = member_data[m].copy()
            if "last_comment" in member_json and member_json["last_comment"] is not None:
                member_json["last_comment"] = str(member_json["last_comment"])
            if "last_post" in member_json and member_json["last_post"] is not None:
                member_json["last_post"] = str(member_json["last_post"])
            if "original_enrollment" in member_json and member_json["original_enrollment"] is not None:
                member_json["original_enrollment"] = str(member_json["original_enrollment"])
            if "latest_enrollment" in member_json and member_json["latest_enrollment"] is not None:
                member_json["latest_enrollment"] = str(member_json["latest_enrollment"])
            if "updated_at" in member_json and member_json["updated_at"] is not None:
                member_json["updated_at"] = str(member_json["updated_at"])
            if "first_cycle_at" in member_json and member_json["first_cycle_at"] is not None:
                member_json["first_cycle_at"] = str(member_json["first_cycle_at"])
            member_data_json.append(member_json)
        memberStorage.add_batch(member_data_list)
        member_data_list = []
        with open('/var/www/html/data.json', 'w') as outfile:
            json.dump(member_data_json, outfile)        
        member_data_json = []
        
        

        if new_cycle:
            confStorage.update({"last_cycle": last_cycle + timedelta(seconds=60 * share_cycle_min)})
        
        # Statistics
        shares = 0
        bonus_shares = 0
        
        delegation_shares = 0
        for m in member_data:
            shares += member_data[m]["shares"]
            bonus_shares += member_data[m]["bonus_shares"]
            if m in delegation:
                delegation_shares += delegation[m]
        print("shares: %d" % shares)
        print("delegation bonus shares: %d" % delegation_shares)
        print("Mngt bonus shares %d" % (mngt_shares))
        
    print("update member script run %.2f s" % (time.time() - start_prep_time))
