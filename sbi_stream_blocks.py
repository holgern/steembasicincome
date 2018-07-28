from beem.account import Account
from beem.amount import Amount
from beem import Steem
from beem.instance import set_shared_steem_instance
from beem.nodelist import NodeList
from beem.blockchain import Blockchain
from beem.utils import formatTimeString, addTzInfo
from datetime import datetime
import re
import os
import json
import time
from steembi.transfer_ops_storage import TransferTrx, AccountTrx
from steembi.storage import TrxDB, MemberDB
from steembi.parse_hist_op import ParseAccountHist
from steembi.memo_parser import MemoParser
from steembi.member import Member
import dataset





if __name__ == "__main__":
    config_file = 'config.json'
    if not os.path.isfile(config_file):
        accounts = ["steembasicincome", "sbi2", "sbi3", "sbi4", "sbi5", "sbi6", "sbi7", "sbi8", "sbi9"]
        path = "E:\\sbi\\"
        database = "sbi_ops.sqlite"
        database_transfer = "sbi_transfer.sqlite"
        databaseConnector = None
        other_accounts = ["minnowbooster"]
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
    
    # sqlDataBaseFile = os.path.join(path, database)
    # databaseConnector = "sqlite:///" + sqlDataBaseFile
    while True:
    
        db = dataset.connect(databaseConnector)    
        
        
        # Update current node list from @fullnodeupdate
        nodes = NodeList()
        nodes.update_nodes(weights={"hist": 1})
        stm = Steem(node=nodes.get_nodes(appbase=False, https=False))
        print(str(stm))
        set_shared_steem_instance(stm)
        
        blockchain = Blockchain()

        
        accountTrx = {}
        newAccountTrxStorage = False
        for account in accounts:
            accountTrx[account] = AccountTrx(db, account)
            
            if not accountTrx[account].exists_table():
                newAccountTrxStorage = True
                accountTrx[account].create_table()
    
        for account_name in accounts:
            account = Account(account_name)
            print("account %s" % account["name"])
            # Go trough all transfer ops
            cnt = 0
            if newAccountTrxStorage:
                ops = []
                start_index = None
            else:
                start_index = accountTrx[account_name].get_latest_index()
                if start_index is not None:
                    start_index = start_index["op_acc_index"] + 1
                    print(start_index)
            data = []
            for op in account.history(start=start_index, stop=None, use_block_num=False):
                virtual_op = op["virtual_op"]
                trx_in_block = op["trx_in_block"]
                if virtual_op > 0:
                    trx_in_block = -1
                d = {"block": op["block"], "op_acc_index": op["index"], "op_acc_name": account["name"], "trx_in_block": trx_in_block,
                     "op_in_trx": op["op_in_trx"], "virtual_op": virtual_op,  "timestamp": formatTimeString(op["timestamp"]), "type": op["type"], "op_dict": json.dumps(op)}
                data.append(d)
                if cnt % 1000 == 0:
                    print(op["timestamp"])
                    accountTrx[account_name].add_batch(data)
                    data = []
                cnt += 1
            if len(data) > 0:
                print(op["timestamp"])
                accountTrx[account_name].add_batch(data)
                data = []
    
        trxStorage = TransferTrx(db)
        
        newTrxStorage = False
        if not trxStorage.exists_table():
            newTrxStorage = True
            trxStorage.create_table()
        for account in other_accounts:
            account = Account(account)
            print("account %s" % account["name"])
            cnt = 0
            if newTrxStorage:
                ops = []
                start_index = None
            else:
                start_index = trxStorage.get_latest_index(account["name"])
                if start_index is not None:
                    start_index = start_index["op_acc_index"] + 1            
                    print(start_index)
            data = []
            for op in account.history(start=start_index, use_block_num=False, only_ops=["transfer"]):
                amount = Amount(op["amount"])
                virtual_op = op["virtual_op"]
                trx_in_block = op["trx_in_block"]
                if virtual_op > 0:
                    trx_in_block = -1
                memo = ascii(op["memo"])
                d = {"block": op["block"], "op_acc_index": op["index"], "op_acc_name": account["name"], "trx_in_block": trx_in_block,
                     "op_in_trx": op["op_in_trx"], "virtual_op": virtual_op, "timestamp": formatTimeString(op["timestamp"]), "from": op["from"], "to": op["to"],
                        "amount": amount.amount, "amount_symbol": amount.symbol, "memo": memo, "op_type": op["type"]}
                data.append(d)
                if cnt % 1000 == 0:
                    print(op["timestamp"])
                    trxStorage.add_batch(data)
                    data = []
                cnt += 1
            if len(data) > 0:
                print(op["timestamp"])
                trxStorage.add_batch(data)
                data = []
                
        db2 = dataset.connect(databaseConnector2)
        trxStorage = TrxDB(db2)
        memberStorage = MemberDB(db2)    
        
        stop_index = None
        # stop_index = addTzInfo(datetime(2018, 7, 21, 23, 46, 00))
        # stop_index = formatTimeString("2018-07-21T23:46:09")
        
        for account_name in ["steembasicincome"]:
            parse_vesting = (account_name == "steembasicincome")
            accountTrx[account_name].db = dataset.connect(databaseConnector)
            account = Account(account_name)
            print(account["name"])
            pah = ParseAccountHist(account, path, trxStorage)
            memo_parser = MemoParser()
            
            op_index = trxStorage.get_all_op_index(account["name"])
            
            if len(op_index) == 0:
                start_index = 0
                op_counter = 0
            else:
                op = trxStorage.get(op_index[-1])
                start_index = op["index"] + 1
                op_counter = op_index[-1] + 1
            print("start_index %d" % start_index)
            # ops = []
            # 
            ops = accountTrx[account_name].get_all(op_types=["transfer", "delegate_vesting_shares"])
            if ops[-1]["op_acc_index"] < start_index:
                continue
            for op in ops:
                if op["op_acc_index"] < start_index:
                    continue
                if stop_index is not None and addTzInfo(op["timestamp"]) > stop_index:
                    continue
                op = json.loads(op["op_dict"])
                
                if op['type'] == "delegate_vesting_shares" and parse_vesting:
                    vests = Amount(op['vesting_shares'])
                    #print(op)
                    #if op['delegator'] == account_name:
                    #    delegation = {'account': op['delegatee'], 'amount': vests}
                    #    pah.update_delegation(op, 0, delegation)
                    #    return
                    #if op['delegatee'] == account_name:
                    #    delegation = {'account': op['delegator'], 'amount': vests}
                    #    pah.update_delegation(op, delegation, 0)
        
                elif op['type'] == "transfer":
                    amount = Amount(op['amount'])
                    # print(op)
                    #if op['from'] == account_name and op["to"] not in pah.excluded_accounts:
                    #    pah.parse_transfer_out_op(op)
        
                    if op['to'] == account_name and op["from"] not in pah.excluded_accounts:
                    #    pah.parse_transfer_in_op(op)
                        share_type = "Standard"
                        if amount.amount < 1:
                            continue
                        if amount.symbol == "SBD":
                            share_type = "SBD"
                
                        index = op["index"]
                        account = op["from"]
                        timestamp = op["timestamp"]
                        sponsee = {}
                        memo = op["memo"]
                        shares = int(amount.amount)
                        if memo.lower().replace(',', '  ').replace('"', '') == "":
                            print("Error: will return %.3f to %s" % (float(amount), account))
                            continue
                            
                        [sponsor, sponsee, not_parsed_words, account_error] = memo_parser.parse_memo(memo, shares, account)
                        
                        sponsee_amount = 0
                        for a in sponsee:
                            sponsee_amount += sponsee[a]                  
                        if sponsee_amount == 0 and not account_error:
                            print("Error: will return %.3f to %s" % (float(amount), account))
                            continue
                        if sponsee_amount != shares and not account_error:
                            print("Error: will return %.3f to %s" % (float(amount), account))
                            continue
                        if account_error:
                            print("Error: will return %.3f to %s" % (float(amount), account))
                            continue
                        print("Sucess: accept %d shares for %s and %s" % (shares, sponsor, str(sponsee)))
                                      
                    # print(op, vests)
                    # self.update(ts, vests, 0, 0)
                                    
                # update member table
                # add rshares
                
                
                #if (op_counter % 100) == 0 and (account_name == "steembasicincome"):
                #    pah.add_mngt_shares(json.loads(op["op_dict"]), mgnt_shares)
                op_counter += 1
        
        print("sleeping now...")
        time.sleep(60)