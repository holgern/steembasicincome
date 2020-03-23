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
from steembi.storage import TrxDB, MemberDB, ConfigurationDB, KeysDB, TransactionMemoDB, AccountsDB
import dataset


if __name__ == "__main__":
    config_file = 'config.json'
    if not os.path.isfile(config_file):
        raise Exception("config.json is missing!")
    else:
        with open(config_file) as json_data_file:
            config_data = json.load(json_data_file)
        # print(config_data)
        databaseConnector = config_data["databaseConnector"]
        databaseConnector2 = config_data["databaseConnector2"]
        hive_blockchain = config_data["hive_blockchain"]
    start_prep_time = time.time()
    # sqlDataBaseFile = os.path.join(path, database)
    # databaseConnector = "sqlite:///" + sqlDataBaseFile
    db = dataset.connect(databaseConnector)
    db2 = dataset.connect(databaseConnector2)
    accountStorage = AccountsDB(db2)
    accounts = accountStorage.get()    
    other_accounts = accountStorage.get_transfer()
    
    confStorage = ConfigurationDB(db2)
    conf_setup = confStorage.get()
    last_cycle = conf_setup["last_cycle"]
    share_cycle_min = conf_setup["share_cycle_min"]
    
    print("sbi_store_ops_db: last_cycle: %s - %.2f min" % (formatTimeString(last_cycle), (datetime.utcnow() - last_cycle).total_seconds() / 60))
    
    if last_cycle is not None and  (datetime.utcnow() - last_cycle).total_seconds() > 60 * share_cycle_min:
        
        # Update current node list from @fullnodeupdate
        nodes = NodeList()
        nodes.update_nodes()
        # nodes.update_nodes(weights={"hist": 1})
        stm = Steem(node=nodes.get_nodes(hive=hive_blockchain))
        print(str(stm))
        
        print("Fetch new account history ops.")
        
        blockchain = Blockchain(steem_instance=stm)
        
        
        accountTrx = {}
        for account in accounts:
            if account == "steembasicincome":
                accountTrx["sbi"] = AccountTrx(db, "sbi")
            else:
                accountTrx[account] = AccountTrx(db, account)
    
        # stop_index = addTzInfo(datetime(2018, 7, 21, 23, 46, 00))
        # stop_index = formatTimeString("2018-07-21T23:46:09")
        
        for account_name in accounts:
            if account_name == "steembasicincome":
                account = Account(account_name, steem_instance=stm)
                account_name = "sbi"
            else:
                account = Account(account_name, steem_instance=stm)
            
            # Go trough all transfer ops
            cnt = 0
    
            start_block = accountTrx[account_name].get_latest_block()
            if start_block is not None:
                trx_in_block = start_block["trx_in_block"]
                op_in_trx = start_block["op_in_trx"]
                virtual_op = start_block["virtual_op"]        
                start_block = start_block["block"]
                
                print("account %s - %d" % (account["name"], start_block))
            else:
                start_block = 0
                trx_in_block = 0
                op_in_trx = 0
                virtual_op = 0
    
        
            start_index = accountTrx[account_name].get_latest_index()
            if start_index is not None:
                start_index = start_index["op_acc_index"] + 1
                # print("account %s - %d" % (account["name"], start_index))
            else:
                start_index = 0
    
            data = []
            last_block = 0
            last_trx = trx_in_block
            for op in account.history(start=start_block - 5, use_block_num=True):
                if op["block"] < start_block:
                    # last_block = op["block"]
                    continue
                elif op["block"] == start_block:
                    if op["virtual_op"] == 0:
                        if op["trx_in_block"] < trx_in_block:
                            last_trx = op["trx_in_block"]
                            continue
                        if op["op_in_trx"] <= op_in_trx and (trx_in_block != last_trx or last_block == 0):
                            continue
                    else:
                        if op["virtual_op"] <= virtual_op and (trx_in_block == last_trx):
                            continue
                start_block = op["block"]
                virtual_op = op["virtual_op"]
                trx_in_block = op["trx_in_block"]
    
                if trx_in_block != last_trx or op["block"] != last_block:
                    op_in_trx = op["op_in_trx"]
                else:
                    op_in_trx += 1
                if virtual_op > 0:
                    op_in_trx = 0
                    if trx_in_block > 255:
                        trx_in_block = 0
    
                d = {"block": op["block"], "op_acc_index": start_index, "op_acc_name": account["name"], "trx_in_block": trx_in_block,
                     "op_in_trx": op_in_trx, "virtual_op": virtual_op,  "timestamp": formatTimeString(op["timestamp"]), "type": op["type"], "op_dict": json.dumps(op)}
                #op_in_trx += 1
                start_index += 1
                last_block = op["block"]
                last_trx = trx_in_block
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
    
        # Create keyStorage
        db = dataset.connect(databaseConnector)
        trxStorage = TransferTrx(db)
        
        for account in other_accounts:
            account = Account(account, steem_instance=stm)
            cnt = 0
          
            
            start_index = trxStorage.get_latest_index(account["name"])
            if start_index is not None:
                start_index = start_index["op_acc_index"] + 1            
                print("account %s - %d" % (account["name"], start_index))
            data = []
            for op in account.history(start=start_index, use_block_num=False, only_ops=["transfer"]):
                amount = Amount(op["amount"], steem_instance=stm)
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
        print("store ops script run %.2f s" % (time.time() - start_prep_time))