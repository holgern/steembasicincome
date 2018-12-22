from beem.account import Account
from beem.amount import Amount
from beem import Steem
from beem.instance import set_shared_steem_instance
from beem.nodelist import NodeList
import re
import os
import json
import time
from time import sleep
from steembi.parse_hist_op import ParseAccountHist
from steembi.storage import TrxDB, MemberDB, TransactionMemoDB, TransactionOutDB, KeysDB
from steembi.transfer_ops_storage import TransferTrx, AccountTrx
import dataset
from datetime import datetime
from steembi.member import Member
    

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
        other_accounts = config_data["other_accounts"]
        mgnt_shares = config_data["mgnt_shares"]


    start_prep_time = time.time()
    db = dataset.connect(databaseConnector)
    db2 = dataset.connect(databaseConnector2)
    accountTrx = {}
    for account in accounts:
        accountTrx[account] = AccountTrx(db, account)
        
        if not accountTrx[account].exists_table():
            accountTrx[account].create_table()
    accountTrx["sbi"] = AccountTrx(db, "sbi")
    
    # Create keyStorage
    trxStorage = TrxDB(db2)
    memberStorage = MemberDB(db2)
    keyStorage = KeysDB(db2)
    transactionStorage = TransactionMemoDB(db2)
    transactionOutStorage = TransactionOutDB(db2)
    
    key_list = []
    print("Parse new transfers.")
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
    # set_shared_steem_instance(stm)    
    
    if not trxStorage.exists_table():
        trxStorage.create_table()
    
    if not memberStorage.exists_table():
        memberStorage.create_table()

    if not transactionStorage.exists_table():
        transactionStorage.create_table()

    if not transactionOutStorage.exists_table():
        transactionOutStorage.create_table()
    
    # print("load member database")
    member_accounts = memberStorage.get_all_accounts()
    member_data = {}
    n_records = 0
    share_age_member = {}    
    for m in member_accounts:
        member_data[m] = Member(memberStorage.get(m))
    
    
    if True:
        print("delete from transaction_memo... ")
        transactionStorage.delete_sender("dtube.rewards")
        transactionStorage.delete_to("sbi2")
        transactionStorage.delete_to("sbi3")
        transactionStorage.delete_to("sbi4")
        transactionStorage.delete_to("sbi5")
        transactionStorage.delete_to("sbi6")
        transactionStorage.delete_to("sbi7")
        transactionStorage.delete_to("sbi8")
        transactionStorage.delete_to("sbi9")
        transactionStorage.delete_to("sbi10")

    stop_index = None
    # stop_index = addTzInfo(datetime(2018, 7, 21, 23, 46, 00))
    # stop_index = formatTimeString("2018-07-21T23:46:09")    

    for account_name in accounts:
        if account_name == "steembasicincome":
            account_trx_name = "sbi"
        else:
            account_trx_name = account_name
        parse_vesting = (account_name == "steembasicincome")
        accountTrx[account_trx_name].db = dataset.connect(databaseConnector)
        account = Account(account_name, steem_instance=stm)
        # print(account["name"])
        pah = ParseAccountHist(account, "", trxStorage, transactionStorage, transactionOutStorage, member_data, memberStorage=memberStorage, steem_instance=stm)
        
        op_index = trxStorage.get_all_op_index(account["name"])
        
        if len(op_index) == 0:
            start_index = 0
            op_counter = 0
            start_index_offset = 0
        else:
            op = trxStorage.get(op_index[-1], account["name"])
            start_index = op["index"] + 1
            op_counter = op_index[-1] + 1
            if account_name == "steembasicincome":
                start_index_offset = 316
            else:
                start_index_offset = 0

        # print("start_index %d" % start_index)
        # ops = []
        # 
        if True:
            
            ops = accountTrx[account_trx_name].get_all(op_types=["transfer", "delegate_vesting_shares"])
            if len(ops) == 0:
                continue
            
            if ops[-1]["op_acc_index"] < start_index - start_index_offset:
                continue
            for op in ops:
                if op["op_acc_index"] < start_index - start_index_offset:
                    continue
                if stop_index is not None and formatTimeString(op["timestamp"]) > stop_index:
                    continue
                json_op = json.loads(op["op_dict"])
                json_op["index"] = op["op_acc_index"] + start_index_offset
                if account_name != "steembasicincome" and json_op["type"] == "transfer":
                    if float(Amount(json_op["amount"], steem_instance=stm)) < 1:
                        continue
                    if json_op["memo"][:8] == 'https://':
                        continue
                    
                pah.parse_op(json_op, parse_vesting=parse_vesting)
                # op_counter += 1
                # if (op_counter % 100) == 0 and op_counter > 0 and (account_name == "steembasicincome") and False:
                #    pah.add_mngt_shares(json.loads(op["op_dict"]), mgnt_shares, op_counter)
                #    op_counter += len(mgnt_shares)
                
        else:
            for op in account.history(start=start_index, use_block_num=False):
                pah.parse_op(op, parse_vesting=parse_vesting)
                op_counter += 1
                if (op_counter % 100) == 0 and (account_name == "steembasicincome"):
                    pah.add_mngt_shares(op, mgnt_shares)                
                


    print("transfer script run %.2f s" % (time.time() - start_prep_time))