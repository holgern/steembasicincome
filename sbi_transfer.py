from beem.account import Account
from beem.amount import Amount
from beem import Steem
from beem.instance import set_shared_steem_instance
from beem.nodelist import NodeList
import re
import os
import json
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
        # print(config_data)
        accounts = config_data["accounts"]
        path = config_data["path"]
        database = config_data["database"]
        database_transfer = config_data["database_transfer"]
        databaseConnector = config_data["databaseConnector"]
        databaseConnector2 = config_data["databaseConnector2"]
        other_accounts = config_data["other_accounts"]
        mgnt_shares = config_data["mgnt_shares"]


    
    db = dataset.connect(databaseConnector)
    db2 = dataset.connect(databaseConnector2)
    accountTrx = {}
    for account in accounts:
        accountTrx[account] = AccountTrx(db, account)
        
        if not accountTrx[account].exists_table():
            accountTrx[account].create_table()
            
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
    # nodes.update_nodes()
    stm = Steem(keys=key_list, node=nodes.get_nodes(normal=True, appbase=False))
    set_shared_steem_instance(stm)    
    
    if not trxStorage.exists_table():
        trxStorage.create_table()
    
    if not memberStorage.exists_table():
        memberStorage.create_table()

    if not transactionStorage.exists_table():
        transactionStorage.create_table()

    if not transactionOutStorage.exists_table():
        transactionOutStorage.create_table()
    
    print("load member daatabase")
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

    stop_index = None
    # stop_index = addTzInfo(datetime(2018, 7, 21, 23, 46, 00))
    # stop_index = formatTimeString("2018-07-21T23:46:09")    

    for account_name in accounts:
        parse_vesting = (account_name == "steembasicincome")
        accountTrx[account_name].db = dataset.connect(databaseConnector)
        account = Account(account_name)
        # print(account["name"])
        pah = ParseAccountHist(account, path, trxStorage, transactionStorage, transactionOutStorage, member_data, steem_instance=stm)
        
        op_index = trxStorage.get_all_op_index(account["name"])
        
        if len(op_index) == 0:
            start_index = 0
            op_counter = 0
        else:
            op = trxStorage.get(op_index[-1], account["name"])
            start_index = op["index"] + 1
            op_counter = op_index[-1] + 1
        # print("start_index %d" % start_index)
        # ops = []
        # 
        if True:
            ops = accountTrx[account_name].get_all(op_types=["transfer", "delegate_vesting_shares"])
            if ops[-1]["op_acc_index"] < start_index:
                continue
            for op in ops:
                if op["op_acc_index"] < start_index:
                    continue
                if stop_index is not None and formatTimeString(op["timestamp"]) > stop_index:
                    continue
                pah.parse_op(json.loads(op["op_dict"]), parse_vesting=parse_vesting)
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
                


