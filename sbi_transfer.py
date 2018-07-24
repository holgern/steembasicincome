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
from steembi.storage import Trx, Member
from steembi.transfer_ops_storage import TransferTrx, AccountTrx
import dataset
from datetime import datetime
    

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

    nodes = NodeList()
    nodes.update_nodes()
    stm = Steem(node=nodes.get_nodes())
    set_shared_steem_instance(stm)
    
    db = dataset.connect(databaseConnector)
    db2 = dataset.connect(databaseConnector2)
    accountTrx = {}
    newAccountTrxStorage = False
    for account in accounts:
        accountTrx[account] = AccountTrx(db, account)
        
        if not accountTrx[account].exists_table():
            newAccountTrxStorage = True
            accountTrx[account].create_table()


            
    # Create keyStorage
    trxStorage = Trx(db2)
    memberStorage = Member(db2)
    
    newTrxStorage = False
    if not trxStorage.exists_table():
        newTrxStorage = True
        trxStorage.create_table()
    
    newMemberStorage = False
    if not memberStorage.exists_table():
        newMemberStorage = True
        memberStorage.create_table()


    
    stop_index = None

    for account_name in accounts:
        parse_vesting = (account == "steembasicincome")
        account = Account(account_name)
        print(account["name"])
        pah = ParseAccountHist(account, path, trxStorage)
        
        op_index = trxStorage.get_all_op_index(account["name"])
        if len(op_index) == 0:
            start_index = 0
            op_counter = 0
        else:
            start_index = op_index[-1] + 1
            op_counter = op_index[-1] + 1
        print("start_index %d" % start_index)
        # ops = []
        # 
        if True:
            ops = accountTrx[account_name].get_all(op_types=["transfer", "delegate_vesting_shares"])
            if ops[-1]["op_acc_index"] < start_index:
                continue
            for op in ops[start_index:]:
                pah.parse_op(json.loads(op["op_dict"]), parse_vesting=parse_vesting)
                if (op_counter % 100) == 0:
                    pah.add_mngt_shares(op_last, mgnt_shares)
                op_counter += 1
        else:
            for op in account.history(start=start_index, use_block_num=False):
                pah.parse_op(op, parse_vesting=parse_vesting)
                if (op_counter % 100) == 0:
                    pah.add_mngt_shares(op_last, mgnt_shares)                
                op_counter += 1


