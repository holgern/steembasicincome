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
    phase = 0

    
    db = dataset.connect(databaseConnector)    
    
    
    # Update current node list from @fullnodeupdate
    nodes = NodeList()
    try:
        nodes.update_nodes(weights={"hist": 1})
    except:
        print("could not update nodes")           
    stm = Steem(node=nodes.get_nodes())
    print(str(stm))
    set_shared_steem_instance(stm)
    
    blockchain = Blockchain()

    
    accountTrx = {}
    for account in accounts:
        accountTrx[account] = AccountTrx(db, account)
        
        if not accountTrx[account].exists_table():
            accountTrx[account].create_table()

    for account_name in accounts:
        account = Account(account_name)
        print("account %s" % account["name"])
        # Go trough all transfer ops
        cnt = 0
        cnt = 0
        ops = accountTrx[account_name].get_all()
        last_op_index = -1
        for op in ops:
            
            if op["op_acc_index"] - last_op_index != 1:
                print("%s - has missing ops %d - %d != 1" % (account_name, op["op_acc_index"], last_op_index))
            else:
                last_op_index = op["op_acc_index"]
                continue              


    for account in other_accounts:
        account = Account(account)
        print("account %s" % account["name"])
        cnt = 0
        ops = accountTrx[account_name].get_all()
        last_op_index = -1
        for op in ops:
            
            if op["op_acc_index"] - last_op_index != 1:
                print("%s - has missing ops %d - %d != 1" % (account_name, op["op_acc_index"], last_op_index))
            else:
                last_op_index = op["op_acc_index"]
                continue            
    
    stop_index = None
    # stop_index = addTzInfo(datetime(2018, 7, 21, 23, 46, 00))
    # stop_index = formatTimeString("2018-07-21T23:46:09")
