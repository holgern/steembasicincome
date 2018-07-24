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
from steembi.transfer_ops_storage import TransferTrx, AccountTrx
    

if __name__ == "__main__":
    config_file = 'config.json'
    if not os.path.isfile(config_file):
        accounts = ["steembasicincome", "sbi2", "sbi3", "sbi4", "sbi5", "sbi6", "sbi7", "sbi8"]
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
        other_accounts = config_data["other_accounts"]

    nodes = NodeList()
    nodes.update_nodes()
    stm = Steem(node=nodes.get_nodes())
    set_shared_steem_instance(stm)
    
    db = dataset.connect(databaseConnector)
    accountTrxStorage = AccountTrx(db)
    
    stop_index = None
    stop_index = datetime(2018, 7, 21, 23, 46, 00)    

    for account in accounts:
        parse_vesting = (account == "steembasicincome")
        account = Account(account)
        print(account["name"])
        pah = ParseAccountHist(account, path)
        
        op_index = pah.trxStorage.get_all_op_index(account["name"])
        if len(op_index) == 0:
            start_index = 0
        else:
            start_index = op_index[-1] + 1
        print("start_index %d" % start_index)
        # ops = []
        # 
        if load_ops_from_database:
            ops = accountTrxStorage.get_all(account["name"])
            ops = db_load(path, database_ops, account["name"])
            if ops[-1]["index"] < start_index:
                continue
            for op in ops[start_index:]:
                pah.parse_op(op, parse_vesting=parse_vesting)
        else:
            for op in account.history(start=start_index, use_block_num=False):
                pah.parse_op(op, parse_vesting=parse_vesting)


