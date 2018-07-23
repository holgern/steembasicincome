from beem.account import Account
from beem.amount import Amount
from beem import Steem
from beem.instance import set_shared_steem_instance
from beem.nodelist import NodeList
import re
import os
from time import sleep
from steembi.sqlite_dict import db_store, db_load, db_append, db_extend, db_has_database, db_has_key
from steembi.parse_hist_op import ParseAccountHist
    

if __name__ == "__main__":
    accounts = ["steembasicincome", "sbi2", "sbi3", "sbi4", "sbi5", "sbi6", "sbi7", "sbi8"]
    database_ops = "sbi_ops.sqlite"
    path = ""
    path = "E:\\sbi\\"
    load_ops_from_database = True
    
    with open('config.json') as json_data_file:
        config_data = json.load(json_data_file)
    print(config_data)
    accounts = config_data["accounts"]
    path = config_data["path"]
    database = config_data["database"]
    database_transfer = config_data["database_transfer"]
    databaseConnector = config_data["databaseConnector"]
    other_accounts = config_data["other_accounts"]    
    # Update current node list from @fullnodeupdate

    nodes = NodeList()
    nodes.update_nodes()
    stm = Steem(node=nodes.get_nodes())
    set_shared_steem_instance(stm)

    for account in accounts:
        parse_vesting=account == "steembasicincome"
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
            ops = db_load(path, database_ops, account["name"])
            if ops[-1]["index"] < start_index:
                continue
            for op in ops[start_index:]:
                pah.parse_op(op, parse_vesting=parse_vesting)
        else:
            for op in account.history(start=start_index, use_block_num=False):
                pah.parse_op(op, parse_vesting=parse_vesting)


