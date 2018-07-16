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
    database_ops = "sbi.sqlite"
    database_transfer = "sbi_tranfer.sqlite"
    path = ""
    path = "E:\\sbi\\"
    # Update current node list from @fullnodeupdate

    nodes = NodeList()
    nodes.update_nodes()
    stm = Steem(node=nodes.get_nodes())
    set_shared_steem_instance(stm)
    transfer_table = [{"index": -1}]

    for account in accounts:
        account = Account(account)
        pah = ParseAccountHist(account, path, transfer_table)
        ops = db_load(path, database_ops, account["name"])
        
        start_index = ops[-1]["index"] + 1
        start_index = pah.transfer_table[-1]["index"] + 1
        print("start_index %d" % start_index)
        # ops = []
        # for op in account.history(start=start_index, use_block_num=False): 
        for op in ops[start_index:]:
            pah.parse_op(op)
            # ops.append(op)

            # db_extend(path, database_ops, account["name"], ops)
