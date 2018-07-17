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
    
    from steembi.storage import (trxStorage)
    
    
    database_ops = "sbi.sqlite"
    database_transfer = "sbi_tranfer.sqlite"
    path = ""
    path = "E:\\sbi\\"
    # Update current node list from @fullnodeupdate

    nodes = NodeList()
    nodes.update_nodes()
    stm = Steem(node=nodes.get_nodes())
    data = trxStorage.get_all_data()
    delegation = {}
    sum_sp = {}
    for account in accounts:
        delegation[account] = {}
        sum_sp[account] = 0

    for d in data:
        if d["share_type"] == "Delegation":
            delegation[d["source"]][d["account"]] = stm.vests_to_sp(d["vests"])
        elif d["share_type"] == "RemovedDelegation":
            delegation[d["source"]][d["account"]] = 0
    
    for account in accounts:
        dd = delegation[account]
        for d in dd:
            sum_sp[account] += dd[d]
        print("%s: %.6f SP" % (account, sum_sp[account]))
