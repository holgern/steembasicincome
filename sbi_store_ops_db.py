from beem.account import Account
from beem.amount import Amount
from beem import Steem
from beem.instance import set_shared_steem_instance
from beem.nodelist import NodeList
from beem.utils import formatTimeString
import re
import os
from steembi.sqlite_dict import db_store, db_load, db_append, db_extend, db_has_database, db_has_key
from steembi.ops_storage import store_all_ops, check_all_ops


if __name__ == "__main__":
    accounts = ["steembasicincome", "sbi2", "sbi3", "sbi4", "sbi5", "sbi6", "sbi7", "sbi8"]
    path = "E:\\sbi\\"
    database = "sbi_ops.sqlite"
    # Update current node list from @fullnodeupdate
    nodes = NodeList()
    nodes.update_nodes(weights={"hist": 1})
    stm = Steem(node=nodes.get_nodes(appbase=False, https=False))
    print(str(stm))
    set_shared_steem_instance(stm)
    
    for account in accounts:
        ops_ok = False
        while not ops_ok:
            store_all_ops(path, database, account)
            ops_ok = check_all_ops(path, database, account)
        