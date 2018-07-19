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
from steembi.transfer_ops_storage import TransferTrx
    

if __name__ == "__main__":
    accounts = ["steembasicincome", "sbi2", "sbi3", "sbi4", "sbi5", "sbi6", "sbi7", "sbi8"]
    path = "E:\\sbi\\"
    database = "sbi_ops.sqlite"
    database_transfer = "sbi_transfer.sqlite"    
    from steembi.storage import (trxStorage)
    transferStorage = TransferTrx(path, database_transfer)
    
    # Update current node list from @fullnodeupdate

    nodes = NodeList()
    nodes.update_nodes()
    stm = Steem(node=nodes.get_nodes())
    data = trxStorage.get_all_data()
    delegation = {}
    sum_sp = {}
    sum_sp_shares = {}
    sum_sp_leased = {}
    account = "steembasicincome"
    delegation = {}
    sum_sp = 0
    sum_sp_leased = 0
    sum_sp_shares = 0
    shares_per_sp = 20

    for d in data:
        if d["share_type"] == "Delegation":
            delegation[d["account"]] = stm.vests_to_sp(float(d["vests"]))
        elif d["share_type"] == "RemovedDelegation":
            delegation[d["account"]] = 0
    
    delegation_leased = {}
    delegation_shares = {}

    delegation_account = delegation
    for acc in delegation_account:
        if delegation_account[acc] == 0:
            continue
        leased = transferStorage.find(acc, account)
        if len(leased) == 0:
            delegation_shares[acc] = delegation_account[acc]
            shares = int(delegation_account[acc] / shares_per_sp)
            trxStorage.update_delegation_shares(account, acc, shares)
            continue
        delegation_leased[acc] = delegation_account[acc]
        trxStorage.update_delegation_state(account, acc, "Delegation", 
                                          "DelegationLeased")
        
    
    dd = delegation
    for d in dd:
        sum_sp += dd[d]
    dd = delegation_leased
    for d in dd:
        sum_sp_leased += dd[d]
    dd = delegation_shares
    for d in dd:
        sum_sp_shares += dd[d]                
    print("%s: sum %.6f SP - shares %.6f SP - leased %.6f SP" % (account, sum_sp,  sum_sp_shares,  sum_sp_leased))
