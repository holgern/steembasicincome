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
    for account in accounts:
        delegation[account] = {}
        sum_sp[account] = 0
        sum_sp_leased[account] = 0
        sum_sp_shares[account] = 0

    for d in data:
        if d["share_type"] == "Delegation":
            delegation[d["source"]][d["account"]] = stm.vests_to_sp(d["vests"])
        elif d["share_type"] == "RemovedDelegation":
            delegation[d["source"]][d["account"]] = 0
    
    delegation_leased = {}
    delegation_shares = {}
    for account in accounts:
        delegation_leased[account] = {}
        delegation_shares[account] = {}
        delegation_account = delegation[account]
        for acc in delegation_account:
            if delegation_account[acc] == 0:
                continue
            leased = transferStorage.find(acc, account)
            if len(leased) == 0:
                delegation_shares[account][acc] = delegation_account[acc]
                continue
            delegation_leased[account][acc] = delegation_account[acc]
    
    for account in accounts:
        dd = delegation[account]
        for d in dd:
            sum_sp[account] += dd[d]
        dd = delegation_leased[account]
        for d in dd:
            sum_sp_leased[account] += dd[d]
        dd = delegation_shares[account]
        for d in dd:
            sum_sp_shares[account] += dd[d]                
        print("%s: sum %.6f SP - shares %.6f SP - leased %.6f SP" % (account, sum_sp[account],  sum_sp_shares[account],  sum_sp_leased[account]))
