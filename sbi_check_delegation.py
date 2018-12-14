from beem.account import Account
from beem.amount import Amount
from beem import Steem
from beem.instance import set_shared_steem_instance
from beem.nodelist import NodeList
import re
import os
from time import sleep
from datetime import datetime
import json
import dataset
from steembi.parse_hist_op import ParseAccountHist
from steembi.transfer_ops_storage import TransferTrx
from steembi.storage import TrxDB, MemberDB, ConfigurationDB


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
    confStorage = ConfigurationDB(db2)
    
    conf_setup = confStorage.get()
    
    last_cycle = conf_setup["last_cycle"]
    share_cycle_min = conf_setup["share_cycle_min"]
    sp_share_ratio = conf_setup["sp_share_ratio"]
    rshares_per_cycle = conf_setup["rshares_per_cycle"]
    last_delegation_check = conf_setup["last_delegation_check"]
    
    nodes = NodeList()
    try:
        nodes.update_nodes()
    except:
        print("could not update nodes") 
    stm = Steem(node=nodes.get_nodes())
    set_shared_steem_instance(stm)
    

    transferStorage = TransferTrx(db)
    trxStorage = TrxDB(db2)
    memberStorage = MemberDB(db2)
    
    if not trxStorage.exists_table():
        trxStorage.create_table()
    
    if not memberStorage.exists_table():
        memberStorage.create_table()
    
    # Update current node list from @fullnodeupdate

    delegation = {}
    sum_sp = {}
    sum_sp_shares = {}
    sum_sp_leased = {}
    account = "steembasicincome"
    delegation = {}
    delegation_shares = {}
    sum_sp = 0
    sum_sp_leased = 0
    sum_sp_shares = 0
    delegation_timestamp = {}
    
    print("load delegation")
    delegation_list = []
    for d in trxStorage.get_share_type(share_type="Delegation"):
        if d["share_type"] == "Delegation":
            delegation_list.append(d)
    for d in trxStorage.get_share_type(share_type="DelegationLeased"):
        if d["share_type"] == "DelegationLeased":    
            delegation_list.append(d)
    for d in trxStorage.get_share_type(share_type="RemovedDelegation"):
        if d["share_type"] == "RemovedDelegation":
            delegation_list.append(d)
            
            
    sorted_delegation_list = sorted(delegation_list, key=lambda x: (datetime.utcnow() - x["timestamp"]).total_seconds(), reverse=True)

    for d in sorted_delegation_list:
        if d["share_type"] == "Delegation":
            delegation[d["account"]] = stm.vests_to_sp(float(d["vests"]))
            delegation_timestamp[d["account"]] = d["timestamp"]
            delegation_shares[d["account"]] = d["shares"]
        elif d["share_type"] == "DelegationLeased":
            delegation[d["account"]] = 0
            delegation_timestamp[d["account"]] = d["timestamp"]
            delegation_shares[d["account"]] = d["shares"]    
        elif d["share_type"] == "RemovedDelegation":
            delegation[d["account"]] = 0
            delegation_timestamp[d["account"]] = d["timestamp"]
            delegation_shares[d["account"]] = 0
    
    delegation_leased = {}
    delegation_shares = {}
    print("update delegation")
    delegation_account = delegation
    for acc in delegation_account:
        if delegation_account[acc] == 0:
            continue
        if last_delegation_check is not None and delegation_timestamp[acc] <= last_delegation_check:
            continue
        if last_delegation_check is not None and last_delegation_check < delegation_timestamp[acc]:
            last_delegation_check = delegation_timestamp[acc]
        elif last_delegation_check is None:
            last_delegation_check = delegation_timestamp[acc]
        # if acc in delegation_shares and delegation_shares[acc] > 0:
        #    continue
        print(acc)
        leased = transferStorage.find(acc, account)
        if len(leased) == 0:
            delegation_shares[acc] = delegation_account[acc]
            shares = int(delegation_account[acc] / sp_share_ratio)
            trxStorage.update_delegation_shares(account, acc, shares)
            continue
        delegation_leased[acc] = delegation_account[acc]
        trxStorage.update_delegation_state(account, acc, "Delegation", 
                                          "DelegationLeased")
        print("set delegration from %s to leased" % acc)
        
    
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


    confStorage.update({"last_delegation_check": last_delegation_check})