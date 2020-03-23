from beem.account import Account
from beem.amount import Amount
from beem import Steem
from beem.instance import set_shared_steem_instance
from beem.nodelist import NodeList
from beem.utils import formatTimeString
import re
import json
import os
from time import sleep
import dataset
import json
from steembi.parse_hist_op import ParseAccountHist
from steembi.storage import TrxDB, MemberDB, ConfigurationDB, AccountsDB


if __name__ == "__main__":
    config_file = 'config.json'
    if not os.path.isfile(config_file):
        raise Exception("config.json is missing!")
    else:
        with open(config_file) as json_data_file:
            config_data = json.load(json_data_file)
        databaseConnector = config_data["databaseConnector"]
        databaseConnector2 = config_data["databaseConnector2"]
        mgnt_shares = config_data["mgnt_shares"]
        hive_blockchain = config_data["hive_blockchain"]

    db2 = dataset.connect(databaseConnector2)
    # Create keyStorage
    trxStorage = TrxDB(db2)
    memberStorage = MemberDB(db2)
    confStorage = ConfigurationDB(db2)
    
    accStorage = AccountsDB(db2)
    accounts = accStorage.get()
    other_accounts = accStorage.get_transfer()
    
    sp_share_ratio = confStorage.get()["sp_share_ratio"]
    
    nodes = NodeList()
    try:
        nodes.update_nodes()
    except:
        print("could not update nodes")    
    stm = Steem(node=nodes.get_nodes(hive=hive_blockchain))    


    # Update current node list from @fullnodeupdate
    print("check member database")
    # memberStorage.wipe(True)
    member_accounts = memberStorage.get_all_accounts()
    data = trxStorage.get_all_data()
    
    missing_accounts = []
    member_data = {}
    aborted = False
    for m in member_accounts:
        member_data[m] = memberStorage.get(m)
    # Check wrong account names:
    if False:
        cnt = 0
        for m in member_accounts:
            if aborted:
                continue
            cnt += 1
            if cnt % 100 == 0:
                print("%d/%d scanned" % ())
            try:
                acc = Account(m, steem_instance=stm)
            except KeyboardInterrupt:
                aborted = True
            except:
                print("%s is not a valid account" % m)
                missing_accounts.append(m)
    


    shares = 0
    bonus_shares = 0
    balance_rshares = 0
    for m in member_data:
        shares += member_data[m]["shares"]
        bonus_shares += member_data[m]["bonus_shares"]
        balance_rshares += member_data[m]["balance_rshares"]
    
    
    print("units: %d" % shares)
    print("bonus units: %d" % bonus_shares)
    print("total units: %d" % (shares + bonus_shares))
    print("----------")
    print("balance_rshares: %d" % balance_rshares)
    print("balance_rshares: %.3f $" % stm.rshares_to_sbd(balance_rshares))
    if len(missing_accounts) > 0:
        print("%d not existing accounts: " % len(missing_accounts))
        print(missing_accounts)
    
