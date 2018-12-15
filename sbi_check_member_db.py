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
from steembi.storage import TrxDB, MemberDB, ConfigurationDB


if __name__ == "__main__":
    config_file = 'config.json'
    if not os.path.isfile(config_file):
        raise Exception("config.json is missing!")
    else:
        with open(config_file) as json_data_file:
            config_data = json.load(json_data_file)
        print(config_data)
        accounts = config_data["accounts"]
        databaseConnector = config_data["databaseConnector"]
        databaseConnector2 = config_data["databaseConnector2"]
        other_accounts = config_data["other_accounts"]
        mgnt_shares = config_data["mgnt_shares"]

    db2 = dataset.connect(databaseConnector2)
    # Create keyStorage
    trxStorage = TrxDB(db2)
    memberStorage = MemberDB(db2)
    confStorage = ConfigurationDB(db2)
    
    nodes = NodeList()
    try:
        nodes.update_nodes()
    except:
        print("could not update nodes")    
    stm = Steem(node=nodes.get_nodes())    
    
    newTrxStorage = False
    if not trxStorage.exists_table():
        newTrxStorage = True
        trxStorage.create_table()
    
    newMemberStorage = False
    if not memberStorage.exists_table():
        newMemberStorage = True
        memberStorage.create_table()

    # Update current node list from @fullnodeupdate
    print("check member database")
    # memberStorage.wipe(True)
    member_accounts = memberStorage.get_all_accounts()
    data = trxStorage.get_all_data()
    
    member_data = {}
    for m in member_accounts:
        member_data[m] = memberStorage.get(m)
    
    for op in data:
        if op["status"].lower() == "valid":
            share_type = op["share_type"]
            if share_type.lower() in ["delegation"]:
                if op["shares"] > 0 and op["sponsor"] in member_data:
                    # print("del. bonus_shares: %s - %d" % (op["sponsor"], op["shares"]))
                    member_data[op["sponsor"]]["bonus_shares"] += op["shares"]
                elif op["vests"] > 0 and op["sponsor"] in member_data:
                    sp = stm.vests_to_sp(float(op["vests"]))
                    member_data[op["sponsor"]]["bonus_shares"] += int(sp / confStorage.get()["sp_share_ratio"])
            elif share_type.lower() in ["mgmt", "mgmttransfer"]:
                if op["shares"] > 0 and op["sponsor"] in member_data:
                    member_data[op["sponsor"]]["bonus_shares"] += op["shares"]
                    # print("mngt bonus_shares: %s - %d" % (op["sponsor"], op["shares"]))

    shares = 0
    bonus_shares = 0
    for m in member_data:
        shares += member_data[m]["shares"]
        bonus_shares += member_data[m]["bonus_shares"]
    print("shares: %d" % shares)
    print("bonus shares: %d" % bonus_shares)
    print("total shares: %d" % (shares + bonus_shares))
    
