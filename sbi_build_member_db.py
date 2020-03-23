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
from steembi.parse_hist_op import ParseAccountHist
from steembi.storage import TrxDB, MemberDB
from steembi.member import Member


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
        hive_blockchain = config_data["hive_blockchain"]

    db2 = dataset.connect(databaseConnector2)
    # Create keyStorage
    trxStorage = TrxDB(db2)
    memberStorage = MemberDB(db2)
    
    newTrxStorage = False
    if not trxStorage.exists_table():
        newTrxStorage = True
        trxStorage.create_table()
    
    newMemberStorage = False
    if not memberStorage.exists_table():
        newMemberStorage = True
        memberStorage.create_table()

    # Update current node list from @fullnodeupdate
    print("build member database")
    # memberStorage.wipe(True)
    accs = memberStorage.get_all_accounts()
    for a in accs:
        memberStorage.delete(a)
    nodes = NodeList()
    try:
        nodes.update_nodes()
    except:
        print("could not update nodes")    
    stm = Steem(node=nodes.get_nodes(hive=hive_blockchain))
    data = trxStorage.get_all_data()
    status = {}
    share_type = {}
    n_records = 0
    member_data = {}
    for op in data:
        if op["status"] == "Valid":
            share_type = op["share_type"]
            if share_type in ["RemovedDelegation", "Delegation", "DelegationLeased", "Mgmt", "MgmtTransfer"]:
                continue
            sponsor = op["sponsor"]
            sponsee = json.loads(op["sponsee"])
            shares = op["shares"]
            share_age = 0
            if isinstance(op["timestamp"], str):
                timestamp = formatTimeString(op["timestamp"])
            else:
                timestamp = op["timestamp"]
            if shares == 0:
                continue
            if sponsor not in member_data:
                member = Member(sponsor, shares, timestamp)
                member.append_share_age(timestamp, shares)
                member_data[sponsor] = member
            else:
                member_data[sponsor]["latest_enrollment"] = timestamp
                member_data[sponsor]["shares"] += shares
                member_data[sponsor].append_share_age(timestamp, shares)
            if len(sponsee) == 0:
                continue
            for s in sponsee:
                shares = sponsee[s]
                if s not in member_data:
                    member = Member(s, shares, timestamp)
                    member.append_share_age(timestamp, shares)
                    member_data[s] = member
                else:
                    member_data[s]["latest_enrollment"] = timestamp
                    member_data[s]["shares"] += shares
                    member_data[s].append_share_age(timestamp, shares)

    empty_shares = []       
    for m in member_data:
        if member_data[m]["shares"] <= 0:
            empty_shares.append(m)
    
    for del_acc in empty_shares:
        del member_data[del_acc]
    

    shares = 0
    bonus_shares = 0
    for m in member_data:
        member_data[m].calc_share_age()
        shares += member_data[m]["shares"]
        bonus_shares += member_data[m]["bonus_shares"]
    print("shares: %d" % shares)
    print("bonus shares: %d" % bonus_shares)
    print("total shares: %d" % (shares + bonus_shares))
    
    member_list = []
    for m in member_data:
        member_list.append(member_data[m])
    memberStorage.add_batch(member_list)