from beem.account import Account
from beem.amount import Amount
from beem import Steem
from beem.instance import set_shared_steem_instance
from beem.nodelist import NodeList
from beem.blockchain import Blockchain
from beem.vote import Vote
from beem.utils import formatTimeString, addTzInfo, construct_authorperm
from datetime import datetime, timedelta
import re
import time
import os
import json
from steembi.transfer_ops_storage import TransferTrx, AccountTrx, MemberHistDB
from steembi.storage import TrxDB, MemberDB, ConfigurationDB
from steembi.member import Member
import dataset


if __name__ == "__main__":
    config_file = 'config.json'
    if not os.path.isfile(config_file):
        accounts = ["steembasicincome", "sbi2", "sbi3", "sbi4", "sbi5", "sbi6", "sbi7", "sbi8", "sbi9"]
        path = "E:\\sbi\\"
        database = "sbi_ops.sqlite"
        database_transfer = "sbi_transfer.sqlite"
        databaseConnector = None
        other_accounts = ["minnowbooster"]
    else:
        with open(config_file) as json_data_file:
            config_data = json.load(json_data_file)
        # print(config_data)
        accounts = config_data["accounts"]
        path = config_data["path"]
        databaseConnector = config_data["databaseConnector"]
        databaseConnector2 = config_data["databaseConnector2"]
        other_accounts = config_data["other_accounts"]
    
    # sqlDataBaseFile = os.path.join(path, database)
    # databaseConnector = "sqlite:///" + sqlDataBaseFile
    
    db2 = dataset.connect(databaseConnector2)
    # Create keyStorage
    trxStorage = TrxDB(db2)
    memberStorage = MemberDB(db2)
    confStorage = ConfigurationDB(db2)
    
    conf_setup = confStorage.get()
    
    last_cycle = conf_setup["last_cycle"]
    share_cycle_min = conf_setup["share_cycle_min"]
    sp_share_ratio = conf_setup["sp_share_ratio"]
    rshares_per_cycle = conf_setup["rshares_per_cycle"]    
    
    member_accounts = memberStorage.get_all_accounts()
    print("%d members in list" % len(member_accounts))
    
    member_data = {}
    latest_enrollment = None
    share_age_member = {}    
    for m in member_accounts:
        member_data[m] = Member(memberStorage.get(m))
        if latest_enrollment is None:
            latest_enrollment = member_data[m]["latest_enrollment"]
        elif latest_enrollment < member_data[m]["latest_enrollment"]:
            latest_enrollment = member_data[m]["latest_enrollment"]        
        
    print("latest member enrollment %s" % str(latest_enrollment))
    
    
    updated_member_data = []
    db = dataset.connect(databaseConnector)
    
    # Update current node list from @fullnodeupdate
    nodes = NodeList()
    # nodes.update_nodes(weights={"hist": 1})
    nodes.update_nodes()
    stm = Steem(node=nodes.get_nodes(), num_retries=3, timeout=10)
    print(str(stm))
    set_shared_steem_instance(stm)
    
    accountTrx = {}
    newAccountTrxStorage = False
    accountTrx = MemberHistDB(db)
    if not accountTrx.exists_table():
        newAccountTrxStorage = True
        accountTrx.create_table()

    b = Blockchain(steem_instance=stm)
    current_block = b.get_current_block()
    stop_time = latest_enrollment
    stop_time = current_block["timestamp"]
    start_time = stop_time - timedelta(seconds=30 * 24 * 60 * 60)
    
    
    
    blocks_per_day = 20 * 60 * 24
    #start_block = 2612571 - 1
    if newAccountTrxStorage:
        start_block = b.get_estimated_block_num(addTzInfo(start_time))
        # block_id_list = []
        trx_id_list = []
    else:
        start_block = accountTrx.get_latest_block_num()
        # block_id_list = blockTrxStorage.get_block_id(start_block)
        trx_id_list = accountTrx.get_block_trx_id(start_block)
    if start_block is None:
        start_block = b.get_estimated_block_num(addTzInfo(start_time))
        # block_id_list = []
        trx_id_list = []
    #end_block = b.get_estimated_block_num(stop_time)
    end_block = current_block["id"]
    
    print(start_block)        
    
    
    print("clear not needed blocks")
    
    
    deleting = True
    while deleting:
        delete_ops = []
        delete_before = False
        for op in accountTrx.get_ordered_block_num():
            if addTzInfo(op["timestamp"]) < start_time:
                delete_ops.append({"block_num": op["block_num"], "trx_id": op["trx_id"], "op_num": op["op_num"]})
        if len(delete_ops) > 0:
            delete_before = True
            print("delete %d - %d" % ((delete_ops[0]["block_num"]), (delete_ops[-1]["block_num"])))
        for op in accountTrx.get_ordered_block_num_reverse():
            if addTzInfo(op["timestamp"]) > stop_time:
                delete_ops.append({"block_num": op["block_num"], "trx_id": op["trx_id"], "op_num": op["op_num"]})
        if len(delete_ops) > 0 and not delete_before:
            print("delete %d - %d" % ((delete_ops[0]["block_num"]), (delete_ops[-1]["block_num"])))
        for ops in delete_ops:    
            accountTrx.delete(block_num=ops["block_num"], trx_id=ops["trx_id"], op_num=ops["op_num"])
        if len(delete_ops) > 0:
            deleting = True
        else:
            deleting = False
    
    print("start to stream")
    db_data = []

    last_block_num = None
    last_trx_id = '0' * 40
    op_num = 0
    cnt = 0
    comment_cnt = 0
    vote_cnt = 0
    for op in b.stream(start=int(start_block), stop=int(end_block), opNames=["comment", "vote"], threading=False, thread_num=8):
        block_num = op["block_num"]
        if last_block_num is None:
            start_time = time.time()
            last_block_num = block_num
        if op["trx_id"] == last_trx_id:
            op_num += 1
        else:
            op_num = 0
        if "trx_num" in op:
            trx_num = op["trx_num"]
        else:
            trx_num = 0
        data = {"block_num": block_num, "block_id": op["_id"], "trx_id": op["trx_id"], "trx_num": trx_num, "op_num": op_num, "timestamp": formatTimeString(op["timestamp"]), "type": op["type"]}
        if op["trx_id"] in trx_id_list :
            continue
        if op["type"] == "comment":
            if op["author"] not in member_accounts:
                continue
            comment_cnt += 1
            post_age_min = (addTzInfo(datetime.utcnow()) - op["timestamp"]).total_seconds() / 60
            # print("new post from %s - age %.2f min" % (op["author"], post_age_min))
            data["parent_permlink"] = op["parent_permlink"]
            data["parent_author"] = op["parent_author"]
            data["permlink"] = op["permlink"]
            data["author"] = op["author"]
        elif op["type"] == "vote":
            if op["author"] not in accounts or op["voter"] not in member_accounts:
                continue
            if op["author"] not in member_accounts or op["voter"] not in accounts:
                continue
            if op["author"] in member_accounts and op["voter"] in accounts:
                print("member %s upvoted with %d" % (op["author"], int(vote["rshares"])))
                vote = Vote(op["voter"], authorperm=construct_authorperm(op["author"], op["permlink"]), steem_incstance=stm)
                member_data[op["author"]]["rewarded_rshares"] += int(vote["rshares"])
                member_data[op["author"]]["balance_rshares"] -= int(vote["rshares"])
                updated_member_data.append(member_data[op["author"]])
            #if op["author"] in accounts and op["voter"] in member_accounts:
            #    vote = Vote(op["voter"], authorperm=construct_authorperm(op["author"], op["permlink"]), steem_incstance=stm)
            #    member_data[op["voter"]]["balance_rshares"] += int(vote["rshares"])
            #    member_data[op["voter"]]["earned_rshares"] += int(vote["rshares"])
            #    updated_member_data.append(member_data[op["voter"]])
            data["permlink"] = op["permlink"]
            data["author"] = op["author"]
            data["voter"] = op["voter"]
            data["weight"] = op["weight"]
            vote_cnt += 1
        else:
            continue

        db_data.append(data)
        last_trx_id = op["trx_id"]

        if cnt % 1000 == 0:
            time_for_blocks = time.time() - start_time
            block_diff_for_db_storage = block_num - last_block_num
            if block_diff_for_db_storage == 0:
                block_diff_for_db_storage = 1
            print("\n---------------------\n")
            percentage_done = (block_num - start_block) / (end_block - start_block) * 100
            print("Block %d -- Datetime %s -- %.2f %% finished" % (block_num, op["timestamp"], percentage_done))
            running_hours = (end_block - block_num) * time_for_blocks / block_diff_for_db_storage / 60 / 60
            print("Duration for %d blocks: %.2f s (%.3f s per block) -- %.2f hours to go" % (block_diff_for_db_storage, time_for_blocks, time_for_blocks / block_diff_for_db_storage, running_hours))
            print("%d  new comments, %d new votes" % (comment_cnt, vote_cnt))
            start_time = time.time()
            comment_cnt = 0
            vote_cnt = 0
            last_block_num = block_num
            
            db = dataset.connect(databaseConnector)
            accountTrx.db = db
            accountTrx.add_batch(db_data)
            db_data = []
            if len(updated_member_data) > 0:
                memberStorage.add_batch(updated_member_data)
                updated_member_data = []
        cnt += 1
    if len(db_data) > 0:
        print(op["timestamp"])
        db = dataset.connect(databaseConnector)
        accountTrx.db = db        
        accountTrx.add_batch(db_data)
        db_data = []
        if len(updated_member_data) > 0:
            memberStorage.add_batch(updated_member_data)
            updated_member_data = []


        print("\n---------------------\n")
        percentage_done = (block_num - start_block) / (end_block - start_block) * 100
        print("Block %d -- Datetime %s -- %.2f %% finished" % (block_num, op["timestamp"], percentage_done))