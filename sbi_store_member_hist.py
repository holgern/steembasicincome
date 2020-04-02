from beem.account import Account
from beem.amount import Amount
from beem.comment import Comment
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
from steembi.transfer_ops_storage import TransferTrx, AccountTrx, MemberHistDB, CurationOptimizationTrx
from steembi.storage import TrxDB, MemberDB, ConfigurationDB, AccountsDB
from steembi.member import Member
import dataset
from steembi.memo_parser import MemoParser

if __name__ == "__main__":
    config_file = 'config.json'
    if not os.path.isfile(config_file):
        raise Exception("config.json is missing!")
    else:
        with open(config_file) as json_data_file:
            config_data = json.load(json_data_file)
        # print(config_data)
        databaseConnector = config_data["databaseConnector"]
        databaseConnector2 = config_data["databaseConnector2"]
        hive_blockchain = config_data["hive_blockchain"]
    
    # sqlDataBaseFile = os.path.join(path, database)
    # databaseConnector = "sqlite:///" + sqlDataBaseFile
    start_prep_time = time.time()
    db2 = dataset.connect(databaseConnector2)
    
    
    
    accountStorage = AccountsDB(db2)
    accounts = accountStorage.get()
    other_accounts = accountStorage.get_transfer()     
    
    # Create keyStorage
    trxStorage = TrxDB(db2)
    memberStorage = MemberDB(db2)
    confStorage = ConfigurationDB(db2)
    
    conf_setup = confStorage.get()
    
    last_cycle = conf_setup["last_cycle"]
    share_cycle_min = conf_setup["share_cycle_min"]
    sp_share_ratio = conf_setup["sp_share_ratio"]
    rshares_per_cycle = conf_setup["rshares_per_cycle"]
    upvote_multiplier = conf_setup["upvote_multiplier"]
    last_paid_post = conf_setup["last_paid_post"]
    last_paid_comment = conf_setup["last_paid_comment"]
    upvote_multiplier_adjusted = conf_setup["upvote_multiplier_adjusted"]
    
    # print("Count rshares of upvoted members.")
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
    curationOptimTrx = CurationOptimizationTrx(db)
    curationOptimTrx.delete_old_posts(days=7)
    # Update current node list from @fullnodeupdate
    nodes = NodeList()
    # nodes.update_nodes(weights={"hist": 1})
    try:
        nodes.update_nodes()
    except:
        print("could not update nodes")
        
    node_list = nodes.get_nodes(hive=hive_blockchain)
    stm = Steem(node=node_list, num_retries=3, timeout=10)
    # print(str(stm))
    set_shared_steem_instance(stm)
    
    accountTrx = {}
    accountTrx = MemberHistDB(db)

    b = Blockchain(steem_instance=stm)
    current_block = b.get_current_block()
    stop_time = latest_enrollment
    stop_time = current_block["timestamp"]
    start_time = stop_time - timedelta(seconds=30 * 24 * 60 * 60)
    
    
    blocks_per_day = 20 * 60 * 24

    start_block = accountTrx.get_latest_block_num()
    
    if start_block is None:
        start_block = b.get_estimated_block_num(addTzInfo(start_time))
        # block_id_list = []
        trx_id_list = []
    else:
        trx_id_list = accountTrx.get_block_trx_id(start_block)
    # end_block = current_block["id"] 
    end_block = current_block["id"] - (20 * 10)
    if end_block > start_block + 6000:
        end_block = start_block + 6000
    
    print("Checking member upvotes from %d to %d" % (start_block, end_block))
    
    date_now = datetime.utcnow()
    date_7_before = addTzInfo(date_now - timedelta(seconds=7 * 24 * 60 * 60))
    date_28_before = addTzInfo(date_now - timedelta(seconds=28 * 24 * 60 * 60))
    date_72h_before = addTzInfo(date_now - timedelta(seconds=72 * 60 * 60))    
    print("delete old hist data")
    accountTrx.delete_old_data(end_block - (20 * 60 * 24 * 7))
    print("delete done")
    
    # print("start to stream")
    db_data = []
    curation_vote_list = []

    last_block_num = None
    last_trx_id = '0' * 40
    op_num = 0
    cnt = 0
    comment_cnt = 0
    vote_cnt = 0
    # print("Check rshares from %d - %d" % (int(start_block), int(end_block)))
    for op in b.stream(start=int(start_block), stop=int(end_block), opNames=["vote", "comment"], threading=False, thread_num=8):
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
            try:
                c = Comment(op, use_tags_api=True, steem_instance=stm)
                c.refresh()
            except:
                continue
            main_post = c.is_main_post()            
            comment_cnt += 1

            if main_post:
                member_data[op["author"]]["last_post"] = c["created"]
            else:
                member_data[op["author"]]["last_comment"] = c["created"]
                
            if member_data[op["author"]]["last_post"] is None:
                member_data[op["author"]]["comment_upvote"] = 1
            elif addTzInfo(member_data[op["author"]]["last_post"]) < date_7_before:
                member_data[op["author"]]["comment_upvote"] = 1
            elif member_data[op["author"]]["comment_upvote"] == 1:
                member_data[op["author"]]["comment_upvote"] = 0
            member_data[op["author"]]["updated_at"] = c["created"]
            updated_member_data.append(member_data[op["author"]])
        elif op["type"] == "vote":
            if op["author"] not in accounts and op["author"] not in member_accounts:
                continue
            if op["voter"] not in member_accounts and op["voter"] not in accounts:
                continue
            if op["author"] in member_accounts and op["voter"] in accounts:
                authorperm=construct_authorperm(op["author"], op["permlink"])
                vote = Vote(op["voter"], authorperm=authorperm, steem_instance=stm)
                print("member %s upvoted with %d" % (op["author"], int(vote["rshares"])))
                member_data[op["author"]]["rewarded_rshares"] += int(vote["rshares"])
                member_data[op["author"]]["balance_rshares"] -= int(vote["rshares"])
                
                upvote_delay = member_data[op["author"]]["upvote_delay"]
                if upvote_delay is None:
                    upvote_delay = 300
                performance = 0
                c = Comment(authorperm, steem_instance=stm)
                
                try:
                    
                    curation_rewards_SBD = c.get_curation_rewards(pending_payout_SBD=True)
                    curation_SBD = curation_rewards_SBD["active_votes"][vote["voter"]]
                    if vote_SBD > 0:
                        performance = (float(curation_SBD) / vote_SBD * 100)
                    else:
                        performance = 0                    
                except:
                    performance = 0
                    curation_rewards_SBD = None
                    
                rshares = int(vote["rshares"])
                vote_SBD = stm.rshares_to_sbd(int(vote["rshares"]))

                best_performance = 0
                best_time_delay = 0
                for v in c.get_votes():
                    v_SBD = stm.rshares_to_sbd(int(v["rshares"]))
                    if v_SBD > 0 and int(v["rshares"]) > rshares * 0.5 and curation_rewards_SBD is not None:
                        p = float(curation_rewards_SBD["active_votes"][v["voter"]]) / v_SBD * 100
                        if p > best_performance:
                            best_performance = p
                            if "time" in v:
                                best_time_delay = ((v["time"]) - c["created"]).total_seconds()
                            elif "last_update" in v:
                                best_time_delay = ((v["last_update"]) - c["created"]).total_seconds()
                            else:
                                best_time_delay = upvote_delay
                
                if best_performance > performance * 1.05:
                    member_data[op["author"]]["upvote_delay"] = (upvote_delay * 19 + best_time_delay) / 20
                    if member_data[op["author"]]["upvote_delay"] > 300:
                        member_data[op["author"]]["upvote_delay"] = 300
                    elif member_data[op["author"]]["upvote_delay"] < 100:
                        member_data[op["author"]]["upvote_delay"] = 100
                updated_member_data.append(member_data[op["author"]])
                    
                curation_data = {"authorperm": authorperm, "member": op["author"], "created": c["created"], "best_time_delay": best_time_delay, "best_curation_performance": best_performance,
                                 "vote_rshares": int(vote["rshares"]), "updated": datetime.utcnow(), "vote_delay": ((op["timestamp"]) - c["created"]).total_seconds(),
                                 "performance": performance}
                curation_vote_list.append(curation_data)
            data["permlink"] = op["permlink"]
            data["author"] = op["author"]
            data["voter"] = op["voter"]
            data["weight"] = op["weight"]
            
            
            
            vote_cnt += 1
        else:
            continue
        if op["type"] == "vote":
            db_data.append(data)
            last_trx_id = op["trx_id"]

        if cnt % 200 == 0 and cnt > 0:
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
            db2 = dataset.connect(databaseConnector2)
            accountTrx.db = db
            curationOptimTrx.db = db
            memberStorage.db = db2
            accountTrx.add_batch(db_data)
            db_data = []
            if len(updated_member_data) > 0:
                memberStorage.add_batch(updated_member_data)
                updated_member_data = []
            
                
            if len(curation_vote_list) > 0:
                curationOptimTrx.add_batch(curation_vote_list)
                curation_vote_list = []            
            
        cnt += 1
    if len(db_data) > 0:
        print(op["timestamp"])
        db = dataset.connect(databaseConnector)
        accountTrx.db = db        
        accountTrx.add_batch(db_data)
        db_data = []
    if len(updated_member_data) > 0:
        db2 = dataset.connect(databaseConnector2)
        memberStorage.db = db2
        memberStorage.add_batch(updated_member_data)
        updated_member_data = []


        print("\n---------------------\n")
        percentage_done = (block_num - start_block) / (end_block - start_block) * 100
        print("Block %d -- Datetime %s -- %.2f %% finished" % (block_num, op["timestamp"], percentage_done))
    
    if len(curation_vote_list) > 0:
        db = dataset.connect(databaseConnector)
        curationOptimTrx.db = db        
        curationOptimTrx.add_batch(curation_vote_list)
        curation_vote_list = []
    
    print("member hist script run %.2f s" % (time.time() - start_prep_time))
