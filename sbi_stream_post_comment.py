from beem.utils import formatTimeString, resolve_authorperm, construct_authorperm, addTzInfo
from beem.nodelist import NodeList
from beem.comment import Comment
from beem import Steem
from datetime import datetime, timedelta
from beem.instance import set_shared_steem_instance
from beem.blockchain import Blockchain
import time 
import json
import os
import math
import dataset
import random
from datetime import date, datetime, timedelta
from dateutil.parser import parse
from beem.constants import STEEM_100_PERCENT 
from steembi.transfer_ops_storage import TransferTrx, AccountTrx, PostsTrx
from steembi.storage import TrxDB, MemberDB, ConfigurationDB, AccountsDB, KeysDB
from steembi.parse_hist_op import ParseAccountHist
from steembi.memo_parser import MemoParser
from steembi.member import Member
import dataset



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
        other_accounts = config_data["other_accounts"]    

    start_prep_time = time.time()
    db = dataset.connect(databaseConnector)
    db2 = dataset.connect(databaseConnector2)
    # Create keyStorage
    trxStorage = TrxDB(db2)
    memberStorage = MemberDB(db2)
    confStorage = ConfigurationDB(db2)
    accStorage = AccountsDB(db2)
    keyStorage = KeysDB(db2)    
    
    accounts = accStorage.get()
    
    conf_setup = confStorage.get()
    
    last_cycle = conf_setup["last_cycle"]
    share_cycle_min = conf_setup["share_cycle_min"]
    sp_share_ratio = conf_setup["sp_share_ratio"]
    rshares_per_cycle = conf_setup["rshares_per_cycle"]    
    minimum_vote_threshold = conf_setup["minimum_vote_threshold"]
    
    member_accounts = memberStorage.get_all_accounts()
    print("%d members in list" % len(member_accounts)) 
    
    nobroadcast = False
    # nobroadcast = True    

    member_data = {}
    for m in member_accounts:
        member_data[m] = Member(memberStorage.get(m))    
    
    postTrx = PostsTrx(db)

    print("stream new posts")


    if True:
        max_batch_size = 50
        threading = False
        wss = False
        https = True
        normal = False
        appbase = True
    elif False:
        max_batch_size = None
        threading = True
        wss = True
        https = False
        normal = True
        appbase = True
    else:
        max_batch_size = None
        threading = False
        wss = True
        https = True
        normal = True
        appbase = True        

    nodes = NodeList()
    # nodes.update_nodes(weights={"block": 1})
    try:
        nodes.update_nodes()
    except:
        print("could not update nodes")
    
    keys = []
    account_list = []
    for acc in accounts:
        account_list.append(acc)
        keys.append(keyStorage.get(acc, "posting"))
    keys_list = []
    for k in keys:
        if k["key_type"] == 'posting':
            keys_list.append(k["wif"].replace("\n", '').replace('\r', ''))    
    node_list = nodes.get_nodes(normal=normal, appbase=appbase, wss=wss, https=https)
    stm = Steem(node=node_list, keys=keys_list, num_retries=5, call_num_retries=3, timeout=15, nobroadcast=nobroadcast) 
    
    b = Blockchain(steem_instance = stm)
    print("deleting old posts")
    postTrx.delete_old_posts(7)
    # print("reading all authorperm")
    already_voted_posts = []
    flagged_posts = []
    start_block = b.get_current_block_num() - int(201600)
    stop_block = b.get_current_block_num()
    last_block_print = start_block
    
    latest_update = postTrx.get_latest_post()
    if latest_update is not None:
        latest_update_block = b.get_estimated_block_num(latest_update)
    else:
        latest_update_block = start_block
    print("latest update %s - %d to %d" % (str(latest_update), latest_update_block, stop_block))
    
    start_block = max([latest_update_block, start_block])
    cnt = 0
    updated_accounts = []
    posts_dict = {}
    for ops in b.stream(start=start_block, stop=stop_block, opNames=["comment"], max_batch_size=max_batch_size, threading=threading, thread_num=8):
        #print(ops)
        timestamp = ops["timestamp"]
        # timestamp = timestamp.replace(tzinfo=None)
            # continue
        if ops["author"] not in member_accounts:
            continue
        if ops["block_num"] - last_block_print > 50:
            last_block_print = ops["block_num"]
            print("blocks left %d - post found: %d" % (ops["block_num"] - stop_block, len(posts_dict)))
        authorperm = construct_authorperm(ops)
  

        
        try:
            c = Comment(authorperm, steem_instance=stm)
        except:
            continue
        main_post = c.is_main_post()
        if main_post:
            member_data[ops["author"]]["last_post"] = c["created"]
        else:
            member_data[ops["author"]]["last_comment"] = c["created"]
            status_command = c.body.find("!sbi status")
            if status_command > -1 and abs((ops["timestamp"] - c["created"]).total_seconds()) <= 10:
                reply_body = "Hi @%s!\n\n" % ops["author"]
                reply_body += "* you have %d units and %d bonus units\n" % (member_data[ops["author"]]["shares"], member_data[ops["author"]]["bonus_shares"])
                reply_body += "* your rshares balance is %d or %.3f $\n" % (member_data[ops["author"]]["balance_rshares"], stm.rshares_to_sbd(member_data[ops["author"]]["balance_rshares"])) 
                
                if member_data[ops["author"]]["comment_upvote"] == 0:
                    if member_data[ops["author"]]["balance_rshares"] * 0.2 > minimum_vote_threshold:
                        reply_body += "* your next SBI upvote is predicted to be %.3f $\n" % (stm.rshares_to_sbd(member_data[ops["author"]]["balance_rshares"] * 0.2))
                    else:
                        reply_body += "* you need to wait until your upvote value (current value: %.3f $) is above %.3f $\n" % (stm.rshares_to_sbd(member_data[ops["author"]]["balance_rshares"] * 0.2), stm.rshares_to_sbd(minimum_vote_threshold))
                else:
                    reply_body += "* as you did not write a post within the last 7 days, your comments will be upvoted.\n"
                    if member_data[ops["author"]]["balance_rshares"] * 0.2 > minimum_vote_threshold * 2:
                        reply_body += "* your next SBI upvote is predicted to be %.3f $\n" % (stm.rshares_to_sbd(member_data[ops["author"]]["balance_rshares"] * 0.2))
                    else:
                        reply_body += "* you need to wait until your upvote value (current value: %.3f $) is above %.3f $\n" % (stm.rshares_to_sbd(member_data[ops["author"]]["balance_rshares"] * 0.2), stm.rshares_to_sbd(minimum_vote_threshold * 2))
                    
                    
                    
                account_name = account_list[random.randint(0, len(account_list) - 1)]
                if len(c.permlink) < 255:
                    c.reply(reply_body, author=account_name)
                    time.sleep(4)
            
                
        already_voted = False
    
        for v in c["active_votes"]:
            if v["voter"] in accounts:
                already_voted = True
                  
        dt_created = c["created"]
        dt_created = dt_created.replace(tzinfo=None)
        
        posts_dict[authorperm] = {"authorperm": authorperm, "author": ops["author"], "created": dt_created, "main_post": main_post,
                     "voted": already_voted}
        
        if len(posts_dict) > 100:
            start_time = time.time()
            postTrx.add_batch(posts_dict)
            print("Adding %d post took %.2f seconds" % (len(posts_dict), time.time() - start_time))
            posts_dict = {}
            

        cnt += 1

    print("write member database")
    member_data_list = []
    for m in member_data:
        member_data_list.append(member_data[m])
    memberStorage.add_batch(member_data_list)
    member_data_list = []
    if len(posts_dict) > 0:
        start_time = time.time()
        postTrx.add_batch(posts_dict)
        print("Adding %d post took %.2f seconds" % (len(posts_dict), time.time() - start_time))
        posts_dict = {}        

    print("stream posts script run %.2f s" % (time.time() - start_prep_time))