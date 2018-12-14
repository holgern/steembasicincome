from beem.utils import formatTimeString, resolve_authorperm, construct_authorperm, addTzInfo
from beem.nodelist import NodeList
from beem.comment import Comment
from beem import Steem
from datetime import datetime, timedelta
from beem.instance import set_shared_steem_instance
from beem.blockchain import Blockchain
from beem.account import Account
import time 
import json
import os
import math
import dataset
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
        database = config_data["database"]
        database_transfer = config_data["database_transfer"]
        databaseConnector = config_data["databaseConnector"]
        databaseConnector2 = config_data["databaseConnector2"]
        other_accounts = config_data["other_accounts"]    

        
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
    comment_vote_divider = conf_setup["comment_vote_divider"]
    comment_vote_timeout_h = conf_setup["comment_vote_timeout_h"]
    
    member_accounts = memberStorage.get_all_accounts()
    
    nobroadcast = False
    # nobroadcast = True
    
    member_data = {}
    for m in member_accounts:
        member_data[m] = Member(memberStorage.get(m))    
    
    print("%d members in list" % len(member_accounts))    
    postTrx = PostsTrx(db)

    print("Upvote new posts")
    start_timestamp = datetime(2018, 12, 14, 9, 18, 20)


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
    for acc in accounts:
        keys.append(keyStorage.get(acc, "posting"))
    keys_list = []
    for k in keys:
        if k["key_type"] == 'posting':
            keys_list.append(k["wif"].replace("\n", '').replace('\r', ''))
    node_list = nodes.get_nodes(normal=normal, appbase=appbase, wss=wss, https=https)
    stm = Steem(node=node_list, keys=keys_list, num_retries=5, call_num_retries=3, timeout=15, nobroadcast=nobroadcast)      
    
    voter_accounts = {}
    for acc in accounts:
        voter_accounts[acc] = Account(acc, steem_instance=stm)    
    
    b = Blockchain(steem_instance = stm)
    print("deleting old posts")
    postTrx.delete_old_posts(7)
    # print("reading all authorperm")
    already_voted_posts = []
    flagged_posts = []
    rshares_sum = 0
    
    post_list = postTrx.get_unvoted_post()
    for authorperm in post_list:
        created = post_list[authorperm]["created"]
        if (datetime.utcnow() - created).total_seconds() > 3 * 24 * 60 * 60:
            continue
        if (start_timestamp > created):
            continue
        author = post_list[authorperm]["author"]
        if author not in member_accounts:
            continue
        member = member_data[author]
        if member["comment_upvote"] == 0 and post_list[authorperm]["main_post"] == 0:
            continue
        if member["blacklisted"]:
            continue
        elif member["blacklisted"] is None and (member["steemcleaners"] or member["buildawhale"]):
            continue
        if post_list[authorperm]["main_post"] == 0 and (datetime.utcnow() - created).total_seconds() > comment_vote_timeout_h * 60 * 60:
            continue
        rshares = member["balance_rshares"] / comment_vote_divider
        if rshares < minimum_vote_threshold:
            continue
        
        try:
            c = Comment(authorperm, steem_instance=stm)
        except:
            continue
        main_post = c.is_main_post()
        already_voted = False
        if c.time_elapsed() > timedelta(hours=156):
            continue        
    
        for v in c["active_votes"]:
            if v["voter"] in accounts:
                already_voted = True
        if already_voted:
            postTrx.update_voted(author, created, already_voted)
            continue
                  
        if c.time_elapsed() < timedelta(seconds=60 * 15):
            continue

        
        if post_list[authorperm]["main_post"] == 0:
            highest_pct = 0
            voter = None
            current_mana = {}
            for acc in voter_accounts:
                mana = voter_accounts[acc].get_manabar()
                vote_percentage = rshares / (mana["max_mana"] / 50 * mana["current_mana_pct"] / 100) * 100
                if highest_pct < mana["current_mana_pct"] and vote_percentage > 0.01:
                    highest_pct = mana["current_mana_pct"]
                    current_mana = mana
                    voter = acc
             
            vote_percentage = rshares / (current_mana["max_mana"] / 50 * current_mana["current_mana_pct"] / 100) * 100
            if vote_percentage > 1 / comment_vote_divider * 100:
                vote_percentage = 1 / comment_vote_divider * 100            
            if nobroadcast:
                print(c["authorperm"])
                print("Vote %s from %s with %.2f %%" % (author, voter, vote_percentage))            
            else:
                print("Upvote %s from %s with %.2f %%" % (author, voter, vote_percentage)) 
                vote_sucessfull = False
                cnt = 0
                while not vote_sucessfull and cnt < 5:
                    try:
                        c.upvote(vote_percentage, voter=voter)
                        time.sleep(4)
                        c.refresh()
                        for v in c["active_votes"]:
                            if voter == v["voter"]:
                                vote_sucessfull = True
                    except:
                        time.sleep(4)
                        if cnt > 0:
                            c.steem.rpc.next()
                        print("retry to vote %s" % c["authorperm"])
                    cnt += 1
                postTrx.update_voted(author, created, vote_sucessfull)
        else:
            highest_pct = 0
            voter = None
            current_mana = {}
            pool_rshars = []
            for acc in voter_accounts:
                voter_accounts[acc].refresh()
                mana = voter_accounts[acc].get_manabar()
                vote_percentage = rshares / (mana["max_mana"] / 50 * mana["current_mana_pct"] / 100) * 100
                if highest_pct < mana["current_mana_pct"] and rshares < mana["max_mana"] / 50 * mana["current_mana_pct"] / 100 and vote_percentage > 0.01:
                    highest_pct = mana["current_mana_pct"]
                    current_mana = mana
                    voter = acc
           
            if voter is None:
                print("Could not find voter for %s" % author)
                current_mana = {}
                pool_rshars = []
                pool_completed = False
                while rshares > 0 and not pool_completed:
                    highest_mana = 0
                    voter = None
                    for acc in voter_accounts:
                        voter_accounts[acc].refresh()
                        mana = voter_accounts[acc].get_manabar()
                        vote_percentage = rshares / (mana["max_mana"] / 50 * mana["current_mana_pct"] / 100) * 100
                        if highest_mana < mana["max_mana"] / 50 * mana["current_mana_pct"] / 100 and acc not in pool_rshars and vote_percentage > 0.01:
                            highest_mana = mana["max_mana"] / 50 * mana["current_mana_pct"] / 100
                            current_mana = mana
                            voter = acc
                    if voter is None:
                        pool_completed = True
                        continue
                    pool_rshars.append(voter)
                    vote_percentage = rshares / (current_mana["max_mana"] / 50 * current_mana["current_mana_pct"] / 100) * 100
                    if vote_percentage > 100:
                        vote_percentage = 100
                    if nobroadcast:
                        print(c["authorperm"])
                        print("Vote %s from %s with %.2f %%" % (author, voter, vote_percentage))
                    else:
                        print("Upvote %s from %s with %.2f %%" % (author, voter, vote_percentage))
                        vote_sucessfull = False
                        cnt = 0
                        while not vote_sucessfull and cnt < 5:
                            try:
                                c.upvote(vote_percentage, voter=voter)
                                time.sleep(4)
                                c.refresh()
                                for v in c["active_votes"]:
                                    if voter == v["voter"]:
                                        vote_sucessfull = True
                            except:
                                time.sleep(4)
                                if cnt > 0:
                                    c.steem.rpc.next()
                                print("retry to vote %s" % c["authorperm"])
                            cnt += 1                        
                    rshares_sum += (current_mana["max_mana"] / 50 * current_mana["current_mana_pct"] / 100)
                    rshares -= (current_mana["max_mana"] / 50 * current_mana["current_mana_pct"] / 100)
                
            else:
                vote_percentage = rshares / (current_mana["max_mana"] / 50 * current_mana["current_mana_pct"] / 100) * 100
                rshares_sum += current_mana["max_mana"] / 50 * current_mana["current_mana_pct"] / 100
                if nobroadcast:
                    print(c["authorperm"])
                    print("Vote %s from %s with %.2f %%" % (author, voter, vote_percentage))
                else:
                    print("Upvote %s from %s with %.2f %%" % (author, voter, vote_percentage))
                    vote_sucessfull = False
                    cnt = 0
                    while not vote_sucessfull and cnt < 5:
                        try:
                            c.upvote(vote_percentage, voter=voter)
                            time.sleep(4)
                            c.refresh()
                            for v in c["active_votes"]:
                                if voter == v["voter"]:
                                    vote_sucessfull = True
                        except:
                            time.sleep(4)
                            if cnt > 0:
                                c.steem.rpc.next()
                            print("retry to vote %s" % c["authorperm"])
                        cnt += 1
                    postTrx.update_voted(author, created, vote_sucessfull)
                    
            print("rshares_sum %d" % rshares_sum)
            
            
            
            
            
     