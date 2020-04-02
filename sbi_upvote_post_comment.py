from beem.utils import formatTimeString, resolve_authorperm, construct_authorperm, addTzInfo
from beem.nodelist import NodeList
from beem.comment import Comment
from beem import Steem
from datetime import datetime, timedelta
from beem.instance import set_shared_steem_instance
from beem.blockchain import Blockchain
from beem.account import Account
from beem.vote import Vote
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
        raise Exception("config.json is missing")
    else:
        with open(config_file) as json_data_file:
            config_data = json.load(json_data_file)
        # print(config_data)
        databaseConnector = config_data["databaseConnector"]
        databaseConnector2 = config_data["databaseConnector2"]
        hive_blockchain = config_data["hive_blockchain"]

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
    comment_vote_divider = conf_setup["comment_vote_divider"]
    comment_vote_timeout_h = conf_setup["comment_vote_timeout_h"]
    upvote_delay_correction = 18
    member_accounts = memberStorage.get_all_accounts()
    
    nobroadcast = False
    # nobroadcast = True
    
    upvote_counter = {}
    
    for m in member_accounts:
        upvote_counter[m] = 0
    
    # print("%d members in list" % len(member_accounts))    
    postTrx = PostsTrx(db)

    print("Upvote posts/comments")
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
    node_list = nodes.get_nodes(hive=hive_blockchain)
   
    stm = Steem(node=node_list, keys=keys_list, num_retries=5, call_num_retries=3, timeout=15, nobroadcast=nobroadcast)      
    
    voter_accounts = {}
    for acc in accounts:
        voter_accounts[acc] = Account(acc, steem_instance=stm)    
    
    b = Blockchain(steem_instance = stm)
    # print("reading all authorperm")
    already_voted_posts = []
    flagged_posts = []
    rshares_sum = 0
    start_reading = time.time()
    post_list = postTrx.get_unvoted_post()
    #print("Reading posts from database took %.2f s" % (time.time() - start_prep_time))
    # print("prep time took %.2f s" % (time.time() - start_prep_time))
    for authorperm in post_list:

        created = post_list[authorperm]["created"]
        if (datetime.utcnow() - created).total_seconds() > 1 * 24 * 60 * 60:
            continue
        if (start_timestamp > created):
            continue
        author = post_list[authorperm]["author"]
        if author not in member_accounts:
            continue
        if upvote_counter[author] > 0:
            continue
        if post_list[authorperm]["main_post"] == 0 and (datetime.utcnow() - created).total_seconds() > comment_vote_timeout_h * 60 * 60:
            postTrx.update_comment_to_old(author, created, True)

        member = Member(memberStorage.get(author))
#        if member["comment_upvote"] == 0 and post_list[authorperm]["main_post"] == 0:
        if post_list[authorperm]["main_post"] == 0:
            continue
        if member["blacklisted"]:
            continue
        elif member["blacklisted"] is None and (member["steemcleaners"] or member["buildawhale"]):
            continue

        if post_list[authorperm]["main_post"] == 1:
            rshares = member["balance_rshares"] / comment_vote_divider
        else:
            rshares = member["balance_rshares"] / (comment_vote_divider ** 2)
        if post_list[authorperm]["main_post"] == 1 and rshares < minimum_vote_threshold:
            continue
        elif post_list[authorperm]["main_post"] == 0 and rshares < minimum_vote_threshold * 2:
            continue
        cnt = 0
        c = None
        while c is None and cnt < 5:
            cnt += 1
            try:
                c = Comment(authorperm, use_tags_api=True, steem_instance=stm)
            except:
                c = None
                stm.rpc.next()
        if c is None:
            print("Error getting %s" % authorperm)
            continue
        main_post = c.is_main_post()
        already_voted = False
        if c.time_elapsed() > timedelta(hours=156):
            continue        
        voted_after = 300
            
        for v in c.get_votes():
            if v["voter"] in accounts:
                already_voted = True
                try:
                    if "time" in v:
                        voted_after = (v["time"] - c["created"]).total_seconds()
                    elif "last_update" in v:
                        voted_after = (v["last_update"] - c["created"]).total_seconds()
                    else:
                        voted_after =300
                        
                except:
                    voted_after =300
        if already_voted:
            postTrx.update_voted(author, created, already_voted, voted_after)
            continue
        vote_delay_sec = 5 * 60
        if member["upvote_delay"] is not None:
            vote_delay_sec = member["upvote_delay"]
        if c.time_elapsed() < timedelta(seconds=(vote_delay_sec - upvote_delay_correction)):
            continue
        if member["last_received_vote"] is not None and (datetime.utcnow() - member["last_received_vote"]).total_seconds() / 60 < 15:
            continue

        
        if post_list[authorperm]["main_post"] == 0:
            highest_pct = 0
            highest_rshares = 0
            voter = None
            current_mana = {}
            if rshares > minimum_vote_threshold * 20:
                rshares = int(minimum_vote_threshold * 20)
            for acc in voter_accounts:
                mana = voter_accounts[acc].get_manabar()
                vote_percentage = rshares / (mana["max_mana"] / 50 * mana["current_mana_pct"] / 100) * 100
                if highest_pct < mana["current_mana_pct"] and rshares < mana["max_mana"] / 50 * mana["current_mana_pct"] / 100 and vote_percentage > 0.01:
                    highest_pct = mana["current_mana_pct"]
                    current_mana = mana
                    voter = acc
            if voter is None:
                voter = "steembasicincome"
                current_mana = voter_accounts[acc].get_manabar()
            vote_percentage = rshares / (current_mana["max_mana"] / 50 * current_mana["current_mana_pct"] / 100) * 100

            if nobroadcast and voter is not None:
                print(c["authorperm"])
                print("Comment Vote %s from %s with %.2f %%" % (author, voter, vote_percentage))            
            elif voter is not None:
                print("Comment Upvote %s from %s with %.2f %%" % (author, voter, vote_percentage)) 
                vote_sucessfull = False
                voted_after = 300
                cnt = 0
                vote_time = None
                while not vote_sucessfull and cnt < 5:
                    try:
                        c.upvote(vote_percentage, voter=voter)
                        time.sleep(4)
                        c.refresh()
                        for v in c.get_votes():
                            if voter == v["voter"]:
                                vote_sucessfull = True
                                if "time" in v:
                                    vote_time = v["time"]
                                    voted_after = (v["time"] - c["created"]).total_seconds()
                                else:
                                    vote_time = v["last_update"]
                                    voted_after = (v["last_update"] - c["created"]).total_seconds()                                    
                    except Exception as e:
                        print(e)
                        time.sleep(4)
                        if cnt > 0:
                            c.steem.rpc.next()
                        print("retry to vote %s" % c["authorperm"])
                    cnt += 1
                if vote_sucessfull:
                    print("Vote for %s at %s was sucessfully" % (author, str(vote_time)))
                    memberStorage.update_last_vote(author, vote_time)
                    upvote_counter[author] += 1
                postTrx.update_voted(author, created, vote_sucessfull, voted_after)
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
                        vote_time = None
                        while not vote_sucessfull and cnt < 5:
                            try:
                                c.upvote(vote_percentage, voter=voter)
                                time.sleep(4)
                                c.refresh()
                                for v in c.get_votes():
                                    if voter == v["voter"]:
                                        vote_sucessfull = True
                                        if "time" in v:
                                            vote_time = v["time"]
                                        else:
                                            vote_time = v["last_update"]
                            except Exception as e:
                                print(e)
                                time.sleep(4)
                                if cnt > 0:
                                    c.steem.rpc.next()
                                print("retry to vote %s" % c["authorperm"])
                            cnt += 1
                        if vote_sucessfull:
                            print("Vote for %s at %s was sucessfully" % (author, str(vote_time)))
                            memberStorage.update_last_vote(author, vote_time)                        
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
                    voted_after = 300
                    vote_time = None
                    while not vote_sucessfull and cnt < 5:
                        try:
                            c.upvote(vote_percentage, voter=voter)
                            time.sleep(4)
                            c.refresh()
                            for v in c.get_votes():
                                if voter == v["voter"]:
                                    vote_sucessfull = True
                                    if "time" in v:
                                        vote_time = v["time"]
                                        voted_after = (v["time"] - c["created"]).total_seconds()
                                    else:
                                        vote_time = v["last_update"]
                                        voted_after = (v["last_update"] - c["created"]).total_seconds()                                        
                        except Exception as e:
                            print(e)
                            time.sleep(4)
                            if cnt > 0:
                                c.steem.rpc.next()
                            print("retry to vote %s" % c["authorperm"])
                        cnt += 1
                    if vote_sucessfull:
                        print("Vote for %s at %s was sucessfully" % (author, str(vote_time)))
                        memberStorage.update_last_vote(author, vote_time)
                        upvote_counter[author] += 1                    
                    postTrx.update_voted(author, created, vote_sucessfull, voted_after)
                    
            print("rshares_sum %d" % rshares_sum)
    print("upvote script run %.2f s" % (time.time() - start_prep_time))
            
            
            
            
     
