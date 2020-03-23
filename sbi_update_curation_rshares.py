from beem.account import Account
from beem.comment import Comment
from beem.vote import ActiveVotes
from beem.amount import Amount
from beem import Steem
from beem.instance import set_shared_steem_instance
from beem.nodelist import NodeList
from beem.memo import Memo
from beem.utils import addTzInfo, resolve_authorperm, formatTimeString, construct_authorperm
from datetime import datetime, timedelta
import requests
import re
import json
import os
import time
from time import sleep
import dataset
from steembi.parse_hist_op import ParseAccountHist
from steembi.storage import TrxDB, MemberDB, ConfigurationDB, KeysDB, TransactionMemoDB, AccountsDB, TransferMemoDB
from steembi.transfer_ops_storage import TransferTrx, AccountTrx, MemberHistDB
from steembi.memo_parser import MemoParser
from steembi.member import Member



if __name__ == "__main__":
    config_file = 'config.json'
    if not os.path.isfile(config_file):
        raise Exception("config.json is missing!")
    else:
        with open(config_file) as json_data_file:
            config_data = json.load(json_data_file)
        # print(config_data)
        accounts = config_data["accounts"]
        databaseConnector = config_data["databaseConnector"]
        databaseConnector2 = config_data["databaseConnector2"]
        mgnt_shares = config_data["mgnt_shares"]
        hive_blockchain = config_data["hive_blockchain"]
        
    start_prep_time = time.time()
    db2 = dataset.connect(databaseConnector2)
    db = dataset.connect(databaseConnector)
    transferStorage = TransferTrx(db)    
    # Create keyStorage
    trxStorage = TrxDB(db2)
    keyStorage = KeysDB(db2)
    memberStorage = MemberDB(db2)
    # accountStorage = MemberHistDB(db)
    confStorage = ConfigurationDB(db2)
    transactionStorage = TransactionMemoDB(db2)

    transferMemosStorage = TransferMemoDB(db2)

    accountStorage = AccountsDB(db2)
    accounts = accountStorage.get()
    accounts_data = accountStorage.get_data()
    other_accounts = accountStorage.get_transfer()     
    
    conf_setup = confStorage.get()
    
    last_cycle = conf_setup["last_cycle"]
    share_cycle_min = conf_setup["share_cycle_min"]
    sp_share_ratio = conf_setup["sp_share_ratio"]
    rshares_per_cycle = conf_setup["rshares_per_cycle"]
    del_rshares_per_cycle = conf_setup["del_rshares_per_cycle"]
    upvote_multiplier = conf_setup["upvote_multiplier"]
    last_paid_post = conf_setup["last_paid_post"]
    last_paid_comment = conf_setup["last_paid_comment"]
    last_delegation_check = conf_setup["last_delegation_check"]
    minimum_vote_threshold = conf_setup["minimum_vote_threshold"]
    upvote_multiplier_adjusted = conf_setup["upvote_multiplier_adjusted"]
    
    accountTrx = {}
    for account in accounts:
        if account == "steembasicincome":
            accountTrx["sbi"] = AccountTrx(db, "sbi")
        else:
            accountTrx[account] = AccountTrx(db, account)    

    
    
    print("sbi_update_curation_rshares: last_cycle: %s - %.2f min" % (formatTimeString(last_cycle), (datetime.utcnow() - last_cycle).total_seconds() / 60))
    print("last_paid_post: %s - last_paid_comment: %s" % (formatTimeString(last_paid_post), formatTimeString(last_paid_comment)))

    if (datetime.utcnow() - last_cycle).total_seconds() > 60 * share_cycle_min:
        
        
        new_cycle = (datetime.utcnow() - last_cycle).total_seconds() > 60 * share_cycle_min
        current_cycle = last_cycle + timedelta(seconds=60 * share_cycle_min)
        
        
        print("Update member database, new cycle: %s" % str(new_cycle))
        # memberStorage.wipe(True)
        member_accounts = memberStorage.get_all_accounts()

        
        #print(key_list)
        nodes = NodeList()
        nodes.update_nodes()
        stm = Steem(node=nodes.get_nodes(hive=hive_blockchain))
        stm2 = Steem(node=nodes.get_nodes(hive=hive_blockchain), use_condenser=True)

        member_data = {}
        n_records = 0
        share_age_member = {}    
        for m in member_accounts:
            member_data[m] = Member(memberStorage.get(m))


        if True:    
            print("reward voted steembasicincome post and comments")
            # account = Account("steembasicincome", steem_instance=stm)
            
            if last_paid_post is None:
                last_paid_post = datetime(2018, 8, 9, 3, 36, 48)
            new_paid_post = last_paid_post
            if last_paid_comment is None:
                last_paid_comment = datetime(2018, 8, 9, 3, 36, 48)
            # elif (datetime.utcnow() - last_paid_comment).total_seconds() / 60 / 60 / 24 < 6.5:
            #    last_paid_comment = datetime.utcnow() - timedelta(days=7)
            new_paid_comment = last_paid_comment
            
            for account in accounts:
                last_paid_post = conf_setup["last_paid_post"]
                last_paid_comment = conf_setup["last_paid_post"]                
                
                if accounts_data[account]["last_paid_comment"] is not None:
                    last_paid_comment = accounts_data[account]["last_paid_comment"]
                if accounts_data[account]["last_paid_post"] is not None:
                    last_paid_post = accounts_data[account]["last_paid_post"]                
                
                account = Account(account, steem_instance=stm)
                if last_paid_post < last_paid_comment:
                    oldest_timestamp = last_paid_post
                else:
                    oldest_timestamp = last_paid_comment
                if account["name"] == "steembasicincome":
                    ops = accountTrx["sbi"].get_newest(oldest_timestamp, op_types=["comment"], limit=500)
                else:
                    ops = accountTrx[account["name"]].get_newest(oldest_timestamp, op_types=["comment"], limit=50)
                blog = []
                posts = []
                for op in ops[::-1]:
                    try:
                        comment = (json.loads(op["op_dict"]))
                        created = formatTimeString(comment["timestamp"])
                    except:
                        op_dict = op["op_dict"]
                        comment = json.loads(op_dict[:op_dict.find("body")-3] + '}')
                    try:
                        comment = Comment(comment, steem_instance=stm)
                        comment.refresh()
                        created = comment["created"]
                    except:
                        continue
                    if comment.is_pending():
                        continue
                    if comment["author"] != account["name"]:
                        continue
                    
                    if comment["parent_author"] == "" and created > addTzInfo(last_paid_post):
                        print("add post %s" %  comment["authorperm"])
                        blog.append(comment["authorperm"])
                    elif comment["parent_author"] != "" and created > addTzInfo(last_paid_comment):
                        print("add comment %s" %  comment["authorperm"])
                        posts.append(comment["authorperm"])
    
    
                post_rshares = 0
                for authorperm in blog:
                    post = Comment(authorperm, steem_instance=stm)
                    print("Checking post %s" % post["authorperm"])
                    if post["created"] > addTzInfo(new_paid_post):
                        new_paid_post = post["created"].replace(tzinfo=None) 
                    last_paid_post = post["created"].replace(tzinfo=None) 
                    all_votes = ActiveVotes(post["authorperm"], steem_instance=stm2)
                    for vote in all_votes:
                        if vote["voter"] in member_data:
                            if member_data[vote["voter"]]["shares"] <= 0:
                                continue
                            if account["name"] == "steembasicincome":
                                rshares = vote["rshares"] * upvote_multiplier
                                if rshares < rshares_per_cycle:
                                    rshares = rshares_per_cycle
                            else:
                                rshares = vote["rshares"] * upvote_multiplier * upvote_multiplier_adjusted
                            member_data[vote["voter"]]["earned_rshares"] += rshares
                            member_data[vote["voter"]]["curation_rshares"] += rshares
                            member_data[vote["voter"]]["balance_rshares"] += rshares
                            post_rshares += rshares

            
                comment_rshares = 0
                for authorperm in posts:
                    post = Comment(authorperm, steem_instance=stm)
                    if post["created"] > addTzInfo(new_paid_comment):
                        new_paid_comment = post["created"].replace(tzinfo=None)
                    last_paid_comment = post["created"].replace(tzinfo=None) 
                    all_votes = ActiveVotes(post["authorperm"], steem_instance=stm2)
                    for vote in all_votes:
                        if vote["voter"] in member_data:
                            if member_data[vote["voter"]]["shares"] <= 0:
                                continue                    
                            rshares = vote["rshares"]
                            if rshares < 50000000:
                                continue
                            rshares = rshares * upvote_multiplier * upvote_multiplier_adjusted
                            member_data[vote["voter"]]["earned_rshares"] += rshares
                            member_data[vote["voter"]]["curation_rshares"] += rshares
                            member_data[vote["voter"]]["balance_rshares"] += rshares
                            comment_rshares += rshares
                accounts_data[account["name"]]["last_paid_comment"] = last_paid_comment
                accounts_data[account["name"]]["last_paid_post"] = last_paid_post
                print("%d new curation rshares for posts" % post_rshares)
                print("%d new curation rshares for comments" % comment_rshares)
        print("write member database")
        memberStorage.db = dataset.connect(databaseConnector2)
        member_data_list = []
        for m in member_data:
            member_data_list.append(member_data[m])
        memberStorage.add_batch(member_data_list)
        member_data_list = []
        for acc in accounts_data:
            accountStorage.update(accounts_data[acc])

        
    print("update curation rshares script run %.2f s" % (time.time() - start_prep_time))
