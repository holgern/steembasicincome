from beem.account import Account
from beem.amount import Amount
from beem import Steem
from beem.instance import set_shared_steem_instance
from beem.nodelist import NodeList
from beem.utils import addTzInfo, resolve_authorperm, formatTimeString
from datetime import datetime, timedelta
import re
import json
import os
from time import sleep
import dataset
from steembi.parse_hist_op import ParseAccountHist
from steembi.storage import TrxDB, MemberDB, ConfigurationDB
from steembi.transfer_ops_storage import TransferTrx, AccountTrx, MemberHistDB
from steembi.member import Member

    

if __name__ == "__main__":
    config_file = 'config.json'
    if not os.path.isfile(config_file):
        accounts = ["steembasicincome", "sbi2", "sbi3", "sbi4", "sbi5", "sbi6", "sbi7", "sbi8", "sbi9"]
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
        
        
    update_comment_date = True
    update_delegation_data = True

    db2 = dataset.connect(databaseConnector2)
    db = dataset.connect(databaseConnector)
    transferStorage = TransferTrx(db)    
    # Create keyStorage
    trxStorage = TrxDB(db2)
    memberStorage = MemberDB(db2)
    accountStorage = MemberHistDB(db)
    confStorage = ConfigurationDB(db2)
    print("update member database")
    # memberStorage.wipe(True)
    member_accounts = memberStorage.get_all_accounts()
    # data = trxStorage.get_all_data()
    
    # Update current node list from @fullnodeupdate
    nodes = NodeList()
    # nodes.update_nodes()
    # stm = Steem(node=nodes.get_nodes())
    stm = Steem()
    
    member_data = {}
    n_records = 0
    share_age_member = {}
    updated_at = None
    for m in member_accounts:
        member_data[m] = Member(memberStorage.get(m))
        if updated_at is None:
            updated_at = member_data[m]["updated_at"]
            last_updated_member = m
        elif member_data[m]["updated_at"] < updated_at:
            updated_at = member_data[m]["updated_at"]
            last_updated_member = m
    
    
    empty_shares = []
    latest_enrollment = None
    for m in member_data:
        if member_data[m]["shares"] <= 0:
            empty_shares.append(m)
        if latest_enrollment is None:
            latest_enrollment = member_data[m]["latest_enrollment"]
        elif latest_enrollment < member_data[m]["latest_enrollment"]:
            latest_enrollment = member_data[m]["latest_enrollment"]
    
    print("latest member enrollment %s" % (str(latest_enrollment)))
      

    # date_now = datetime.utcnow()
    date_now = latest_enrollment
    date_7_before = addTzInfo(date_now - timedelta(seconds=7 * 24 * 60 * 60))
    date_28_before = addTzInfo(date_now - timedelta(seconds=28 * 24 * 60 * 60))

        
    print("update last post and comment date")
    new_member = []
    for m in [last_updated_member]:
        if addTzInfo(member_data[m]["original_enrollment"]) > date_28_before:
            new_member.append(m)
            continue
        print(m)
        all_comments = accountStorage.get_comments(m)
        latest_post = []
        latest_comment = []            
        for c in all_comments:
            if addTzInfo(c["timestamp"]) < date_28_before:
                continue
            if c["parent_author"] == "":
                latest_post.append(c)
            else:
                latest_comment.append(c)
        all_votes = accountStorage.get_votes(m)
        latest_votes = []
        for v in all_votes:
            if addTzInfo(v["timestamp"]) < date_28_before:
                continue
            latest_votes.append(v)
        post_count_7 = 0
        post_count_28 = 0
        norm_post_count_7 = 0
        upvote_count_28 = 0
        upvote_weight_28 = 0
        if len(latest_votes) > 0:
            for v in latest_votes:
                if addTzInfo(v["timestamp"]) >= date_28_before:
                    upvote_count_28 += 1
                    upvote_weight_28 += v["weight"]
        member_data[m]["upvote_count_28"] = upvote_count_28
        if upvote_count_28 == 0:
            member_data[m]["upvote_weight_28"] = 0
        else:
            member_data[m]["upvote_weight_28"] = upvote_weight_28 / upvote_count_28 / 100.

        if len(latest_post) > 0:
            member_data[m]["last_post"] = latest_post[-1]["timestamp"]
            for p in latest_post:
                if addTzInfo(p["timestamp"]) >= date_28_before:
                    post_count_28 += 1
                if addTzInfo(p["timestamp"]) >= date_7_before:
                    post_count_7 += 1
        member_data[m]["post_hist_28"] = post_count_28
        member_data[m]["post_hist_7"] = post_count_7
        member_data[m]["norm_post_hist_7"] = post_count_28 / 4.0
        if len(latest_comment) > 0:
            member_data[m]["last_comment"] = latest_comment[-1]["timestamp"]
        member_data[m]["updated_at"] = latest_enrollment
    print("update new accounts")
    for m in new_member:
        print(m)
        acc = Account(m)
        post_count_7 = 0
        post_count_28 = 0
        norm_post_count_7 = 0
        all_votes = acc.get_account_votes()
        upvote_count_28 = 0
        upvote_weight_28 = 0
        if len(all_votes) > 0:
            for v in all_votes:
                if formatTimeString(v["time"]) >= date_28_before:
                    author, permlink = resolve_authorperm(v["authorperm"])
                    if author in accounts:
                        upvote_count_28 += 1
                        upvote_weight_28 += v["weight"]
        member_data[m]["upvote_count_28"] = upvote_count_28
        if upvote_count_28 == 0:
            member_data[m]["upvote_weight_28"] = 0
        else:
            member_data[m]["upvote_weight_28"] = upvote_weight_28 / upvote_count_28 / 100.
        member_data[m]["updated_at"] = latest_enrollment

        latest_post = acc.get_blog(limit=100)
        latest_comment = None
        for r in acc.comment_history(limit=1):
            latest_comment = r
        if latest_comment is not None:
            member_data[m]["last_comment"] = latest_comment["created"]

        if len(latest_post) > 0:
            member_data[m]["last_post"] = latest_post[0]["created"]
            for p in latest_post:
                if p["created"] >= date_28_before:
                    post_count_28 += 1
                if p["created"] >= date_7_before:
                    post_count_7 += 1
        member_data[m]["post_hist_28"] = post_count_28
        member_data[m]["post_hist_7"] = post_count_7
        member_data[m]["norm_post_hist_7"] = post_count_28 / 4.0
        member_data[m]["updated_at"] = latest_enrollment

    memberStorage.db = dataset.connect(databaseConnector2)
    print("write member database")
    for m in [last_updated_member]:
        data = Member(memberStorage.get(m))
        data["last_comment"] = member_data[m]["last_comment"]
        data["last_post"] = member_data[m]["last_post"]
        data["post_hist_28"] = member_data[m]["post_hist_28"]
        data["post_hist_7"] = member_data[m]["post_hist_7"]
        data["norm_post_hist_7"] = member_data[m]["norm_post_hist_7"]
        data["upvote_count_28"] = member_data[m]["upvote_count_28"]
        data["upvote_weight_28"] = member_data[m]["upvote_weight_28"]
        data["updated_at"] = member_data[m]["updated_at"]

        memberStorage.update(data)
