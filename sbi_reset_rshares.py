from beem.account import Account
from beem.amount import Amount
from beem import Steem
from beem.instance import set_shared_steem_instance
from beem.nodelist import NodeList
from beem.utils import addTzInfo, resolve_authorperm, formatTimeString
from beem.vote import AccountVotes
from beem.comment import Comment
from beem.block import Block
from beem.blockchain import Blockchain
from beem.wallet import Wallet
from beembase.signedtransactions import Signed_Transaction
from beemgraphenebase.base58 import Base58
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
        
        
    db2 = dataset.connect(databaseConnector2)
    db = dataset.connect(databaseConnector)
    transferStorage = TransferTrx(db)    
    # Create keyStorage
    trxStorage = TrxDB(db2)
    memberStorage = MemberDB(db2)
    accountStorage = MemberHistDB(db)
    confStorage = ConfigurationDB(db2)
    
    accountTrx = {}
    for account in accounts:
        accountTrx[account] = AccountTrx(db, account)    
    
    conf_setup = confStorage.get()
    
    last_cycle = conf_setup["last_cycle"]
    share_cycle_min = conf_setup["share_cycle_min"]
    sp_share_ratio = conf_setup["sp_share_ratio"]
    rshares_per_cycle = conf_setup["rshares_per_cycle"]
    upvote_multiplier = conf_setup["upvote_multiplier"]
    last_paid_post = conf_setup["last_paid_post"]
    last_paid_comment = conf_setup["last_paid_comment"]
    

    minimum_vote_threshold = conf_setup["minimum_vote_threshold"]
    comment_vote_divider = conf_setup["comment_vote_divider"]
    comment_vote_timeout_h = conf_setup["comment_vote_timeout_h"]    
    
    print("last_cycle: %s - %.2f min" % (formatTimeString(last_cycle), (datetime.utcnow() - last_cycle).total_seconds() / 60))
    if True:
        last_cycle = datetime.utcnow() - timedelta(seconds = 60 * 145)
        confStorage.update({"last_cycle": last_cycle})        
        print("update member database")
        # memberStorage.wipe(True)
        member_accounts = memberStorage.get_all_accounts()
        data = trxStorage.get_all_data()
        
        
        
        # Update current node list from @fullnodeupdate
        nodes = NodeList()
        nodes.update_nodes()
        stm = Steem(node=nodes.get_nodes())    
        # stm = Steem()
        member_data = {}
        n_records = 0
        share_age_member = {}    
        for m in member_accounts:
            member_data[m] = Member(memberStorage.get(m))
        

        print("reset rshares")
        if True:
            for m in member_data:
                total_share_days = member_data[m]["total_share_days"]
                member_data[m]["first_cycle_at"] = datetime(1970,1,1,0,0,0)
                member_data[m]["balance_rshares"] = total_share_days * rshares_per_cycle * 10
                member_data[m]["earned_rshares"]  = total_share_days * rshares_per_cycle * 10
                member_data[m]["rewarded_rshares"] = 0
                member_data[m]["subscribed_rshares"] = total_share_days * rshares_per_cycle * 10
                member_data[m]["delegation_rshares"] = 0          
                member_data[m]["curation_rshares"] = 0 
        
        
            for acc_name in accounts:
                acc = Account(acc_name, steem_instance=stm)
                
                a = AccountVotes(acc_name, steem_instance=stm)
                print(acc_name)
                for vote in a:
                    author = vote["author"]
                    if author in member_data:
                        member_data[author]["rewarded_rshares"] += int(vote["rshares"])
                        member_data[author]["balance_rshares"] -= int(vote["rshares"])
                    
        if True:
            b = Blockchain(steem_instance=stm)
            wallet = Wallet(steem_instance=stm)
            
            for acc_name in accounts:
                print(acc_name)
                ops = accountTrx[acc_name].get_all(op_types=["comment"])
                cnt = 0
                for o in ops:
                    cnt += 1
                    if cnt % 10 == 0:
                        print("%d/%d" % (cnt, len(ops)))
                    op = json.loads(o["op_dict"])
                    c = Comment(op, steem_instance=stm)
                    try:
                        c.refresh()
                    except:
                        continue
                    for vote in c["active_votes"]:
                        if vote["rshares"] == 0:
                            continue
                        if (addTzInfo(datetime.utcnow()) - (vote["time"])).total_seconds() / 60 / 60 / 24 <= 7:
                            continue
                        if (addTzInfo(o["timestamp"]) - c["created"]).total_seconds() > 10:
                            continue
                        if vote["voter"] not in member_data:
                            continue
                        if False:
                            try:
                                block_num = b.get_estimated_block_num(vote["time"])
                                current_block_num = b.get_current_block_num()
                                transaction = None
                                block_search_list = [0, 1, -1, 2, -2, 3, -3, 4, -4, 5, -5]                       
                                block_cnt = 0
                                while transaction is None and block_cnt < len(block_search_list):
                                    if block_num + block_search_list[block_cnt] > current_block_num:
                                        block_cnt += 1
                                        continue
                                    block = Block(block_num + block_search_list[block_cnt], steem_instance=stm)
                                    for tt in block.transactions:
                                        for op in tt["operations"]:
                                            if isinstance(op, dict) and op["type"][:4] == "vote":
                                                if op["value"]["voter"] == vote["voter"]:
                                                    transaction = tt
                                            elif isinstance(op, list) and len(op) > 1 and op[0][:4] == "vote":
                                                if op[1]["voter"] == vote["voter"]:
                                                    transaction = tt
                                    block_cnt += 1
                                vote_did_sign = True
                                key_accounts = []
                                if transaction is not None:
                                    signed_tx = Signed_Transaction(transaction)
                                    public_keys = []
                                    for key in signed_tx.verify(chain=stm.chain_params, recover_parameter=True):
                                        public_keys.append(format(Base58(key, prefix=stm.prefix), stm.prefix))                            
                                    
                                    empty_public_keys = []
                                    for key in public_keys:
                                        pubkey_account = wallet.getAccountFromPublicKey(key)
                                        if pubkey_account is None:
                                            empty_public_keys.append(key)
                                        else:
                                            key_accounts.append(pubkey_account)
                                if len(key_accounts) > 0:
                                    vote_did_sign = True
        
                                for a in key_accounts:
                                    if vote["voter"] == a:
                                        continue
                                    if a not in ["quarry", "steemdunk"]:
                                        print(a)
                                    if a in ["smartsteem", "smartmarket", "minnowbooster"]:
                                        vote_did_sign = False               
                                
                                if not vote_did_sign:
                                    continue
                            except:
                                continue
                        
                        if c.is_main_post():
                            if acc_name == "steembasicincome":
                                rshares = vote["rshares"] * upvote_multiplier
                                if rshares < rshares_per_cycle:
                                    rshares = rshares_per_cycle
                            else:
                                rshares = vote["rshares"] * upvote_multiplier
                            member_data[vote["voter"]]["earned_rshares"] += rshares
                            member_data[vote["voter"]]["curation_rshares"] += rshares
                            member_data[vote["voter"]]["balance_rshares"] += rshares
                        else:
                            rshares = vote["rshares"]
                            if rshares < 50000000:
                                continue
                            member_data[vote["voter"]]["earned_rshares"] += rshares
                            member_data[vote["voter"]]["curation_rshares"] += rshares
                            member_data[vote["voter"]]["balance_rshares"] += rshares                            

                
                    
     
        print("write member database")
        memberStorage.db = dataset.connect(databaseConnector2)
        member_data_list = []
        for m in member_data:
            member_data_list.append(member_data[m])
        memberStorage.add_batch(member_data_list)
        member_data_list = []
