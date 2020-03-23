# This Python file uses the following encoding: utf-8
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals
from builtins import bytes, int, str
import pytz
import json
import re
from datetime import datetime, timedelta, date, time
import math
import random
import logging
from bisect import bisect_left
from beem.utils import formatTimeString, formatTimedelta, remove_from_dict, reputation_to_score, addTzInfo, parse_time
from beem.amount import Amount
from beem.account import Account
from beem.vote import Vote
from beem.memo import Memo
from beem.instance import shared_steem_instance
from beem.blockchain import Blockchain
from beem.constants import STEEM_VOTE_REGENERATION_SECONDS, STEEM_1_PERCENT, STEEM_100_PERCENT
from steembi.memo_parser import MemoParser


log = logging.getLogger(__name__)


class ParseAccountHist(list):
    
    def __init__(self, account, path, trxStorage, transactionStorage, transactionOutStorage, member_data, memberStorage = None, steem_instance=None):
        self.steem = steem_instance or shared_steem_instance()
        self.account = Account(account, steem_instance=self.steem)    
        self.delegated_vests_in = {}
        self.delegated_vests_out = {}
        self.timestamp = addTzInfo(datetime(1970, 1, 1, 0, 0, 0, 0))
        self.path = path
        self.member_data = member_data
        self.memberStorage = memberStorage
        self.memo_parser = MemoParser(steem_instance=self.steem)
        self.excluded_accounts = ["minnowbooster", "smartsteem", "randowhale", "steemvoter", "jerrybanfield",
                                  "boomerang", "postpromoter", "appreciator", "buildawhale", "upme", "smartmarket",
                                  "minnowhelper", "pushup", "steembasicincome", "sbi2", "sbi3", "sbi4", "sbi5", "sbi6", "sbi7", "sbi8", "sbi9"]

        self.trxStorage = trxStorage
        self.transactionStorage = transactionStorage
        self.transactionOutStorage = transactionOutStorage

    def get_highest_avg_share_age_account(self):
        max_avg_share_age = 0
        account_name = None
        for m in self.member_data:
            self.member_data[m].calc_share_age()
        for m in self.member_data:  
            if max_avg_share_age < self.member_data[m]["avg_share_age"]:
                max_avg_share_age = self.member_data[m]["avg_share_age"]
                account_name = m

        return account_name

    def update_delegation(self, op, delegated_in=None, delegated_out=None):
        """ Updates the internal state arrays

            :param datetime timestamp: datetime of the update
            :param Amount/float own: vests
            :param dict delegated_in: Incoming delegation
            :param dict delegated_out: Outgoing delegation
            :param Amount/float steem: steem
            :param Amount/float sbd: sbd

        """

        self.timestamp = op["timestamp"]

        new_deleg = dict(self.delegated_vests_in)
        if delegated_in is not None and delegated_in:
            if delegated_in['amount'] == 0 and delegated_in['account'] in new_deleg:
                self.new_delegation_record(op["index"], delegated_in['account'], delegated_in['amount'], op["timestamp"], share_type="RemovedDelegation")
                del new_deleg[delegated_in['account']]
            elif delegated_in['amount']  > 0:
                self.new_delegation_record(op["index"], delegated_in['account'], delegated_in['amount'], op["timestamp"], share_type="Delegation")
                new_deleg[delegated_in['account']] = delegated_in['amount']
            else:
                self.new_delegation_record(op["index"], delegated_in['account'], delegated_in['amount'], op["timestamp"], share_type="RemovedDelegation")
        self.delegated_vests_in = new_deleg

        new_deleg = dict(self.delegated_vests_out)
        if delegated_out is not None and delegated_out:
            if delegated_out['account'] is None:
                # return_vesting_delegation
                for delegatee in new_deleg:
                    if new_deleg[delegatee]['amount'] == delegated_out['amount']:
                        del new_deleg[delegatee]
                        break

            elif delegated_out['amount'] != 0:
                # new or updated non-zero delegation
                new_deleg[delegated_out['account']] = delegated_out['amount']

                # skip undelegations here, wait for 'return_vesting_delegation'
                # del new_deleg[delegated_out['account']]

        self.delegated_vests_out = new_deleg

        delegated_sp_in = {}
        for acc in self.delegated_vests_in:
            vests = Amount(self.delegated_vests_in[acc])
            delegated_sp_in[acc] = str(self.steem.vests_to_sp(vests))
        delegated_sp_out = {}
        for acc in self.delegated_vests_out:
            vests = Amount(self.delegated_vests_out[acc])
            delegated_sp_out[acc] = str(self.steem.vests_to_sp(vests))

        if self.path is None:
            return        
        #with open(self.path + 'sbi_delegation_in_'+self.account["name"]+'.txt', 'w') as the_file:
        #    the_file.write(str(delegated_sp_in) + '\n')
        #with open(self.path + 'sbi_delegation_out_'+self.account["name"]+'.txt', 'w') as the_file:
        #    the_file.write(str(delegated_sp_out) + '\n')



    def parse_transfer_out_op(self, op):
        amount = Amount(op["amount"], steem_instance=self.steem)
        index = op["index"]
        account = op["from"]
        timestamp = op["timestamp"]
        encrypted = False
        processed_memo = ascii(op["memo"]).replace('\n', '').replace('\\n', '').replace('\\', '')
        if len(processed_memo) > 2 and (processed_memo[0] == '#' or processed_memo[1] == '#' or processed_memo[2] == '#') and account == "steembasicincome":
            if processed_memo[1] == '#':
                processed_memo = processed_memo[1:-1]
            elif processed_memo[2] == '#':
                processed_memo = processed_memo[2:-2]        
            memo = Memo(account, op["to"], steem_instance=self.steem)
            processed_memo = ascii(memo.decrypt(processed_memo)).replace('\n', '')
            encrypted = True
        
        if amount.amount < 1:
            data = {"index": index, "sender": account, "to": op["to"], "memo": processed_memo, "encrypted": encrypted, "referenced_accounts": None, "amount": amount.amount, "amount_symbol": amount.symbol, "timestamp": timestamp}
            self.transactionOutStorage.add(data)            
            return
        if amount.symbol == self.steem.sbd_symbol:
            # self.trxStorage.get_account(op["to"], share_type="SBD")
            shares = -int(amount.amount)
            if "http" in op["memo"] or self.steem.steem_symbol not in op["memo"]:
                data = {"index": index, "sender": account, "to": op["to"], "memo": processed_memo, "encrypted": encrypted, "referenced_accounts": None, "amount": amount.amount, "amount_symbol": amount.symbol, "timestamp": timestamp}
                self.transactionOutStorage.add(data)                
                return
            trx = self.trxStorage.get_SBD_transfer(op["to"], shares, formatTimeString(op["timestamp"]), SBD_symbol=self.steem.sbd_symbol)
            sponsee = json.dumps({})
            if trx:
                sponsee = trx["sponsee"]
            self.new_transfer_record(op["index"], processed_memo, op["to"], op["to"], sponsee, shares, op["timestamp"], share_type="Refund")
            # self.new_transfer_record(op["index"], op["to"], "", shares, op["timestamp"], share_type="Refund")
            data = {"index": index, "sender": account, "to": op["to"], "memo": processed_memo, "encrypted": encrypted, "referenced_accounts": sponsee, "amount": amount.amount, "amount_symbol": amount.symbol, "timestamp": timestamp}
            self.transactionOutStorage.add(data)             
            return

        else:
            data = {"index": index, "sender": account, "to": op["to"], "memo": processed_memo, "encrypted": encrypted, "referenced_accounts": None, "amount": amount.amount, "amount_symbol": amount.symbol, "timestamp": timestamp}
            self.transactionOutStorage.add(data)
            return            

    def parse_transfer_in_op(self, op):
        amount = Amount(op["amount"], steem_instance=self.steem)
        share_type = "Standard"
        index = op["index"]
        account = op["from"]
        timestamp = op["timestamp"]
        sponsee = {}
        processed_memo = ascii(op["memo"]).replace('\n', '').replace('\\n', '').replace('\\', '')
        if len(processed_memo) > 2 and (processed_memo[0] == '#' or processed_memo[1] == '#' or processed_memo[2] == '#') and account == "steembasicincome":
            if processed_memo[1] == '#':
                processed_memo = processed_memo[1:-1]
            elif processed_memo[2] == '#':
                processed_memo = processed_memo[2:-2]        
            memo = Memo(account, op["to"], steem_instance=self.steem)
            processed_memo = ascii(memo.decrypt(processed_memo)).replace('\n', '')

        shares = int(amount.amount)
        if processed_memo.lower().replace(',', '  ').replace('"', '') == "":
            self.new_transfer_record(index, processed_memo, account, account, json.dumps(sponsee), shares, timestamp)
            return
        [sponsor, sponsee, not_parsed_words, account_error] = self.memo_parser.parse_memo(processed_memo, shares, account)        
        if amount.amount < 1:
            data = {"index": index, "sender": account, "to": self.account["name"], "memo": processed_memo, "encrypted": False, "referenced_accounts": sponsor + ";" + json.dumps(sponsee), "amount": amount.amount, "amount_symbol": amount.symbol, "timestamp": timestamp}
            self.transactionStorage.add(data)
            return
        if amount.symbol == self.steem.sbd_symbol:
            share_type = self.steem.sbd_symbol
        
        sponsee_amount = 0
        for a in sponsee:
            sponsee_amount += sponsee[a]
        
        
        if sponsee_amount == 0 and not account_error and True:
            sponsee_account = self.get_highest_avg_share_age_account()
            sponsee = {sponsee_account: shares}
            print("%s sponsers %s with %d shares" % (sponsor, sponsee_account, shares))
            self.new_transfer_record(index, processed_memo, account, sponsor, json.dumps(sponsee), shares, timestamp, share_type=share_type)
            self.memberStorage.update_avg_share_age(sponsee_account, 0)
            self.member_data[sponsee_account]["avg_share_age"] = 0            
            return
        elif sponsee_amount == 0 and not account_error:
            sponsee = {}
            message = op["timestamp"] + " to: " + self.account["name"] + " from: " + sponsor + ' amount: ' + str(amount) + ' memo: ' + processed_memo + '\n'
            self.new_transfer_record(index, processed_memo, account, sponsor, json.dumps(sponsee), shares, timestamp, status="LessOrNoSponsee", share_type=share_type)
            return
        if sponsee_amount != shares and not account_error and True:
            sponsee_account = self.get_highest_avg_share_age_account()
            sponsee_shares = shares-sponsee_amount
            if sponsee_shares > 0 and sponsee_account is not None:
                sponsee = {sponsee_account: sponsee_shares}
                print("%s sponsers %s with %d shares" % (sponsor, sponsee_account, sponsee_shares))
                self.new_transfer_record(index, processed_memo, account, sponsor, json.dumps(sponsee), shares, timestamp, share_type=share_type)
                self.memberStorage.update_avg_share_age(sponsee_account, 0)
                self.member_data[sponsee_account]["avg_share_age"] = 0
                return
            else:
                sponsee = {}
                self.new_transfer_record(index, processed_memo, account, sponsor, json.dumps(sponsee), shares, timestamp, status="LessOrNoSponsee", share_type=share_type)
                return
        elif sponsee_amount != shares and not account_error:
            message = op["timestamp"] + " to: " + self.account["name"] + " from: " + sponsor + ' amount: ' + str(amount) + ' memo: ' + ascii(op["memo"]) + '\n'
            self.new_transfer_record(index, processed_memo, account, sponsor, json.dumps(sponsee), shares, timestamp, status="LessOrNoSponsee", share_type=share_type)            

            return        
        if account_error:
            message = op["timestamp"] + " to: " + self.account["name"] + " from: " + sponsor + ' amount: ' + str(amount) + ' memo: ' + ascii(op["memo"]) + '\n'
            self.new_transfer_record(index, processed_memo, account, sponsor, json.dumps(sponsee), shares, timestamp, status="AccountDoesNotExist", share_type=share_type)

            return
        
        self.new_transfer_record(index, processed_memo, account, sponsor, json.dumps(sponsee), shares, timestamp, share_type=share_type)

    def new_transfer_record(self, index, memo, account, sponsor, sponsee, shares, timestamp,  status="Valid", share_type="Standard"):
        data = {"index": index, "source": self.account["name"], "memo": memo, "account": account, "sponsor": sponsor, "sponsee": sponsee, "shares": shares, "vests": float(0), "timestamp": formatTimeString(timestamp),
                 "status": status, "share_type": share_type}
        self.trxStorage.add(data)

    def new_delegation_record(self, index, account, vests, timestamp, status="Valid", share_type="Delegation"):
        data = {"index": index, "source": self.account["name"], "memo": "", "account": account, "sponsor": account, "sponsee": json.dumps({}), "shares": 0, "vests": float(vests), "timestamp": formatTimeString(timestamp),
                "status": status, "share_type": share_type}
        self.trxStorage.add(data)

    def parse_op(self, op, parse_vesting=True):
        if op['type'] == "delegate_vesting_shares" and parse_vesting:
            vests = Amount(op['vesting_shares'], steem_instance=self.steem)
            # print(op)
            if op['delegator'] == self.account["name"]:
                delegation = {'account': op['delegatee'], 'amount': vests}
                self.update_delegation(op, 0, delegation)
                return
            if op['delegatee'] == self.account["name"]:
                delegation = {'account': op['delegator'], 'amount': vests}
                self.update_delegation(op, delegation, 0)
                return

        elif op['type'] == "transfer":
            amount = Amount(op['amount'], steem_instance=self.steem)
            # print(op)
            if op['from'] == self.account["name"] and op["to"] not in self.excluded_accounts:
                self.parse_transfer_out_op(op)

            if op['to'] == self.account["name"] and op["from"] not in self.excluded_accounts:
                self.parse_transfer_in_op(op)
                
            # print(op, vests)
            # self.update(ts, vests, 0, 0)
            return

    def add_mngt_shares(self, last_op, mgnt_shares, op_count):
        
        index = last_op["index"]
        timestamp = last_op["timestamp"]
        sponsee = {}
        memo = ""
        latest_share = self.trxStorage.get_lastest_share_type("Mgmt")
        if latest_share is not None:
            start_index = latest_share["index"] + 1
        else:
            start_index = op_count / 100 * 3
        for account in mgnt_shares:
            shares = mgnt_shares[account]
            sponsor = account
            data = {"index": start_index, "source": "mgmt", "memo": "", "account": account, "sponsor": sponsor, "sponsee": sponsee, "shares": shares, "vests": float(0), "timestamp": formatTimeString(timestamp),
                     "status": "Valid", "share_type": "Mgmt"}
            start_index += 1
            self.trxStorage.add(data)

