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
from beem.instance import shared_steem_instance
from beem.blockchain import Blockchain
from beem.constants import STEEM_VOTE_REGENERATION_SECONDS, STEEM_1_PERCENT, STEEM_100_PERCENT

log = logging.getLogger(__name__)


class ParseAccountHist(list):
    
    def __init__(self, account, path, transfer_table, steem_instance=None):
        self.steem = steem_instance or shared_steem_instance()
        self.account = Account(account, steem_instance=self.steem)    
        self.delegated_vests_in = {}
        self.delegated_vests_out = {}
        self.timestamp = addTzInfo(datetime(1970, 1, 1, 0, 0, 0, 0))
        self.path = path
        self.excluded_accounts = ["minnowbooster", "smartsteem", "randowhale", "steemvoter", "jerrybanfield",
                                  "boomerang", "postpromoter", "appreciator", "buildawhale", "upme", "smartmarket",
                                  "minnowhelper", "pushup", "steembasicincome", "sbi2", "sbi3", "sbi4", "sbi5", "sbi6", "sbi7", "sbi8"]

        self.allowed_memo_words = ['for', 'and', 'sponsor', 'shares', 'share', 'sponsorship',
                                   'please', 'steem', 'thanks', 'additional',
                                   'sponsee', 'sponsoring', 'sponser', 'one', 'you', 'thank', 'enroll',
                                   'sponsering:', 'sponsoring;', 'sponsoring:', 'would', 'like', 'too', 'enroll:',
                                   'sponsor:']
        from .storage import (trxStorage)
        self.trxStorage = trxStorage
        self.transfer_table = transfer_table

    def update_delegation(self, timestamp, delegated_in=None, delegated_out=None):
        """ Updates the internal state arrays

            :param datetime timestamp: datetime of the update
            :param Amount/float own: vests
            :param dict delegated_in: Incoming delegation
            :param dict delegated_out: Outgoing delegation
            :param Amount/float steem: steem
            :param Amount/float sbd: sbd

        """

        self.timestamp = timestamp

        new_deleg = dict(self.delegated_vests_in)
        if delegated_in is not None and delegated_in:
            if delegated_in['amount'] == 0 and delegated_in['account'] in new_deleg:
                del new_deleg[delegated_in['account']]
            elif delegated_in['amount']  > 0:
                new_deleg[delegated_in['account']] = delegated_in['amount']
            else:
                print(delegated_in)
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
        if self.path is None:
            return
        delegated_sp_in = {}
        for acc in self.delegated_vests_in:
            vests = Amount(self.delegated_vests_in[acc])
            delegated_sp_in[acc] = str(self.steem.vests_to_sp(vests))
        delegated_sp_out = {}
        for acc in self.delegated_vests_out:
            vests = Amount(self.delegated_vests_out[acc])
            delegated_sp_out[acc] = str(self.steem.vests_to_sp(vests))
        with open(self.path + 'sbi_delegation_in_'+self.account["name"]+'.txt', 'w') as the_file:
            the_file.write(str(delegated_sp_in) + '\n')
        with open(self.path + 'sbi_delegation_out_'+self.account["name"]+'.txt', 'w') as the_file:
            the_file.write(str(delegated_sp_out) + '\n')

    def parse_memo(self, memo, shares, account):
        words_memo = memo.lower().replace(',', '  ').replace('"', '').split(" ")
        sponsors = {}
        no_numbers = True
        amount_left = shares
        word_count = 0
        not_parsed_words = []
        n_words = len(words_memo)
        digit_found = None
        sponsor = None
        account_error = False

        for w in words_memo:
            if len(w) == 0:
                continue            
            if w in self.allowed_memo_words:
                continue
            if amount_left >= 1:
                account_name = ""
                account_found = False
                w_digit = w.replace('x', '', 1).replace('-', '', 1).replace(';', '', 1)
                if w_digit.isdigit():
                    no_numbers = False
                    digit_found = int(w_digit)
                elif len(w) < 3:
                    continue
                elif w[:21] == 'https://steemit.com/@' and '/' not in w[21:]:
                    try:
                        account_name = w[21:].replace('!', '').replace('"', '').replace(';', '')
                        if account_name[-1] == '.':
                            account_name = account_name[:-1]                        
                        acc = Account(account_name)
                        account_found = True
                    except:
                        print(account_name + " is not an account")
                        account_error = True
                elif len(w.split(":")) == 2 and '/' not in w:
                    try:
                        account_name1 = w.split(":")[0]
                        account_name = w.split(":")[1]
                        if account_name1[0] == '@':
                            account_name1 = account_name1[1:]
                        if account_name[0] == '@':
                            account_name = account_name[1:]                            
                        acc1 = Account(account_name1)
                        acc = Account(account_name)
                        account_found = True
                        if sponsor is None:
                            sponsor = account_name1
                        else:
                            account_error = True
                    except:
                        print(account_name + " is not an account")
                        account_error = True                    
                elif w[0] == '@':
                    
                    try:
                        account_name = w[1:].replace('!', '').replace('"', '').replace(';', '')
                        if account_name[-1] == '.':
                            account_name = account_name[:-1]
                        acc = Account(account_name)
                        account_found = True

                    except:
                        print(account_name + " is not an account")
                        account_error = True
                        
                elif len(w) > 16:
                    continue
                
                else:
                    try:
                        account_name = w.replace('!', '').replace('"', '')
                        if account_name[-1] == '.':
                            account_name = account_name[:-1]
                        acc = Account(account_name)
                        account_found = True
                    except:
                        print(account_name + " is not an account")                
                        not_parsed_words.append(w)
                        word_count += 1
                        account_error = True
                if account_found and account_name != '' and account_name != account:
                    if digit_found is not None:
                        sponsors[account_name] = digit_found
                        amount_left -= digit_found
                        digit_found = None
                    elif account_name in sponsors:
                        sponsors[account_name] += 1
                        amount_left -= 1                        
                    else:
                        sponsors[account_name] = 1
                        amount_left -= 1                
        if n_words == 1 and len(sponsors) == 0:
            try:
                account_name = words_memo[0].replace(',', ' ').replace('!', ' ').replace('"', '').replace('/', ' ')
                if account_name[-1] == '.':
                    account_name = account_name[:-1]                        
                Account(account_name)
                if account_name != account:
                    sponsors[account_name] = 1
                    amount_left -= 1
            except:
                print(account_name + " is not an account")
        if len(sponsors) == 1 and shares > 1 and no_numbers:
            for a in sponsors:
                sponsors[a] = shares
        elif len(sponsors) == 1 and shares > 1 and not no_numbers and digit_found is not None:
            for a in sponsors:
                sponsors[a] = digit_found
        elif len(sponsors) > 0 and shares % len(sponsors) == 0 and no_numbers:
            for a in sponsors:
                sponsors[a] = shares // len(sponsors)
        if sponsor is None:
            sponsor = account
        if account_error and len(sponsors) == shares:
            account_error = False
        return sponsor, sponsors, not_parsed_words, account_error

    def parse_transfer_out_op(self, op):
        amount = Amount(op["amount"], steem_instance=self.steem)
        if amount.amount < 1:
            if self.path is None:
                return
            with open(self.path + 'sbi_skipped_transfer_out.txt', 'a') as the_file:
                the_file.write(ascii(op) + '\n')
            return
        if amount.symbol == "SBD":
            
            shares = -amount.amount
            # self.new_transfer_record(op["index"], op["to"], "", shares, op["timestamp"], share_type="Refund")
            if self.path is None:
                return
            with open(self.path + 'sbi_skipped_SBD_transfer_out.txt', 'a') as the_file:
                the_file.write(ascii(op) + '\n')
            return
        else:
            if self.path is None:
                return
            with open(self.path + 'sbi_skipped_STEEM_transfer_out.txt', 'a') as the_file:
                the_file.write(ascii(op) + '\n')
            return            

    def parse_transfer_in_op(self, op):
        amount = Amount(op["amount"], steem_instance=self.steem)
        share_type = "standard"
        if amount.amount < 1:
            if self.path is None:
                return
            with open(self.path + 'sbi_skipped_transfer.txt', 'a') as the_file:
                the_file.write(ascii(op) + '\n')
            return
        if amount.symbol == "SBD":
            share_type = "SBD"

        index = op["index"]
        account = op["from"]
        timestamp = op["timestamp"]
        sponsee = {}
        memo = op["memo"]
        shares = int(amount.amount)
        if memo.lower().replace(',', '  ').replace('"', '') == "":
            self.new_transfer_record(index, account, account, sponsee, shares, timestamp)
            return
        [sponsor, sponsee, not_parsed_words, account_error] = self.parse_memo(memo, shares, account)
        
        sponsee_amount = 0
        for a in sponsee:
            sponsee_amount += sponsee[a]
        
        
        if sponsee_amount == 0 and not account_error:
            message = op["timestamp"] + " to: " + self.account["name"] + " from: " + sponsor + ' amount: ' + str(amount) + ' memo: ' + ascii(op["memo"]) + '\n'
            if self.path is None:
                return            
            with open(self.path + 'sbi_no_sponsee.txt', 'a') as the_file:
                the_file.write(message)
            return
        if sponsee_amount != shares and not account_error:
            message = op["timestamp"] + " to: " + self.account["name"] + " from: " + sponsor + ' amount: ' + str(amount) + ' memo: ' + ascii(op["memo"]) + '\n'
            if self.path is None:
                return            
            with open(self.path + 'sbi_wrong_amount.txt', 'a') as the_file:
                the_file.write(message)
            return        
        if account_error:
            message = op["timestamp"] + " to: " + self.account["name"] + " from: " + sponsor + ' amount: ' + str(amount) + ' memo: ' + ascii(op["memo"]) + '\n'
            if self.path is None:
                return            
            with open(self.path + 'sbi_wrong_account_name.txt', 'a') as the_file:
                the_file.write(message)
            return
        
        self.new_transfer_record(index, account, sponsor, sponsee, shares, timestamp, share_type=share_type)

    def new_transfer_record(self, index, account, sponsor, sponsee, shares, timestamp, share_age=1, status="valid", share_type="standard"):
        data = {"index": index, "pool": self.account["name"], "account": account, "sponsor": sponsor, "sponsee": sponsee, "shares": shares, "timestamp": timestamp,
                "share_age": share_age, "status": status, "share_type": share_type}
        self.trxStorage.add(index, self.account["name"], account, sponsor, json.dumps(sponsee), shares, timestamp, share_age, status, share_type)
        self.transfer_table.append(data)
        if self.path is None:
            return        
        with open(self.path + 'sbi_transfer_ok.txt', 'a') as the_file:
            the_file.write(str(data) + '\n')

    def parse_op(self, op):
        if op['type'] == "delegate_vesting_shares":
            vests = Amount(op['vesting_shares'], steem_instance=self.steem)
            # print(op)
            if op['delegator'] == self.account["name"]:
                delegation = {'account': op['delegatee'], 'amount': vests}
                self.update_delegation(op["timestamp"], 0, delegation)
                return
            if op['delegatee'] == self.account["name"]:
                delegation = {'account': op['delegator'], 'amount': vests}
                self.update_delegation(op["timestamp"], delegation, 0)
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