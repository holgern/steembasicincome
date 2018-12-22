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

class MemoParser(object):
    
    def __init__(self, steem_instance=None):
        self.steem = steem_instance or shared_steem_instance()
        self.allowed_memo_words = ['for', 'and', 'sponsor', 'shares', 'share', 'sponsorship',
                                   'please', 'steem', 'thanks', 'additional',
                                   'sponsee', 'sponsoring', 'sponser', 'one', 'you', 'thank', 'enroll',
                                   'sponsering:', 'sponsoring;', 'sponsoring:', 'would', 'like', 'too', 'enroll:',
                                   'sponsor:']



    def parse_memo(self, memo, shares, account):
        if memo[0] == "'":
            memo = memo[1:]
        if memo[-1] == "'":
            memo = memo[:-1]
        words_memo = memo.strip().lower().replace(',', '  ').replace('"', '').split(" ")

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
                        if account_name[0] == "'":
                            account_name = account_name[1:]
                        if account_name[-1] == "'":
                            account_name = account_name[:-1]
                        if account_name[-1] == '.':
                            account_name = account_name[:-1]
                        if account_name[0] == '@':
                            account_name = account_name[1:]
                        account_name = account_name.strip()
                        acc = Account(account_name, steem_instance=self.steem)
                        account_found = True
                    except:
                        print(account_name + " is not an account")
                        account_error = True
                elif len(w.split(":")) == 2 and '/' not in w:
                    try:
                        account_name1 = w.split(":")[0]
                        account_name = w.split(":")[1]
                        if account_name[0] == "'":
                            account_name = account_name[1:]
                        if account_name[-1] == "'":
                            account_name = account_name[:-1]
                        if account_name1[0] == "'":
                            account_name1 = account_name1[1:]
                        if account_name1[-1] == "'":
                            account_name1 = account_name1[:-1]                            
                        if account_name1[0] == '@':
                            account_name1 = account_name1[1:]
                        if account_name[0] == '@':
                            account_name = account_name[1:]
                        account_name = account_name.strip()
                        account_name1 = account_name1.strip()
                        acc1 = Account(account_name1, steem_instance=self.steem)
                        acc = Account(account_name, steem_instance=self.steem)
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
                        if account_name[0] == "'":
                            account_name = account_name[1:]
                        if account_name[-1] == "'":
                            account_name = account_name[:-1]                        
                        if account_name[-1] == '.':
                            account_name = account_name[:-1]
                        if account_name[0] == '@':
                            account_name = account_name[1:]
                        account_name = account_name.strip()
                        acc = Account(account_name, steem_instance=self.steem)
                        account_found = True

                    except:
                        print(account_name + " is not an account")
                        account_error = True
                elif len(w.split('@')) > 1:
                    
                    try:
                        account_name = w.replace('!', '').replace('"', '').replace(';', '').split("@")[1]
                        if account_name[0] == "'":
                            account_name = account_name[1:]
                        if account_name[-1] == "'":
                            account_name = account_name[:-1]                        
                        if account_name[-1] == '.':
                            account_name = account_name[:-1]
                        if account_name[0] == '@':
                            account_name = account_name[1:]
                        account_name = account_name.strip()
                        acc = Account(account_name, steem_instance=self.steem)
                        account_found = True

                    except:
                        print(account_name + " is not an account")
                        account_error = True
                
                elif len(w) > 16:
                    continue
                
                else:
                    try:
                        account_name = w.replace('!', '').replace('"', '')
                        if account_name[0] == "'":
                            account_name = account_name[1:]
                        if account_name[-1] == "'":
                            account_name = account_name[:-1]                        
                        if account_name[-1] == '.':
                            account_name = account_name[:-1]
                        if account_name[0] == '@':
                            account_name = account_name[1:]
                        account_name = account_name.strip()
                        acc = Account(account_name, steem_instance=self.steem)
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
                if account_name[0] == "'":
                    account_name = account_name[1:]
                if account_name[-1] == "'":
                    account_name = account_name[:-1]                
                if account_name[-1] == '.':
                    account_name = account_name[:-1]
                if account_name[0] == '@':
                    account_name = account_name[1:]
                account_name = account_name.strip()
                Account(account_name, steem_instance=self.steem)
                if account_name != account:
                    sponsors[account_name] = 1
                    amount_left -= 1
            except:
                account_error = True
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