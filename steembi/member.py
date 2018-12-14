# This Python file uses the following encoding: utf-8
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals
from builtins import bytes, int, str
from future.utils import python_2_unicode_compatible
from datetime import datetime, timedelta


@python_2_unicode_compatible
class Member(dict):
    def __init__(self, account, shares=0, timestamp=None):
        if isinstance(account, dict):
            member = account
        else:
            member = {"account": account, "shares": shares, "bonus_shares": 0, "total_share_days": 0, "avg_share_age": float(0),
                      "original_enrollment": timestamp, "latest_enrollment": timestamp, "earned_rshares": 0, "rewarded_rshares": 0,
                      "subscribed_rshares": 0, "curation_rshares": 0, "delegation_rshares": 0, "other_rshares": 0,
                      "balance_rshares": 0, "comment_upvote": False}
        self.share_age_list = []
        self.shares_list = []
        self.share_timestamp = []
        super(Member, self).__init__(member)

    def reset_share_age_list(self):
        self.share_age_list = []
        self.shares_list = []
        self.share_timestamp = []

    def append_share_age(self, timestamp, shares):
        if shares == 0:
            return
        age = (datetime.utcnow()) - (timestamp)
        share_age = int(age.total_seconds() / 60 / 60 / 24)          
        self.share_age_list.append(share_age)
        self.shares_list.append(shares)
        self.share_timestamp.append(timestamp)

    def calc_share_age(self):
        total_share_days = 0
        sum_days = 0        
        if len(self.share_age_list) == 0:
            self["total_share_days"] = total_share_days
            self["avg_share_age"] = total_share_days
            return
        for i in range(len(self.share_age_list)):
            total_share_days += self.share_age_list[i] * self.shares_list[i]
        self["total_share_days"] = total_share_days
        if sum(self.shares_list) > 0:
            self["avg_share_age"] = total_share_days / sum(self.shares_list)
        else:
            self["avg_share_age"] = total_share_days

    def calc_share_age_until(self, timestamp):
        if len(self.share_age_list) == 0:
            return
        total_share_days = 0
        sum_days = 0
        index = 0
        for i in range(len(self.share_age_list)):
            if self.share_timestamp[i] <= timestamp:
                
                total_share_days += self.share_age_list[i] * self.shares_list[i]
                index += 1
        self["total_share_days"] = total_share_days
        if index > 0:
            self["avg_share_age"] = total_share_days / index
        else:
            self["avg_share_age"] = total_share_days        