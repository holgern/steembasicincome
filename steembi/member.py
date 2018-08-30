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
                      "balance_rshares": 0, "comment_upvote": False}
        self.share_age_list = []
        self.shares_list = []
        super(Member, self).__init__(member)

    def reset_share_age_list(self):
        self.share_age_list = []
        self.shares_list = []

    def append_share_age(self, timestamp, shares):
        if shares == 0:
            return
        age = (datetime.utcnow()) - (timestamp)
        share_age = int(age.total_seconds() / 60 / 60 / 24)          
        self.share_age_list.append(share_age)
        self.shares_list.append(shares)

    def calc_share_age(self):
        if len(self.share_age_list) == 0:
            return
        total_share_days = 0
        sum_days = 0
        for i in range(len(self.share_age_list)):
            total_share_days += self.share_age_list[i] * self.shares_list[i]
        self["total_share_days"] = total_share_days
        if sum(self.shares_list) > 0:
            self["avg_share_age"] = total_share_days / sum(self.shares_list)
        else:
            self["avg_share_age"] = total_share_days
