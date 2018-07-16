from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals
import unittest
from datetime import datetime, date, timedelta
from steembi.parse_hist_op import ParseAccountHist


class Testcases(unittest.TestCase):
    def test_different_sponsor(self):
        memo = '@mliz35:@adewararilwan'
        shares = 1
        account = "malloryblythe"
        pah = ParseAccountHist("steembasicincome", None, {})
        [sponsor, sponsee, not_parsed_words, account_error] = pah.parse_memo(memo, shares, account)
        self.assertEqual(sponsor, "mliz35")
        self.assertEqual(sponsee, {"adewararilwan": 1})
        self.assertFalse(account_error)

    def test_steemit_url(self):
        memo = 'https://steemit.com/@abhinavmendhe'
        shares = 1
        account = "abhinavmendhe"
        pah = ParseAccountHist("steembasicincome", None, {})
        [sponsor, sponsee, not_parsed_words, account_error] = pah.parse_memo(memo, shares, account)
        self.assertEqual(sponsor, "abhinavmendhe")
        self.assertEqual(sponsee, {})
        self.assertFalse(account_error)

    def test_wrong_account(self):
        memo = "cameronl.jull"
        shares = 1
        account = "andrewharland"
        pah = ParseAccountHist("steembasicincome", None, {})
        [sponsor, sponsee, not_parsed_words, account_error] = pah.parse_memo(memo, shares, account)
        self.assertEqual(sponsor, "andrewharland")
        self.assertEqual(sponsee, {})
        self.assertTrue(account_error)

    def test_several_sponsee(self):
        memo = "@veejay2312 @baa.steemit @antonette @rabiujaga @preshey @bebeomega"
        shares = 12
        account = "dynamicrypto"
        pah = ParseAccountHist("steembasicincome", None, {})
        [sponsor, sponsee, not_parsed_words, account_error] = pah.parse_memo(memo, shares, account)
        self.assertEqual(sponsor, "dynamicrypto")
        self.assertEqual(sponsee, {'veejay2312': 2, 'baa.steemit': 2, 'antonette': 2, 'rabiujaga': 2, 'preshey': 2, 'bebeomega': 2})
        self.assertFalse(account_error)

    def test_space1(self):
        memo = '@ tmholdings'
        shares = 1
        account = "madstacks"
        pah = ParseAccountHist("steembasicincome", None, {})
        [sponsor, sponsee, not_parsed_words, account_error] = pah.parse_memo(memo, shares, account)
        self.assertEqual(sponsor, "madstacks")
        self.assertEqual(sponsee, {"tmholdings": 1})
        self.assertFalse(account_error)

    def test_amount(self):
        memo = "3 for @francosteemvotes ! Thanks :)"
        shares = 3
        account = "steemquebec"
        pah = ParseAccountHist("steembasicincome", None, {})
        [sponsor, sponsee, not_parsed_words, account_error] = pah.parse_memo(memo, shares, account)
        self.assertEqual(sponsor, "steemquebec")
        self.assertEqual(sponsee, {"francosteemvotes": 3})
        self.assertFalse(account_error)

    def test_account1(self):
        memo = "Sponsor: @herbertholmes"
        shares = 1
        account = "impending.doom"
        pah = ParseAccountHist("steembasicincome", None, {})
        [sponsor, sponsee, not_parsed_words, account_error] = pah.parse_memo(memo, shares, account)
        self.assertEqual(sponsor, "impending.doom")
        self.assertEqual(sponsee, {"herbertholmes": 1})
        self.assertFalse(account_error)

    def test_account2(self):
        memo = "I'd like to sponsor @beeyou"
        shares = 1
        account = "simplymike"
        pah = ParseAccountHist("steembasicincome", None, {})
        [sponsor, sponsee, not_parsed_words, account_error] = pah.parse_memo(memo, shares, account)
        self.assertEqual(sponsor, "simplymike")
        self.assertEqual(sponsee, {"beeyou": 1})
        self.assertFalse(account_error)

    def test_url(self):
        memo = 'https://steemit.com/top10/@dynamicrypto/top-ten-1-unbelievable-sexual-rituals-in-the-world'
        shares = 1
        account = "dynamicrypto"
        pah = ParseAccountHist("steembasicincome", None, {})
        [sponsor, sponsee, not_parsed_words, account_error] = pah.parse_memo(memo, shares, account)        
        self.assertEqual(sponsor, "dynamicrypto")
        self.assertEqual(sponsee, {})
        self.assertFalse(account_error)

    def test_double_sponsee(self):
        memo = '@irishcoffee, @corsica, @mayrie28, @cryptofrench, @irishcoffee, @deboas'
        shares = 6
        account = "deadzy"
        pah = ParseAccountHist("steembasicincome", None, {})
        [sponsor, sponsee, not_parsed_words, account_error] = pah.parse_memo(memo, shares, account)
        self.assertEqual(sponsor, "deadzy")
        self.assertEqual(sponsee, {"irishcoffee": 2, "corsica": 1, "mayrie28": 1, "cryptofrench": 1, "deboas": 1})
        self.assertFalse(account_error)

    def test_amount_number(self):
        memo = 'Sponsor @bashadow x 3'
        shares = 3
        account = "thehive"
        pah = ParseAccountHist("steembasicincome", None, {})
        [sponsor, sponsee, not_parsed_words, account_error] = pah.parse_memo(memo, shares, account)
        self.assertEqual(sponsor, "thehive")
        self.assertEqual(sponsee, {"bashadow": 3})
        self.assertFalse(account_error)        
        