from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals
import unittest
from datetime import datetime, date, timedelta
from steembi.memo_parser import MemoParser


class Testcases(unittest.TestCase):
    def test_different_sponsor(self):
        memo = '@mliz35:@adewararilwan'
        shares = 1
        account = "malloryblythe"
        memo_parser = MemoParser()
        [sponsor, sponsee, not_parsed_words, account_error] = memo_parser.parse_memo(memo, shares, account)
        self.assertEqual(sponsor, "mliz35")
        self.assertEqual(sponsee, {"adewararilwan": 1})
        self.assertFalse(account_error)

    def test_different_sponsor2(self):
        memo = "'@trufflepig:@steemchiller'"
        shares = 1
        account = "josephsavage"
        memo_parser = MemoParser()
        [sponsor, sponsee, not_parsed_words, account_error] = memo_parser.parse_memo(memo, shares, account)
        self.assertEqual(sponsor, "trufflepig")
        self.assertEqual(sponsee, {"steemchiller": 1})
        self.assertFalse(account_error)

    def test_steemit_url(self):
        memo = 'https://steemit.com/@abhinavmendhe'
        shares = 1
        account = "abhinavmendhe"
        memo_parser = MemoParser()
        [sponsor, sponsee, not_parsed_words, account_error] = memo_parser.parse_memo(memo, shares, account)
        self.assertEqual(sponsor, "abhinavmendhe")
        self.assertEqual(sponsee, {})
        self.assertFalse(account_error)

    def test_wrong_account(self):
        memo = "cameronl.jull"
        shares = 1
        account = "andrewharland"
        memo_parser = MemoParser()
        [sponsor, sponsee, not_parsed_words, account_error] = memo_parser.parse_memo(memo, shares, account)
        self.assertEqual(sponsor, "andrewharland")
        self.assertEqual(sponsee, {})
        self.assertTrue(account_error)

    def test_several_sponsee(self):
        memo = "@veejay2312 @baa.steemit @antonette @rabiujaga @preshey @bebeomega"
        shares = 12
        account = "dynamicrypto"
        memo_parser = MemoParser()
        [sponsor, sponsee, not_parsed_words, account_error] = memo_parser.parse_memo(memo, shares, account)
        self.assertEqual(sponsor, "dynamicrypto")
        self.assertEqual(sponsee, {'veejay2312': 2, 'baa.steemit': 2, 'antonette': 2, 'rabiujaga': 2, 'preshey': 2, 'bebeomega': 2})
        self.assertFalse(account_error)

    def test_space1(self):
        memo = '@ tmholdings'
        shares = 1
        account = "madstacks"
        memo_parser = MemoParser()
        [sponsor, sponsee, not_parsed_words, account_error] = memo_parser.parse_memo(memo, shares, account)
        self.assertEqual(sponsor, "madstacks")
        self.assertEqual(sponsee, {"tmholdings": 1})
        self.assertFalse(account_error)

    def test_amount(self):
        memo = "3 for @francosteemvotes ! Thanks :)"
        shares = 3
        account = "steemquebec"
        memo_parser = MemoParser()
        [sponsor, sponsee, not_parsed_words, account_error] = memo_parser.parse_memo(memo, shares, account)
        self.assertEqual(sponsor, "steemquebec")
        self.assertEqual(sponsee, {"francosteemvotes": 3})
        self.assertFalse(account_error)

    def test_account1(self):
        memo = "Sponsor: @herbertholmes"
        shares = 1
        account = "impending.doom"
        memo_parser = MemoParser()
        [sponsor, sponsee, not_parsed_words, account_error] = memo_parser.parse_memo(memo, shares, account)
        self.assertEqual(sponsor, "impending.doom")
        self.assertEqual(sponsee, {"herbertholmes": 1})
        self.assertFalse(account_error)

    def test_account2(self):
        memo = "I'd like to sponsor @beeyou"
        shares = 1
        account = "simplymike"
        memo_parser = MemoParser()
        [sponsor, sponsee, not_parsed_words, account_error] = memo_parser.parse_memo(memo, shares, account)
        self.assertEqual(sponsor, "simplymike")
        self.assertEqual(sponsee, {"beeyou": 1})
        self.assertFalse(account_error)

    def test_url(self):
        memo = 'https://steemit.com/top10/@dynamicrypto/top-ten-1-unbelievable-sexual-rituals-in-the-world'
        shares = 1
        account = "dynamicrypto"
        memo_parser = MemoParser()
        [sponsor, sponsee, not_parsed_words, account_error] = memo_parser.parse_memo(memo, shares, account)        
        self.assertEqual(sponsor, "dynamicrypto")
        self.assertEqual(sponsee, {})
        self.assertTrue(account_error)

    def test_double_sponsee(self):
        memo = '@irishcoffee, @corsica, @mayrie28, @cryptofrench, @irishcoffee, @deboas'
        shares = 6
        account = "deadzy"
        memo_parser = MemoParser()
        [sponsor, sponsee, not_parsed_words, account_error] = memo_parser.parse_memo(memo, shares, account)
        self.assertEqual(sponsor, "deadzy")
        self.assertEqual(sponsee, {"irishcoffee": 2, "corsica": 1, "mayrie28": 1, "cryptofrench": 1, "deboas": 1})
        self.assertFalse(account_error)

    def test_amount_number(self):
        memo = 'Sponsor @bashadow x 3'
        shares = 3
        account = "thehive"
        memo_parser = MemoParser()
        [sponsor, sponsee, not_parsed_words, account_error] = memo_parser.parse_memo(memo, shares, account)
        self.assertEqual(sponsor, "thehive")
        self.assertEqual(sponsee, {"bashadow": 3})
        self.assertFalse(account_error)

    def test_no_sponsee(self):
        memo = '@awesomianist'
        shares = 4
        account = "awesomianist"
        memo_parser = MemoParser()
        [sponsor, sponsee, not_parsed_words, account_error] = memo_parser.parse_memo(memo, shares, account)
        self.assertEqual(sponsor, "awesomianist")
        self.assertEqual(sponsee, {})
        self.assertFalse(account_error)

    def test_sponsee(self):
        memo ="'@reseller, @asonintrigue, @endastory, @spicevsfood, @spicereviews, @steeminute, @marrissajoy, @elainefaye, @wolfnworbeikood, @larrymorrison, @steemcafe'"
        shares = 12
        account = "josephsavage"
        memo_parser = MemoParser()
        [sponsor, sponsee, not_parsed_words, account_error] = memo_parser.parse_memo(memo, shares, account)
        self.assertEqual(sponsor, "josephsavage")
        self.assertEqual(sponsee, {"reseller": 1, "asonintrigue": 1, "endastory": 1, "spicevsfood": 1, "spicereviews": 1, "steeminute": 1,
                                   "marrissajoy": 1, "elainefaye": 1, "wolfnworbeikood": 1, "larrymorrison": 1, "steemcafe": 1})
        self.assertFalse(account_error)        