from beem.account import Account
from beem.amount import Amount
from beem import Steem
from beem.instance import set_shared_steem_instance
from beem.nodelist import NodeList
import re
import os
from sqlitedict import SqliteDict
from contextlib import closing


def db_store(path, database, key, data):
    with closing(SqliteDict(path + database, autocommit=True)) as db:
        db[key] = data

def db_load(path, database, key):
    with closing(SqliteDict(path + database, autocommit=True)) as db:
        return db[key]

def db_append(path, database, key, new_data):
    with closing(SqliteDict(path + database, autocommit=True)) as db:
        data = db[key]
        data.append(new_data)
        db[key] = data

def db_extend(path, database, key, new_data):
    with closing(SqliteDict(path + database, autocommit=True)) as db:
        data = db[key]
        data.extend(new_data)
        db[key] = data

def db_has_database(path, database):
    if not os.path.isfile(path + database):
        return False
    else:
        return True

def db_has_key(path, database, key):
    if not os.path.isfile(path + database):
        return False
    with closing(SqliteDict(path + database, autocommit=True)) as db:
        if key in db:
            return True
        else:
            return False

if __name__ == "__main__":
    account = "steembasicincome"
    database_ops = "sbi.sqlite"
    database_transfer = "sbi_tranfer.sqlite"
    path = ""
    path = "E:\\curation_data\\"
    # Update current node list from @fullnodeupdate

    account = Account(account)
    ops = db_load(path, database_ops, account["name"])
    
    # Go trough all transfer ops
    for op in ops:
        if op["type"] != "transfer":
            continue
        amount = Amount(op["amount"])
        line = ""
        if amount.symbol != "STEEM":
            with open(path + 'sbi_skipped_transfer.txt', 'a') as the_file:
                the_file.write(ascii(op) + '\n')          
            continue
        if op["from"] == account["name"] or op["from"] == "minnowbooster":
            with open(path + 'sbi_skipped_transfer.txt', 'a') as the_file:
                the_file.write(ascii(op) + '\n')            
            continue
        if amount.amount < 1:
            with open(path + 'sbi_skipped_transfer.txt', 'a') as the_file:
                the_file.write(ascii(op) + '\n')            
            continue
        if op["memo"] == "":
            with open(path + 'sbi_skipped_transfer.txt', 'a') as the_file:
                the_file.write(ascii(op) + '\n')            
            continue
        print(op["memo"])
        from_acc = Account(op["from"])
        message = ""
        memo = op["memo"].lower().replace(',', '  ')
        words_memo = memo.split(" ")
        sponsors = []
        amount_left = amount.amount
        word_count = 0
        for w in words_memo:
            if amount_left >= 1:
                if len(w) == 0:
                    continue
                elif w[0] == '@':
                    try:
                        account_name = w[1:].replace('!', '').replace('"', '')
                        if account_name[-1] == '.':
                            account_name = account_name[:-1]
                        Account(account_name)
                        if account_name != from_acc["name"]:
                            sponsors.append(account_name)
                            amount_left -= 1
                    except:
                        print(account_name + " is not an account")
                elif w[:21] == 'https://steemit.com/@':
                    try:
                        account_name = w[21:].replace('!', '').replace('"', '')
                        if account_name[-1] == '.':
                            account_name = account_name[:-1]                        
                        Account(account_name)
                        if account_name != from_acc["name"]:
                            sponsors.append(account_name)
                            amount_left -= 1
                    except:
                        print(account_name + " is not an account")
                else:
                    word_count += 1
        if word_count == 1 and len(sponsors) == 0:
            try:
                account_name = words_memo[0].replace(',', '').replace('!', '').replace('"', '')
                if account_name[-1] == '.':
                    account_name = account_name[:-1]                        
                Account(account_name)
                if account_name != from_acc["name"]:
                    sponsors.append(account_name)
                    amount_left -= 1
            except:
                print(account_name + " is not an account")
                
        if amount_left > 0 and (word_count > 0 and (len(sponsors) == 0 or len(sponsors) > 1)):
            message = op["timestamp"] + " from: " + from_acc["name"] + ' amount: ' + str(amount) + ' memo: ' + ascii(op["memo"]) + '\n'
            with open(path + 'sbi_manual_review_needed.txt', 'a') as the_file:
                the_file.write(message)
        else:
            message = op["timestamp"] + " from: " + from_acc["name"] + ' amount: ' + str(amount) + ' memo: ' + ascii(op["memo"])
            message += ' sponsors: ' + str(sponsors) + 'remaining STEEM: ' + str(amount_left) + '\n'
            data = {"sponsor": from_acc["name"], "sponsee": sponsors, "shares": int(amount.amount), "timestamp": op["timestamp"],
                    "share_age": 1, "status": "valid", "share_type": "standard"}
            with open(path + 'sbi_transfer_ok.txt', 'a') as the_file:
                the_file.write(data)            
