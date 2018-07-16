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