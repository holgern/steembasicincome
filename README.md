# steembasicincome

python scripts for automation of steembasicincome

## How to start

### Installation of needed packages

The following packages are needed, when running the scripts on Ubuntu:
```
apt-get install libmariadbclient-dev
```

```
pip3 install beem dataset  mysqlclient
```

Compile and install steembi, the helper library for all steembasicincome scripts

```
python setup.py install
```

### Prepare the database

```
mysql -u username -p sbi < sql/sbi.sql
mysql -u username -p sbi_steem_ops < sql/sbi_steem_ops.sql
```


### Creating a service script

Main runner script can be automatically run through systemd:

```
useradd -r -s /bin/false sbiuser
chown -R sbiuser:sbiuser /etc/sbi

cp systemd/sbirunner.service to /etc/systemd/system/


systemctl enable sbirunner
systemctl start sbirunner

systemctl status sbirunner
```

The blacklist script is run once a day:
```

cp systemd/blacklist.service to /etc/systemd/system/
cp systemd/blacklist.timer to /etc/systemd/system/

systemctl enable blacklist.timer
systemctl start blacklist.timer

systemctl list-timers
```

## Config file for accesing the database

A file `config.json` needs to be created:

```
{

        "databaseConnector": "mysql://user:password@localhost/sbi_steem_ops",
        "databaseConnector2": "mysql://user:password@localhost/sbi",
        "hive_blockchain": true,
        "mgnt_shares": {"josephsavage": 4, "holger80": 1}
}
```
For STEEM set hive_blockchain to false.

## Running steembasicincome

The following scripts need to run:
```
python3 sbi_upvote_post_comment.py
python3 sbi_store_ops_db.py
python3 sbi_transfer.py
python3 sbi_update_member_db.py
python3 sbi_store_member_hist.py
python3 sbi_update_post_count.py
python3 sbi_stream_post_comment.py
python3 sbi_check_delegation.py

```