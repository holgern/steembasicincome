# steembi
steembasicincome - scripts for python

## How to start
Compile and install steembi::

    python setup.py install --user

Store account history operations from steembasicincome to sbi8 and minnowbooster::

    python sbi_store_ops_db.py
    
Build the trx database::

    python sbi_transfer.py
    

## Install

apt-get install libmariadbclient-dev


## Creating a service script

useradd -r -s /bin/false sbiuser
chown -R sbiuser:sbiuser /etc/sbi

cp systemd/sbirunner.service to /etc/systemd/system/sbirunner.service


systemctl enable sbirunner
systemctl start sbirunner

systemctl status sbirunner