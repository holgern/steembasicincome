#!/bin/bash
beempy updatenodes
/usr/local/bin/python3.6 -u /root/steembasicincome/sbi_store_ops_db.py
/usr/local/bin/python3.6 -u /root/steembasicincome/sbi_transfer.py
/usr/local/bin/python3.6 -u /root/steembasicincome/sbi_update_member_db.py
/usr/local/bin/python3.6 -u /root/steembasicincome/sbi_store_member_hist.py
/usr/local/bin/python3.6 -u /root/steembasicincome/sbi_update_post_count.py
