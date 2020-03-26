#!/bin/bash
beempy updatenodes --hive

/usr/local/bin/python3.6 -u /root/steembasicincome/sbi_store_ops_db.py
/usr/local/bin/python3.6 -u /root/steembasicincome/sbi_transfer.py
/usr/local/bin/python3.6 -u /root/steembasicincome/sbi_check_delegation.py
/usr/local/bin/python3.6 -u /root/steembasicincome/sbi_update_curation_rshares.py
/usr/local/bin/python3.6 -u /root/steembasicincome/sbi_update_member_db.py

/usr/local/bin/python3.6 -u /root/steembasicincome/sbi_store_member_hist.py
/usr/local/bin/python3.6 -u /root/steembasicincome/sbi_upvote_post_comment.py

/usr/local/bin/python3.6 -u /root/steembasicincome/sbi_stream_post_comment.py



#/usr/local/bin/python3.6 -u /root/steembasicincome/sbi_reset_rshares.py
