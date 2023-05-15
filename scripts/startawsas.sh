#!/bin/bash
source /home/ec2-user/.bash_profile #added sbin for rotatelogs
cd /home/ec2-user/reconciliation/scripts
nohup aws s3 sync /home/ec2-user/reconciliation/data/ycba/activity_stream s3://ycba-lux/activity_stream --acl public-read 2>&1 | rotatelogs -l logs/awsas_%Y-%m-%d.log 86400 &
