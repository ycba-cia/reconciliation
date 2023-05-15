#!/bin/bash
source /home/ec2-user/.bash_profile #added sbin for rotatelogs
cd /home/ec2-user/reconciliation/scripts
nohup aws s3 sync /home/ec2-user/reconciliation/data/ycba/linked_art s3://ycba-lux/v3 --acl public-read 2>&1 | rotatelogs -l logs/aws_%Y-%m-%d.log 86400 &
