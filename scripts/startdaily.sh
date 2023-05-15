#!/bin/bash
source /home/ec2-user/.bash_profile #added sbin for rotatelogs
cd /home/ec2-user/reconciliation/scripts
nohup python3 builder_ycba.py 2>&1 | rotatelogs -l logs/daily_%Y-%m-%d.log 86400 &
