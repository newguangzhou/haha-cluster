#!/bin/bash

source ../../pyenv/bin/activate

if [ ! -d /data/logs/broadcast_srv_d ]; then
    mkdir -p /data/logs/broadcast_srv_d
fi
exec python ./main.py --log_file_prefix=/data/logs/broadcast_srv_d/broadcast_srv_d.log $*