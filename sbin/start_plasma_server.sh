#!/bin/bash

usage() {
  echo "Usage: $0 please input size, mount point, socket"
  echo "For example: ./start_plasma_server 350000000000 /mnt/pmem /tmp/plasmaStore"
}

if [ "$#" -ne 3 ]; then
                usage
                exit 0
fi

rm $3
plasma-store-server -m $1 -d $2 -s $3
