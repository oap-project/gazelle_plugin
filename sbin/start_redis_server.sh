#!/bin/bash

usage() {
  echo "Please change redis settings in redis.conf according to your environment."
}

usage
./src/redis-server --protected-mode no
