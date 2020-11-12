#!/bin/bash
# install redis
mkdir redisInstall
cd redisInstall
wget https://download.redis.io/releases/redis-6.0.8.tar.gz
tar xzf redis-6.0.8.tar.gz
cd redis-6.0.8
make
