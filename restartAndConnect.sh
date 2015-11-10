#!/bin/bash

./dev/dev1/bin/antidote stop
./dev/dev2/bin/antidote stop
./dev/dev3/bin/antidote stop

sudo rm -r ./dev/dev1/data
sudo rm -r ./dev/dev2/data
sudo rm -r ./dev/dev3/data
sudo rm -r ./dev/dev1/log
sudo rm -r ./dev/dev2/log
sudo rm -r ./dev/dev3/log

sleep 2 
sudo ./dev/dev1/bin/antidote start
sudo ./dev/dev2/bin/antidote start
sudo ./dev/dev3/bin/antidote start

sleep 3
sudo ./dev/dev2/bin/antidote-admin cluster join dev1@127.0.0.1
sudo ./dev/dev3/bin/antidote-admin cluster join dev1@127.0.0.1
sleep 1
sudo ./dev/dev1/bin/antidote-admin cluster plan
sudo ./dev/dev1/bin/antidote-admin cluster commit 
