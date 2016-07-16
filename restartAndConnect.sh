#!/bin/bash

if [ $# -eq 3 ]
then
    DoSpecula=$1
    SpeculaLength=$2
    SpeculaRead=$3
else
    DoSpecula=true
    FastReply=true
    SpeculaLength=5
    SpeculaRead=true
fi

sudo sed -i '' "s/{specula_read,.*/{specula_read, $SpeculaRead}./g" ./dev/dev1/antidote.config
sudo sed -i '' "s/{specula_read,.*/{specula_read, $SpeculaRead}./g" ./dev/dev2/antidote.config
sudo sed -i '' "s/{specula_read,.*/{specula_read, $SpeculaRead}./g" ./dev/dev3/antidote.config
sudo sed -i '' "s/{specula_read,.*/{specula_read, $SpeculaRead}./g" ./dev/dev4/antidote.config
sudo sed -i '' "s/{do_specula,.*/{do_specula, $DoSpecula}./g" ./dev/dev1/antidote.config
sudo sed -i '' "s/{do_specula,.*/{do_specula, $DoSpecula}./g" ./dev/dev2/antidote.config
sudo sed -i '' "s/{do_specula,.*/{do_specula, $DoSpecula}./g" ./dev/dev3/antidote.config
sudo sed -i '' "s/{do_specula,.*/{do_specula, $DoSpecula}./g" ./dev/dev4/antidote.config
sudo sed -i '' "s/{do_repl,.*/{do_repl, true}./g" ./dev/dev1/antidote.config
sudo sed -i '' "s/{do_repl,.*/{do_repl, true}./g" ./dev/dev2/antidote.config
sudo sed -i '' "s/{do_repl,.*/{do_repl, true}./g" ./dev/dev3/antidote.config
sudo sed -i '' "s/{do_repl,.*/{do_repl, true}./g" ./dev/dev4/antidote.config
sudo sed -i '' "s/{specula_length,.*/{specula_length, $SpeculaLength}./g" ./dev/dev1/antidote.config
sudo sed -i '' "s/{specula_length,.*/{specula_length, $SpeculaLength}./g" ./dev/dev2/antidote.config
sudo sed -i '' "s/{specula_length,.*/{specula_length, $SpeculaLength}./g" ./dev/dev3/antidote.config
sudo sed -i '' "s/{specula_length,.*/{specula_length, $SpeculaLength}./g" ./dev/dev4/antidote.config

./dev/dev1/bin/antidote stop
./dev/dev2/bin/antidote stop
./dev/dev3/bin/antidote stop
./dev/dev4/bin/antidote stop

sudo rm -r ./dev/dev1/data
sudo rm -r ./dev/dev2/data
sudo rm -r ./dev/dev3/data
sudo rm -r ./dev/dev4/data
sudo rm -r ./dev/dev1/log
sudo rm -r ./dev/dev2/log
sudo rm -r ./dev/dev3/log
sudo rm -r ./dev/dev4/log

sleep 2 
sudo ./dev/dev1/bin/antidote start
sudo ./dev/dev2/bin/antidote start
sudo ./dev/dev3/bin/antidote start
sudo ./dev/dev4/bin/antidote start

sleep 3
sudo ./dev/dev2/bin/antidote-admin cluster join dev1@127.0.0.1
sudo ./dev/dev3/bin/antidote-admin cluster join dev1@127.0.0.1
sudo ./dev/dev4/bin/antidote-admin cluster join dev1@127.0.0.1
sleep 1
sudo ./dev/dev1/bin/antidote-admin cluster plan
sudo ./dev/dev1/bin/antidote-admin cluster commit 
