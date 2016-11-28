#!/bin/bash

if [ $# -eq 3 ]
then
    DoSpecula=$1
    SpeculaLength=$2
    SpeculaRead=$3
else
    DoSpecula=true
    SpeculaLength=5
    SpeculaRead=true
fi

sudo sed -i '' "s/{do_specula,.*/{do_specula, $DoSpecula}./g" ./dev/dev1/antidote.config
sudo sed -i '' "s/{do_specula,.*/{do_specula, $DoSpecula}./g" ./dev/dev2/antidote.config
sudo sed -i '' "s/{do_specula,.*/{do_specula, $DoSpecula}./g" ./dev/dev3/antidote.config
sudo sed -i '' "s/{do_specula,.*/{do_specula, $DoSpecula}./g" ./dev/dev4/antidote.config
sudo sed -i '' "s/{do_specula,.*/{do_specula, $DoSpecula}./g" ./dev/dev5/antidote.config
sudo sed -i '' "s/{do_specula,.*/{do_specula, $DoSpecula}./g" ./dev/dev6/antidote.config
sudo sed -i '' "s/{do_repl,.*/{do_repl, true}./g" ./dev/dev1/antidote.config
sudo sed -i '' "s/{do_repl,.*/{do_repl, true}./g" ./dev/dev2/antidote.config
sudo sed -i '' "s/{do_repl,.*/{do_repl, true}./g" ./dev/dev3/antidote.config
sudo sed -i '' "s/{do_repl,.*/{do_repl, true}./g" ./dev/dev4/antidote.config
sudo sed -i '' "s/{do_repl,.*/{do_repl, true}./g" ./dev/dev5/antidote.config
sudo sed -i '' "s/{do_repl,.*/{do_repl, true}./g" ./dev/dev6/antidote.config
sudo sed -i '' "s/{specula_length,.*/{specula_length, $SpeculaLength}./g" ./dev/dev1/antidote.config
sudo sed -i '' "s/{specula_length,.*/{specula_length, $SpeculaLength}./g" ./dev/dev2/antidote.config
sudo sed -i '' "s/{specula_length,.*/{specula_length, $SpeculaLength}./g" ./dev/dev3/antidote.config
sudo sed -i '' "s/{specula_length,.*/{specula_length, $SpeculaLength}./g" ./dev/dev4/antidote.config
sudo sed -i '' "s/{specula_length,.*/{specula_length, $SpeculaLength}./g" ./dev/dev5/antidote.config
sudo sed -i '' "s/{specula_length,.*/{specula_length, $SpeculaLength}./g" ./dev/dev6/antidote.config
sudo sed -i '' "s/{specula_read,.*/{specula_read, $SpeculaRead}./g" ./dev/dev1/antidote.config
sudo sed -i '' "s/{specula_read,.*/{specula_read, $SpeculaRead}./g" ./dev/dev2/antidote.config
sudo sed -i '' "s/{specula_read,.*/{specula_read, $SpeculaRead}./g" ./dev/dev3/antidote.config
sudo sed -i '' "s/{specula_read,.*/{specula_read, $SpeculaRead}./g" ./dev/dev4/antidote.config
sudo sed -i '' "s/{specula_read,.*/{specula_read, $SpeculaRead}./g" ./dev/dev5/antidote.config
sudo sed -i '' "s/{specula_read,.*/{specula_read, $SpeculaRead}./g" ./dev/dev6/antidote.config

./dev/dev1/bin/antidote stop
./dev/dev2/bin/antidote stop
./dev/dev3/bin/antidote stop
./dev/dev4/bin/antidote stop
./dev/dev5/bin/antidote stop
./dev/dev6/bin/antidote stop

sudo rm -r ./dev/dev1/data
sudo rm -r ./dev/dev2/data
sudo rm -r ./dev/dev3/data
sudo rm -r ./dev/dev4/data
sudo rm -r ./dev/dev5/data
sudo rm -r ./dev/dev6/data
sudo rm -r ./dev/dev1/log
sudo rm -r ./dev/dev2/log
sudo rm -r ./dev/dev3/log
sudo rm -r ./dev/dev4/log
sudo rm -r ./dev/dev5/log
sudo rm -r ./dev/dev6/log

sleep 2 
sudo ./dev/dev1/bin/antidote start
sudo ./dev/dev2/bin/antidote start
sudo ./dev/dev3/bin/antidote start
sudo ./dev/dev4/bin/antidote start
#sudo ./dev/dev5/bin/antidote start
#sudo ./dev/dev6/bin/antidote start

sleep 3
sudo ./dev/dev2/bin/antidote-admin cluster join dev1@127.0.0.1
sudo ./dev/dev3/bin/antidote-admin cluster join dev1@127.0.0.1
sudo ./dev/dev4/bin/antidote-admin cluster join dev1@127.0.0.1
#sudo ./dev/dev5/bin/antidote-admin cluster join dev1@127.0.0.1
#sudo ./dev/dev6/bin/antidote-admin cluster join dev1@127.0.0.1
sleep 1
sudo ./dev/dev1/bin/antidote-admin cluster plan
sudo ./dev/dev1/bin/antidote-admin cluster commit 
