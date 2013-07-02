#!/bin/bash
Pa=ebin
cmNode=$1
CMErlCmd=""
ErlCookie=imem

if [ $# == 2 ]; then
     cmNode=$2
     if [ $1 == $2 ]; then
         CMErlCmd="erl -sname CM@$2 -pa $Pa -setcookie $ErlCookie -detached"
         eval $CMErlCmd
     fi
 else
     echo "Starting CM on same machine"
     CMErlCmd="erl -sname CM@$1 -pa $Pa -setcookie $ErlCookie -detached"
     eval $CMErlCmd
 fi

echo "CM on $cmNode"

Opts="-pa deps/*/ebin -setcookie $ErlCookie -env ERL_MAX_ETS_TABLES 10000 -s imem start -imem start_monitor true -imem erl_cluster_mgr 'CM@$cmNode'"

start //MAX werl.exe -sname imem@$1 -pa $Pa $Opts

# Cmd="gnome-terminal \
#     --tab -e \"$CMErlCmd\" \
#     &"
# 
#     #--tab -e 'erl -name A@$1 -pa $Pa $Opts -imem mnesia_node_type disc' \
#     #--tab -e \"erl -name B@$1 -pa $Pa $Opts -imem mnesia_node_type disc\" \
#     #--tab -e \"erl -name C@$1 -pa $Pa $Opts -imem mnesia_node_type disc\" \
#     #--tab -e \"erl -name D@$1 -pa $Pa $Opts -imem mnesia_node_type ram\"  \

# eval $Cmd
#echo $Cmd
