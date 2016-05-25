#!/bin/bash

nodes=1
port=9000
if [ "$#" -ge 1 ]; then
    nodes=$1
fi

paths="-pa"
paths=$paths" $PWD/ebin"
paths=$paths" $PWD/deps/*/ebin"

cookie="-setcookie imem"

imem="-imem"
imem=$imem" mnesia_node_type ram"
imem=$imem" mnesia_schema_name imem"
imem_cm="-imem erl_cluster_mgrs []"

kernel="-kernel inet_dist_listen_min 7000 inet_dist_listen_max 8000"

exename='start //MAX werl.exe'
#$exename -name imemc@127.0.0.1 $cookie $kernel
node=0
node_name="-name imem$node@127.0.0.1"
$exename $paths $node_name $cookie $kernel $imem tcp_port $port $imem_cm -s imem

while [ $node -lt $nodes ]; do
    let node=node+1
    let privnode=node-1
    let prt=$port+$node
    node_name="-name imem$node@127.0.0.1"
    imem_cm="-imem erl_cluster_mgrs ['imem$privnode@127.0.0.1']"
    $exename $paths $node_name $cookie $kernel $imem tcp_port $prt $imem_cm -s imem
done
