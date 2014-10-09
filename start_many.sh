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
imem="-imem mnesia_node_type ram erl_cluster_mgrs ['imemc@WKS006'] mnesia_schema_name imem"

kernel="-kernel inet_dist_listen_min 7000 inet_dist_listen_max 8000"

exename='start //MAX werl.exe'
$exename $paths -sname imemc@WKS006 $cookie $kernel $imem

node=0
while [ $node -lt $nodes ]; do
    let prt=$port+$node
    $exename $paths -sname imem$node@WKS006 $cookie $kernel $imem -imem tcp_port $prt -s imem
    let node=node+1
done
