#!/bin/bash

# Copyright 2012 K2Informatics GmbH, Root Längenbold, Switzerland
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

ck=imem

unamestr=`uname`
#host=`hostname`
host=127.0.0.1
if [[ "$unamestr" == 'Linux' ]]; then
    exename=erl
elif [[ "$unamestr" == 'Darwin' ]]; then
    exename=erl
else
    #exename=erl.exe
    exename='start //MAX werl.exe'
fi

node=imem$1@$host
cms=""
for i in "${@:2}"
do
    if [ ! -z "$cms" ]; then
        cms="$cms,'imem$i@$host'"
    else
        cms="'imem$i@$host'"
    fi
done
echo "Node $node and CMs $cms"

os_env=""
os_env=$os_env" -env ERL_MAX_ETS_TABLES 50"

# PATHS
paths="-pa"
paths=$paths" $PWD/_build/default/lib/*/ebin"

# Node name
node_name="-name $node"

# Cookie
cookie="-setcookie $ck"

# Proto dist module
dist_opts="-proto_dist"
dist_opts=$dist_opts" imem_inet_tcp"

# Kernel Opts
kernel_opts="-kernel"
kernel_opts=$kernel_opts" inet_dist_listen_min 7000"
kernel_opts=$kernel_opts" inet_dist_listen_max 7020"

# IMEM Opts
imem_opts="-imem"
imem_opts=$imem_opts" mnesia_node_type ram"
imem_opts=$imem_opts" erl_cluster_mgrs [$cms]"
imem_opts=$imem_opts" mnesia_schema_name imem"
imem_opts=$imem_opts" tcp_port 8125"
imem_opts=$imem_opts" cold_start_recover false"

lager_config="imem.config"

# sasl opts
sasl_opts="-sasl"
sasl_opts=$sasl_opts"  sasl_error_logger false"

start_opts="$os_env $node_name $cookie $paths $dist_opts $kernel_opts $imem_opts $sasl_opts -config $lager_config"

# IMEM start options
echo "------------------------------------------"
echo "Starting IMEM (Opts)"
echo "------------------------------------------"
echo "Node Name : $node_name"
echo "Cookie    : $cookie"
echo "EBIN Path : $paths"
echo "IMEM      : $imem_opts"
echo "Dist      : $dist_opts"
echo "Kernel    : $kernel_opts"
echo "OS Env    : $os_env"
echo "------------------------------------------"

$exename $start_opts -s imem
