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

cookie=imem

unamestr=`uname`
host=`hostname`
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

Opts="-sname $node -pa ebin -pa deps/*/ebin -setcookie $cookie -env ERL_MAX_ETS_TABLES 10000 -imem start_monitor true -imem tcp_port 8125 -imem erl_cluster_mgrs [$cms] -s imem"
echo "VM options $Opts"

$exename $Opts
