@ECHO OFF

REM Copyright 2012 K2Informatics GmbH, Root Längenbold, Switzerland
REM
REM Licensed under the Apache License, Version 2.0 (the "License");
REM you may not use this file except in compliance with the License.
REM You may obtain a copy of the License at
REM
REM     http://www.apache.org/licenses/LICENSE-2.0
REM
REM Unless required by applicable law or agreed to in writing, software
REM distributed under the License is distributed on an "AS IS" BASIS,
REM WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
REM See the License for the specific language governing permissions and
REM limitations under the License.

SET ck=imem

SET unamestr=%USERNAME%
REM host=`hostname`
SET host=127.0.0.1

SET node=imem%1@%host%
SET cms=
REM FOR i IN "${@:2}" (
REM     cms=imem$i@$host'"
REM     if [ ! -z "$cms" ]; then
REM         cms="$cms,'imem$i@$host'"
REM     else
REM         cms="'imem$i@$host'"
REM     fi
REM )
ECHO Node %node% and CMs %cms%

SET os_env=-env ERL_MAX_ETS_TABLES 50

REM PATHS
SET paths=-pa
SET paths=%paths% %cd%\_build\default\lib\erlscrypt\ebin
SET paths=%paths% %cd%\_build\default\lib\goldrush\ebin
SET paths=%paths% %cd%\_build\default\lib\imem\ebin
SET paths=%paths% %cd%\_build\default\lib\jpparse\ebin
SET paths=%paths% %cd%\_build\default\lib\jsx\ebin
SET paths=%paths% %cd%\_build\default\lib\lager\ebin
SET paths=%paths% %cd%\_build\default\lib\ranch\ebin
SET paths=%paths% %cd%\_build\default\lib\sext\ebin
SET paths=%paths% %cd%\_build\default\lib\sqlparse\ebin

REM Node name
SET node_name=-name %node%

REM Cookie
SET cookie=-setcookie %ck%

REM IMEM Opts
SET imem_opts=-imem
SET imem_opts=%imem_opts% mnesia_node_type ram
SET imem_opts=%imem_opts% erl_cluster_mgrs [%cms%]
SET imem_opts=%imem_opts% mnesia_schema_name imem
SET imem_opts=%imem_opts% tcp_port 8125

REM Proto dist module
SET dist_opts=-proto_dist
SET dist_opts=%dist_opts% imem_inet_tcp

REM Kernel Opts
SET kernel_opts=-kernel
SET kernel_opts=%kernel_opts% inet_dist_listen_min 7000
SET kernel_opts=%kernel_opts% inet_dist_listen_max 7020

SET start_opts=%os_env% %node_name% %cookie% %paths% %dist_opts% %kernel_opts% %imem_opts%

REM IMEM start options
ECHO ------------------------------------------
ECHO Starting IMEM (Opts)
ECHO ------------------------------------------
ECHO Node Name : %node_name%
ECHO Cookie    : %cookie%
ECHO EBIN Path : %paths%
ECHO IMEM      : %imem_opts%
ECHO Dist      : %dist_opts%
ECHO Kernel    : %kernel_opts%
ECHO OS Env    : %os_env%
ECHO ------------------------------------------

REM Starting imem
START /MAX werl %start_opts% -s imem
