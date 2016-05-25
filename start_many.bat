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

REM Starts depending on the input parameter at least 2 nodes.

REM Input parameter number of nodes
SET param=%1

REM Input parameter number of nodes must be an number
IF DEFINED param (
    SET "var="&for /f "delims=0123456789" %%i in ("%1") do set "var=%%i"

    IF DEFINED var (
            ECHO Abort: Input parameter %1 is not a number !!
            GOTO :EOF
    )
)

SET /A nodes=1
IF DEFINED param (
    SET /A nodes=param-1
)
IF %nodes% LSS 1 (
    SET /A nodes=1
)

SET paths=-pa
SET paths=%paths% %cd%\ebin
SET paths=%paths% %cd%\deps\edown\ebin
SET paths=%paths% %cd%\deps\erlscrypt\ebin
SET paths=%paths% %cd%\deps\goldrush\ebin
SET paths=%paths% %cd%\deps\jpparse\ebin
SET paths=%paths% %cd%\deps\jsx\ebin
SET paths=%paths% %cd%\deps\lager\ebin
SET paths=%paths% %cd%\deps\ranch\ebin
SET paths=%paths% %cd%\deps\sext\ebin
SET paths=%paths% %cd%\deps\sqlparse\ebin

SET cookie=-setcookie imem

SET imem=-imem
SET imem=%imem% mnesia_node_type ram
SET imem=%imem% mnesia_schema_name imem

SET kernel=-kernel inet_dist_listen_min 7000 inet_dist_listen_max 8000

SET imem_cm=-imem erl_cluster_mgrs []

CALL start_many_helper.bat "%cookie%" "%imem%" "%imem_cm%" "%kernel%" "%paths%" 0 "is_first"

FOR /L %%n IN (1,1,%nodes%) DO (
    CALL start_many_helper.bat "%cookie%" "%imem%" "%imem_cm%" "%kernel%" "%paths%" %%n
)

:EOF


