@echo off
set Pa=ebin
set argC=0
for %%x in (%*) do Set /A argC+=1

set cmNode=%1
echo "%argC%"
if %argC% EQU 2 (
    set cmNode=%2
    echo "CM on %cmNode%"
) ELSE (
    echo "Starting CM on same machine"
)
::    if %1==%2 (start werl.exe -name CM@%2 -pa %Pa% -setcookie imem)
::    start werl.exe -name CM@%1 -pa %Pa% -setcookie imem

:: echo "%cmNode%"

set Opts= -pa deps\sqlparse\ebin -setcookie imem -env ERL_MAX_ETS_TABLES 10000
set ImemOpts= -s imem start -imem start_monitor true -imem mnesia_node_type ram

start werl.exe -name A@%1 -imem erl_cluster_mgr 'B@%1' -pa %Pa% %Opts% %ImemOpts%
PING 1.1.1.1 -n 1 -w 1000 >NUL
start werl.exe -name B@%1 -imem erl_cluster_mgr 'A@%1' -pa %Pa% %Opts% %ImemOpts%
::start werl.exe -name C@%1 -pa %Pa% %Opts% -imem mnesia_node_type disc
::start werl.exe -name D@%1 -pa %Pa% %Opts% -imem mnesia_node_type ram
