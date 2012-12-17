@echo off
set Pa=ebin
set argC=0
for %%x in (%*) do Set /A argC+=1

set cmNode=%1
echo "%argC%"
if %argC% EQU 2 (
    set cmNode=%2
    echo "CM on %cmNode%"
    if %1==%2 (start werl.exe -name CM@%2 -pa %Pa% -setcookie imem)
) ELSE (
    echo "Starting CM on same machine"
    start werl.exe -name CM@%1 -pa %Pa% -setcookie imem
)

echo "%cmNode%"

set Opts= -pa deps\sqlparse\ebin -pa deps\erlimem\ebin -pa deps\lager\ebin -pa deps\lager_imem\ebin -setcookie imem -env ERL_MAX_ETS_TABLES 10000
set ImemOpts= -s imem start -imem start_monitor true -imem erl_cluster_mgr 'CM@%cmNode%' -imem mnesia_node_type disc

::start werl.exe -name A@%1 -pa %Pa% %Opts% %ImemOpts%
start werl.exe -name B@%1 -pa %Pa% %Opts% %ImemOpts%
::start werl.exe -name C@%1 -pa %Pa% %Opts% -imem mnesia_node_type disc
::start werl.exe -name D@%1 -pa %Pa% %Opts% -imem mnesia_node_type ram
