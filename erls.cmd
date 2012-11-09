@echo off
set Pa=ebin
set argC=0
for %%x in (%*) do Set /A argC+=1

set cmNode=%1
echo "%argC%"
if %argC% EQU 2 (
    set cmNode=%2
    echo "CM on %cmNode%"
    if %1==%2 (start werl.exe -name CM@%2 -pa %Pa% -setcookie imem -kernel inet_dist_listen_min 9000 inet_dist_listen_max 9020)
) ELSE (
    echo "Starting CM on same machine"
    start werl.exe -name CM@%1 -pa %Pa% -setcookie imem -kernel inet_dist_listen_min 9000 inet_dist_listen_max 9020
)

echo "%cmNode%"

set Opts= -pa deps\sqlparse\ebin -setcookie imem -env ERL_MAX_ETS_TABLES 10000 -kernel inet_dist_listen_min 9000 inet_dist_listen_max 9020 -eval "apply(net_adm, ping, ['CM@%cmNode%'])" -s imem start -imem start_monitor true

start werl.exe -name A@%1 -pa %Pa% %Opts% -imem node_type disc
::start werl.exe -name B@%1 -pa %Pa% %Opts% -imem node_type ram
::start werl.exe -name C@%1 -pa %Pa% %Opts% -imem node_type ram
::start werl.exe -name D@%1 -pa %Pa% %Opts% -imem node_type ram
