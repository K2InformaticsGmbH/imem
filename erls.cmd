set Pa=ebin
set Opts= -setcookie imem -env ERL_MAX_ETS_TABLES 10000 -kernel inet_dist_listen_min 9000 inet_dist_listen_max 9020 -eval "apply(net_adm, ping, ['CM@127.0.0.1'])" -s imem start -imem start_monitor true

start werl.exe -name CM@127.0.0.1 -pa %Pa% -setcookie imem -kernel inet_dist_listen_min 9000 inet_dist_listen_max 9020

start werl.exe -name A@127.0.0.1 -pa %Pa% %Opts% -imem node_type disc
start werl.exe -name B@127.0.0.1 -pa %Pa% %Opts% -imem node_type ram
start werl.exe -name C@127.0.0.1 -pa %Pa% %Opts% -imem node_type ram
start werl.exe -name D@127.0.0.1 -pa %Pa% %Opts% -imem node_type ram
