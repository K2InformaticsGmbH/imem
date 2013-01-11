@echo off

set Opts= -pa ebin -pa deps\sqlparse\ebin -setcookie imem -env ERL_MAX_ETS_TABLES 10000
set ImemOpts= -s imem start -imem start_monitor true -imem mnesia_node_type disc
start werl.exe %Opts% %ImemOpts%
