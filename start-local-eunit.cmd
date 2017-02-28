@echo off

set Opts= -pa _build\default\lib\imem\ebin -pa _build\default\lib\sqlparse\ebin -setcookie imem -env ERL_MAX_ETS_TABLES 10000
set ImemOpts= -s imem start -imem mnesia_node_type disc
start werl.exe %Opts% %ImemOpts%
