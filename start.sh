#!/bin/sh

erl -name imem@mspsbstun4.local -pa ebin -setcookie imem -eval "apply(net_adm, ping, ['cm@mspsbstun2.local'])" -s imem start -imem start_monitor true -kernel inet_dist_listen_min 9000 inet_dist_listen_max 9020 -env ERL_MAX_ETS_TABLES 10000
