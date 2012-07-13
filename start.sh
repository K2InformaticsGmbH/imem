#!/bin/sh
if [ $# -eq 0 ]
then
    sname=imem
else
    sname=$1
fi
erl -pa ebin -boot start_sasl -config ${sname}_log -name ${sname}@WKS009.k2informatics.ch -env ERL_MAX_ETS_TABLES 10000 -mnesia schema_location ram dc_dump_limit 40 -setcookie ErlangSolutions -kernel inet_dist_listen_min 9000 inet_dist_listen_max 9020 -eval "apply(application, start, [sub_info])"
