set _Machine.01=A@127.0.0.1
set _Machine.02=B@127.0.0.1
::set _Machine.03=C@127.0.0.1
::set _Machine.04=D@127.0.0.1

set Pa=D:\Work\Git\imem\ebin
set Opts= -setcookie imem -env ERL_MAX_ETS_TABLES 10000 -mnesia schema_location ram dc_dump_limit 40 -kernel inet_dist_listen_min 9000 inet_dist_listen_max 9020

FOR /F "tokens=2* delims=.=" %%A IN ('set _Machine.') DO (
    start werl.exe -name %%B -pa %Pa% %Opts% -eval "apply(net_adm, ping, ['%_Machine.01%'])" -s imem start
)

:: -pa ebin -boot start_sasl -config ${sname}_log -name ${sname}@WKS009.k2informatics.ch -env ERL_MAX_ETS_TABLES 10000 -mnesia schema_location ram dc_dump_limit 40 -setcookie ErlangSolutions -kernel inet_dist_listen_min 9000 inet_dist_listen_max 9020 -eval "apply(application, start, [sub_info])"

::    start werl.exe -name %%B -setcookie imem -pa %Pa% ^ 
::                  -env ERL_MAX_ETS_TABLES 10000 ^ 
::                  -mnesia schema_location ram dc_dump_limit 40 ^ 
::                  -kernel inet_dist_listen_min 9000 inet_dist_listen_max 9020 ^ 
::                  -eval "apply(net_adm, ping, ['%_Machine.01%'])"

