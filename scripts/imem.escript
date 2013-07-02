#!/usr/bin/env escript
%%! -noshell -noinput
%% -*- mode: erlang;erlang-indent-level: 4;indent-tabs-mode: nil -*-
%% ex: ft=erlang ts=4 sw=4 et

-include_lib("kernel/include/file.hrl").
-define(TIMEOUT, 60000).

-define(P(__Fmt),           io:format(user, __Fmt, [])).
-define(P(__Fmt, __Args),   io:format(user, __Fmt, __Args)).
-define(SCRIPT, filename:rootname(filename:basename(escript:script_name()))).
-define(PADARG, [length(?SCRIPT), ""]).

main([NodeName, Cookie | Args]) ->
    cmd(start_distribution(NodeName, Cookie), Args);
main(_) ->
    init:stop(1).

% help
cmd(_, []) ->
    % print usage
    ?P("~nThe command ~s supports two classes of sub commands 'snap' and 'log'.~n", [?SCRIPT]),
    ?P("~n'log' helps to manage lager interface if used my host application.~n"),
    ?P("~n'snap' helps to take backup of in memory IMEM tables.~n"),
    ?P("A .bkp file is created for a regular backup in the snapshot~n"),
    ?P("folder as configured for the installed rpm. The existing or~n"),
    ?P("or a subset of .bkp files can also be bundled into a .zip file.~n"),
    ?P("ZIP file name are in this form snapshot_YYYYMMDD_HHMMSS.mmm.zip.~n"),
    ?P("Tables data can also be restored from these .bkp and .zip files~n"),
    ?P("using extensive 'restore' command with various option.~n"),
    ?P("See the Examples section below the usage for some usecases.~n"),
    ?P("~nUsage:"),
    ?P("~n       ~s log [id] [level]~n", [?SCRIPT]),
    ?P("~n       ~s snap info [zip]~n", [?SCRIPT]),
    ?P("       ~*s      take [_regex1_, _regex2_ ...]~n", ?PADARG),
    ?P("       ~*s      zip [re _pattern_]~n", ?PADARG),
    ?P("       ~*s          [table_name1, ...]~n", ?PADARG),
    ?P("       ~*s      restore zip [simulate|destroy|replace|none] zip_file_path [table_name1, ...]~n", ?PADARG),
    ?P("       ~*s              bkp [simulate|destroy|replace|none] [table_name1, ...]~n", ?PADARG),
    ?P("~nCmd 'restore' can work in simulate and non simulate modes.~n"),
    ?P("The general signature of the restore command is~n"),
    ?P("'restore bkp|zip [simulate|destroy|replace|none]' where~n"),
    ?P("'simulate'  will only show the effect of a restore but this mode~n"),
    ?P("            will not restore anything.~n"),
    ?P("'destroy'   delete all rows from the table and restore them from~n"),
    ?P("            backup.~n"),
    ?P("'replace'   will not delete any rows, rows with matching keys~n"),
    ?P("            are replaced. Rows without any key match in existing~n"),
    ?P("            table are added/appended.~n"),
    ?P("'none'      no delete or replace, only new rows are added/appended.~n"),
    ?P("~nExamples:~n"),
    ?P("    get lager active handler - loglevel list~n"),
    ?P("    ~*s log~n", ?PADARG),
    ?P("    set lager loglevel to handler id as given by handler list~n"),
    ?P("    ~*s log 1 debug~n", ?PADARG),
    ?P("~n    list information about the current .bkp files~n"),
    ?P("    ~*s snap info~n", ?PADARG),
    ?P("    list information about the current .zip files~n"),
    ?P("    ~*s snap info zip~n", ?PADARG),
    ?P("~n    create .bkp files for all tables in IMEM DB~n"),
    ?P("    ~*s snap take~n", ?PADARG),
    ?P("    create .bkp files for tables table1 table2 etc~n"),
    ?P("    ~*s snap take table1 table2~n", ?PADARG),
    ?P("    create .bkp files for tables matching table name patterns~n"),
    ?P("    tab.* and tbl.*~n"),
    ?P("    ~*s snap take tab.* tbl.*~n", ?PADARG),
    ?P("~n    bundle all .bkp to .zip~n"),
    ?P("    ~*s snap zip~n", ?PADARG),
    ?P("    bundle table1.bkp, table2.bkp files to .zip~n"),
    ?P("    ~*s snap zip table1 table2~n", ?PADARG),
    ?P("    bundle .bkp files matching name pattern tab.* to .zip~n"),
    ?P("    ~*s snap zip re tab.*~n", ?PADARG),
    ?P("~n    simulate restore of all bkp files into IMEM DB~n"),
    ?P("    ~*s snap restore bkp simulate~n", ?PADARG),
    ?P("    simulate restore all from snapshot_YYYYMMDD_HHMMSS.mmm.zip~n"),
    ?P("    ~*s snap restore zip simulate snapshot_YYYYMMDD_HHMMSS.mmm.zip~n", ?PADARG),
    ?P("    restore all .bkp files into IMEM DB~n"),
    ?P("    ~*s snap restore bkp destroy~n", ?PADARG),
    ?P("    restore of tabel1.bkp into IMEM DB~n"),
    ?P("    ~*s snap restore bkp destroy table1~n", ?PADARG),
    ?P("    replace-appned from tabel1.bkp~n"),
    ?P("    ~*s snap restore bkp replace table1~n", ?PADARG),
    ?P("    only appned new rows from tabel1.bkp~n"),
    ?P("    ~*s snap restore bkp none table1~n", ?PADARG),
    ?P("    restore all from .zip~n"),
    ?P("    ~*s snap restore zip destroy snapshot_YYYYMMDD_HHMMSS.mmm.zip~n", ?PADARG),
    ?P("    replace-appned all tables from .zip~n"),
    ?P("    ~*s snap restore zip replace snapshot_YYYYMMDD_HHMMSS.mmm.zip~n", ?PADARG),
    ?P("    only appned new rows of all tables from .zip~n"),
    ?P("    ~*s snap restore zip replace snapshot_YYYYMMDD_HHMMSS.mmm.zip~n", ?PADARG),
    ?P("    restore tabe1, table2 from .zip~n"),
    ?P("    ~*s snap restore zip destroy snapshot_YYYYMMDD_HHMMSS.mmm.zip table1 table2~n", ?PADARG),
    ?P("    replace and appned rows of tabe1 and table2 from .zip~n"),
    ?P("    ~*s snap restore zip replace snapshot_YYYYMMDD_HHMMSS.mmm.zip tabe1 and table2~n", ?PADARG),
    ?P("    only appned new rows of tabe1 and table2 from .zip~n"),
    ?P("    ~*s snap restore zip replace snapshot_YYYYMMDD_HHMMSS.mmm.zip tabe1 and table2~n", ?PADARG),
    ?P("~n");

% lager
cmd(Node, ["log"]) ->
    ?P(lists:flatten(
        case rpc:call(Node, gen_event, which_handlers, [lager_event], ?TIMEOUT) of
            {badrpc, Error} -> io_lib:format("~p~n",[Error]);
            Handlers ->
                HndlNameLen = lists:max([length(lists:flatten(io_lib:format("~p", [H]))) || H <-  Handlers]),
                Header = lists:flatten(io_lib:format("index ~*s level~n", [-HndlNameLen, "handler"])),
                Seperator = lists:duplicate(length(Header), $-),
                [
                    io_lib:format(Seperator++"~n", []),
                    io_lib:format(Header, []),
                    io_lib:format(Seperator++"~n", []),
                    [io_lib:format("~*B ~*s ~p~n",
                                  [-5,N, -HndlNameLen, lists:flatten(io_lib:format("~p", [H])), rpc:call(Node, lager, get_loglevel, [H], ?TIMEOUT)]
                                  )
                    || {N,H} <- lists:zip(lists:seq(1,length(Handlers)), Handlers)],
                    io_lib:format(Seperator++"~n", [])
                ]
        end
    ));
cmd(Node, ["log", Id]) ->
    ?P(lists:flatten(
        case rpc:call(Node, gen_event, which_handlers, [lager_event], ?TIMEOUT) of
            {badrpc, Error} -> io_lib:format("~p~n",[Error]);
            Handlers ->
                case lists:nth(list_to_integer(Id), Handlers) of
                    {'EXIT', Error} -> io_lib:format("~p~n",[Error]);
                    H ->
                        HndlNameLen = length(lists:flatten(io_lib:format("~p", [H]))),
                        Header = lists:flatten(io_lib:format("index ~*s level~n", [-HndlNameLen, "handler"])),
                        Seperator = lists:duplicate(length(Header), $-),
                        [
                            io_lib:format(Seperator++"~n", []),
                            io_lib:format(Header, []),
                            io_lib:format(Seperator++"~n", []),
                            io_lib:format("~*B ~*s ~p~n",
                                          [-5,list_to_integer(Id), -HndlNameLen
                                          , lists:flatten(io_lib:format("~p", [H])), rpc:call(Node, lager, get_loglevel, [H], ?TIMEOUT)
                                          ]),
                            io_lib:format(Seperator++"~n", [])
                        ]
                end
        end
    ));
cmd(Node, ["log", Id, Val]) when  Val =:= "debug"
                                ; Val =:= "info"
                                ; Val =:= "error"
                                ; Val =:= "none" ->
    ?P(lists:flatten(
        case rpc:call(Node, gen_event, which_handlers, [lager_event], ?TIMEOUT) of
            {badrpc, Error} -> io_lib:format("~p~n",[Error]);
            Handlers ->
                case lists:nth(list_to_integer(Id), Handlers) of
                    {'EXIT', Error} -> io_lib:format("~p~n",[Error]);
                    H ->
                        io_lib:format("~p ~p~n", [lists:flatten(io_lib:format("~p", [H]))
                                            , rpc:call(Node, lager, set_loglevel, [H, list_to_atom(Val)], ?TIMEOUT)])
                end
        end
    ));

% print snap info
cmd(Node, ["snap", "info"]) ->
    SI = rpc:call(Node, imem_snap, info, [bkp], ?TIMEOUT),
    ?P(rpc:call(Node, imem_snap, format, [SI], ?TIMEOUT));
cmd(Node, ["snap", "info", "zip"]) ->
    SZI = rpc:call(Node, imem_snap, info, [zip], ?TIMEOUT),
    ?P(rpc:call(Node, imem_snap, format, [SZI], ?TIMEOUT));

% taek snapshots
cmd(Node, ["snap", "take"]) ->
    ?P(rpc:call(Node, imem_snap, take, [[all]], ?TIMEOUT));
cmd(Node, ["snap", "take" | OptTableRegExs]) ->
    ?P(rpc:call(Node, imem_snap, take, [{tabs, OptTableRegExs}], ?TIMEOUT));

% backup snapshots
cmd(Node, ["snap", "zip", "re", Pattern]) ->
    ?P(rpc:call(Node, imem_snap, zip, [{re, Pattern}], ?TIMEOUT));
cmd(Node, ["snap", "zip"]) ->
    ?P(rpc:call(Node, imem_snap, zip, [all], ?TIMEOUT));
cmd(Node, ["snap", "zip" | OptTables]) ->
    ?P(rpc:call(Node, imem_snap, zip, [{files, OptTables}], ?TIMEOUT));

% restore from snap
cmd(Node, ["snap", "restore", "zip", Type, FileNameWithPath | OptTables]) when
    Type /= "simulate";
    Type /= "destroy";
    Type /= "replace";
    Type /= "none" ->
    {Preserve, Simulate} = (if Type =:= "simulate" -> {replace, true}; true -> {list_to_atom(Type), false} end),
    RR = rpc:call(Node, imem_snap, restore, [zip, FileNameWithPath, OptTables, Preserve, Simulate], ?TIMEOUT),
    ?P(rpc:call(Node, imem_snap, format, [{restore,RR}], ?TIMEOUT));
cmd(Node, ["snap", "restore", "bkp", Type | OptTables]) when
    Type /= "simulate";
    Type /= "destroy";
    Type /= "replace";
    Type /= "none" ->
    {Preserve, Simulate} = (if Type =:= "simulate" -> {replace, true}; true -> {list_to_atom(Type), false} end),
    RR = rpc:call(Node, imem_snap, restore, [bkp, OptTables, Preserve, Simulate], ?TIMEOUT),
    ?P(rpc:call(Node, imem_snap, format, [{restore,RR}], ?TIMEOUT));

cmd(Node, ["snap", "restore", "zip" | ZipArgs]) -> cmd(Node, ["snap", "restore", "zip", "destroy" | ZipArgs]);
cmd(Node, ["snap", "restore", "zip", Type, FileNameWithPath | OptTables]) ->
    ?P(rpc:call(Node, imem_snap, restore, [zip, FileNameWithPath, OptTables, list_to_atom(Type), false], ?TIMEOUT));
cmd(Node, ["snap", "restore", "bkp" | BkpArgs]) -> cmd(Node, ["snap", "restore", "destroy", "bkp" | BkpArgs]);
cmd(Node, ["snap", "restore", Type, "bkp" | OptTables]) ->
    ?P(rpc:call(Node, imem_snap, restore, [bkp, OptTables, list_to_atom(Type), false], ?TIMEOUT));

% unsupported
cmd(Node, Args) ->
    ?P("Error: unknown command ~p~n", [Args]),
    cmd(Node, []).

start_distribution(NodeName, Cookie) ->
    MyNode = make_script_node(NodeName),
    {ok, _Pid} = net_kernel:start([MyNode, longnames]),
    erlang:set_cookie(node(), list_to_atom(Cookie)),
    TargetNode = list_to_atom(NodeName),
    case {net_kernel:hidden_connect_node(TargetNode),
          net_adm:ping(TargetNode)} of
        {true, pong} ->
            ok;
        {_, pang} ->
            io:format("Node ~p not responding to pings.\n", [TargetNode]),
            halt(1)
    end,
    TargetNode.

make_script_node(Node) ->
    list_to_atom(lists:concat(["config_", os:getpid(), Node])).
