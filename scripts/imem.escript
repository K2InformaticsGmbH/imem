#!/usr/bin/env escript
%%! -noshell -noinput
%% -*- mode: erlang;erlang-indent-level: 4;indent-tabs-mode: nil -*-
%% ex: ft=erlang ts=4 sw=4 et

-include_lib("kernel/include/file.hrl").
-define(TIMEOUT, 3600000). % 1 hr timeout

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
    ?P("    ~*s snap zip table1.bkp table2.bkp~n", ?PADARG),
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
    case rpc:call(Node, gen_event, which_handlers, [lager_event], ?TIMEOUT) of
        {badrpc, Error} -> ?P("~p~n",[Error]);
        Handlers ->
            HndlNameLen = lists:max([length(lists:flatten(io_lib:format("~p", [H]))) || H <-  Handlers]),
            Header = lists:flatten(io_lib:format("index ~*s level~n", [-HndlNameLen, "handler"])),
            Seperator = lists:duplicate(length(Header), $-),
            ?P(Seperator++"~n", []),
            ?P(Header, []),
            ?P(Seperator++"~n", []),
            [?P("~*B ~*s ~p~n", [-5,N, -HndlNameLen, lists:flatten(io_lib:format("~p", [H])), rpc:call(Node, lager, get_loglevel, [H], ?TIMEOUT)])
            || {N,H} <- lists:zip(lists:seq(1,length(Handlers)), Handlers)],
            ?P(Seperator++"~n", [])
    end;
cmd(Node, ["log", Id]) ->
    case rpc:call(Node, gen_event, which_handlers, [lager_event], ?TIMEOUT) of
        {badrpc, Error} -> ?P("~p~n",[Error]);
        Handlers ->
            case lists:nth(list_to_integer(Id), Handlers) of
                {'EXIT', Error} -> ?P("~p~n",[Error]);
                H ->
                    HndlNameLen = length(lists:flatten(io_lib:format("~p", [H]))),
                    Header = lists:flatten(io_lib:format("index ~*s level~n", [-HndlNameLen, "handler"])),
                    Seperator = lists:duplicate(length(Header), $-),
                    ?P(Seperator++"~n", []),
                    ?P(Header, []),
                    ?P(Seperator++"~n", []),
                    ?P("~*B ~*s ~p~n", [-5,list_to_integer(Id), -HndlNameLen
                                       , lists:flatten(io_lib:format("~p", [H])), rpc:call(Node, lager, get_loglevel, [H], ?TIMEOUT)
                                       ]),
                    ?P(Seperator++"~n", [])
            end
    end;
cmd(Node, ["log", Id, Val]) when  Val =:= "debug"
                                ; Val =:= "info"
                                ; Val =:= "error"
                                ; Val =:= "none" ->
    case rpc:call(Node, gen_event, which_handlers, [lager_event], ?TIMEOUT) of
        {badrpc, Error} -> ?P("~p~n",[Error]);
        Handlers ->
            case lists:nth(list_to_integer(Id), Handlers) of
                {'EXIT', Error} -> ?P("~p~n",[Error]);
                H -> ?P("~p ~p~n", [lists:flatten(io_lib:format("~p", [H]))
                                   , rpc:call(Node, lager, set_loglevel, [H, list_to_atom(Val)], ?TIMEOUT)])
            end
    end;

% print snap info
cmd(Node, ["snap", "info"]) ->
    SI = rpc:call(Node, imem_snap, info, [bkp], ?TIMEOUT),
    format(SI);
cmd(Node, ["snap", "info", "zip"]) ->
    SZI = rpc:call(Node, imem_snap, info, [zip], ?TIMEOUT),
    format(SZI);

% taek snapshots
cmd(Node, ["snap", "take"]) ->
    RR = rpc:call(Node, imem_snap, take, [[all]], ?TIMEOUT),
    format({take, RR});
cmd(Node, ["snap", "take" | OptTableRegExs]) ->
    if OptTableRegExs =/= [] ->
        [begin
            RR = rpc:call(Node, imem_snap, take, [{tabs, [OptTableRegEx]}], ?TIMEOUT),
            format({take, RR})
        end
        || OptTableRegEx <- OptTableRegExs];
    true ->
        RR = rpc:call(Node, imem_snap, take, [{tabs, OptTableRegExs}], ?TIMEOUT),
        format({take, RR})
    end;

% backup snapshots
cmd(Node, ["snap", "zip", "re", Pattern]) ->
    ?P(rpc:call(Node, imem_snap, zip, [{re, Pattern}], ?TIMEOUT));
cmd(Node, ["snap", "zip"]) ->
    ?P(rpc:call(Node, imem_snap, zip, [all], ?TIMEOUT));
cmd(Node, ["snap", "zip" | OptTables]) ->
    if OptTables =/= [] ->
        [?P(rpc:call(Node, imem_snap, zip, [{files, [OptTable]}], ?TIMEOUT)) || OptTable <- OptTables];
    true ->
        ?P(rpc:call(Node, imem_snap, zip, [{files, OptTables}], ?TIMEOUT))
    end;

% restore from snap
cmd(Node, ["snap", "restore", "zip", Type, FileNameWithPath | OptTables]) when
    Type /= "simulate";
    Type /= "destroy";
    Type /= "replace";
    Type /= "none" ->
    ZipFile = case filelib:is_file(FileNameWithPath) of
        true -> FileNameWithPath;
        _ ->
            ?P("zipfile ~p doesn't exists, trying current working dir...~n", [FileNameWithPath]),
            {ok, CDir} = file:get_cwd(),
            PossibleZipFile = filename:join([CDir, filename:basename(FileNameWithPath)]),
            case filelib:is_file(PossibleZipFile) of
                true -> PossibleZipFile;
                _ ->
                    ?P("zipfile ~p doesn't exists, trying default dir...~n", [PossibleZipFile]),
                    filename:basename(FileNameWithPath)
            end            
    end,
    {Preserve, Simulate} = (if Type =:= "simulate" -> {replace, true}; true -> {list_to_atom(Type), false} end),
    if OptTables =/= [] ->
        [begin
            RR = rpc:call(Node, imem_snap, restore, [zip, ZipFile, [OptTable], Preserve, Simulate], ?TIMEOUT),
            format({restore,RR})
        end
        || OptTable <- OptTables];
    true ->
        RR = rpc:call(Node, imem_snap, restore, [zip, ZipFile, OptTables, Preserve, Simulate], ?TIMEOUT),
        format({restore,RR})
    end;
cmd(Node, ["snap", "restore", "zip" | BkpArgs]) -> cmd(Node, ["snap", "restore", "zip", "destroy" | BkpArgs]);
cmd(Node, ["snap", "restore", "bkp", Type | OptTables]) when
    ((Type =:= "simulate") orelse
    (Type =:= "destroy") orelse
    (Type =:= "replace") orelse
    (Type =:= "none")) ->
    {Preserve, Simulate} = (if Type =:= "simulate" -> {replace, true}; true -> {list_to_atom(Type), false} end),
    if OptTables =/= [] ->
        [begin
            RR = rpc:call(Node, imem_snap, restore, [bkp, [OptTable], Preserve, Simulate], ?TIMEOUT),
            format({restore,RR})
        end
        || OptTable <- OptTables];
    true ->
        RR = rpc:call(Node, imem_snap, restore, [bkp, OptTables, Preserve, Simulate], ?TIMEOUT),
        format({restore,RR})
    end;
cmd(Node, ["snap", "restore", "bkp" | BkpArgs]) -> cmd(Node, ["snap", "restore", "destroy", "bkp" | BkpArgs]);

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
            ?P("Node ~p not responding to pings.\n", [TargetNode]),
            halt(1)
    end,
    TargetNode.

make_script_node(Node) ->
    list_to_atom(lists:concat(["config_", os:getpid(), Node])).

% formatting
% - snapshot info
% - restore info
% for better display
% (a wrapper around info/1 and restore/* interfaces)
-define(FMTTIME(DT),
(fun() ->
    {{_Y,_M,_D},{_H,_Mm,_S}} = DT,
    lists:flatten(io_lib:format("~4..0B.~2..0B.~2..0B ~2..0B:~2..0B:~2..0B", [_Y,_M,_D,_H,_Mm,_S]))
end)()
).
format({_, {error, Error}}) -> ?P(Error);
format({bkp, [ {dbtables, DbTables}
             , {snaptables, SnapTables}
             , {restorabletables, RestorableTables}]
            }) ->
    MTLen = lists:max([length(MTab) || {MTab, _, _} <- DbTables]),
    Header = lists:flatten(io_lib:format("~*s ~-10s ~-15s ~-10s  ~-20s ~7s", [-MTLen, "name", "rows", "memory", "snap_size", "snap_time", "restore"])),
    Sep = lists:duplicate(length(Header),$-),
    ?P("~s~n", [Sep]),
    ?P("~s~n", [Header]),
    ?P("~s~n", [Sep]),
    [(fun() ->
        {SnapSize,SnapTime} = case proplists:lookup(Tab, SnapTables) of
            {Tab, Sz, Tm} -> {integer_to_list(Sz), ?FMTTIME(Tm)};
            none -> {"", ""}
        end,
        Restotable = case lists:member(Tab, RestorableTables) of
            true -> "Y";
            _ -> ""
        end,
        ?P("~*s ~-10B ~-15B ~-10s ~20s ~7s~n", [-MTLen, Tab, Rows, Mem, SnapSize, SnapTime, Restotable])
    end)() || {Tab, Rows, Mem} <- DbTables],
    ?P("~s~n", [Sep]);
format({zip, ContentFiles}) ->
    [(fun() ->
        FLen = lists:max([length(filename:basename(_F)) || {_F,_} <- CntFiles]),
        Header = lists:flatten(io_lib:format("~*s ~-15s  ~-20s ~-20s ~-20s", [-FLen, "name", "size", "created", "accessed", "modified"])),
        Sep = lists:duplicate(length(Header),$-),
        [?P("~s~n", [Sep]),
         ?P("File : ~s~n", [Z]),
         ?P("~s~n", [Header]),
         ?P("~s~n", [Sep]),
         [(fun()->
             ?P("~*s ~-15B ~20s ~20s ~20s~n",
                                 [-FLen, filename:basename(F), Fi#file_info.size
                                 , ?FMTTIME(Fi#file_info.ctime)
                                 , ?FMTTIME(Fi#file_info.atime)
                                 , ?FMTTIME(Fi#file_info.mtime)])
         end)()
         || {F,Fi} <- CntFiles],
         ?P("~s~n", [Sep])]
    end)()
    || {Z,CntFiles} <- ContentFiles];
format({restore, []}) ->
    ?P("nothing is restored~n", []);
format({restore, RestoreRes}) ->
    FLen = lists:max([length(if is_atom(_F) -> atom_to_list(_F); true -> _F end) || {_F, _} <- RestoreRes]),
    Header = lists:flatten(io_lib:format("~*s ~-10s ~-10s ~-10s", [-FLen, "name", "identical", "replaced", "added"])),
    Sep = lists:duplicate(length(Header),$-),
    ?P("~s~n", [Sep]),
    ?P("~s~n", [Header]),
    ?P("~s~n", [Sep]),
    [begin
        _T = if is_atom(T) -> atom_to_list(T); true -> T end,
        case Res of
           {I,E,A} ->
               ?P("~*s ~-10B ~-10B ~-10B~n", [-FLen, _T, I, E, A]);
           Error -> ?P("~*s ~p~n", [-FLen, _T, Error])
        end
    end
    || {T, Res} <- RestoreRes],
    ?P("~s~n", [Sep]);
format({take, TakeRes}) ->
    [case R of
        {ok, T}             -> ?P("snapshot created for ~p~n", [T]);
        {error, T, Reason}  -> ?P("snapshot of ~p failed for ~p~n", [T, Reason])
    end || R <- TakeRes].
