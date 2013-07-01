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
    ?P("Usage: ~s snap info [zip]~n", [?SCRIPT]),
    ?P("       ~*s      take [_regex1_ _regex2_ ...]~n", ?PADARG),
    ?P("       ~*s      zip re _pattern_~n", ?PADARG),
    ?P("       ~*s          [table_name1, ...]~n", ?PADARG),
    ?P("       ~*s      restore zip [simulate|destroy|replace|none] zip_file_path [table_name1, ...]~n", ?PADARG),
    ?P("       ~*s              bkp [simulate|destroy|replace|none] [table_name1, ...]~n", ?PADARG);

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
cmd(Node, ["snap", "zip", OptTables]) ->
    ?P(rpc:call(Node, imem_snap, zip, [{files, OptTables}], ?TIMEOUT));

% restore from snap
cmd(Node, ["snap", "restore", "zip", "simulate", FileNameWithPath | OptTables]) ->
    RR = rpc:call(Node, imem_snap, restore, [zip, FileNameWithPath, OptTables, replace, true], ?TIMEOUT),
    ?P(rpc:call(Node, imem_snap, format, [{restore,RR}], ?TIMEOUT));
cmd(Node, ["snap", "restore", "bkp", "simulate" | OptTables]) ->
    RR = rpc:call(Node, imem_snap, restore, [bkp, OptTables, replace, true], ?TIMEOUT),
    ?P(rpc:call(Node, imem_snap, format, [{restore,RR}], ?TIMEOUT));

cmd(Node, ["snap", "restore", "zip" | ZipArgs]) -> cmd(Node, ["snap", "restore", "destroy", "zip" | ZipArgs]);
cmd(Node, ["snap", "restore", Type, "zip", FileNameWithPath | OptTables]) ->
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
            init:stop(1)
    end,
    TargetNode.

make_script_node(Node) ->
    list_to_atom(lists:concat(["config_", os:getpid(), Node])).
