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
    ?P("       ~*s      zip re _pattern_~n", ?PADARG),
    ?P("       ~*s          [table_name1, ...]~n", ?PADARG),
    ?P("       ~*s      restore [simulate] zip file_path | file_name [table_name1, ...]~n", ?PADARG),
    ?P("       ~*s              [table_name1, ...]~n", ?PADARG);

% print snap info
cmd(Node, ["snap", "info"]) ->
    SI = rpc:call(Node, imem_snap, snap_info, [], ?TIMEOUT),
    ?P(rpc:call(Node, imem_snap, format, [SI], ?TIMEOUT));
cmd(Node, ["snap", "info", "zip"]) ->
    SZI = rpc:call(Node, imem_snap, snap_info, [zip], ?TIMEOUT),
    ?P(rpc:call(Node, imem_snap, format, [SZI], ?TIMEOUT));

% take snapshot
cmd(Node, ["snap", "zip", "re", Pattern]) ->
    ?P(rpc:call(Node, imem_snap, zip_snap, [{re, Pattern}], ?TIMEOUT));
cmd(Node, ["snap", "zip", OptTables]) ->
    ?P(rpc:call(Node, imem_snap, zip_snap, [{files, OptTables}], ?TIMEOUT));

% restore from snap
cmd(_Node, ["snap", "restore", "simulate", "zip", FileNameOrPath | OptTables]) ->
    ?P("coming soon restore simulate zip ~p tables ~p~n", [FileNameOrPath, OptTables]);
cmd(_Node, ["snap", "restore", "zip", FileNameOrPath | OptTables]) ->
    ?P("coming soon restore zip ~p tables ~p~n", [FileNameOrPath, OptTables]);
cmd(_Node, ["snap", "restore" | OptTables]) ->
    ?P("coming soon restore bkp tables ~p~n", [OptTables]);

% unsupported
cmd(_, Args) ->
    ?P("Errpr: unknown command ~p~n", [Args]).

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
