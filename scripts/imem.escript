#!/usr/bin/env escript
%%! -noshell -noinput
%% -*- mode: erlang;erlang-indent-level: 4;indent-tabs-mode: nil -*-
%% ex: ft=erlang ts=4 sw=4 et

-include_lib("kernel/include/file.hrl").
-define(TIMEOUT, 60000).

-define(P(__Fmt),           io:format(user, __Fmt, [])).
-define(P(__Fmt, __Args),   io:format(user, __Fmt, __Args)).

main([NodeName, Cookie | Args]) ->
    cmd(start_distribution(NodeName, Cookie), Args);
main(_) ->
    init:stop(1).

cmd(_, []) ->
    % print usage
    ?P("cmd       arguments~n", []),
    ?P("-----------------------------------~n", []),
    ?P("snap      info [zip]~n", []);

cmd(Node, ["snap", "info"]) ->
    SI = rpc:call(Node, imem_if, snap_info, [], ?TIMEOUT),
    ?P(rpc:call(Node, imem_if, snap_format, [SI], ?TIMEOUT));
cmd(Node, ["snap", "info", "zip"]) ->
    SZI = rpc:call(Node, imem_if, snap_info, [zip], ?TIMEOUT),
     ?P(rpc:call(Node, imem_if, snap_format, [SZI], ?TIMEOUT));
cmd(_, Args)                        -> ?P("unsupported ~p~n", [Args]).

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
