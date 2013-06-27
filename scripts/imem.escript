#!/usr/bin/env escript
%%! -noshell -noinput
%% -*- mode: erlang;erlang-indent-level: 4;indent-tabs-mode: nil -*-
%% ex: ft=erlang ts=4 sw=4 et

-define(TIMEOUT, 60000).

main([NodeName, Cookie | Args]) ->
    TargetNode = start_distribution(NodeName, Cookie),
    io:format(rpc:call(TargetNode, imem_if, cmd, Args, ?TIMEOUT));
main(_) ->
    init:stop(1).

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

