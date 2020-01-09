%%%-------------------------------------------------------------------
%%% File        : imem_test_slave.erl
%%% Description : Common slave node handling for testing.
%%%
%%% Created     : 02.09.2019
%%%
%%% Copyright (C) 2019 K2 Informatics GmbH
%%%-------------------------------------------------------------------

-module(imem_test_slave).

-export(
    [
        % string(): slave node short name -> slave node full name atom()
        start/1,
        % string()|atom():slave node short of full name -> ok
        stop/1,
        % string(): slave node short name -> slave node full name atom()
        name/1,
        % string()|atom():slave node short of full name -> true|false
        is_running/1
    ]
).

-spec name(string()) -> atom().

%% Returns full name for a local test slave node.

name(SlaveStr) ->
    [_NodeName, Host] = string:tokens(atom_to_list(node()), "@"),
    list_to_atom(SlaveStr ++ "@" ++ Host).

-spec is_running(string() | atom()) -> boolean().

%% Checks connectivity to slave node.

is_running(SlaveStr) when is_list(SlaveStr) -> is_running(name(SlaveStr));
is_running(Slave) when is_atom(Slave) -> lists:member(Slave, nodes()).

-spec start(string()) -> atom().

%% Create a slave node running imem on local host
%% for given short node name (string, without @host).
%% Slave will cluster with current imem node.
%% Returns full name of started node.

start(SlaveStr) when is_list(SlaveStr) ->
    [_NodeName, Host] = string:tokens(atom_to_list(node()), "@"),
    StartArgFmt =
        lists:concat(
            [
                " -setcookie ",
                erlang:get_cookie(),
                " -pa ",
                string:join(code:get_path(), " "),
                " -kernel" " inet_dist_listen_min 7000" " inet_dist_listen_max 7020",
                " -imem"
                " mnesia_node_type ram"
                " mnesia_schema_name imem"
                " node_shard ~p"
                " tcp_server false"
                " cold_start_recover false"
            ]
        ),
    SlaveArgs = lists:flatten(io_lib:format(StartArgFmt, [SlaveStr])),
    {ok, SlaveNode} = slave:start(Host, SlaveStr, SlaveArgs),
    ok = rpc:call(SlaveNode, application, load, [imem]),
    ok = rpc:call(SlaveNode, application, set_env, [imem, erl_cluster_mgrs, [node()]]),
    {ok, _} = rpc:call(SlaveNode, application, ensure_all_started, [imem]),
    SlaveNode.

-spec stop(string() | atom()) -> ok.

%% Stop slave nodes running imem on local host
%% for given short or full node name (string() or atom().

stop(Slave) when is_atom(Slave) -> slave:stop(Slave);
stop(SlaveStr) when is_list(SlaveStr) -> slave:stop(name(SlaveStr)).
