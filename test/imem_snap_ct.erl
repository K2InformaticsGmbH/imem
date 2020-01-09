%%%-------------------------------------------------------------------
%%% File        : imem_snap_ct.erl
%%% Description : Common testing imem_snap.
%%%
%%% Created     : 09.12.2017
%%%
%%% Copyright (C) 2017 K2 Informatics GmbH
%%%-------------------------------------------------------------------

-module(imem_snap_ct).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-export([test_snapshot/1]).

-define(BKP_EXTN, ".bkp").
-define(TABLES, lists:sort([atom_to_list(T) || T <- mnesia:system_info(tables) -- [schema]])).
-define(
    FILENAMES(__M, __Dir),
    lists:sort([filename:rootname(filename:basename(F)) || F <- filelib:wildcard(__M, __Dir)])
).
-define(
    EMPTY_DIR(__Dir),
    [{F, file:delete(filename:absname(filename:join([__Dir, F])))} || F <- filelib:wildcard("*.*", __Dir)]
).
-define(NODEBUG, true).

-include_lib("imem.hrl").

%%====================================================================
%% Test cases.
%%====================================================================

test_snapshot(_Config) ->
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":test_snapshot/1 - Start ===>~n", []),
    {_, SnapDir} = application:get_env(imem, imem_snapshot_dir),
    Take = imem_snap:take(ddTable),
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":take snapshots :~n~p~n", [Take]),
    ?assert(lists:member("ddTable", ?FILENAMES("*" ++ ?BKP_EXTN, SnapDir))),
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":snapshot tests completed!~n", []),
    ok.
