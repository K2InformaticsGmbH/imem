%%%-------------------------------------------------------------------
%%% File        : imem_monitor_ct.erl
%%% Description : Common testing imem_monitor.
%%%
%%% Created     : 09.12.2017
%%%
%%% Copyright (C) 2017 K2 Informatics GmbH
%%%-------------------------------------------------------------------

-module(imem_monitor_ct).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-export([
    monitor_operations/1
]).

-define(NODEBUG, true).

-include_lib("imem.hrl").
-include("imem_meta.hrl").

%%====================================================================
%% Test Cases.
%%====================================================================

monitor_operations(_Config) ->
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":monitor_operations/1 - Start ===>~n", []),

    ?assertEqual(ok, imem_monitor:write_monitor()),
    MonRecs = imem_meta:read(?MONITOR_TABLE),
    ?assert(length(MonRecs) > 0),

    ok.
