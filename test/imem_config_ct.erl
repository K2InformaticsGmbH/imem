%%%-------------------------------------------------------------------
%%% File        : imem_config_ct.erl
%%% Description : Common testing imem_config.
%%%
%%% Created     : 09.12.2017
%%%
%%% Copyright (C) 2017 K2 Informatics GmbH
%%%-------------------------------------------------------------------

-module(imem_config_ct).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-export([
    end_per_group/1,
    config_operations/1
]).

-define(NODEBUG, true).
-define(CONFIG_TABLE_OPTS, [{record_name, ddConfig}, {type, ordered_set}]).

-include("imem_config.hrl").
-include_lib("imem.hrl").

%%--------------------------------------------------------------------
%% Group related setup and teardown functions.
%%--------------------------------------------------------------------

end_per_group(_Config) ->
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":end_per_group/1 - Start ===>~n", []),
    catch imem_meta:drop_table(test_config),
    ok.

%%====================================================================
%% Test Cases.
%%====================================================================

config_operations(_Config) ->
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":config_operations/1 - Start ===>~n", []),
    try
        ?assertMatch({ok, _}, imem_meta:create_table(test_config, {record_info(fields, ddConfig), ?ddConfig, #ddConfig{}}, ?CONFIG_TABLE_OPTS, system)),
        ?assertEqual(test_value, imem_config:get_config_hlk(test_config, {?MODULE, test_param}, test_owner, [test_context], test_value)),
        ?assertMatch([#ddConfig{hkl = [{?MODULE, test_param}], val = test_value}], imem_meta:read(test_config)), %% default created, owner set
        ?assertEqual(test_value, imem_config:get_config_hlk(test_config, {?MODULE, test_param}, not_test_owner, [test_context], other_default)),
        ?assertMatch([#ddConfig{hkl = [{?MODULE, test_param}], val = test_value}], imem_meta:read(test_config)), %% default not overwritten, wrong owner
        ?assertEqual(test_value1, imem_config:get_config_hlk(test_config, {?MODULE, test_param}, test_owner, [test_context], test_value1)),
        ?assertMatch([#ddConfig{hkl = [{?MODULE, test_param}], val = test_value1}], imem_meta:read(test_config)), %% new default overwritten by owner
        ?assertEqual(ok, imem_config:put_config_hlk(test_config, {?MODULE, test_param}, test_owner, [], test_value2, <<"Test Remark">>)),
        ?assertEqual(test_value2, imem_config:get_config_hlk(test_config, {?MODULE, test_param}, test_owner, [test_context], test_value3)),
        ?assertMatch([#ddConfig{hkl = [{?MODULE, test_param}], val = test_value2}], imem_meta:read(test_config)),
        ?assertEqual(ok, imem_config:put_config_hlk(test_config, {?MODULE, test_param}, test_owner, [test_context], context_value, <<"Test Remark">>)),
        ?assertEqual(context_value, imem_config:get_config_hlk(test_config, {?MODULE, test_param}, test_owner, [test_context], test_value)),
        ?assertEqual(context_value, imem_config:get_config_hlk(test_config, {?MODULE, test_param}, test_owner, [test_context, details], test_value)),
        ?assertEqual(test_value2, imem_config:get_config_hlk(test_config, {?MODULE, test_param}, test_owner, [another_context, details], another_value)),

        ?assertEqual(ok, imem_meta:drop_table(test_config)),

        ok
    catch
        Class:Reason ->
            timer:sleep(100),
            ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ "Exception ~p:~p~n~p~n", [Class, Reason, erlang:get_stacktrace()]),
            throw({Class, Reason})
    end,
    ok.
