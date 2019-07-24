%%%-------------------------------------------------------------------
%%% File        : imem_dal_skvh_ct.erl
%%% Description : Common testing imem_dal_skvh.
%%%
%%% Created     : 09.11.2017
%%%
%%% Copyright (C) 2017 K2 Informatics GmbH
%%%-------------------------------------------------------------------

-module(imem_dal_skvh_ct).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-export([
    end_per_testcase/2,
    init_per_testcase/2,
    skvh_concurrency/1,
    skvh_operations/1,
    skvh_purge_history/1
]).

-define(AUDIT_SUFFIX, "Audit_86400@_").
-define(AUDIT(__Channel), binary_to_list(__Channel) ++ ?AUDIT_SUFFIX).
-define(Channel, <<"skvhTest">>).
-define(Channels, [<<"skvhTest", N>> || N <- lists:seq($0, $9)]).
-define(HIST_SUFFIX, "Hist").
-define(HIST(__Channel), binary_to_list(__Channel) ++ ?HIST_SUFFIX).

-define(NODEBUG, true).

-include_lib("imem.hrl").
-include_lib("imem_meta.hrl").

%%--------------------------------------------------------------------
%% Test case related setup and teardown functions.
%%--------------------------------------------------------------------

init_per_testcase(TestCase, Config) ->
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":init_per_testcase/2 - Start(~p) ===>~n", [TestCase]),

    catch imem_meta:drop_table(mapChannel),
    catch imem_meta:drop_table(lstChannel),
    catch imem_meta:drop_table(binChannel),
    catch imem_meta:drop_table(noOptsChannel),
    catch imem_meta:drop_table(noHistoryHChannel),
    catch imem_meta:drop_table(skvhTest),
    catch imem_meta:drop_table(skvhTestAudit_86400@_),
    catch imem_meta:drop_table(skvhTestHist),
    [begin
         catch imem_meta:drop_table(binary_to_atom(imem_dal_skvh:table_name(Ch), utf8)),
         catch imem_meta:drop_table(list_to_atom(?AUDIT(Ch))),
         catch imem_meta:drop_table(list_to_atom(?HIST(Ch)))
     end
        || Ch <- ?Channels
    ],
    timer:sleep(50),

    Config.

end_per_testcase(TestCase, _Config) ->
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":end_per_testcase/2 - Start(~p) ===>~n", [TestCase]),

    catch imem_meta:drop_table(mapChannel),
    catch imem_meta:drop_table(lstChannel),
    catch imem_meta:drop_table(binChannel),
    catch imem_meta:drop_table(noOptsChannel),
    catch imem_meta:drop_table(noHistoryHChannel),
    catch imem_meta:drop_table(skvhTest),
    catch imem_meta:drop_table(skvhTestAudit_86400@_),
    catch imem_meta:drop_table(skvhTestHist),

    ok.

%%====================================================================
%% Test cases.
%%====================================================================

skvh_concurrency(_Config) ->
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":skvh_concurrency/1 - Start ===>~n", []),

    TestKey = ["sum"],
    % CreateResult = [imem_dal_skvh:create_table(Ch, [], [], system) || Ch <- ?Channels],  % serialized version
    Self = self(),
    TabCount = length(?Channels),
    [spawn(fun() ->
        Self ! {Ch, imem_dal_skvh:create_table(Ch, [], [], system)} end) || Ch <- ?Channels],
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":success ~p", [bulk_create_spawned]),
    CreateResult = receive_results(TabCount, []),
    ?assertEqual(TabCount, length(CreateResult)),
    ?assertEqual([ok], lists:usort([R || {_, {R, _}} <- CreateResult])),
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":success ~p~n", [create_tables]),

    [spawn(fun() ->
        Self ! {Ch, imem_dal_skvh:insert(system, Ch, TestKey, <<"0">>)} end) || Ch <- ?Channels],
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":success ~p", [bulk_insert_spawned]),
    InitResult = receive_results(TabCount, []),
    ?assertEqual(TabCount, length(InitResult)),
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":success ~p~n", [bulk_insert]),

    [spawn(fun() ->
        Self ! {N1, update_test(hd(?Channels), TestKey, N1)} end) || N1 <- lists:seq(1, 10)],
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":success ~p", [bulk_update_spawned]),
    UpdateResult = receive_results(10, []),
    ?assertEqual(10, length(UpdateResult)),
    ?assertMatch([{skvhTable, _, <<"55">>, _}], imem_meta:read(skvhTest0, sext:encode(TestKey))),
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":success ~p~n", [bulk_update]),

    % DropResult = [imem_dal_skvh:drop_table(Ch) || Ch <- ?Channels],         % serialized version
    [spawn(fun() ->
        Self ! {Ch, imem_dal_skvh:drop_table(Ch)} end) || Ch <- ?Channels],
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":success ~p", [bulk_drop_spawned]),
    _ = {timeout, 10, fun() ->
        ?assertEqual([ok], lists:usort([R || {_, R} <- receive_results(TabCount, [])])) end},
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":success ~p~n", [drop_tables]),

    ok.

skvh_operations(_Config) ->
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":skvh_operations/1 - Start ===>~n", []),

    ClEr = 'ClientError',

    ?assertMatch(ok, imem_dal_skvh:create_check_channel(<<"mapChannel">>, [{type, map}])),
    ?assertMatch(ok, imem_dal_skvh:create_check_channel(<<"lstChannel">>, [{type, list}])),
    ?assertMatch(ok, imem_dal_skvh:create_check_channel(<<"binChannel">>, [{type, binary}])),

    ?assertMatch({ok, [_, _]}, imem_dal_skvh:write(system, <<"mapChannel">>, [{1, #{a=>1}}, {2, #{b=>2}}])),
    ?assertMatch({ok, [_, _]}, imem_dal_skvh:write(system, <<"lstChannel">>, [{1, [a]}, {2, [b]}])),
    ?assertMatch({ok, [_, _]}, imem_dal_skvh:write(system, <<"binChannel">>, [{1, <<"a">>}, {2, <<"b">>}])),

    ?assertException(throw, {ClEr, {"Bad datatype, expected map", [a]}},
        imem_dal_skvh:write(system, <<"mapChannel">>, [{1, [a]}, {2, [b]}])),
    ?assertException(throw, {ClEr, {"Bad datatype, expected map", <<"a">>}},
        imem_dal_skvh:write(system, <<"mapChannel">>, [{1, <<"a">>}, {2, <<"b">>}])),
    ?assertException(throw, {ClEr, {"Bad datatype, expected list", #{a:=1}}},
        imem_dal_skvh:write(system, <<"lstChannel">>, [{1, #{a=>1}}, {2, #{b=>2}}])),
    ?assertException(throw, {ClEr, {"Bad datatype, expected list", <<"a">>}},
        imem_dal_skvh:write(system, <<"lstChannel">>, [{1, <<"a">>}, {2, <<"b">>}])),
    ?assertException(throw, {ClEr, {"Bad datatype, expected binary", #{a:=1}}},
        imem_dal_skvh:write(system, <<"binChannel">>, [{1, #{a=>1}}, {2, #{b=>2}}])),
    ?assertException(throw, {ClEr, {"Bad datatype, expected binary", [a]}},
        imem_dal_skvh:write(system, <<"binChannel">>, [{1, [a]}, {2, [b]}])),

    ?assertMatch(ok, imem_dal_skvh:create_check_channel(<<"noOptsChannel">>, [])),
    ?assertMatch(ok, imem_dal_skvh:create_check_channel(<<"noHistoryHChannel">>, [audit])),

    ?assertEqual(#{chash => <<"24FBRP">>, ckey => test, cvalue => <<"{\"a\":\"a\"}">>}, imem_dal_skvh:write(system, <<"noOptsChannel">>, test, <<"{\"a\":\"a\"}">>)),
    ?assertEqual(#{chash => <<"24FBRP">>, ckey => test, cvalue => <<"{\"a\":\"a\"}">>}, imem_dal_skvh:write(system, <<"noHistoryHChannel">>, test, <<"{\"a\":\"a\"}">>)),

    ?assertEqual([#{chash => <<"24FBRP">>, ckey => test, cvalue => <<"{\"a\":\"a\"}">>}], imem_dal_skvh:read(system, <<"noOptsChannel">>, [test])),
    ?assertEqual([#{chash => <<"24FBRP">>, ckey => test, cvalue => <<"{\"a\":\"a\"}">>}], imem_dal_skvh:read(system, <<"noHistoryHChannel">>, [test])),

    ?assertEqual(<<"skvhTest">>, imem_dal_skvh:table_name(?Channel)),

    ?assertException(throw, {ClEr, {"Table does not exist", _}}, imem_meta:check_table(skvhTest)),
    ?assertException(throw, {ClEr, {"Table does not exist", _}}, imem_meta:check_table(skvhTestAudit_86400@_)),
    ?assertException(throw, {ClEr, {"Table does not exist", _}}, imem_meta:check_table(skvhTestHist)),

    KVa = <<"[1,a]", 9, "123456">>,
    KVb = <<"[1,b]", 9, "234567">>,
    KVc = <<"[1,c]", 9, "345678">>,
    K0 = <<"{<<\"0\">>,<<>>,<<>>}">>,

    ?assertException(throw, {ClEr, {"Channel does not exist", <<"skvhTest">>}}, imem_dal_skvh:read(system, ?Channel, <<"kvpair">>, K0)),
    ?assertEqual(ok, imem_dal_skvh:create_check_channel(?Channel)),
    ?assertEqual({ok, [<<"{<<\"0\">>,<<>>,<<>>}\tundefined">>]}, imem_dal_skvh:read(system, ?Channel, <<"kvpair">>, K0)),
    ?assertEqual({ok, []}, imem_dal_skvh:readGT(system, ?Channel, <<"khpair">>, <<"{<<\"0\">>,<<>>,<<>>}">>, <<"1000">>)),

    ?assertEqual({ok, [<<"[1,a]\tundefined">>]}, imem_dal_skvh:read(system, ?Channel, <<"kvpair">>, <<"[1,a]">>)),
    ?assertEqual({ok, [<<"[1,a]\tundefined">>]}, imem_dal_skvh:read(system, ?Channel, <<"khpair">>, <<"[1,a]">>)),
    ?assertEqual({ok, [<<"[1,a]">>]}, imem_dal_skvh:read(system, ?Channel, <<"key">>, <<"[1,a]">>)),
    ?assertEqual({ok, [<<"undefined">>]}, imem_dal_skvh:read(system, ?Channel, <<"value">>, <<"[1,a]">>)),
    ?assertEqual({ok, [<<"undefined">>]}, imem_dal_skvh:read(system, ?Channel, <<"hash">>, <<"[1,a]">>)),

    ?assertEqual(ok, imem_meta:check_table(skvhTest)),
    ?assertEqual(ok, imem_meta:check_table(skvhTestAudit_86400@_)),
    ?assertEqual(ok, imem_meta:check_table(skvhTestHist)),

    ?assertEqual({ok, [<<"1EXV0I">>, <<"BFFHP">>, <<"ZCZ28">>]}, imem_dal_skvh:write(system, ?Channel, <<"[1,a]", 9, "123456", 10, "[1,b]", 9, "234567", 13, 10, "[1,c]", 9, "345678">>)),

    ?assertEqual({ok, [KVa]}, imem_dal_skvh:read(system, ?Channel, <<"kvpair">>, <<"[1,a]">>)),
    ?assertEqual({ok, [KVc]}, imem_dal_skvh:read(system, ?Channel, <<"kvpair">>, <<"[1,c]">>)),
    ?assertEqual({ok, [KVb]}, imem_dal_skvh:read(system, ?Channel, <<"kvpair">>, <<"[1,b]">>)),
    ?assertEqual({ok, [<<"[1,c]", 9, "ZCZ28">>]}, imem_dal_skvh:read(system, ?Channel, <<"khpair">>, <<"[1,c]">>)),
    ?assertEqual({ok, [<<"BFFHP">>]}, imem_dal_skvh:read(system, ?Channel, <<"hash">>, <<"[1,b]">>)),

    ?assertEqual({ok, [KVc, KVb, KVa]}, imem_dal_skvh:read(system, ?Channel, <<"kvpair">>, <<"[1,c]", 13, 10, "[1,b]", 10, "[1,a]">>)),
    ?assertEqual({ok, [KVa, <<"[1,ab]", 9, "undefined">>, KVb, KVc]}, imem_dal_skvh:read(system, ?Channel, <<"kvpair">>, <<"[1,a]", 13, 10, "[1,ab]", 13, 10, "[1,b]", 10, "[1,c]">>)),

    Dat = imem_meta:read(skvhTest),
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":TEST data ~n~p~n", [Dat]),
    ?assertEqual(3, length(Dat)),

    ?assertEqual({ok, [<<"1EXV0I">>, <<"BFFHP">>, <<"ZCZ28">>]}, imem_dal_skvh:delete(system, ?Channel, <<"[1,a]", 10, "[1,b]", 13, 10, "[1,c]", 10>>)),

    Aud = imem_meta:read(skvhTestAudit_86400@_),
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":audit trail~n~p~n", [Aud]),
    ?assertEqual(6, length(Aud)),
    {ok, Aud1} = imem_dal_skvh:audit_readGT(system, ?Channel, <<"tkvuquadruple">>, <<"{0,0,0}">>, <<"100">>),
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":audit trail~n~p~n", [Aud1]),
    ?assertEqual(6, length(Aud1)),
    {ok, Aud2} = imem_dal_skvh:audit_readGT(system, ?Channel, <<"tkvtriple">>, <<"{0,0,0}">>, 4),
    ?assertEqual(4, length(Aud2)),
    timer:sleep(10), % windows wall clock may be 17ms behind
    {ok, Aud3} = imem_dal_skvh:audit_readGT(system, ?Channel, <<"kvpair">>, <<"now">>, 100),
    ?assertEqual(0, length(Aud3)),
    {ok, Aud4} = imem_dal_skvh:audit_readGT(system, ?Channel, <<"key">>, <<"2100-01-01">>, 100),
    ?assertEqual(0, length(Aud4)),
    Ex4 = {'ClientError', {"Data conversion format error", {timestamp, "1900-01-01", {"Cannot handle dates before 1970"}}}},
    ?assertException(throw, Ex4, imem_dal_skvh:audit_readGT(system, ?Channel, <<"tkvuquadruple">>, <<"1900-01-01">>, 100)),
    {ok, Aud5} = imem_dal_skvh:audit_readGT(system, ?Channel, <<"tkvuquadruple">>, <<"1970-01-01">>, 100),
    ?assertEqual(Aud1, Aud5),

    Hist = imem_meta:read(skvhTestHist),
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":audit trail~n~p~n", [Hist]),
    ?assertEqual(3, length(Hist)),

    ?assertEqual({ok, [<<"[1,a]", 9, "undefined">>]}, imem_dal_skvh:read(system, ?Channel, <<"kvpair">>, <<"[1,a]">>)),
    ?assertEqual({ok, [<<"[1,b]", 9, "undefined">>]}, imem_dal_skvh:read(system, ?Channel, <<"kvpair">>, <<"[1,b]">>)),
    ?assertEqual({ok, [<<"[1,c]", 9, "undefined">>]}, imem_dal_skvh:read(system, ?Channel, <<"kvpair">>, <<"[1,c]">>)),

    ?assertEqual({ok, [<<"1EXV0I">>, <<"BFFHP">>, <<"ZCZ28">>]}, imem_dal_skvh:write(system, ?Channel, <<"[1,a]", 9, "123456", 10, "[1,b]", 9, "234567", 13, 10, "[1,c]", 9, "345678">>)),

    ?assertEqual({ok, []}, imem_dal_skvh:deleteGELT(system, ?Channel, <<"[]">>, <<"[]">>, <<"2">>)),
    ?assertEqual({ok, []}, imem_dal_skvh:deleteGELT(system, ?Channel, <<"[]">>, <<"[1,a]">>, <<"2">>)),

    ?assertEqual({ok, [<<"1EXV0I">>]}, imem_dal_skvh:deleteGELT(system, ?Channel, <<"[]">>, <<"[1,ab]">>, <<"2">>)),
    ?assertEqual({ok, []}, imem_dal_skvh:deleteGELT(system, ?Channel, <<"[]">>, <<"[1,ab]">>, <<"2">>)),

    ?assertException(throw, {ClEr, {117, "Too many values, Limit exceeded", 1}}, imem_dal_skvh:deleteGELT(system, ?Channel, <<"[1,ab]">>, <<"[1,d]">>, <<"1">>)),
    ?assertEqual({ok, [<<"BFFHP">>, <<"ZCZ28">>]}, imem_dal_skvh:deleteGELT(system, ?Channel, <<"[1,ab]">>, <<"[1,d]">>, <<"2">>)),
    ?assertEqual({ok, []}, imem_dal_skvh:deleteGELT(system, ?Channel, <<"[1,ab]">>, <<"[1,d]">>, <<"2">>)),

    ?assertEqual({ok, [<<"undefined">>, <<"undefined">>, <<"undefined">>]}, imem_dal_skvh:delete(system, ?Channel, <<"[1,a]", 10, "[1,b]", 13, 10, "[1,c]", 10>>)),

    ?assertEqual({ok, [<<"IEXQW">>]}, imem_dal_skvh:write(system, ?Channel, <<"[90074,[],\"AaaEnabled\"]", 9, "true">>)),
    ?assertEqual({ok, [<<"24OMVH">>]}, imem_dal_skvh:write(system, ?Channel, <<"[90074,[],\"ContentSizeMax\"]", 9, "297000">>)),
    ?assertEqual({ok, [<<"1W8TVA">>]}, imem_dal_skvh:write(system, ?Channel, <<"[90074,[],<<\"MmscId\">>]", 9, "\"testMMSC\"">>)),
    ?assertEqual({ok, [<<"22D5ZL">>]}, imem_dal_skvh:write(system, ?Channel, <<"[90074,\"MMS-DEL-90074\",\"TpDeliverUrl\"]", 9, "\"http:\/\/10.132.30.84:18888\/deliver\"">>)),

    %% audit_write_noop test
    imem_dal_skvh:write(system, ?Channel, [1, k], <<"{\"a\":\"a\"}">>),
    Time = ?TIME_UID,
    ok = imem_dal_skvh:audit_write_noop(system, ?Channel, [1, k]),
    AudNoop = imem_dal_skvh:audit_readGT(system, ?Channel, Time, 10),
    ?assertEqual(1, length(AudNoop)),
    ?assertMatch([#{nvalue := V, ovalue := V}], AudNoop),

    %% prune_history test
    [#{cvhist := Hist1}] = imem_dal_skvh:hist_read(system, ?Channel, [[1, k]]),
    %% noop history created
    imem_dal_skvh:write(system, ?Channel, [1, k], <<"{\"a\":\"a\"}">>),
    ?assertEqual(1, length(Hist1)),
    [#{cvhist := Hist2}] = imem_dal_skvh:hist_read(system, ?Channel, [[1, k]]),
    ?assertEqual(2, length(Hist2)),
    imem_dal_skvh:prune_history(system, ?Channel),
    [#{cvhist := Hist3}] = imem_dal_skvh:hist_read(system, ?Channel, [[1, k]]),
    ?assertEqual(1, length(Hist3)),
    ?assertMatch([#{ovalue := undefined, nvalue := <<"{\"a\":\"a\"}">>}], Hist3),

    ?assertEqual(ok, imem_meta:truncate_table(skvhTest)),
    ?assertEqual(1, length(imem_meta:read(skvhTestHist))),

    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":audit trail~n~p~n", [imem_meta:read(skvhTestAudit_86400@_)]),

    ?assertEqual(ok, imem_meta:drop_table(skvhTest)),

    ?assertEqual(ok, imem_dal_skvh:create_check_channel(?Channel)),
    ?assertEqual({ok, [<<"1EXV0I">>, <<"BFFHP">>, <<"ZCZ28">>]}, imem_dal_skvh:write(system, ?Channel, <<"[1,a]", 9, "123456", 10, "[1,b]", 9, "234567", 13, 10, "[1,c]", 9, "345678">>)),

    ?assertEqual({ok, []}, imem_dal_skvh:deleteGTLT(system, ?Channel, <<"[]">>, <<"[]">>, <<"1">>)),
    ?assertEqual({ok, []}, imem_dal_skvh:deleteGTLT(system, ?Channel, <<"[]">>, <<"[1,a]">>, <<"1">>)),

    ?assertEqual({ok, [<<"1EXV0I">>]}, imem_dal_skvh:deleteGTLT(system, ?Channel, <<"[]">>, <<"[1,ab]">>, <<"1">>)),
    ?assertEqual({ok, []}, imem_dal_skvh:deleteGTLT(system, ?Channel, <<"[]">>, <<"[1,ab]">>, <<"1">>)),

    ?assertEqual({ok, [<<"ZCZ28">>]}, imem_dal_skvh:deleteGTLT(system, ?Channel, <<"[1,b]">>, <<"[1,d]">>, <<"1">>)),
    ?assertEqual({ok, []}, imem_dal_skvh:deleteGTLT(system, ?Channel, <<"[1,b]">>, <<"[1,d]">>, <<"1">>)),

    ?assertEqual({ok, [<<"BFFHP">>]}, imem_dal_skvh:deleteGTLT(system, ?Channel, <<"[1,a]">>, <<"[1,c]">>, <<"1">>)),
    ?assertEqual({ok, []}, imem_dal_skvh:deleteGTLT(system, ?Channel, <<"[1,a]">>, <<"[1,c]">>, <<"1">>)),

    ?assertEqual({ok, [<<"1EXV0I">>, <<"BFFHP">>, <<"ZCZ28">>]}, imem_dal_skvh:write(system, ?Channel, <<"[1,a]", 9, "123456", 10, "[1,b]", 9, "234567", 13, 10, "[1,c]", 9, "345678">>)),

    ?assertException(throw, {ClEr, {117, "Too many values, Limit exceeded", 1}}, imem_dal_skvh:readGELT(system, ?Channel, <<"hash">>, <<"[]">>, <<"[1,d]">>, <<"1">>)),
    ?assertEqual({ok, [<<"1EXV0I">>, <<"BFFHP">>, <<"ZCZ28">>]}, imem_dal_skvh:readGELT(system, ?Channel, <<"hash">>, <<"[]">>, <<"[1,d]">>, <<"3">>)),

    ?assertEqual({ok, [<<"1EXV0I">>, <<"BFFHP">>, <<"ZCZ28">>]}, imem_dal_skvh:readGELT(system, ?Channel, <<"hash">>, <<"[1,a]">>, <<"[1,d]">>, <<"5">>)),
    ?assertEqual({ok, [<<"BFFHP">>, <<"ZCZ28">>]}, imem_dal_skvh:readGELT(system, ?Channel, <<"hash">>, <<"[1,ab]">>, <<"[1,d]">>, <<"2">>)),
    ?assertEqual({ok, [<<"[1,b]">>]}, imem_dal_skvh:readGELT(system, ?Channel, <<"key">>, <<"[1,ab]">>, <<"[1,c]">>, <<"2">>)),
    ?assertEqual({ok, [<<"234567">>]}, imem_dal_skvh:readGELT(system, ?Channel, <<"value">>, <<"[1,ab]">>, <<"[1,c]">>, <<"2">>)),
    ?assertEqual({ok, [<<"[1,b]", 9, "234567">>]}, imem_dal_skvh:readGELT(system, ?Channel, <<"kvpair">>, <<"[1,ab]">>, <<"[1,c]">>, <<"2">>)),
    ?assertEqual({ok, [<<"[1,b]", 9, "BFFHP">>]}, imem_dal_skvh:readGELT(system, ?Channel, <<"khpair">>, <<"[1,ab]">>, <<"[1,c]">>, <<"2">>)),
    ?assertEqual({ok, [<<"[1,b]", 9, "234567", 9, "BFFHP">>]}, imem_dal_skvh:readGELT(system, ?Channel, <<"kvhtriple">>, <<"[1,ab]">>, <<"[1,c]">>, <<"2">>)),

    ?assertEqual(ok, imem_meta:drop_table(skvhTestAudit_86400@_)),
    ?assertEqual(ok, imem_dal_skvh:create_check_channel(?Channel)),

    ?assertEqual({ok, [<<"1EXV0I">>, <<"BFFHP">>, <<"ZCZ28">>]}, imem_dal_skvh:readGT(system, ?Channel, <<"hash">>, <<"[]">>, <<"1000">>)),
    ?assertEqual({ok, [<<"BFFHP">>, <<"ZCZ28">>]}, imem_dal_skvh:readGT(system, ?Channel, <<"hash">>, <<"[1,a]">>, <<"1000">>)),
    ?assertEqual({ok, [<<"BFFHP">>]}, imem_dal_skvh:readGT(system, ?Channel, <<"hash">>, <<"[1,a]">>, <<"1">>)),
    ?assertEqual({ok, [<<"[1,b]">>, <<"[1,c]">>]}, imem_dal_skvh:readGT(system, ?Channel, <<"key">>, <<"[1,a]">>, <<"2">>)),
    ?assertEqual({ok, [<<"234567">>, <<"345678">>]}, imem_dal_skvh:readGT(system, ?Channel, <<"value">>, <<"[1,ab]">>, <<"2">>)),
    ?assertEqual({ok, [<<"[1,b]", 9, "234567">>, <<"[1,c]", 9, "345678">>]}, imem_dal_skvh:readGT(system, ?Channel, <<"kvpair">>, <<"[1,ab]">>, <<"2">>)),
    ?assertEqual({ok, [<<"[1,b]", 9, "BFFHP">>, <<"[1,c]", 9, "ZCZ28">>]}, imem_dal_skvh:readGT(system, ?Channel, <<"khpair">>, <<"[1,ab]">>, <<"2">>)),

    ?assertEqual(ok, imem_meta:truncate_table(skvhTest)),
    KVtab = <<"{<<\"52015\">>,<<>>,<<\"AaaEnabled\">>}	false
{<<\"52015\">>,<<\"SMS-SUB-52015\">>,<<\"AaaEnabled\">>}	false">>,
    TabRes1 = imem_dal_skvh:write(system, ?Channel, KVtab),
    ?assertEqual({ok, [<<"2FAJ6">>, <<"G8J8Y">>]}, TabRes1),
    KVLong = <<"{<<\"52015\">>,<<>>,<<\"AllowedContentTypes\">>}	\"audio/amr;audio/mp3;audio/x-rmf;audio/x-beatnic-rmf;audio/sp-midi;audio/imelody;audio/smaf;audio/rmf;text/x-imelody;text/x-vcalendar;text/x-vcard;text/xml;text/html;text/plain;text/x-melody;image/png;image/vnd.wap.wbmp;image/bmp;image/gif;image/ief;image/jpeg;image/jpg;image/tiff;image/x-xwindowdump;image/vnd.nokwallpaper;application/smil;application/postscript;application/rtf;application/x-tex;application/x-texinfo;application/x-troff;audio/basic;audio/midi;audio/x-aifc;audio/x-aiff;audio/x-mpeg;audio/x-wav;video/3gpp;video/mpeg;video/quicktime;video/x-msvideo;video/x-rn-mp4;video/x-pn-realvideo;video/mpeg4;multipart/related;multipart/mixed;multipart/alternative;message/rfc822;application/vnd.oma.drm.message;application/vnd.oma.dm.message;application/vnd.sem.mms.protected;application/vnd.sonyericsson.mms-template;application/vnd.smaf;application/xml;video/mp4;\"">>,
    ?assertEqual({ok, [<<"206MFE">>]}, imem_dal_skvh:write(system, ?Channel, KVLong)),

    ?assertEqual(ok, imem_meta:drop_table(skvhTest)),
    ?assertEqual(ok, imem_meta:drop_table(skvhTestAudit_86400@_)),

    %% Test the raw access interface. %%

    ?assertException(throw, {ClEr, {"Table does not exist", _}}, imem_meta:check_table(skvhTest)),
    ?assertException(throw, {ClEr, {"Table does not exist", _}}, imem_meta:check_table(skvhTestAudit_86400@_)),
    ?assertException(throw, {ClEr, {"Table does not exist", _}}, imem_dal_skvh:read(system, ?Channel, ["1"])),

    %% Base maps
    Map1 = #{ckey => ["1"], cvalue => <<"{\"testKey\": \"testValue\"}">>, chash => <<"1HU42V">>},
    Map2 = #{ckey => ["1", "a"], cvalue => <<"{\"testKey\": \"a\", \"testNumber\": 2}">>, chash => <<"1Y22WI">>},
    Map3 = #{ckey => ["1", "b"], cvalue => <<"{\"testKey\": \"b\", \"testNumber\": 100}">>, chash => <<"3MBW5">>},
    Map4 = #{ckey => ["1", "c"], cvalue => <<"{\"testKey\": \"c\", \"testNumber\": 250}">>, chash => <<"1RZ299">>},
    Map5 = #{ckey => ["1", "d"], cvalue => <<"{\"testKey\": \"d\", \"testNumber\": 300}">>, chash => <<"1DKGDA">>},
    Map6 = #{ckey => ["1", "e"], cvalue => [#{<<"testKey">> => <<"b">>, <<"testNumber">> => 3}, #{<<"testKey">> => <<"a">>, <<"testNumber">> => 1}], chash => <<"1404CV">>},  % 1DN1MP

    %% Keys not in the table.
    FirstKey = [""],
    MidleKey = ["1", "b", "1"],
    LastKey = ["1", "e"],

    ?assertEqual(ok, imem_dal_skvh:create_check_channel(?Channel)),
    ?assertEqual({ok, [<<"{<<\"0\">>,<<>>,<<>>}\tundefined">>]}, imem_dal_skvh:read(system, ?Channel, <<"kvpair">>, K0)),
    ?assertEqual(ok, imem_meta:check_table(skvhTest)),
    ?assertEqual(ok, imem_meta:check_table(skvhTestAudit_86400@_)),

    ?assertEqual([], imem_dal_skvh:read(system, ?Channel, [["1"]])),

    BeforeInsert = ?TIME_UID,

    ?assertEqual(Map1, imem_dal_skvh:insert(system, ?Channel, maps:get(ckey, Map1), maps:get(cvalue, Map1))),
    ?assertEqual(Map2, imem_dal_skvh:insert(system, ?Channel, maps:get(ckey, Map2), maps:get(cvalue, Map2))),

    %% Test insert using encoded key
    ?assertEqual(Map3, imem_dal_skvh:insert(system, ?Channel, imem_datatype:term_to_binterm(maps:get(ckey, Map3)), maps:get(cvalue, Map3))),

    %% Test insert using list of maps
    ?assertEqual(Map6, imem_dal_skvh:insert(system, <<"mapChannel">>, imem_datatype:term_to_binterm(maps:get(ckey, Map6)), maps:get(cvalue, Map6))),

    %% Test insert using maps
    ?assertEqual([Map4, Map5], imem_dal_skvh:insert(system, ?Channel, [Map4#{chash := <<>>}, Map5#{chash := <<>>}])),

    %% Fail to insert concurrency exception
    CoEx = 'ConcurrencyException',

    MapNotInserted = #{ckey => ["1", "1"], cvalue => <<"{\"testKey\": \"roll\", \"testNumber\": 100}">>},
    ?assertException(throw, {CoEx, {"Insert failed, key already exists in", _}}, imem_dal_skvh:insert(system, ?Channel, maps:get(ckey, Map1), maps:get(cvalue, Map1))),
    ?assertException(throw, {CoEx, {"Insert failed, key already exists in", _}}, imem_dal_skvh:insert(system, ?Channel, [MapNotInserted, Map4#{chash := <<>>}])),
    ?assertEqual([], imem_dal_skvh:read(system, ?Channel, [maps:get(ckey, MapNotInserted)])),

    %% Read tests
    ?assertEqual([Map1], imem_dal_skvh:read(system, ?Channel, [maps:get(ckey, Map1)])),
    ?assertEqual([Map1, Map2, Map3], imem_dal_skvh:read(system, ?Channel, [maps:get(ckey, Map1), maps:get(ckey, Map2), maps:get(ckey, Map3)])),

    %% Test read using encoded key
    ?assertEqual([Map1], imem_dal_skvh:read(system, ?Channel, [imem_datatype:term_to_binterm(maps:get(ckey, Map1))])),

    %% Updated maps
    Map1Upd = #{ckey => ["1"], cvalue => <<"{\"testKey\": \"newValue\"}">>, chash => <<"1HU42V">>},
    Map2Upd = #{ckey => ["1", "a"], cvalue => <<"{\"testKey\": \"a\", \"newNumber\": 10}">>, chash => <<"1Y22WI">>},
    Map3Upd = #{ckey => ["1", "b"], cvalue => <<"{\"testKey\": \"b\", \"newNumber\": 150}">>, chash => <<"3MBW5">>},
    Map4Upd = #{ckey => ["1", "c"], cvalue => <<"{\"testKey\": \"c\", \"testNumber\": 150}">>, chash => <<"1RZ299">>},
    Map5Upd = #{ckey => ["1", "d"], cvalue => <<"{\"testKey\": \"d\", \"testNumber\": 400}">>, chash => <<"1DKGDA">>},

    BeforeUpdate = ?TIME_UID,

    %% Update using single maps
    Map1Done = imem_dal_skvh:update(system, ?Channel, Map1Upd),
    ?assertEqual(maps:remove(chash, Map1Upd), maps:remove(chash, Map1Done)),

    %% Update multiple objects
    [Map2Done, Map3Done] = imem_dal_skvh:update(system, ?Channel, [Map2Upd, Map3Upd]),
    ?assertEqual([maps:remove(chash, Map2Upd), maps:remove(chash, Map3Upd)]
        , [maps:remove(chash, M) || M <- [Map2Done, Map3Done]]
    ),


    %% Concurrency exception
    ?assertException(throw, {CoEx, {"Data is modified by someone else", _}}, imem_dal_skvh:update(system, ?Channel, Map1Upd)),
    ?assertException(throw, {CoEx, {"Data is modified by someone else", _}}, imem_dal_skvh:update(system, ?Channel, [Map2Upd, Map3Upd])),

    %% Read tests
    ?assertEqual([Map4, Map5], imem_dal_skvh:readGT(system, ?Channel, maps:get(ckey, Map3), 10)),
    ?assertEqual([Map3Done, Map4], imem_dal_skvh:readGT(system, ?Channel, maps:get(ckey, Map2), 2)),
    ?assertEqual([Map4, Map5], imem_dal_skvh:readGT(system, ?Channel, MidleKey, 10)),
    ?assertEqual([], imem_dal_skvh:readGT(system, ?Channel, maps:get(ckey, Map5), 2)),

    ?assertEqual([Map3Done, Map4, Map5], imem_dal_skvh:readGE(system, ?Channel, maps:get(ckey, Map3), 10)),
    ?assertEqual([Map3Done, Map4], imem_dal_skvh:readGE(system, ?Channel, maps:get(ckey, Map3), 2)),
    ?assertEqual([Map4, Map5], imem_dal_skvh:readGE(system, ?Channel, MidleKey, 10)),
    ?assertEqual([Map5], imem_dal_skvh:readGE(system, ?Channel, maps:get(ckey, Map5), 2)),
    ?assertEqual([], imem_dal_skvh:readGE(system, ?Channel, LastKey, 2)),

    ?assertException(throw, {ClEr, {117, "Too many values, Limit exceeded", 1}}, imem_dal_skvh:readGELT(system, ?Channel, FirstKey, LastKey, 1)),
    ?assertEqual([Map1Done, Map2Done, Map3Done, Map4, Map5], imem_dal_skvh:readGELT(system, ?Channel, FirstKey, LastKey, 10)),

    ?assertEqual([Map3Done, Map4, Map5], imem_dal_skvh:readGELT(system, ?Channel, maps:get(ckey, Map3), LastKey, 10)),
    ?assertEqual([Map3Done, Map4], imem_dal_skvh:readGELT(system, ?Channel, maps:get(ckey, Map3), maps:get(ckey, Map5), 10)),
    ?assertEqual([Map4, Map5], imem_dal_skvh:readGELT(system, ?Channel, MidleKey, LastKey, 10)),
    ?assertEqual([], imem_dal_skvh:readGELT(system, ?Channel, LastKey, [LastKey | "1"], 10)),

    BeforeRemove = ?TIME_UID,

    %% Tests removing rows
    ?assertEqual(Map1Done, imem_dal_skvh:remove(system, ?Channel, Map1Done)),

    %% Concurrency exception
    ?assertException(throw, {CoEx, {"Remove failed, key does not exist", _}}, imem_dal_skvh:remove(system, ?Channel, Map1)),
    ?assertException(throw, {CoEx, {"Data is modified by someone else", _}}, imem_dal_skvh:remove(system, ?Channel, [Map2Upd, Map3])),

    %% Remove in bulk
    ?assertEqual([Map2Done, Map3Done], imem_dal_skvh:remove(system, ?Channel, [Map2Done, Map3Done])),

    %% Test final number of rows
    ?assertEqual(2, length(imem_meta:read(skvhTest))),

    %% Audit Tests maps interface.
    %% TODO: How to test reads with multiple partitions.
    ?assertEqual(11, length(imem_dal_skvh:audit_readGT(system, ?Channel, {0, 0, 0}, 100))),

    %% Set time to 0 since depends on the execution of the test.
    AuditInsert1 = #{time => {0, 0, 0}, ckey => maps:get(ckey, Map1), ovalue => undefined, nvalue => maps:get(cvalue, Map1), cuser => system},
    AuditInsert2 = #{time => {0, 0, 0}, ckey => maps:get(ckey, Map2), ovalue => undefined, nvalue => maps:get(cvalue, Map2), cuser => system},
    AuditInsert3 = #{time => {0, 0, 0}, ckey => maps:get(ckey, Map3), ovalue => undefined, nvalue => maps:get(cvalue, Map3), cuser => system},
    ResultAuditInserts = [AuditRow#{time := {0, 0, 0}} || AuditRow <- imem_dal_skvh:audit_readGT(system, ?Channel, BeforeInsert, 3)],
    ?assertEqual([AuditInsert1, AuditInsert2, AuditInsert3], ResultAuditInserts),

    AuditUpdate1 = #{time => {0, 0, 0}, ckey => maps:get(ckey, Map1), ovalue => maps:get(cvalue, Map1), nvalue => maps:get(cvalue, Map1Upd), cuser => system},
    AuditUpdate2 = #{time => {0, 0, 0}, ckey => maps:get(ckey, Map2), ovalue => maps:get(cvalue, Map2), nvalue => maps:get(cvalue, Map2Upd), cuser => system},
    AuditUpdate3 = #{time => {0, 0, 0}, ckey => maps:get(ckey, Map3), ovalue => maps:get(cvalue, Map3), nvalue => maps:get(cvalue, Map3Upd), cuser => system},
    ResultAuditUpdates = [AuditRow#{time := {0, 0, 0}} || AuditRow <- imem_dal_skvh:audit_readGT(system, ?Channel, BeforeUpdate, 3)],
    ?assertEqual([AuditUpdate1, AuditUpdate2, AuditUpdate3], ResultAuditUpdates),

    AuditRemove1 = #{time => {0, 0, 0}, ckey => maps:get(ckey, Map1), ovalue => maps:get(cvalue, Map1Upd), nvalue => undefined, cuser => system},
    AuditRemove2 = #{time => {0, 0, 0}, ckey => maps:get(ckey, Map2), ovalue => maps:get(cvalue, Map2Upd), nvalue => undefined, cuser => system},
    AuditRemove3 = #{time => {0, 0, 0}, ckey => maps:get(ckey, Map3), ovalue => maps:get(cvalue, Map3Upd), nvalue => undefined, cuser => system},
    ResultAuditRemoves = [AuditRow#{time := {0, 0, 0}} || AuditRow <- imem_dal_skvh:audit_readGT(system, ?Channel, BeforeRemove, 3)],
    ?assertEqual([AuditRemove1, AuditRemove2, AuditRemove3], ResultAuditRemoves),

    % ?assertEqual([], imem_dal_skvh:audit_readGT(system, ?Channel, <<"now">>, 100)),   % may not work
    ?assertEqual([], imem_dal_skvh:audit_readGT(system, ?Channel, <<"2100-01-01">>, 100)),
    ?assertEqual(11, length(imem_dal_skvh:audit_readGT(system, ?Channel, <<"1970-01-01">>, 100))),

    Ex4 = {'ClientError', {"Data conversion format error", {timestamp, "1900-01-01", {"Cannot handle dates before 1970"}}}},
    ?assertException(throw, Ex4, imem_dal_skvh:audit_readGT(system, ?Channel, <<"1900-01-01">>, 100)),

    %% Read History tests, time is reset to 0 for comparison but should be ordered from new to old.
    CL1 = [#{time => {0, 0, 0}, ovalue => maps:get(cvalue, Map1Upd), nvalue => undefined, cuser => system}
        , #{time => {0, 0, 0}, ovalue => maps:get(cvalue, Map1), nvalue => maps:get(cvalue, Map1Upd), cuser => system}
        , #{time => {0, 0, 0}, ovalue => undefined, nvalue => maps:get(cvalue, Map1), cuser => system}],
    History1 = #{ckey => maps:get(ckey, Map1), cvhist => CL1},
    CL2 = [#{time => {0, 0, 0}, ovalue => maps:get(cvalue, Map2Upd), nvalue => undefined, cuser => system}
        , #{time => {0, 0, 0}, ovalue => maps:get(cvalue, Map2), nvalue => maps:get(cvalue, Map2Upd), cuser => system}
        , #{time => {0, 0, 0}, ovalue => undefined, nvalue => maps:get(cvalue, Map2), cuser => system}],
    History2 = #{ckey => maps:get(ckey, Map2), cvhist => CL2},
    CL3 = [#{time => {0, 0, 0}, ovalue => maps:get(cvalue, Map3Upd), nvalue => undefined, cuser => system}
        , #{time => {0, 0, 0}, ovalue => maps:get(cvalue, Map3), nvalue => maps:get(cvalue, Map3Upd), cuser => system}
        , #{time => {0, 0, 0}, ovalue => undefined, nvalue => maps:get(cvalue, Map3), cuser => system}],
    History3 = #{ckey => maps:get(ckey, Map3), cvhist => CL3},
    % CL4 = [#{time => {0,0,0}, ovalue => maps:get(cvalue, Map4), nvalue => maps:get(cvalue, Map4Upd), cuser => system}
    %       ,#{time => {0,0,0}, ovalue => undefined, nvalue => maps:get(cvalue, Map4), cuser => system}],
    % History4 = #{ckey => maps:get(ckey, Map4), cvhist => CL4},
    % CL5 = [#{time => {0,0,0}, ovalue => maps:get(cvalue, Map5Upd), nvalue => undefined, cuser => system}
    %       ,#{time => {0,0,0}, ovalue => maps:get(cvalue, Map5), nvalue => maps:get(cvalue, Map5Upd), cuser => system}
    %       ,#{time => {0,0,0}, ovalue => undefined, nvalue => maps:get(cvalue, Map5), cuser => system}],
    % History5 = #{ckey => maps:get(ckey, Map5), cvhist => CL5},
    % CL7 = [#{time => {0,0,0}, ovalue => maps:get(cvalue, Map7), nvalue => maps:get(cvalue, Map7Upd), cuser => system}
    %       ,#{time => {0,0,0}, ovalue => undefined, nvalue => maps:get(cvalue, Map7), cuser => system}],
    % History7 = #{ckey => maps:get(ckey, Map7), cvhist => CL7},
    % CL8 = [#{time => {0,0,0}, ovalue => maps:get(cvalue, Map8Upd), nvalue => undefined, cuser => system}
    %       ,#{time => {0,0,0}, ovalue => maps:get(cvalue, Map8), nvalue => maps:get(cvalue, Map8Upd), cuser => system}
    %       ,#{time => {0,0,0}, ovalue => undefined, nvalue => maps:get(cvalue, Map8), cuser => system}],
    % History8 = #{ckey => maps:get(ckey, Map8), cvhist => CL8},
    % CL9 = [#{time => {0,0,0}, ovalue => maps:get(cvalue, Map9Upd), nvalue => undefined, cuser => system}
    %       ,#{time => {0,0,0}, ovalue => maps:get(cvalue, Map9), nvalue => maps:get(cvalue, Map9Upd), cuser => system}
    %       ,#{time => {0,0,0}, ovalue => undefined, nvalue => maps:get(cvalue, Map9), cuser => system}],
    % History9 = #{ckey => maps:get(ckey, Map9), cvhist => CL9},

    %% Updating objects for history test cases
    [Map4Done, Map5Done] = imem_dal_skvh:update(system, ?Channel, [Map4Upd, Map5Upd]),
    ?assertEqual([maps:remove(chash, Map4Upd), maps:remove(chash, Map5Upd)]
        , [maps:remove(chash, M) || M <- [Map4Done, Map5Done]]
    ),

    %% Read using a list of term keys.
    HistResult = hist_reset_time(imem_dal_skvh:hist_read(system, ?Channel, [maps:get(ckey, Map1), maps:get(ckey, Map2), maps:get(ckey, Map3)])),
    ?assertEqual(3, length(HistResult)),
    ?assertEqual([History1, History2, History3], HistResult),

    %% Using sext encoded Keys
    EncodedKeys = [imem_datatype:term_to_binterm(maps:get(ckey, Map1))
        , imem_datatype:term_to_binterm(maps:get(ckey, Map2))
        , imem_datatype:term_to_binterm(maps:get(ckey, Map3))],

    HistResultEnc = hist_reset_time(imem_dal_skvh:hist_read(system, ?Channel, EncodedKeys)),
    ?assertEqual([History1, History2, History3], HistResultEnc),

    %% Get the last value of a deleted object with key
    ?assertEqual(maps:get(cvalue, Map1Upd), imem_dal_skvh:hist_read_deleted(system, ?Channel, maps:get(ckey, Map1))),

    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":starting ~p", [drop_table123]),
    ?assertEqual(ok, imem_meta:drop_table(skvhTest)),
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":success drop ~p", [skvhTest]),
    ?assertEqual(ok, imem_meta:drop_table(skvhTestAudit_86400@_)),
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":success drop ~p", [skvhTestAudit_86400@_]),
    ?assertEqual(ok, imem_meta:drop_table(skvhTestHist)),
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":success drop ~p", [skvhTestHist]),

    ?assertMatch({ok, _}, imem_dal_skvh:create_table(skvhTest, [], [], system)),
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":starting ~p", [drop_table]),
    ?assertEqual(ok, imem_dal_skvh:drop_table(skvhTest)),
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":success ~p~n", [drop_table]),

    ?assertEqual(ok, imem_dal_skvh:create_check_channel(<<"skvhTest">>)),
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":starting ~p", [trigger_overwrite_check]),
    Hook = <<"test:test_trigger_hook(OldRec,NewRec,User,AuditInfoList)">>,
    ?assertEqual(ok, imem_dal_skvh:add_channel_trigger_hook(test, <<"skvhTest">>, Hook)),
    ?assertEqual(<<"\n,", Hook/binary>>, imem_dal_skvh:get_channel_trigger_hook(test, <<"skvhTest">>)),
    ?assertEqual(ok, imem_dal_skvh:create_check_channel(<<"skvhTest">>)),
    ?assertEqual(<<"\n,", Hook/binary>>, imem_dal_skvh:get_channel_trigger_hook(test, <<"skvhTest">>)),
    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":success ~p~n", [trigger_overwrite_check]),

    ok.

skvh_purge_history(_Config) ->
    imem_config:put_config_hlk(?CONFIG_TABLE, {imem,imem_dal_skvh,purgeHistDayThreshold}, imem_dal_skvh, [], 0, <<"zero">>),
    ?assertEqual(ok, imem_dal_skvh:create_check_channel(?Channel)),
    ?assertEqual([], imem_dal_skvh:hist_read(system, ?Channel, [[test]])),
    #{ckey := [test]} = imem_dal_skvh:write(system, ?Channel, [test], imem_json:encode(#{test => 1})),
    HistTable = list_to_atom(?HIST(?Channel)),
    [#{cvhist := [First]}] = imem_dal_skvh:hist_read(system, ?Channel, [[test]]),
    #{ckey := [test]} = imem_dal_skvh:write(system, ?Channel, [test], imem_json:encode(#{test => 2})),
    #{ckey := [test]} = imem_dal_skvh:write(system, ?Channel, [test], imem_json:encode(#{test => 3})),
    #{ckey := [test]} = imem_dal_skvh:write(system, ?Channel, [test], imem_json:encode(#{test => 4})),
    [#{cvhist := Hists}] = imem_dal_skvh:hist_read(system, ?Channel, [[test]]),
    ?assertEqual(4, length(Hists)),
    ?assertEqual(ok, imem_dal_skvh:purge_history_tables([HistTable])),
    [#{cvhist := Hists2}] = imem_dal_skvh:hist_read(system, ?Channel, [[test]]),
    ?assertEqual(2, length(Hists2)),
    [Last, First] = Hists2,
    #{nvalue := LastNval} = Last,
    ?assertEqual(imem_json:encode(#{test => 4}), LastNval).

%%====================================================================
%% Helper functions.
%%====================================================================

hist_reset_time([]) -> [];
hist_reset_time([#{cvhist := CList} = Hist | Rest]) ->
    [Hist#{cvhist := [C#{time := {0, 0, 0}} || C <- CList]} | hist_reset_time(Rest)].

receive_results(N, Acc) ->
    receive
        Result ->
            case N of
                1 ->
                    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":Result ~p", [Result]),
                    [Result | Acc];
                _ ->
                    ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":Result ~p", [Result]),
                    receive_results(N - 1, [Result | Acc])
            end
    after 4000 ->
        ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING ++ ":Result timeout ~p", [4000]),
        Acc
    end.

update_test(Ch, Key, N) ->
    Upd = fun() ->
        [RowMap] = imem_dal_skvh:read(system, Ch, [Key]),
        CVal = list_to_integer(binary_to_list(maps:get(cvalue, RowMap))) + N,
        imem_dal_skvh:update(system, Ch, RowMap#{cvalue => list_to_binary(integer_to_list(CVal))})
          end,
    imem_meta:transaction(Upd).
