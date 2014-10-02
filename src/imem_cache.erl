-module(imem_cache).

-include("imem.hrl").
-include("imem_meta.hrl").

-export([ read/1
        , write/2
        , clear/1
        ]).


read(Key) -> 
    case imem_meta:read(?CACHE_TABLE,Key) of
        [] ->   [];
        [#ddCache{cvalue=Value}] -> [Value]
    end.

write(Key,Value) -> 
    imem_meta:write(?CACHE_TABLE,#ddCache{ckey=Key,cvalue=Value}).

clear(Key) -> 
    catch (imem_meta:delete(?CACHE_TABLE,Key)).


%% TESTS ------------------------------------------------------------------
-ifdef(TEST).

-include_lib("eunit/include/eunit.hrl").

setup() -> 
    ?imem_test_setup().

teardown(_) ->
    ?imem_test_teardown().

db_test_() ->
    {
        setup,
        fun setup/0,
        fun teardown/1,
        {with, [
                 fun test_without_sec/1
        ]}
    }.
    
test_without_sec(_) -> 
    try
        _ClEr = 'ClientError',
        %% SyEx = 'SystemException',    %% difficult to test
        % SeEx = 'SecurityException',
        ?LogDebug("---TEST--- ~p ----Security ~p ~n", [?MODULE, false]),

        ?LogDebug("schema ~p~n", [imem_meta:schema()]),
        ?LogDebug("data nodes ~p~n", [imem_meta:data_nodes()]),
        ?assertEqual(true, is_atom(imem_meta:schema())),
        ?assertEqual(true, lists:member({imem_meta:schema(),node()}, imem_meta:data_nodes())),

        ?LogDebug("~p:test_mnesia~n", [?MODULE]),

        ?assertEqual(true, is_atom(imem_meta:schema())),
        ?LogDebug("success ~p~n", [schema]),
        ?assertEqual(true, lists:member({imem_meta:schema(),node()}, imem_meta:data_nodes())),
        ?LogDebug("success ~p~n", [data_nodes]),

        ?LogDebug("~p:cache_operations~n", [?MODULE]),

        ?assertEqual([], read(some_test_key)),
        ?assertEqual(ok, write(some_test_key,"Test Value")),
        ?assertEqual(["Test Value"], read(some_test_key)),
        ?assertEqual(ok, clear(some_test_key)),
        ?assertEqual([], read(some_test_key)),

        ?LogDebug("success ~p~n", [cache_operations])

    catch
        Class:Reason ->  ?LogDebug("Exception ~p:~p~n~p~n", [Class, Reason, erlang:get_stacktrace()]),
        ?assert( true == "all tests completed")
    end,
    ok. 

-endif.
