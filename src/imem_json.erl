-module(imem_json).

%% @doc == JSON operations ==
%% Every operation is valid for:
%% - DataObject as proplist
%% - DataObject as map
%% - DataObject as raw json binary
%% and returns in the same format as the provided data object
%% (except for conversion functions)

-export([%% Standard data type operations
         find/2,
         fold/3,
         get/2, get/3,
         has_key/2,
         keys/1, %TODO
         map/2, %TODO
         new/0, new/1, %TODO
         put/3, %TODO
         remove/2, %TODO
         size/1, %TODO
         update/3, %TODO
         values/1, %TODO

         %% Conversions
         to_proplist/1,
         to_map/1,
         to_binary/1,

         %% Original Functionality
         include/2,   %TODO  %% Only include specified keys
         exclude/2,   %TODO  %% Exclude specified keys
         merge/2,     %TODO  %% Merge data objects together (join)
         diff/2,      %TODO  %% Diff between data objects
         equal/2,       %% Check if data objects are perfectly equal
         is_subset/2,   %% Check if first data object is a subset of the second
         is_disjoint/2  %% Check if data objects have no keys in common
         ]).


%% Configuration
%% ===================================================================

-define(JSON_TO_MAPS,true).

-define(TEST,true).

-define(NO_JSON_LIB,throw(no_DataObject_library_present)).

-define(NOT_SUPPORTED,throw(not_yet_supported)).

-ifdef(JSON_TO_MAPS).
%%                  {Module, Decode, Encode}
-define(JSON_LIBS, [{jsx, 
                     fun(__JS) -> maps:from_list(jsx:decode(__JS)) end,
                     fun(__JS) when is_list(__JS) -> jsx:encode(__JS);
                        (__JS) when is_map(__JS) -> jsx:encode(maps:to_list(__JS)) end},
                    {jiffy,
                     fun(__JS) -> jiffy:decode(__JS,[return_maps]) end,
                     fun(__JS) when is_list(__JS) -> jiffy:encode({__JS});
                        (__JS) when is_map(__JS) -> jiffy:encode(__JS) end}
                   ]).
-else.
%%                  {Module, Decode, Encode}
-define(JSON_LIBS, [{jsx, 
                     fun(__JS) -> jsx:decode(__JS) end,
                     fun(__JS) when is_list(__JS) -> jsx:encode(__JS);
                        (__JS) when is_map(__JS) -> jsx:encode(maps:to_list(__JS)) end},
                    {jiffy,
                     fun(__JS) -> {O} = jiffy:decode(__JS), O end,
                     fun(__JS) when is_list(__JS) -> jiffy:encode({__JS});
                        (__JS) when is_map(__JS) -> jiffy:encode(__JS) end}
                   ]).
-endif.

-define(TO_JSON,json_binary_encode).
-define(FROM_JSON,json_binary_decode).
-define(CL,clean_null_values).

%% Type definitions
%% ===================================================================
-type data_object() :: list() | map() | binary().
-type value() :: term().
-type key() :: binary().

%% ===================================================================
%% Exported functions
%% ===================================================================

%% @doc Find function, returns {ok,Value} or error
-spec find(key(),data_object()) -> {ok, value()} | error.
find(Key,DataObject) when is_list(DataObject) ->
    case proplists:get_value(Key,DataObject,undefined) of
        undefined -> error;
        Value -> {ok,Value}
    end;

find(Key,DataObject) when is_map(DataObject) ->
    maps:find(Key,DataObject);

find(Key,DataObject) ->
    find(Key,?FROM_JSON(DataObject)).


%% @doc Standard fold function, returns accumulator. 
%% Fun = fun((K, V, AccIn) -> AccOut)
-spec fold(fun(), Init, data_object()) -> Acc when Init :: term(), Acc :: term().
fold(Fun,Init,RawDataObject) when is_list(RawDataObject) -> 
    DataObject = clean_null_values(RawDataObject),
    lists:foldl(fun({K,V},Acc) -> Fun(K,V,Acc) end,
                Init,
                proplists:unfold(DataObject));

fold(Fun,Init,RawDataObject) when is_map(RawDataObject) -> 
    DataObject = clean_null_values(RawDataObject),
    maps:fold(Fun,Init,DataObject);

fold(Fun,Init,DataObject) -> 
    fold(Fun,Init,?FROM_JSON(DataObject)).


%% @doc Get function, same as get(Key,DataObject,undefined)
-spec get(key(),data_object()) -> value() | undefined.
get(Key,DataObject) ->
    get(Key,DataObject,undefined).


%% @doc Return value corresponding to key. If key not present, returns Default.
-spec get(key(),data_object(),Default) -> value() | Default when Default :: term().
get(Key,DataObject,Default) when is_list(DataObject) ->
    proplists:get_value(Key,DataObject,Default);

get(Key,DataObject,Default) when is_map(DataObject) ->
    case maps:find(Key,DataObject) of
        {ok,Value} -> Value;
        error -> Default
    end;

get(Key,DataObject,Default) ->
    get(Key,?FROM_JSON(DataObject), Default).


%% @doc Check if data object has key
-spec has_key(key(), data_object()) -> boolean().
has_key(Key,DataObject) when is_list(DataObject) ->
    proplists:is_defined(Key,DataObject);

has_key(Key,DataObject) when is_map(DataObject) ->
    maps:is_key(Key,DataObject);

has_key(Key,DataObject) -> 
    has_key(Key,?FROM_JSON(DataObject)).


keys(_DataObject) -> ?NOT_SUPPORTED.

map(_Fun,_DataObject) -> ?NOT_SUPPORTED.

new() -> ?NOT_SUPPORTED.

new(_Type) -> ?NOT_SUPPORTED.

put(_Key,_Value,_DataObject) -> ?NOT_SUPPORTED.

remove(_Key,_DataObject) -> ?NOT_SUPPORTED.

size(_DataObject) -> ?NOT_SUPPORTED.

update(_Key,_Value,_DataObject) -> ?NOT_SUPPORTED.

values(_DataObject) -> ?NOT_SUPPORTED.


%% @doc Convert any format of DataObject to proplist
-spec to_proplist(data_object()) -> list().
to_proplist(DataObject) when is_list(DataObject) ->
    DataObject;

to_proplist(DataObject) when is_map(DataObject) ->
    maps:to_list(DataObject);

to_proplist(DataObject) ->
    to_proplist(?FROM_JSON(DataObject)).


%% @doc Convert any format of DataObject to map
-spec to_map(data_object()) -> map().
to_map(DataObject) when is_list(DataObject) ->
    maps:from_list(DataObject);

to_map(DataObject) when is_map(DataObject) ->
    DataObject;

to_map(DataObject) ->
    to_map(?FROM_JSON(DataObject)).


%% @doc Convert any format of DataObject to binary JSON
-spec to_binary(data_object()) -> binary().
to_binary(DataObject) when is_list(DataObject); is_map(DataObject) ->
    ?TO_JSON(DataObject);

to_binary(DataObject) ->
    DataObject.

include(_Keys,_DataObject) -> ?NOT_SUPPORTED.

exclude(_Keys,_DataObject) -> ?NOT_SUPPORTED.

merge(_DataObject1,_DataObject2) -> ?NOT_SUPPORTED.

diff(_DataObject1, _DataObject2) -> ?NOT_SUPPORTED.
    %% [{added,[{field,value}|_]},
    %%  {removed,[{fields,value}|_]},
    %%  {updated,[{field,value}|_]},
    %%  {replaced,[{field,value}|_]},
    %%  {changes,count}]


%% @doc Compare data objects for equality (null values are ignored)
-spec equal(data_object(), data_object()) -> boolean.
equal(DataObject1, DataObject2) when is_list(DataObject1), is_list(DataObject2) -> 
    lists:sort(?CL(DataObject1)) =:= lists:sort(?CL(DataObject2));

equal(DataObject1, DataObject2) when is_map(DataObject1), is_map(DataObject2) ->
    ?CL(DataObject1) =:= ?CL(DataObject2);

equal(DataObject1, DataObject2) ->
    equal(?FROM_JSON(DataObject1),?FROM_JSON(DataObject2)).
    

%% @doc Returns true when every element of data_object1 is also a member of data_object2, otherwise false.
-spec is_subset(data_object(), data_object()) -> boolean().
is_subset(DataObject1, DataObject2) when is_list(DataObject1), is_list(DataObject2) ->
    Is_Member = fun ({_,null}) -> true;
                    ({K1,V1}) -> V1 =:= proplists:get_value(K1,DataObject2) end,
    lists:all(Is_Member,DataObject1);

is_subset(DataObject1, DataObject2) when is_map(DataObject1), is_map(DataObject2) ->
    Is_Member = fun(_,_,false) -> false;
                   (_,null,true) -> true;
                   (K1,V1,true) -> {ok,V1} =:= maps:find(K1,DataObject2)
                   end,
    maps:fold(Is_Member,true,DataObject1);

is_subset(DataObject1, DataObject2) ->
    is_subset(?FROM_JSON(DataObject1), ?FROM_JSON(DataObject2)).
    

%% @doc Returns true when no element of data_object1 is a member of data_object2, otherwise false.
-spec is_disjoint(data_object(), data_object()) -> boolean().
is_disjoint(DataObject1, DataObject2) when is_list(DataObject1), is_list(DataObject2) ->
    Is_Member = fun({K1,V1}) ->
                    V1 =:= proplists:get_value(K1,DataObject2)
                    end,
    not lists:any(Is_Member,DataObject1);

is_disjoint(DataObject1, DataObject2) when is_map(DataObject1), is_map(DataObject2) ->
    No_Common_Element = fun(_,_,false) -> false;
                        (K1,V1,true) -> {ok,V1} =/= maps:find(K1,DataObject2)
                   end,
    maps:fold(No_Common_Element,true,DataObject1);

is_disjoint(DataObject1, DataObject2) ->
    is_disjoint(?FROM_JSON(DataObject1), ?FROM_JSON(DataObject2)).

%% ===================================================================
%% Internal functions
%% ===================================================================

-spec json_binary_decode(Json) -> data_object() when Json :: binary().
json_binary_decode(Json) ->
    %% Json Lib detection should probably be replaced by an environment variable
    Apps = [App || {App,_,_} <- application:loaded_applications()],
    case [_Fun || {_Lib,_Fun,_} <- ?JSON_LIBS, lists:member(_Lib,Apps)] of
        [Decode|_] -> Decode(Json);
        []     -> ?NO_JSON_LIB 
    end.

-spec json_binary_encode(data_object()) -> Json when Json :: binary().
json_binary_encode(Json) ->
    %% Json Lib detection should probably be replaced by an environment variable
    Apps = [App || {App,_,_} <- application:loaded_applications()],
    case [_Fun || {_Lib,_,_Fun} <- ?JSON_LIBS, lists:member(_Lib,Apps)] of
        [Encode|_] -> Encode(Json);
        []     -> ?NO_JSON_LIB 
    end.

-spec clean_null_values(data_object()) -> data_object().
clean_null_values(DataObject) when is_list(DataObject) ->
    [{K,V} || {K,V} <- DataObject, V =/= null];

clean_null_values(DataObject) when is_map(DataObject) ->
    maps:fold(fun(_,null,OutMap) -> OutMap;
                 (K,V,OutMap) -> maps:put(K,V,OutMap) end,
              #{},
              DataObject);

clean_null_values(DataObject) ->
    ?TO_JSON(clean_null_values(?FROM_JSON(DataObject))).

%% ===================================================================
%% TESTS
%% ===================================================================
-ifdef(TEST).

-include_lib("eunit/include/eunit.hrl").

-define(TEST_JSON, <<"{\"surname\":\"Doe\",\"name\":\"John\",\"foo\":\"bar\",\"earthling\":true,\"age\":981,\"empty\":null}">>).
-define(TEST_PROP, [{<<"surname">>,<<"Doe">>}, {<<"name">>,<<"John">>}, {<<"foo">>,<<"bar">>}, {<<"earthling">>,true}, {<<"age">>,981},{<<"empty">>,null}]).
-define(TEST_MAP,#{<<"age">> => 981, <<"earthling">> => true, <<"foo">> => <<"bar">>, <<"name">> => <<"John">>, <<"surname">> => <<"Doe">>,<<"empty">> => null}).

%% JSON is tested for each function as well even if, basically, it only tests the 
%% JSON to map/proplist conversion each time. This could at least be used as regression
%% testing should JSON data be handled differently.
    
find_test_() ->
    [{"proplist_success",?_assertEqual({ok,<<"Doe">>}, find(<<"surname">>,?TEST_PROP))},
     {"map_success",?_assertEqual({ok,<<"Doe">>}, find(<<"surname">>,?TEST_MAP))},
     {"json_success",?_assertEqual({ok,<<"Doe">>}, find(<<"surname">>,?TEST_JSON))},
     {"proplist_fail",?_assertEqual(error, find(<<"sme">>,?TEST_PROP))},
     {"map_fail",?_assertEqual(error, find(<<"sme">>,?TEST_MAP))},
     {"json_fail",?_assertEqual(error, find(<<"sme">>,?TEST_JSON))}].

fold_test_() ->
    TestFun = fun(_,_,Acc) -> Acc+1 end,
    [{"proplist",?_assertEqual(5, fold(TestFun,0,?TEST_PROP))},
     {"map",?_assertEqual(5, fold(TestFun,0,?TEST_MAP))},
     {"json",?_assertEqual(5, fold(TestFun,0,?TEST_JSON))}].

get_test_() ->
    [{"proplist_success",?_assertEqual(<<"Doe">>, ?MODULE:get(<<"surname">>,?TEST_PROP))},
     {"map_success",?_assertEqual(<<"Doe">>, ?MODULE:get(<<"surname">>,?TEST_MAP))},
     {"json_success",?_assertEqual(<<"Doe">>, ?MODULE:get(<<"surname">>,?TEST_JSON))},
     {"proplist_fail",?_assertEqual(undefined, ?MODULE:get(<<"sme">>,?TEST_PROP))},
     {"map_fail",?_assertEqual(undefined, ?MODULE:get(<<"sme">>,?TEST_MAP))},
     {"json_fail",?_assertEqual(undefined, ?MODULE:get(<<"sme">>,?TEST_JSON))}, 
     {"proplist_custom_default",?_assertEqual(test, ?MODULE:get(<<"sme">>,?TEST_PROP,test))},
     {"map_custom_default",?_assertEqual(test, ?MODULE:get(<<"sme">>,?TEST_MAP,test))},
     {"json_custom_default",?_assertEqual(test, ?MODULE:get(<<"sme">>,?TEST_JSON,test))}]. 

has_key_test_() ->
    [{"proplist_success",?_assert(has_key(<<"surname">>,?TEST_PROP))},
     {"map_success",?_assert(has_key(<<"surname">>,?TEST_MAP))},
     {"json_success",?_assert(has_key(<<"surname">>,?TEST_JSON))},
     {"proplist_fail",?_assert(not has_key(<<"sname">>,?TEST_PROP))},
     {"map_fail",?_assert(not has_key(<<"sname">>,?TEST_MAP))},
     {"json_fail",?_assert(not has_key(<<"sname">>,?TEST_JSON))}].

keys_test_() -> [].

map_test_() -> [].

new_test_() -> [].

put_test_() -> [].

remove_test_() -> [].

size_test_() -> [].

update_test_() -> [].

values_test_() -> [].

to_proplist_test_() ->
    [{"proplist",?_assert(is_list(to_proplist(?TEST_PROP)))},
     {"map",?_assert(is_list(to_proplist(?TEST_MAP)))},
     {"json",?_assert(is_list(to_proplist(?TEST_JSON)))}].

to_map_test_() ->
    [{"proplist",?_assert(is_map(to_map(?TEST_PROP)))},
     {"map",?_assert(is_map(to_map(?TEST_MAP)))},
     {"json",?_assert(is_map(to_map(?TEST_JSON)))}].

to_binary_test_() ->
    [{"proplist",?_assert(is_binary(to_binary(?TEST_PROP)))},
     {"map",?_assert(is_binary(to_binary(?TEST_MAP)))},
     {"json",?_assert(is_binary(to_binary(?TEST_JSON)))}].

include_test_() -> [].

exclude_test_() -> [].

merge_test_() -> [].

diff_test_() -> [].

equal_test_() -> 
    Json_Ok = <<"{\"name\":\"John\",\"surname\":\"Doe\",\"foo\":\"bar\",\"earthling\":true,\"age\":981,\"other\":null}">>,
    Prop_Ok = [{<<"surname">>,<<"Doe">>},{<<"foo">>,<<"bar">>}, {<<"earthling">>,true}, {<<"name">>,<<"John">>}, {<<"age">>,981}],
    Map_Ok = #{<<"earthling">> => true, <<"age">> => 981, <<"name">> => <<"John">>,  <<"foo">> => <<"bar">>,<<"surname">> => <<"Doe">>,<<"empty">> => null},
    Json_Nook = <<"{\"name\":\"John\",\"surname\":null,\"foo\":\"bar\",\"earthling\":true,\"age\":981,\"other\":null}">>,
    Prop_Nook = [{<<"surname">>,<<"Doe">>},{<<"foo">>,<<"barbara">>}, {<<"earthling">>,true}, {<<"name">>,<<"John">>}, {<<"age">>,981}],
    Map_Nook = #{<<"earthling">> => true, <<"age">> => 981, <<"name">> => <<"John">>,  <<"foo">> => <<"bar">>, <<"empty">> => null},
    [{"map_success",?_assert(equal(Map_Ok,?TEST_MAP))},
     {"prop_success",?_assert(equal(Prop_Ok,?TEST_PROP))},
     {"json_success",?_assert(equal(Json_Ok,?TEST_JSON))},
     {"map_fail",?_assert(not equal(Map_Nook,?TEST_MAP))},
     {"prop_fail",?_assert(not equal(Prop_Nook,?TEST_PROP))},
     {"json_fail",?_assert(not equal(Json_Nook,?TEST_JSON))} ].

is_subset_test_() -> 
    Sub_Ok_Json =  <<"{\"surname\":\"Doe\",\"age\":981,\"nothing\":null}">>,
    Sub_Ok_Prop = [{<<"surname">>,<<"Doe">>}, {<<"age">>,981}, {<<"nothing">>,null}],
    Sub_Ok_Map  = #{<<"age">> => 981, <<"surname">> => <<"Doe">>, <<"nothing">> => null},
    Sub_Nook_Json =  <<"{\"suame\":\"Doe\",\"age\":981}">>,
    Sub_Nook_Prop = [{<<"surname">>,<<"Doe">>}, {<<"age">>,91}],
    Sub_Nook_Map  = #{<<"age">> => 981, <<"surname">> => <<"De">>},
    [{"map_success",?_assert(is_subset(Sub_Ok_Map ,?TEST_MAP))},
     {"proplist_success",?_assert(is_subset(Sub_Ok_Prop,?TEST_PROP))},
     {"json_success",?_assert(is_subset(Sub_Ok_Json,?TEST_JSON))},
     {"map_fail",?_assert(not is_subset(Sub_Nook_Map ,?TEST_MAP))},
     {"proplist_fail",?_assert(not is_subset(Sub_Nook_Prop,?TEST_PROP))},
     {"json_fail",?_assert(not is_subset(Sub_Nook_Json,?TEST_JSON))}].

is_disjoint_test_() -> 
    Sub_Ok_Json =  <<"{\"suame\":\"Doe\",\"age\":91}">>,
    Sub_Ok_Prop = [{<<"surname">>,<<"De">>}, {<<"ae">>,981}],
    Sub_Ok_Map  = #{<<"ag">> => 981, <<"surname">> => <<"De">>},
    Sub_Nook_Json =  <<"{\"suame\":\"Doe\",\"age\":981}">>,
    Sub_Nook_Prop = [{<<"surname">>,<<"Doe">>}, {<<"age">>,981}],
    Sub_Nook_Map  = #{<<"age">> => 981, <<"surname">> => <<"De">>},
    [{"map_success",?_assert(is_disjoint(Sub_Ok_Map ,?TEST_MAP))},
     {"proplist_success",?_assert(is_disjoint(Sub_Ok_Prop,?TEST_PROP))},
     {"json_success",?_assert(is_disjoint(Sub_Ok_Json,?TEST_JSON))},
     {"map_fail",?_assert(not is_disjoint(Sub_Nook_Map ,?TEST_MAP))},
     {"proplist_fail",?_assert(not is_disjoint(Sub_Nook_Prop,?TEST_PROP))},
     {"json_fail",?_assert(not is_disjoint(Sub_Nook_Json,?TEST_JSON))}].

-endif.
