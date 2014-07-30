-module(imem_json).

%% @doc == JSON operations ==
%% Every operation is valid for:
%% - DataObject as proplist
%% - DataObject as map
%% - DataObject as raw json binary
%% and returns in the same format as the provided data object
%% (except for conversion functions)

-export([%% Standard data type operations, inspired by maps operations
         find/2,
         fold/3,
         get/2, get/3,
         has_key/2,
         keys/1, 
         map/2, 
         new/0, new/1, 
         put/3,
         remove/2,
         size/1,
         update/3,
         values/1,

         %% Conversions
         to_proplist/1,
         to_map/1,
         to_binary/1,

         %% Other Functionality
         filter/2,      %% Only keep key/value pairs based on boolean filter
         include/2,     %% Only include specified keys
         exclude/2,     %% Exclude specified keys
         merge/2,       %% Merge data objects together (join)
         diff/2,        %% Diff between data objects
         equal/2,       %% Check if data objects are equal
         is_subset/2,   %% Check if first data object is a subset of the second
         is_disjoint/2  %% Check if data objects have no keys in common
         ]).


%% Configuration
%% ===================================================================

-define(DEFAULT_TYPE, json). % proplist | map | json

-define(JSON_TO_MAPS,true). % parse raw binary json to maps

-define(TEST,true). % testing, to be removed and managed by rebar

-define(NO_JSON_LIB,throw(no_json_library_found)).

-define(NOT_SUPPORTED,throw(not_yet_supported)).

-ifdef(JSON_TO_MAPS).
%%                  {Module, Decode, Encode}
-define(JSON_LIBS, [{jsx, 
                     fun(<<"{}">>) -> #{};
                        (__JS) -> maps:from_list(jsx:decode(__JS)) end,
                     fun(__JS) when is_list(__JS) -> jsx:encode(__JS);
                        (__JS) when is_map(__JS) -> jsx:encode(maps:to_list(__JS)) end},
                    {jiffy,
                     fun(<<"{}">>) -> #{};
                        (__JS) -> jiffy:decode(__JS,[return_maps]) end,
                     fun(__JS) when is_list(__JS) -> jiffy:encode({__JS});
                        (__JS) when is_map(__JS) -> jiffy:encode(__JS) end}
                   ]).
-else.
%%                  {Module, Decode, Encode}
-define(JSON_LIBS, [{jsx, 
                     fun(<<"{}">>) -> [];
                        (__JS) -> jsx:decode(__JS) end,
                     fun(__JS) when is_list(__JS) -> jsx:encode(__JS);
                        (__JS) when is_map(__JS) -> jsx:encode(maps:to_list(__JS)) end},
                    {jiffy,
                     fun(<<"{}">>) -> [];
                        (__JS) -> {O} = jiffy:decode(__JS), O end,
                     fun(__JS) when is_list(__JS) -> jiffy:encode({__JS});
                        (__JS) when is_map(__JS) -> jiffy:encode(__JS) end}
                   ]).
-endif.

%% Technical macros
%% ===================================================================
-define(TO_JSON,json_binary_encode).
-define(FROM_JSON,json_binary_decode).
-define(CL,clean_null_values).

%% Type definitions
%% ===================================================================
-type data_object() :: list() | map() | binary().
-type diff_object() :: list() | map() | binary().
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


%% @doc Standard fold function, returns accumulator. null values are ignored.
%% Fun = fun((K, V, AccIn) -> AccOut)
-spec fold(fun(), Init, data_object()) -> Acc when Init :: term(), Acc :: term().
fold(Fun,Init,DataObject) when is_list(DataObject) -> 
    lists:foldl(fun({K,V},Acc) -> Fun(K,V,Acc) end,
                Init,
                proplists:unfold(?CL(DataObject)));
fold(Fun,Init,DataObject) when is_map(DataObject) -> 
    maps:fold(Fun,Init,?CL(DataObject));
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


%% @doc Unordered list of keys used by data object
-spec keys(data_object()) -> list().
keys(DataObject) when is_list(DataObject) -> 
    proplists:get_keys(DataObject);
keys(DataObject) when is_map(DataObject) -> 
    maps:keys(DataObject);
keys(DataObject) -> 
    keys(?FROM_JSON(DataObject)).

%% @doc Produces a new data object by calling the function fun F(K, V1) 
%% for every K to value V1 association in data object in arbitrary order. 
%% The function fun F/2 must return the value V2 to be associated with key K 
%% for the new data object. 
%% null values are ignored and not carried over to new data object.
-spec map(fun(), data_object()) -> data_object().
map(Fun,DataObject) when is_list(DataObject) ->
    lists:map(fun({K,V}) -> {K,Fun(K,V)} end,
              proplists:unfold(DataObject));
map(Fun,DataObject) when is_map(DataObject) ->
    maps:map(Fun,?CL(DataObject));
map(Fun,DataObject) ->
    ?TO_JSON(map(Fun,?FROM_JSON(DataObject))).

%% @doc Create new imen json data object using default type
-spec new() -> data_object().
new() -> new(?DEFAULT_TYPE).

%% @doc Create new imen json data object
-spec new(Type) -> data_object() when Type :: json | proplist | map.
new(proplist) -> [];
new(map) -> #{};
new(json) -> <<"{}">>.


%% @doc Add a value to a IMEM Json data object
%% Should Key already exist, old value will be replaced
-spec put(key(), value(), data_object()) -> data_object().
put(Key,Value,DataObject) when is_list(DataObject) ->
    [{Key,Value}| proplists:delete(Key,DataObject)];
put(Key,Value,DataObject) when is_map(DataObject) ->
    maps:put(Key,Value,DataObject);
put(Key,Value,DataObject) ->
    ?TO_JSON(?MODULE:put(Key,Value,?FROM_JSON(DataObject))).


%% @doc Remove a key from a data object
-spec remove(key(), data_object()) -> data_object().
remove(Key,DataObject) when is_list(DataObject) ->
    proplists:delete(Key,DataObject);
remove(Key,DataObject) when is_map(DataObject) ->
    maps:remove(Key,DataObject);
remove(Key,DataObject) ->
    ?TO_JSON(remove(Key,?FROM_JSON(DataObject))).
    

%% @doc Size of a data object, ignoring null values
-spec size(data_object()) -> integer().
size(DataObject) when is_list(DataObject) -> 
    length(?CL(DataObject));
size(DataObject) when is_map(DataObject) -> 
    maps:size(?CL(DataObject));
size(DataObject) -> 
 ?MODULE:size(?FROM_JSON(DataObject)).


%% @doc Update a key with a new value. If key is not present, 
%% error:badarg exception is raised.
-spec update(key(),value(),data_object()) -> data_object().
update(Key,Value,DataObject) when is_list(DataObject) ->
    case proplists:is_defined(Key,DataObject) of
        true -> [{Key,Value} | proplists:delete(Key,DataObject)];
        false -> erlang:error(badarg)
    end;
update(Key,Value,DataObject) when is_map(DataObject) ->
    %% Maps update raises error:badarg by default
    %% if key is not present
    maps:update(Key,Value,DataObject);
update(Key,Value,DataObject) ->
    ?TO_JSON(update(Key,Value,?FROM_JSON(DataObject))).


%% @doc Returns a complete list of values, in arbitrary order, from data object
-spec values(data_object()) -> list().
values(DataObject) when is_list(DataObject) ->
    [V || {_,V} <- DataObject];
values(DataObject) when is_map(DataObject) ->
    maps:values(DataObject);
values(DataObject) -> 
    values(?FROM_JSON(DataObject)).


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

%% @doc Builds a new data object with key/value pairs from provided data
%% object according if boolean filter function (fun(K,V) -> true | false) 
%% return true.
-spec filter(fun(),data_object()) -> data_object().
filter(Fun,DataObject) when is_list(DataObject) ->
    lists:foldl(fun({K,V},In) ->
                    case Fun(K,V) of
                        true -> [{K,V}|In];
                        false -> In
                    end end,
                [],
                DataObject);
filter(Fun,DataObject) when is_map(DataObject) ->
    map:fold(fun(K,V,In) ->
                    case Fun(K,V) of
                        true -> maps:put(K,V,In);
                        false -> In
                    end end,
             #{},
             DataObject);
filter(Fun,DataObject) ->
    ?TO_JSON(filter(Fun,?FROM_JSON(DataObject))).
    
    

%% @doc Build data object with provided keys and associated values from
%% provided data object
-spec include(Keys,data_object()) -> data_object() when Keys :: [key() | Keys].
include(Keys,DataObject) when is_list(DataObject) -> 
    [{K,proplists:get_value(K,DataObject)} || K <- Keys];
include(Keys,DataObject) when is_map(DataObject) -> 
    lists:foldl(fun(K,MapIn) ->
                    maps:put(K,maps:get(K,DataObject),MapIn) end,
                #{},
                Keys);
include(Keys,DataObject) ->
    ?TO_JSON(include(Keys,?FROM_JSON(DataObject))).

%% @doc Build data object with data from provided data object, excluding
%% the elements associated with provided keys
-spec exclude(Keys,data_object()) -> data_object() when Keys :: [key() | Keys].
exclude(Keys,DataObject) when is_list(DataObject) -> 
    [{K,V} || {K,V} <- DataObject, lists:member(K,Keys) =:= false];
exclude(Keys,DataObject) when is_map(DataObject) -> 
    map:fold(fun(K,V,MapIn) ->
                case lists:member(K,Keys) of
                    true -> MapIn;
                    false -> maps:put(K,V,MapIn)
                end end,
             #{},
             DataObject);
exclude(Keys,DataObject) ->
    ?TO_JSON(exclude(Keys,?FROM_JSON(DataObject))).

%% @doc Merges two data objects into a data object. 
%% If two keys exists in both data objects  the value in the first data object
%% will be superseded by the value in the second data object, unless the value 
%% of the second data object is null.
-spec merge(data_object(),data_object()) -> data_object().
merge(DataObject1,DataObject2) when is_list(DataObject1), is_list(DataObject2) ->
    CleanObject2 = ?CL(DataObject2),
    Object2Keys = proplists:get_keys(CleanObject2),
    CleanObject2 ++ [{K,V} || {K,V} <- DataObject1, lists:member(K,Object2Keys) =:= false];
merge(DataObject1,DataObject2) when is_map(DataObject1), is_map(DataObject2) ->
    maps:merge(DataObject1,?CL(DataObject2));
merge(DataObject1,DataObject2) ->
    ?TO_JSON(merge(?FROM_JSON(DataObject1),?FROM_JSON(DataObject2))).

%% @doc creates a diff data object between the first (old) and second (new) provided data
%% objects. null values are ignored. Resulting data object will have 5 root keys, each containg
%% corresponding key/value pairs. Outgoing format is the same as ingoing format.
%%
%% added : contains key/value pairs who were in data object 2 but not in data object 1
%% removed : contains key/value pairs who were in data object 1 but not in data objec 2
%% updated : containts key/value pairs who are not indentical between data object 1 and 2,
%%           with the values from data object 2
%% replaced : containts key/value pairs who are not indentical between data object 1 and 2,
%%            with the values from data object 1
-spec diff(data_object(), data_object()) -> diff_object().
diff(RawDataObject1, RawDataObject2) when is_list(RawDataObject1), is_list(RawDataObject2) ->
    DataObject1 = ?CL(RawDataObject1),
    DataObject2 = ?CL(RawDataObject2),
    Object1Keys = proplists:keys(DataObject1),
    Object2Keys = proplists:keys(DataObject2),
    CommonKeys = lists:filter(fun(K) -> lists:member(K,Object1Keys) end, Object2Keys),

    UpdatedJob = fun(K,Acc) -> 
                    case {proplists:get_value(K,DataObject1),
                          proplists:get_value(K,DataObject2)} of
                          {__Same,__Same} -> Acc;
                          {__Old,__New}   -> [{K,__Old,__New} | Acc]
                    end end,
    UpdatedTuples = lists:foldl(UpdatedJob,
                                [],
                                CommonKeys),

    Added = lists:filter(fun({K,_}) -> lists:member(K,Object1Keys) =:= false end, DataObject2),
    Removed = lists:filter(fun({K,_}) -> lists:member(K,Object2Keys) =:= false end, DataObject1),
    Updated = [{K,V} || {K,_,V} <- UpdatedTuples],
    Replaced = [{K,V} || {K,V,_} <- UpdatedTuples],

    [{added,Added},
     {removed,Removed},
     {updated,Updated},
     {replaced,Replaced},
     {changes, length(Added) + length(Removed) + length(Updated)}];
diff(RawDataObject1, RawDataObject2) when is_map(RawDataObject1), is_map(RawDataObject2) ->
    DataObject1 = ?CL(RawDataObject1),
    DataObject2 = ?CL(RawDataObject2),

    Object1Keys = maps:keys(DataObject1),
    Object2Keys = maps:keys(DataObject2),
    CommonKeys = lists:filter(fun(K) -> lists:member(K,Object1Keys) end, Object2Keys),

    UpdatedJob = fun(K,Acc) -> 
                    case {maps:get(K,DataObject1),
                          maps:get(K,DataObject2)} of
                          {__Same,__Same} -> Acc;
                          {__Old,__New}   -> [{K,__Old,__New} | Acc]
                    end end,
    UpdatedTuples = lists:foldl(UpdatedJob,
                                [],
                                CommonKeys),
    Added = lists:foldl(fun(K,MapIn) -> 
                         case lists:member(K,Object1Keys) of
                                false -> maps:put(K,maps:get(K,DataObject2),MapIn);
                                true  -> MapIn
                         end end,
                        #{},
                        Object2Keys),
    Removed = lists:foldl(fun(K,MapIn) -> 
                           case lists:member(K,Object2Keys) of
                                  false -> maps:put(K,maps:get(K,DataObject1),MapIn);
                                  true  -> MapIn
                           end end,
                          #{},
                          Object1Keys),
    Updated = lists:foldl(fun({K,_,V},MapIn) -> maps:put(K,V,MapIn) end,
                          #{},
                          UpdatedTuples),
    Replaced = lists:foldl(fun({K,V,_},MapIn) -> maps:put(K,V,MapIn) end,
                           #{},
                           UpdatedTuples),

    #{added => Added,
      removed => Removed,
      updated => Updated,
      replaced => Replaced,
      changes =>  maps:size(Added) + maps:size(Removed) + maps:size(Updated)};
diff(DataObject1, DataObject2) ->
    ?TO_JSON(diff(?FROM_JSON(DataObject1),?FROM_JSON(DataObject2))).

% Added: keys in 2 who are not in 1
% Removed: keys in 1 who are not in 2
% Update: keys in 2 who are in 1 but with different values
% Replaced: keys in 1 who are in 2 but with different values
% Changes: count added + count removed + count replaced

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

put_test_() -> 
    [{"prop_empty",?_assertEqual([{test,1}],?MODULE:put(test,1,[]))},
     {"prop_replace",?_assertEqual([{test,1}],?MODULE:put(test,1,[{test,2}]))},
     {"map_empty",?_assertEqual(#{test => 1},?MODULE:put(test,1,#{}))},
     {"map_replace",?_assertEqual(#{test => 1},?MODULE:put(test,1,#{test => 2}))},
     {"json_empty",?_assertEqual(<<"{\"test\":1}">>,?MODULE:put(<<"test">>,1,<<"{}">>))},
     {"json_replace",?_assertEqual(<<"{\"test\":1}">>,?MODULE:put(<<"test">>,1,<<"{\"test\":2}">>))}].

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

filter_test_() -> [].

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
