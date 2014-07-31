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
         is_disjoint/2 %% Check if data objects have no keys in common
         %match/2        %% Check if a data object matches a match object
         ]).


%% Basic Configuration
%% ===================================================================
%
-define(ALLOW_MAPS,true). % Allow maps module to be used (requires Erlang >= 17.0)

-define(DECODE_TO_MAPS,true). % Decode raw json to a map

-define(DEFAULT_TYPE, proplist). % proplist | map | json

-define(JSON_POWER_MODE,false). % Use binary parsing, when implemented, to handle raw binary data

% Testing available when maps are enabled.
%-define(TEST,true). % testing, to be removed and managed by rebar

-define(NO_JSON_LIB,throw(no_json_library_found)).

-define(NOT_SUPPORTED,throw(not_yet_supported)).

-define(BAD_VERSION,throw(json_imem_disabled)).

%-define(TEMP_ALLOW_MATCH,true).


%% Json Decoding & Encoding Library Configuration
%% ===================================================================
-ifdef(DECODE_TO_MAPS).
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
                        (__JS) when is_binary(__JS) -> __JS;
                        (__JS) -> jsx:encode(__JS) end},
                    {jiffy,
                     fun(<<"{}">>) -> [];
                        (__JS) -> {O} = jiffy:decode(__JS), O end,
                     fun(__JS) when is_list(__JS) -> jiffy:encode({__JS});
                        (__JS) when is_binary(__JS) -> __JS;
                        (__JS) -> jiffy:encode(__JS) end}
                   ]).
-endif.

%% Technical macros
%% ===================================================================
-define(TO_JSON,json_binary_encode).
-define(FROM_JSON,json_binary_decode).
-define(CL,clean_null_values).

%% Type definitions
%% ===================================================================
-ifdef(ALLOW_MAPS).
-type data_object() :: list() | map() | binary().
-type diff_object() :: list() | map() | binary().
-else.
-type data_object() :: list() | binary().
-type diff_object() :: list() | binary().
-endif.

-type value() :: term().
-type key() :: binary().

-ifdef(TEMP_ALLOW_MATCH).
-type match_object() :: list() | map() | binary().
-type match_pos() :: str_start | bin_start | str_end | bin_end.
-type match_expression() :: {match_pos(),match_pos(),binary()}.
-endif.
%% ===================================================================
%% Exported functions
%% ===================================================================
%% @doc Find function, returns {ok,Value} or error
-spec find(key(),data_object()) -> {ok, value()} | error.
-ifdef(ALLOW_MAPS).
find(Key,DataObject) when is_list(DataObject) ->
    case proplists:get_value(Key,DataObject,undefined) of
        undefined -> error;
        Value -> {ok,Value}
    end;
find(Key,DataObject) when is_map(DataObject) ->
    maps:find(Key,DataObject);
find(Key,DataObject) ->
    find(Key,?FROM_JSON(DataObject)).
-else.
find(Key,DataObject) when is_list(DataObject) ->
    case proplists:get_value(Key,DataObject,undefined) of
        undefined -> error;
        Value -> {ok,Value}
    end;
find(Key,DataObject) ->
    find(Key,?FROM_JSON(DataObject)).
-endif.


%% @doc Standard fold function, returns accumulator. null values are ignored.
%% Fun = fun((K, V, AccIn) -> AccOut)
-spec fold(fun(), Init, data_object()) -> Acc when Init :: term(), Acc :: term().
-ifdef(ALLOW_MAPS).
fold(Fun,Init,DataObject) when is_list(DataObject) -> 
    lists:foldl(fun({K,V},Acc) -> Fun(K,V,Acc) end,
                Init,
                proplists:unfold(?CL(DataObject)));
fold(Fun,Init,DataObject) when is_map(DataObject) -> 
    maps:fold(Fun,Init,?CL(DataObject));
fold(Fun,Init,DataObject) -> 
    fold(Fun,Init,?FROM_JSON(DataObject)).
-else.
fold(Fun,Init,DataObject) when is_list(DataObject) -> 
    lists:foldl(fun({K,V},Acc) -> Fun(K,V,Acc) end,
                Init,
                proplists:unfold(?CL(DataObject)));
fold(Fun,Init,DataObject) -> 
    fold(Fun,Init,?FROM_JSON(DataObject)).
-endif.


%% @doc Get function, same as get(Key,DataObject,undefined)
-spec get(key(),data_object()) -> value() | undefined.
get(Key,DataObject) ->
    get(Key,DataObject,undefined).


%% @doc Return value corresponding to key. If key not present, returns Default.
-spec get(key(),data_object(),Default) -> value() | Default when Default :: term().
-ifdef(ALLOW_MAPS).
get(Key,DataObject,Default) when is_list(DataObject) ->
    proplists:get_value(Key,DataObject,Default);
get(Key,DataObject,Default) when is_map(DataObject) ->
    case maps:find(Key,DataObject) of
        {ok,Value} -> Value;
        error -> Default
    end;
get(Key,DataObject,Default) ->
    get(Key,?FROM_JSON(DataObject), Default).
-else.
get(Key,DataObject,Default) when is_list(DataObject) ->
    proplists:get_value(Key,DataObject,Default);
get(Key,DataObject,Default) ->
    get(Key,?FROM_JSON(DataObject), Default).
-endif.


%% @doc Check if data object has key
-spec has_key(key(), data_object()) -> boolean().
-ifdef(ALLOW_MAPS).
has_key(Key,DataObject) when is_list(DataObject) ->
    proplists:is_defined(Key,DataObject);
has_key(Key,DataObject) when is_map(DataObject) ->
    maps:is_key(Key,DataObject);
has_key(Key,DataObject) -> 
    has_key(Key,?FROM_JSON(DataObject)).
-else.
has_key(Key,DataObject) when is_list(DataObject) ->
    proplists:is_defined(Key,DataObject);
has_key(Key,DataObject) -> 
    has_key(Key,?FROM_JSON(DataObject)).
-endif.

%% @doc Erlang term ordered list of keys used by data object
-spec keys(data_object()) -> list().
-ifdef(ALLOW_MAPS).
keys(DataObject) when is_list(DataObject) -> 
    lists:sort(proplists:get_keys(DataObject));
keys(DataObject) when is_map(DataObject) -> 
    lists:sort(maps:keys(DataObject));
keys(DataObject) -> 
    keys(?FROM_JSON(DataObject)).
-else.
keys(DataObject) when is_list(DataObject) -> 
    lists:sort(proplists:get_keys(DataObject));
keys(DataObject) -> 
    keys(?FROM_JSON(DataObject)).
-endif.


%% @doc Produces a new data object by calling the function fun F(K, V1) 
%% for every K to value V1 association in data object in arbitrary order. 
%% The function fun F/2 must return the value V2 to be associated with key K 
%% for the new data object. 
%% null values are ignored and not carried over to new data object.
-spec map(fun(), data_object()) -> data_object().
-ifdef(ALLOW_MAPS).
map(Fun,DataObject) when is_list(DataObject) ->
    lists:map(fun({K,V}) -> {K,Fun(K,V)} end,
              proplists:unfold(DataObject));
map(Fun,DataObject) when is_map(DataObject) ->
    maps:map(Fun,?CL(DataObject));
map(Fun,DataObject) ->
    ?TO_JSON(map(Fun,?FROM_JSON(DataObject))).
-else.
map(Fun,DataObject) when is_list(DataObject) ->
    lists:map(fun({K,V}) -> {K,Fun(K,V)} end,
              proplists:unfold(DataObject));
map(Fun,DataObject) ->
    ?TO_JSON(map(Fun,?FROM_JSON(DataObject))).
-endif.


%% @doc Create new imen json data object using default type
-spec new() -> data_object().
new() -> new(?DEFAULT_TYPE).

%% @doc Create new imen json data object
-spec new(Type) -> data_object() when Type :: json | proplist | map.
-ifdef(ALLOW_MAPS).
new(proplist) -> [];
new(map) -> #{};
new(json) -> <<"{}">>.
-else.
new(proplist) -> [];
new(json) -> <<"{}">>.
-endif.



%% @doc Add a value to a IMEM Json data object
%% Should Key already exist, old value will be replaced
-spec put(key(), value(), data_object()) -> data_object().
-ifdef(ALLOW_MAPS).
put(Key,Value,DataObject) when is_list(DataObject) ->
    [{Key,Value}| proplists:delete(Key,DataObject)];
put(Key,Value,DataObject) when is_map(DataObject) ->
    maps:put(Key,Value,DataObject);
put(Key,Value,DataObject) ->
    ?TO_JSON(?MODULE:put(Key,Value,?FROM_JSON(DataObject))).
-else.
put(Key,Value,DataObject) when is_list(DataObject) ->
    [{Key,Value}| proplists:delete(Key,DataObject)];
put(Key,Value,DataObject) ->
    ?TO_JSON(?MODULE:put(Key,Value,?FROM_JSON(DataObject))).
-endif.



%% @doc Remove a key from a data object
-spec remove(key(), data_object()) -> data_object().
-ifdef(ALLOW_MAPS).
remove(Key,DataObject) when is_list(DataObject) ->
    proplists:delete(Key,DataObject);
remove(Key,DataObject) when is_map(DataObject) ->
    maps:remove(Key,DataObject);
remove(Key,DataObject) ->
    ?TO_JSON(remove(Key,?FROM_JSON(DataObject))).
    
-else.
remove(Key,DataObject) when is_list(DataObject) ->
    proplists:delete(Key,DataObject);
remove(Key,DataObject) ->
    ?TO_JSON(remove(Key,?FROM_JSON(DataObject))).
    
-endif.


%% @doc Size of a data object, ignoring null values
-spec size(data_object()) -> integer().
-ifdef(ALLOW_MAPS).
size(DataObject) when is_list(DataObject) -> 
    length(?CL(DataObject));
size(DataObject) when is_map(DataObject) -> 
    maps:size(?CL(DataObject));
size(DataObject) -> 
 ?MODULE:size(?FROM_JSON(DataObject)).
-else.
size(DataObject) when is_list(DataObject) -> 
    length(?CL(DataObject));
size(DataObject) -> 
 ?MODULE:size(?FROM_JSON(DataObject)).
-endif.



%% @doc Update a key with a new value. If key is not present, 
%% error:badarg exception is raised.
-spec update(key(),value(),data_object()) -> data_object().
-ifdef(ALLOW_MAPS).
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
-else.
update(Key,Value,DataObject) when is_list(DataObject) ->
    case proplists:is_defined(Key,DataObject) of
        true -> [{Key,Value} | proplists:delete(Key,DataObject)];
        false -> erlang:error(badarg)
    end;
update(Key,Value,DataObject) ->
    ?TO_JSON(update(Key,Value,?FROM_JSON(DataObject))).
-endif.



%% @doc Returns a complete list of values, in arbitrary order, from data object
-spec values(data_object()) -> list().
-ifdef(ALLOW_MAPS).
values(DataObject) when is_list(DataObject) ->
    [V || {_,V} <- DataObject];
values(DataObject) when is_map(DataObject) ->
    maps:values(DataObject);
values(DataObject) -> 
    values(?FROM_JSON(DataObject)).
-else.
values(DataObject) when is_list(DataObject) ->
    [V || {_,V} <- DataObject];
values(DataObject) -> 
    values(?FROM_JSON(DataObject)).
-endif.



%% @doc Convert any format of DataObject to proplist
-spec to_proplist(data_object()) -> list().
-ifdef(ALLOW_MAPS).
to_proplist(DataObject) when is_list(DataObject) ->
    DataObject;
to_proplist(DataObject) when is_map(DataObject) ->
    maps:to_list(DataObject);
to_proplist(DataObject) ->
    to_proplist(?FROM_JSON(DataObject)).
-else.
to_proplist(DataObject) when is_list(DataObject) ->
    DataObject;
to_proplist(DataObject) ->
    to_proplist(?FROM_JSON(DataObject)).
-endif.



%% @doc Convert any format of DataObject to map
-ifdef(ALLOW_MAPS).
-spec to_map(data_object()) -> map().
to_map(DataObject) when is_list(DataObject) ->
    maps:from_list(DataObject);
to_map(DataObject) when is_map(DataObject) ->
    DataObject;
to_map(DataObject) ->
    to_map(?FROM_JSON(DataObject)).
-else.
to_map(_) -> ?NOT_SUPPORTED.
-endif.



%% @doc Convert any format of DataObject to binary JSON
-spec to_binary(data_object()) -> binary().
to_binary(DataObject) when is_binary(DataObject)-> DataObject;
to_binary(DataObject) -> ?TO_JSON(DataObject).


%% @doc Builds a new data object with key/value pairs from provided data
%% object according if boolean filter function (fun(K,V) -> true | false) 
%% return true.
-spec filter(fun(),data_object()) -> data_object().
-ifdef(ALLOW_MAPS).
filter(Fun,DataObject) when is_list(DataObject) ->
    lists:foldl(fun({K,V},In) ->
                    case Fun(K,V) of
                        true -> [{K,V}|In];
                        false -> In
                    end end,
                [],
                DataObject);
filter(Fun,DataObject) when is_map(DataObject) ->
    maps:fold(fun(K,V,In) ->
                    case Fun(K,V) of
                        true -> maps:put(K,V,In);
                        false -> In
                    end end,
             #{},
             DataObject);
filter(Fun,DataObject) ->
    ?TO_JSON(filter(Fun,?FROM_JSON(DataObject))).
    
    
-else.
filter(Fun,DataObject) when is_list(DataObject) ->
    lists:foldl(fun({K,V},In) ->
                    case Fun(K,V) of
                        true -> [{K,V}|In];
                        false -> In
                    end end,
                [],
                DataObject);
filter(Fun,DataObject) ->
    ?TO_JSON(filter(Fun,?FROM_JSON(DataObject))).
    
    
-endif.


%% @doc Build data object with provided keys and associated values from
%% provided data object
-spec include(Keys,data_object()) -> data_object() when Keys :: [key() | Keys].
-ifdef(ALLOW_MAPS).
include(Keys,DataObject) when is_list(DataObject) -> 
    [{K,proplists:get_value(K,DataObject)} || K <- Keys];
include(Keys,DataObject) when is_map(DataObject) -> 
    lists:foldl(fun(K,MapIn) ->
                    maps:put(K,maps:get(K,DataObject),MapIn) end,
                #{},
                Keys);
include(Keys,DataObject) ->
    %binary_include(Keys,DataObject).
    ?TO_JSON(include(Keys,?FROM_JSON(DataObject))).
-else.
include(Keys,DataObject) when is_list(DataObject) -> 
    [{K,proplists:get_value(K,DataObject)} || K <- Keys];
include(Keys,DataObject) ->
    ?TO_JSON(include(Keys,?FROM_JSON(DataObject))).
-endif.


%% @doc Build data object with data from provided data object, excluding
%% the elements associated with provided keys
-spec exclude(Keys,data_object()) -> data_object() when Keys :: [key() | Keys].
-ifdef(ALLOW_MAPS).
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
-else.
exclude(Keys,DataObject) when is_list(DataObject) -> 
    [{K,V} || {K,V} <- DataObject, lists:member(K,Keys) =:= false];
exclude(Keys,DataObject) ->
    ?TO_JSON(exclude(Keys,?FROM_JSON(DataObject))).
-endif.


%% @doc Merges two data objects into a data object. 
%% If two keys exists in both data objects  the value in the first data object
%% will be superseded by the value in the second data object, unless the value 
%% of the second data object is null.
-spec merge(data_object(),data_object()) -> data_object().
-ifdef(ALLOW_MAPS).
merge(DataObject1,DataObject2) when is_list(DataObject1), is_list(DataObject2) ->
    CleanObject2 = ?CL(DataObject2),
    Object2Keys = proplists:get_keys(CleanObject2),
    CleanObject2 ++ [{K,V} || {K,V} <- DataObject1, lists:member(K,Object2Keys) =:= false];
merge(DataObject1,DataObject2) when is_map(DataObject1), is_map(DataObject2) ->
    maps:merge(DataObject1,?CL(DataObject2));
merge(DataObject1,DataObject2) ->
    ?TO_JSON(merge(?FROM_JSON(DataObject1),?FROM_JSON(DataObject2))).
-else.
merge(DataObject1,DataObject2) when is_list(DataObject1), is_list(DataObject2) ->
    CleanObject2 = ?CL(DataObject2),
    Object2Keys = proplists:get_keys(CleanObject2),
    CleanObject2 ++ [{K,V} || {K,V} <- DataObject1, lists:member(K,Object2Keys) =:= false];
merge(DataObject1,DataObject2) ->
    ?TO_JSON(merge(?FROM_JSON(DataObject1),?FROM_JSON(DataObject2))).
-endif.


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
-ifdef(ALLOW_MAPS).
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
-else.
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
diff(DataObject1, DataObject2) ->
    ?TO_JSON(diff(?FROM_JSON(DataObject1),?FROM_JSON(DataObject2))).
-endif.

%% @doc Compare data objects for equality (null values are ignored)
-spec equal(data_object(), data_object()) -> boolean.
-ifdef(ALLOW_MAPS).
equal(DataObject1, DataObject2) when is_list(DataObject1), is_list(DataObject2) -> 
    lists:sort(?CL(DataObject1)) =:= lists:sort(?CL(DataObject2));
equal(DataObject1, DataObject2) when is_map(DataObject1), is_map(DataObject2) ->
    ?CL(DataObject1) =:= ?CL(DataObject2);
equal(DataObject1, DataObject2) ->
    equal(?FROM_JSON(DataObject1),?FROM_JSON(DataObject2)).
    
-else.
equal(DataObject1, DataObject2) when is_list(DataObject1), is_list(DataObject2) -> 
    lists:sort(?CL(DataObject1)) =:= lists:sort(?CL(DataObject2));
equal(DataObject1, DataObject2) ->
    equal(?FROM_JSON(DataObject1),?FROM_JSON(DataObject2)).
    
-endif.


%% @doc Returns true when every element of data_object1 is also a member of data_object2, otherwise false.
-spec is_subset(data_object(), data_object()) -> boolean().
-ifdef(ALLOW_MAPS).
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
    
-else.
is_subset(DataObject1, DataObject2) when is_list(DataObject1), is_list(DataObject2) ->
    Is_Member = fun ({_,null}) -> true;
                    ({K1,V1}) -> V1 =:= proplists:get_value(K1,DataObject2) end,
    lists:all(Is_Member,DataObject1);
is_subset(DataObject1, DataObject2) ->
    is_subset(?FROM_JSON(DataObject1), ?FROM_JSON(DataObject2)).
-endif.


%% @doc Returns true when no element of data_object1 is a member of data_object2, otherwise false.
-spec is_disjoint(data_object(), data_object()) -> boolean().
-ifdef(ALLOW_MAPS).
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
-else.
is_disjoint(DataObject1, DataObject2) when is_list(DataObject1), is_list(DataObject2) ->
    Is_Member = fun({K1,V1}) ->
                    V1 =:= proplists:get_value(K1,DataObject2)
                    end,
    not lists:any(Is_Member,DataObject1);
is_disjoint(DataObject1, DataObject2) ->
    is_disjoint(?FROM_JSON(DataObject1), ?FROM_JSON(DataObject2)).
-endif.

-ifdef(TEMP_ALLOW_MATCH).
%% @doc Returns true if a data object matches a match object.
%% A match object is a key value pair, in the same format as the
%% data object, who can have wildcards in the value fields in order
%% to match values. Fields not present in match object are ignored.
-spec match(match_object(), data_object()) -> boolean().
match(MatchObject,DataObject) when is_list(DataObject) ->
    DataObjectProj = include(keys(MatchObject),DataObject),
    lists:all(fun({K,MatchV}) ->
                binary_match(proplists:get_value(K,DataObjectProj),MatchV)
                end,
              MatchObject).
%TODO: Implement for maps and binary json, and test it
-endif.


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
-ifdef(ALLOW_MAPS).
clean_null_values(DataObject) when is_list(DataObject) ->
    [{K,V} || {K,V} <- DataObject, V =/= null];
clean_null_values(DataObject) when is_map(DataObject) ->
    maps:fold(fun(_,null,OutMap) -> OutMap;
                 (K,V,OutMap) -> maps:put(K,V,OutMap) end,
              #{},
              DataObject);
clean_null_values(DataObject) ->
    ?TO_JSON(clean_null_values(?FROM_JSON(DataObject))).
-else.
clean_null_values(DataObject) when is_list(DataObject) ->
    [{K,V} || {K,V} <- DataObject, V =/= null];
clean_null_values(DataObject) ->
    ?TO_JSON(clean_null_values(?FROM_JSON(DataObject))).
-endif.


-ifdef(TEMP_ALLOW_MATCH).
%% @doc Returns true if provided binary matches binary match expression
-spec binary_match(binary(),match_expression() | binary()) -> boolean().
binary_match(Binary,{_,_,Binary}) -> true;
binary_match(_,{bin_start,bin_start,<<"">>}) -> true;
binary_match(<<"">>,{bin_start,bin_end,<<"">>}) -> true;
binary_match(_,{bin_start,bin_end,<<"">>}) -> false;
binary_match(Bin,{bin_start,str_end,Match}) ->
    WalkFun = fun(M,_,M) -> true;
                 (<<_,R/binary>>,F,M) -> F(R,F,M);
                 (_,_,_) -> false end,
    WalkFun(Bin,WalkFun,Match);
binary_match(Bin,{str_start,bin_end,Match}) ->
    Size = byte_size(Match),
    case Bin of
        <<Match:Size/bytes,_/binary>> -> true;
       _ -> false
    end;
binary_match(Bin,{bin_start,bin_end,Match}) ->
    Size = byte_size(Match),
    WalkFun = fun (<<M:Size/bytes,_/binary>>,_,M) -> true;
                 (<<_,R/binary>>,F,M) -> F(R,F,M);
                 (_,_,_) -> false end,
    WalkFun(Bin,WalkFun,Match);
binary_match(_,{_,_,_}) -> false;
binary_match(Binary,MatchBin) -> binary_match(Binary,parse_match(MatchBin)).


%% @doc Parse a binary into a imem_json binary match expression
-spec parse_match(binary()) -> match_expression().
parse_match(Bin) -> parse_match(Bin,{undefined,undefined,undefined}).

-spec parse_match(binary(),match_expression()) -> match_expression().
parse_match(<<"">>,{undefined,undefined,undefined}) -> {bin_start,bin_end,<<"">>};
parse_match(<<"*">>,{undefined,undefined,undefined}) -> {bin_start,bin_start,<<"">>};
parse_match(<<"*",Rem/binary>>,{undefined,undefined,_}) ->
    parse_match(Rem,{bin_start,undefined,<<"">>});
parse_match(<<Rem/binary>>,{undefined,undefined,_}) ->
    parse_match(Rem,{str_start,undefined,<<"">>});
parse_match(<<>>,{Begin,undefined,Acc}) ->
    {Begin,str_end,Acc};
parse_match(<<"*">>,{Begin,undefined,Acc}) ->
    {Begin,bin_end,Acc};
parse_match(<<H,Rem/binary>>,{Begin,undefined,Acc}) ->
    parse_match(Rem,{Begin,undefined,<<Acc/binary,H>>}).
-endif.
    

%% ===================================================================
%% JSON Binary Handling Power Functions
%% ===================================================================
%
%binary_include(Keys,DataObject) ->
%    binary_include_walking(walking,DataObject,Keys,<<"">>,{undefined,undefined}).
%    
%    binary_include_walking(walking,_,[],<<"">>,_) -> <<"">>;
%    binary_include_walking(walking,_,[],GroupAcc,_) -> <<GroupAcc/binary,"}">>;
%    binary_include_walking(walking,<<"">>,_,<<"">>,_) -> <<"">>;
%    binary_include_walking(walking,<<"">>,_,GroupAcc,_) -> <<GroupAcc/binary,"}">>;
%
%    binary_include_walking(walking,<<"{\"",R/binary>>,Keys,<<"">>,_) ->
%        binary_include_walking(reading_key,R,Keys,<<"">>,{<<"">>,<<"">>});
%       
%    binary_include_walking(walking,<<",\"",R/binary>>,Keys,GroupAcc,_) ->
%        binary_include_walking(reading_key,R,Keys,GroupAcc,{<<"">>,<<"">>});
%    binary_include_walking(walking,<<_,R/binary>>,Keys,GroupAcc,TAcc) ->
%        binary_include_walking(walking,R,Keys,GroupAcc,TAcc);
%
%    binary_include_walking(reading_key,<<"\":",R/binary>>,Keys,GroupAcc,{KeyAcc,_}) ->
%        case lists:member(KeyAcc,Keys) of
%            true -> binary_include_walking(reading_value,R,lists:delete(KeyAcc,Keys),GroupAcc,{KeyAcc,<<"">>});
%            false -> binary_include_walking(walking,R,Keys,GroupAcc,{undefined,undefined}) end;
%
%    binary_include_walking(reading_key,<<H,R/binary>>,Keys,GroupAcc,{KeyAcc,_}) ->
%        binary_include_walking(reading_key,R,Keys,GroupAcc,{<<KeyAcc/binary,H>>,undefined});
%
%    binary_include_walking(reading_value,<<"\"",R/binary>>,Keys,GroupAcc,{KeyAcc,<<"">>}) ->
%        binary_include_walking(reading_value,R,Keys,GroupAcc,{KeyAcc,<<"">>});
%    binary_include_walking(reading_value,<<"\"",R/binary>>,Keys,<<"">>,{KeyAcc,ValueAcc}) ->
%        binary_include_walking(walking,R,Keys,<<"{\"",KeyAcc/binary,"\":\"",ValueAcc/binary,"\"">>,{undefined,undefined});
%    binary_include_walking(reading_value,<<"\"",R/binary>>,Keys,GroupAcc,{KeyAcc,ValueAcc}) ->
%        binary_include_walking(walking,R,Keys,<<GroupAcc/binary,",\"",KeyAcc/binary,"\":\"",ValueAcc/binary,"\"">>,{undefined,undefined});
%    binary_include_walking(reading_value,<<"[",R/binary>>,Keys,<<"">>,{KeyAcc,<<"">>}) ->
%        {NewR,ValueAcc} = binary_include_walking_catch_list(R,<<"[">>,1),
%        binary_include_walking(walking,NewR,Keys,<<"{\"",KeyAcc/binary,"\":\"",ValueAcc/binary,"\"">>,{undefined,undefined});
%    binary_include_walking(reading_value,<<"[",R/binary>>,Keys,GroupAcc,{KeyAcc,<<"">>}) ->
%        {NewR,ValueAcc} = binary_include_walking_catch_list(R,<<"[">>,1),
%        binary_include_walking(walking,NewR,Keys,<<GroupAcc/binary,",\"",KeyAcc/binary,"\":\"",ValueAcc/binary,"\"">>,{undefined,undefined});
%    binary_include_walking(reading_value,<<H,R/binary>>,Keys,GroupAcc,{KeyAcc,ValueAcc}) ->
%        binary_include_walking(reading_value,R,Keys,GroupAcc,{KeyAcc,<<ValueAcc/binary,H>>}).
%
%    binary_include_walking_catch_list(<<"]",R/binary>>,Acc,0) -> {R,<<Acc/binary,"]">>};
%    binary_include_walking_catch_list(<<"]",R/binary>>,Acc,Level) ->
%        binary_include_walking_catch_list(R,<<Acc/binary,"]">>,Level-1);
%    binary_include_walking_catch_list(<<"[",R/binary>>,Acc,Level) ->
%        binary_include_walking_catch_list(R,<<Acc/binary,"[">>,Level+1);
%    binary_include_walking_catch_list(<<H,R/binary>>,Acc,Level) ->
%        binary_include_walking_catch_list(R,<<Acc/binary,H>>,Level).
%
%% ===================================================================
%% TESTS
%% ===================================================================

-ifdef(ALLOW_MAPS).
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

keys_test_() -> 
    Keys = lists:sort([<<"surname">>,<<"name">>,<<"foo">>,<<"earthling">>,<<"age">>,<<"empty">>]),
    [{"proplist",?_assertEqual(Keys,keys(?TEST_PROP))},
     {"map",?_assertEqual(Keys,keys(?TEST_MAP))},
     {"json",?_assertEqual(Keys,keys(?TEST_JSON))}].

map_test_() -> 
    Job = fun(<<"age">>,_) -> 8000;
             (_,true) -> false;
             (_,V) -> V end,
    PropOut = map(Job,?TEST_PROP),
    MapOut = map(Job,?TEST_MAP),
    JsonOut = map(Job,?TEST_JSON),
    [{"proplist",?_assertEqual(8000, proplists:get_value(<<"age">>,PropOut))},
     {"proplist",?_assertEqual(false, proplists:get_value(<<"earthling">>,PropOut))},
     {"map",?_assertEqual(8000, maps:get(<<"age">>,MapOut))},
     {"map",?_assertEqual(false,maps:get(<<"earthling">>,MapOut))},
     {"json",?_assertEqual(8000, ?MODULE:get(<<"age">>,JsonOut))},
     {"json",?_assertEqual(false,?MODULE:get(<<"earthling">>,JsonOut))}].

new_test_() -> 
    [{"proplist",?_assertEqual([],new(proplist))},
     {"map",?_assertEqual(#{},new(map))},
     {"json",?_assertEqual(<<"{}">>,new(json))}].

put_test_() -> 
    [{"prop_empty",?_assertEqual([{test,1}],?MODULE:put(test,1,[]))},
     {"prop_replace",?_assertEqual([{test,1}],?MODULE:put(test,1,[{test,2}]))},
     {"map_empty",?_assertEqual(#{test => 1},?MODULE:put(test,1,#{}))},
     {"map_replace",?_assertEqual(#{test => 1},?MODULE:put(test,1,#{test => 2}))},
     {"json_empty",?_assertEqual(<<"{\"test\":1}">>,?MODULE:put(<<"test">>,1,<<"{}">>))},
     {"json_replace",?_assertEqual(<<"{\"test\":1}">>,?MODULE:put(<<"test">>,1,<<"{\"test\":2}">>))}].

remove_test_() ->
    PropOut = remove(<<"age">>,?TEST_PROP),
    MapOut =  remove(<<"age">>,?TEST_MAP),
    JsonOut = remove(<<"age">>,?TEST_JSON),
    [{"proplist",?_assertEqual(undefined, proplists:get_value(<<"age">>,PropOut,undefined))},
     {"map",?_assertEqual(undefined, ?MODULE:get(<<"age">>,MapOut))},
     {"json",?_assertEqual(undefined,?MODULE:get(<<"age">>,JsonOut))}].

size_test_() -> 
    [{"proplist",?_assertEqual(5,?MODULE:size(?TEST_PROP))},
     {"map",?_assertEqual(5,?MODULE:size(?TEST_MAP))},
     {"json",?_assertEqual(5,?MODULE:size(?TEST_JSON))}].

update_test_() ->
    PropOut = update(<<"age">>,8000,?TEST_PROP),
    MapOut =  update(<<"age">>,8000,?TEST_MAP),
    JsonOut = update(<<"age">>,8000,?TEST_JSON),
    [{"proplist",?_assertEqual(8000, proplists:get_value(<<"age">>,PropOut))},
     {"map",?_assertEqual(8000, maps:get(<<"age">>,MapOut))},
     {"json",?_assertEqual(8000,?MODULE:get(<<"age">>,JsonOut))}].

values_test_() ->
    Values = lists:sort([<<"Doe">>,<<"John">>,<<"bar">>,true,981,null]),
    [{"proplist",?_assertEqual(Values,lists:sort(values(?TEST_PROP)))},
     {"map",?_assertEqual(Values,lists:sort(values(?TEST_MAP)))},
     {"json",?_assertEqual(Values,lists:sort(values(?TEST_JSON)))}].

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

filter_test_() -> 
    FilterFun = fun(<<"age">>,_) -> true;
                   (_,true) -> true;
                   (_,_) -> false end,
    PropOut = filter(FilterFun,?TEST_PROP),
    MapOut =  filter(FilterFun,?TEST_MAP),
    JsonOut = filter(FilterFun,?TEST_JSON),
    [{"proplist",?_assertEqual(981, proplists:get_value(<<"age">>,PropOut))},
     {"proplist",?_assertEqual(true, proplists:get_value(<<"earthling">>,PropOut))},
     {"map",?_assertEqual(981, maps:get(<<"age">>,MapOut))},
     {"map",?_assertEqual(true,maps:get(<<"earthling">>,MapOut))},
     {"json",?_assertEqual(981, ?MODULE:get(<<"age">>,JsonOut))},
     {"json",?_assertEqual(true,?MODULE:get(<<"earthling">>,JsonOut))}].

include_test_() ->
    Json_Ok = <<"{\"name\":\"John\"}">>,
    Prop_Ok = [{<<"surname">>,<<"Doe">>},{<<"foo">>,<<"bar">>}],
    Map_Ok = #{<<"earthling">> => true, <<"age">> => 981},
    [{"proplist",?_assertEqual(Prop_Ok, include([<<"surname">>,<<"foo">>],?TEST_PROP))},
     {"map",?_assertEqual(Map_Ok, include([<<"earthling">>,<<"age">>],?TEST_MAP))},
     {"json",?_assertEqual(Json_Ok, include([<<"name">>],?TEST_JSON))}].

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

-ifdef(TEMP_ALLOW_MATCH).
match_test_() ->
    PropMatch = [{<<"name">>,<<"Jo*">>},{<<"foo">>,<<"*r">>}],
    PropNotMatch = [{<<"name">>,<<"*Jo">>},{<<"foo">>,<<"*r">>}],
    [{"prop_success",?_assert(match(PropMatch,?TEST_PROP))},
     {"prop_failure",?_assert(not match(PropNotMatch,?TEST_PROP))}
    ].

parse_match_test_() ->
    [?_assertEqual({bin_start,bin_end,<<"bou">>},parse_match(<<"*bou*">>)),
     ?_assertEqual({str_start,str_end,<<"bou">>},parse_match(<<"bou">>)),
     ?_assertEqual({str_start,bin_end,<<"bou">>},parse_match(<<"bou*">>)),
     ?_assertEqual({bin_start,str_end,<<"bou">>},parse_match(<<"*bou">>)),
     ?_assertEqual({bin_start,bin_end,<<"">>},parse_match(<<"">>)),
     ?_assertEqual({bin_start,bin_start,<<"">>},parse_match(<<"*">>))].

binary_match_test_() ->
     % "bou"
    [?_assert(binary_match(<<"bou">>,{str_start,str_end,<<"bou">>})),
     ?_assert(not binary_match(<<"boue">>,{str_start,str_end,<<"bou">>})),
     ?_assert(not binary_match(<<"zbou">>,{str_start,str_end,<<"bou">>})),
     % "*bou"
     ?_assert(binary_match(<<"bou">>,{bin_start,str_end,<<"bou">>})),
     ?_assert(binary_match(<<"zeoubou">>,{bin_start,str_end,<<"bou">>})),
     ?_assert(not binary_match(<<"zeouboue">>,{bin_start,str_end,<<"bou">>})),
     ?_assert(not binary_match(<<"boueziu">>,{bin_start,str_end,<<"bou">>})),
     % "bou*"
     ?_assert(binary_match(<<"bou">>,{str_start,bin_end,<<"bou">>})),
     ?_assert(binary_match(<<"bouzoub">>,{str_start,bin_end,<<"bou">>})),
     ?_assert(not binary_match(<<"zbouezoub">>,{str_start,bin_end,<<"bou">>})),
     ?_assert(not binary_match(<<"zbou">>,{str_start,bin_end,<<"bou">>})),
     % "*bou*"
     ?_assert(binary_match(<<"bou">>,{bin_start,bin_end,<<"bou">>})),
     ?_assert(binary_match(<<"zoubou">>,{bin_start,bin_end,<<"bou">>})),
     ?_assert(binary_match(<<"bouzou">>,{bin_start,bin_end,<<"bou">>})),
     ?_assert(binary_match(<<"zoubouzou">>,{bin_start,bin_end,<<"bou">>})),
     % "*"
     ?_assert(binary_match(<<"">>,{bin_start,bin_start,<<"">>})),
     ?_assert(binary_match(<<"zoubou">>,{bin_start,bin_start,<<"">>})),
     % ""
     ?_assert(binary_match(<<"">>,{bin_start,bin_end,<<"">>})),
     ?_assert(not binary_match(<<"zoubou">>,{bin_start,bin_end,<<"">>}))
     ].
-endif.

-endif.

-endif.
