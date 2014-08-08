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
         to_proplist/1, to_proplist/2,
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
         is_disjoint/2, %% Check if data objects have no keys in common
         project/2,     %% Create a projection of a json document based on paths
         match/2,       %% Check if a data object matches a json path (using jpparse)
         jpp_match/2    %% Match a jpparse syntax tree to a json document and extract the values
         ]).

%TODO: custom output format:
%       provide way to specify desired output format by:
%           - adding a parameter to each function (much work)
%           - or by adding a single function wich calls
%             the function and converts to desired output (not much work)


%% Basic Configuration
%% ===================================================================

%░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░
%▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒
%  
%  VERY IMPORTANT SETTING: make sure it is commented on every push !!!
%¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯  
%-define(ALLOW_MAPS,true). % Allow maps module to be used (Erlang >= 17.0)
%
%▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒
%░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░
%  (dramatization can be removed once every environment is 17.0 or above

%-define(DECODE_TO_MAPS,true). % Decode raw json to a map

-define(DEFAULT_TYPE, proplist). % proplist | map | json

-define(JSON_PATH_SEPARATOR,":").

-define(JSON_POWER_MODE,false). % Use binary parsing, when implemented, to handle raw binary data

-define(NO_JSON_LIB,throw(no_json_library_found)).

-define(NOT_SUPPORTED,throw(not_yet_supported)).

-define(BAD_VERSION,throw(json_imem_disabled)).



%% Json Decoding & Encoding Library Configuration
%% ===================================================================
%% Format:  [{Module, DecodeFun, EncodeFun} | _ ]
-ifdef(ALLOW_MAPS).
-ifdef(DECODE_TO_MAPS).
-define(JSON_LIBS, [{jsx, 
                     fun(<<"{}">>) -> #{};
                        (__JS) -> case jsx:decode(__JS) of
                                    [{_,_}|_]=__O -> maps:from_list(__O);
                                    __L when is_list(__L) -> __L end end,
                     fun(__JS) when is_list(__JS) -> jsx:encode(__JS);
                        (__JS) when is_map(__JS) -> jsx:encode(to_proplist(__JS,deep)) end},
                    {jiffy,
                     fun(<<"{}">>) -> #{};
                        (__JS) -> jiffy:decode(__JS,[return_maps]) end,
                     fun(__JS) when is_list(__JS) -> jiffy:encode({__JS});
                        (__JS) when is_map(__JS) -> jiffy:encode(__JS) end}
                   ]).
-else.
-define(JSON_LIBS, [{jsx, 
                     fun(<<"{}">>) -> [];
                        (__JS) -> jsx:decode(__JS) end,
                     fun(__JS) when is_list(__JS) -> jsx:encode(__JS);
                        (__JS) when is_binary(__JS) -> __JS;
                        (__JS) when is_map(__JS) -> jsx:encode(to_proplist(__JS,deep)) end},
                    {jiffy,
                     fun(<<"{}">>) -> [];
                        (__JS) -> {O} = jiffy:decode(__JS), O end,
                     fun(__JS) when is_list(__JS) -> jiffy:encode({__JS});
                        (__JS) when is_binary(__JS) -> __JS;
                        (__JS) when is_map(__JS) -> jiffy:encode(__JS) end}
                   ]).
-endif.
-else.
-define(JSON_LIBS, [{jsx, 
                     fun(<<"{}">>) -> [];
                        (__JS) -> jsx:decode(__JS) end,
                     fun(__JS) when is_binary(__JS) -> __JS;
                        (__JS) -> jsx:encode(__JS) end},
                    {jiffy,
                     fun(<<"{}">>) -> [];
                        (__JS) -> {O} = jiffy:decode(__JS), O end,
                     fun(__JS) when is_binary(__JS) -> __JS;
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

to_proplist(DataObject,deep) when is_list(DataObject)-> DataObject;
to_proplist(DataObject,deep) when is_map(DataObject) ->
    maps:to_list(
        maps:map(fun(_K,_V) when is_map(_V) ->
                    to_proplist(_V,deep);
                (_K,_V) -> _V end,
             DataObject));
to_proplist(DataObject,deep) when is_binary(DataObject) ->
    to_proplist(?FROM_JSON(DataObject)).


-else.
to_proplist(DataObject) when is_list(DataObject) ->
    DataObject;
to_proplist(DataObject) ->
    to_proplist(?FROM_JSON(DataObject)).

to_proplist(DataObject,deep) when is_list(DataObject)-> DataObject;
to_proplist(DataObject,deep) when is_binary(DataObject) ->
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
    maps:fold(fun(K,V,MapIn) ->
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
    Object1Keys = proplists:get_keys(DataObject1),
    Object2Keys = proplists:get_keys(DataObject2),
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
    %diff(?FROM_JSON(DataObject1),?FROM_JSON(DataObject2)).

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
-spec equal(data_object(), data_object()) -> boolean().
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

%% @doc Projection of a path, or of multiple paths, from a Json nested object.
%% returns nomatch if path is not found, or if one of the paths is not found.
%%
%% Because projection used imem_json primitives, function is format-neutral.
-spec project(Path,data_object()) -> value() | ListOfValues | nomatch 
           when Path :: list() | binary(),
                ListOfValues :: [value() | ListOfValues].
project([T|_] = Paths,DataObject) when is_list(T) ->
    All = [walk_path(parse_path(Path),DataObject) || Path <- Paths],
    case lists:any(fun(nomatch) -> true; (_) -> false end,All) of
        true -> nomatch;
        false -> All
    end;
project(Path,DataObject) ->
    walk_path(parse_path(Path),DataObject).

%% @doc Tokenizes a path in a series of tokens
-spec parse_path(Path) -> list()
            when Path :: list() | binary().
parse_path(Path)  ->
    parse_path(Path,?JSON_PATH_SEPARATOR).

-spec parse_path(Path,list()) -> list()
            when Path :: list() | binary().
parse_path(Path,Separator) when is_list(Path) ->
    string:tokens(Path,Separator);
parse_path(Path,Separator) when is_binary(Path) ->
    parse_path(binary_to_list(Path),Separator).

%% @doc Walks a tokenized path to extract the corresponding values
-spec walk_path(list(),data_object()) -> value() | ListOfValues | nomatch
            when ListOfValues :: [value() | ListOfValues].
walk_path([],LastLevel) -> LastLevel;
walk_path(["$keys$"],CurrentLevel) ->
    keys(CurrentLevel);
walk_path([Filter|Tail],CurrentLevel) ->
    {ok,Mask} = re:compile("^(.*)\\[([^\\]]+)\\]$"),
    case re:run(Filter,Mask,[{capture,[2],list}]) of
        nomatch -> NextLevel = ?MODULE:get(list_to_binary(Filter),CurrentLevel),
                   case ?MODULE:get(list_to_binary(Filter),CurrentLevel) of
                    undefined -> nomatch;
                    NextLevel -> walk_path(Tail,NextLevel)
                   end;
        {match,["*"]} -> {match,[Field]} = re:run(Filter,Mask,[{capture,[1],binary}]),
                         case ?MODULE:get(Field,CurrentLevel) of
                            undefined -> nomatch;
                            [] -> nomatch;
                            NextLevels -> lists:flatten([walk_path(Tail,NextLevel) || 
                                            NextLevel <- NextLevels])
                         end;
        {match,[_lstIndex]} -> {match,[Field]} = re:run(Filter,Mask,[{capture,[1],binary}]),
                               Index = list_to_integer(_lstIndex),
                               case ?MODULE:get(Field,CurrentLevel) of
                                undefined -> nomatch;
                                [] -> nomatch;
                                List -> Length = length(List),
                                        if Index > Length -> nomatch;
                                           true -> NextLevel = lists:nth(Index,List),
                                                   walk_path(Tail,NextLevel)
                                        end
                               end
    end.
               
%% @doc Projects a binary string json path (as understood by jpparse) on a json object
-spec match(binary(),data_object()) -> value() | ListOfValues | nomatch
    when ListOfValues :: [value() | ListOfValues].
match(JsonPath,DataObject) ->
    {ok,{ParseTree,_}} = jpparse:parsetree(JsonPath),
    jpp_match(ParseTree,DataObject).
    
%% @doc Projects a parsed Json Path (from jpparse) on a json object
%% and extracts matched values. nomatch is return if no match is found.
%% This function uses imem_json primitives, and as such is format neutral.
-spec jpp_match(JPPTree,data_object()) -> value() | ListOfValues | nomatch
    when JPPTree :: tuple(),
         ListOfValues :: [value() | ListOfValues].

-define(OO(_X), {'{}',_X}).
-define(LL(_X), {'[]',_X}).
-define(FF(_X), {'$',_X}).
-define(JV, jpp_value).

jpp_match(Tree,DataObject) when is_binary(DataObject) ->
    jpp_match(Tree,?FROM_JSON(DataObject));
jpp_match(Tree,DataObject) ->
    Out = jpp_walk(Tree,DataObject),
    if is_list(Out) -> Flat = lists:flatten(Out),
                       case lists:any(fun(nomatch) -> true;
                                     (_) -> false end,Flat) of
                           true -> nomatch;
                           false -> Flat
                       end;
       true -> Out
    end.

    %% jpp_walk transposes jpparse constructs to simple unary primitives {'{}',Field} and {'[]',Index}.
    %% Whenever a list of fields or indexes is encountered, it just does a mapping, and each field/index
    %% generates it's own match, witch are then grouped together. Code repetition is avoided, and kept terse.
    %%
    %% Here is an example of the jpparse and its ultimate "primitivized" form:
    %%
    %% form {}[]{b}: [{'{}',{'[]',{'{}','_',[]},[]},[<<"b">>]}]
    %%  -> [{'{}',{'[]',{'{}','_',[]},[]}}, {'{}',[<<"b">>}]]
    %%  -> [{'{}',{'{}','_',[]}}, {'[]',[]} | {'{}',[<<"b">>]}]
    %%  -> [{'{}','_'}, {'{}',[]'} | {'[]',[]}, {'{}',[<<"b">>]}]
    %%  -> [{'$',<<"values">>}, {'[]',[]}, {'{}', [<<"b">>]}]

    %% Higher level parsing
    %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
    jpp_walk({':',Elements},CLevel) ->
        jpp_walk(Elements,CLevel);

    jpp_walk([],CLevel) -> CLevel;

    %% Object Walking
    jpp_walk([Field|Tail],CLevel) when is_binary(Field) -> % <<"field">> => {'{}',<<"field">>}
        jpp_walk([?OO(Field)| Tail], CLevel);
    jpp_walk([{'{}','_',Fields}|Tail],CLevel) when is_list(Fields) -> % {'{}','_',Fields}
        jpp_walk([?OO(Fields)|Tail],CLevel);
    jpp_walk([{'{}',Origin,Field}|Tail],CLevel) -> % {'{}',O,F} => [{'{}',O}, {'{}',F}]
        jpp_walk([?OO(Origin),?OO(Field) | Tail],CLevel);
    jpp_walk([?OO([])|Tail],CLevel) -> % {'{}',[]} => {'$',<<"values">>}
        jpp_walk([?FF(<<"values">>)|Tail], CLevel);
    jpp_walk([?OO(Fields)|Tail],CLevel) when is_list(Fields) -> % {'{}',Fields}
        [jpp_walk([?OO(Field) | Tail],CLevel) || Field <- Fields];
    jpp_walk([?OO('_')|Tail],CLevel) -> % {'{}','_'}
        jpp_walk(Tail, CLevel);
    jpp_walk([?OO(?FF(Any))|Tail],CLevel) ->  % {'{}',{'$',any}} => {'$',any}
        jpp_walk([?FF(Any)|Tail], CLevel);
    jpp_walk([?OO({'-',Start,End})|Tail],CLevel) -> % {'{}',{'-',S,E}}
        Keys = lists:foldl(fun(K,Acc) ->
                            if K >= Start, K =<  End -> [K|Acc];
                            true -> Acc end end,
                           [], ?MODULE:keys(CLevel)),
        jpp_walk([?OO(lists:reverse(Keys))| Tail], CLevel);
    jpp_walk([?OO({'[]',F,I})|Tail],CLevel) -> %{'{}',{'[]',F,I}}
        jpp_walk([?OO(F),?LL(I)|Tail],CLevel);
    jpp_walk([?OO({'{}',O,F})|Tail],CLevel) -> %{'{}',{'{}',F,I}}
        jpp_walk([?OO(O),?OO(F)|Tail], CLevel);

    %% Object "Function-as-a-property"
    jpp_walk([?FF(<<"values">>)|Tail],CLevel) ->
        [jpp_walk(Tail,NLevel) || NLevel <- ?MODULE:values(CLevel)];
    jpp_walk([?FF(<<"firstChild">>)|Tail],CLevel) ->
        [NLevel|_] = ?MODULE:values(CLevel),
        jpp_walk(Tail,NLevel);
    jpp_walk([?FF(<<"keys">>)|Tail],CLevel) ->
        jpp_walk(Tail,?MODULE:keys(CLevel));

    %% List Walking
    jpp_walk([{'[]','_',Index}|Tail],CLevel) -> % {'[]','_',I} => {'[]',I}
        jpp_walk([?LL([Index])|Tail],CLevel);
    jpp_walk([{'[]',Field,Index}|Tail],CLevel) -> % {'[]',F,I} => [{'{}',F}, {'[]',I}]
        jpp_walk([?OO(Field), ?LL(Index)| Tail], CLevel);
    jpp_walk([?LL([])|Tail],CLevel) -> % {'[]',[]} => {'-',1,Max}
        jpp_walk([?LL({'-', 1, ?MODULE:size(CLevel)})| Tail], CLevel);
    jpp_walk([?LL({'-',Start,End})|Tail],CLevel) -> % {'[]',{'-',S,E}}
        Seq = lists:seq(?JV(Start), ?JV(End)),
        jpp_walk([?LL(Seq)| Tail], CLevel);
    jpp_walk([?LL(List)|Tail], CLevel) when is_list(List) -> % {'[]',[Fields]}
        [jpp_walk([?LL(Index)| Tail], CLevel) || Index <- List];

    %% Lower level matching (and actually doing something)
    %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
    jpp_walk([?LL(RawIndex)|Tail],CLevel) ->
        Index = ?JV(RawIndex),
        Length = ?MODULE:size(CLevel),
        if Index > Length -> nomatch;
           true -> NLevel =  lists:nth(Index,CLevel),
                   jpp_walk(Tail,NLevel)
        end;
    jpp_walk([?OO(RawField)|Tail],CLevel)->
        Field = ?JV(RawField),
        case ?MODULE:get(Field,CLevel) of
            undefined -> nomatch;
            NLevel -> jpp_walk(Tail,NLevel)
        end.

    %% Any function-generated value should be dealt with here
    %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
    jpp_value(Value) when is_binary(Value) -> Value;
    jpp_value(Value) when is_integer(Value) -> Value.
         
        
    

%% ===================================================================
%% Internal functions
%% ===================================================================

-spec json_binary_decode(Json) -> data_object() when Json :: binary().
json_binary_decode(Json) ->
    json_binary_decode(Json,?JSON_LIBS).
json_binary_decode(Json,Libs) ->
    %% Json Lib detection should probably be replaced by an environment variable
    Apps = [App || {App,_,_} <- application:loaded_applications()],
    case [_Fun || {_Lib,_Fun,_} <- Libs, lists:member(_Lib,Apps)] of
        [Decode|_] -> Decode(Json);
        []     -> ?NO_JSON_LIB 
    end.

-spec json_binary_encode(data_object()) -> Json when Json :: binary().
json_binary_encode(Json) ->
    json_binary_encode(Json,?JSON_LIBS).
json_binary_encode(Json,Libs) ->
    %% Json Lib detection should probably be replaced by an environment variable
    Apps = [App || {App,_,_} <- application:loaded_applications()],
    case [_Fun || {_Lib,_,_Fun} <- Libs, lists:member(_Lib,Apps)] of
        [Encode|_] -> Encode(Json);
        []     -> ?NO_JSON_LIB 
    end.

-spec clean_null_values(data_object()) -> data_object().
-ifdef(ALLOW_MAPS).
clean_null_values([{_,_}|_] = DataObject) ->
    [{K,V} || {K,V} <- DataObject, V =/= null];
clean_null_values(DataObject) when is_list(DataObject) ->
    DataObject;
clean_null_values(DataObject) when is_map(DataObject) ->
    maps:fold(fun(_,null,OutMap) -> OutMap;
                 (K,V,OutMap) -> maps:put(K,V,OutMap) end,
              #{},
              DataObject);
clean_null_values(DataObject) ->
    ?TO_JSON(clean_null_values(?FROM_JSON(DataObject))).
-else.
clean_null_values([{_,_}|_] = DataObject) ->
    [{K,V} || {K,V} <- DataObject, V =/= null];
clean_null_values(DataObject) when is_list(DataObject) ->
    DataObject;
clean_null_values(DataObject) ->
    ?TO_JSON(clean_null_values(?FROM_JSON(DataObject))).
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
%% TESTS (ONLY IF MAPS ARE ENABLED)
%% ===================================================================

-ifdef(TEST).

-include_lib("eunit/include/eunit.hrl").

%% Testing available when maps are enabled, testing code not duplicated
%% Latest code coverage check: 100%

setup() ->
    application:start(jpparse),
    application:start(jsx).

-define(TEST_JSON, <<"{\"surname\":\"Doe\",\"name\":\"John\",\"foo\":\"bar\",\"earthling\":true,\"age\":981,\"empty\":null}">>).
-define(TEST_PROP, [{<<"surname">>,<<"Doe">>}, {<<"name">>,<<"John">>}, {<<"foo">>,<<"bar">>}, {<<"earthling">>,true}, {<<"age">>,981},{<<"empty">>,null}]).
-define(TEST_JSON_LIST, <<"[{\"surname\":\"Doe\"},{\"surname\":\"Jane\"},{\"surname\":\"DoeDoe\"}]">>).

-ifdef(ALLOW_MAPS).
-define(TEST_MAP,#{<<"age">> => 981, <<"earthling">> => true, <<"foo">> => <<"bar">>, <<"name">> => <<"John">>, <<"surname">> => <<"Doe">>,<<"empty">> => null}).

%% JSON is tested for each function as well even if, basically, it only tests the 
%% JSON to map/proplist conversion each time. This could at least be used as regression
%% testing should JSON data be handled differently.
    
find_test_() ->
    {setup,
     fun setup/0,
    [{"proplist_success",?_assertEqual({ok,<<"Doe">>}, find(<<"surname">>,?TEST_PROP))},
     {"map_success",?_assertEqual({ok,<<"Doe">>}, find(<<"surname">>,?TEST_MAP))},
     {"json_success",?_assertEqual({ok,<<"Doe">>}, find(<<"surname">>,?TEST_JSON))},
     {"proplist_fail",?_assertEqual(error, find(<<"sme">>,?TEST_PROP))},
     {"map_fail",?_assertEqual(error, find(<<"sme">>,?TEST_MAP))},
     {"json_fail",?_assertEqual(error, find(<<"sme">>,?TEST_JSON))}]}.

fold_test_() ->
    TestFun = fun(_,_,Acc) -> Acc+1 end,
    {setup,
     fun setup/0,
    [{"proplist",?_assertEqual(5, fold(TestFun,0,?TEST_PROP))},
     {"map",?_assertEqual(5, fold(TestFun,0,?TEST_MAP))},
     {"json",?_assertEqual(5, fold(TestFun,0,?TEST_JSON))}]}.

get_test_() ->
    {setup,
     fun setup/0,
    [{"proplist_success",?_assertEqual(<<"Doe">>, ?MODULE:get(<<"surname">>,?TEST_PROP))},
     {"map_success",?_assertEqual(<<"Doe">>, ?MODULE:get(<<"surname">>,?TEST_MAP))},
     {"json_success",?_assertEqual(<<"Doe">>, ?MODULE:get(<<"surname">>,?TEST_JSON))},
     {"proplist_fail",?_assertEqual(undefined, ?MODULE:get(<<"sme">>,?TEST_PROP))},
     {"map_fail",?_assertEqual(undefined, ?MODULE:get(<<"sme">>,?TEST_MAP))},
     {"json_fail",?_assertEqual(undefined, ?MODULE:get(<<"sme">>,?TEST_JSON))}, 
     {"proplist_custom_default",?_assertEqual(test, ?MODULE:get(<<"sme">>,?TEST_PROP,test))},
     {"map_custom_default",?_assertEqual(test, ?MODULE:get(<<"sme">>,?TEST_MAP,test))},
     {"json_custom_default",?_assertEqual(test, ?MODULE:get(<<"sme">>,?TEST_JSON,test))}]}.

has_key_test_() ->
    {setup,
     fun setup/0,
    [{"proplist_success",?_assert(has_key(<<"surname">>,?TEST_PROP))},
     {"map_success",?_assert(has_key(<<"surname">>,?TEST_MAP))},
     {"json_success",?_assert(has_key(<<"surname">>,?TEST_JSON))},
     {"proplist_fail",?_assert(not has_key(<<"sname">>,?TEST_PROP))},
     {"map_fail",?_assert(not has_key(<<"sname">>,?TEST_MAP))},
     {"json_fail",?_assert(not has_key(<<"sname">>,?TEST_JSON))}]}.

keys_test_() -> 
    Keys = lists:sort([<<"surname">>,<<"name">>,<<"foo">>,<<"earthling">>,<<"age">>,<<"empty">>]),
    {setup,
     fun setup/0,
    [{"proplist",?_assertEqual(Keys,keys(?TEST_PROP))},
     {"map",?_assertEqual(Keys,keys(?TEST_MAP))},
     {"json",?_assertEqual(Keys,keys(?TEST_JSON))}]}.

map_test_() -> 
    Job = fun(<<"age">>,_) -> 8000;
             (_,true) -> false;
             (_,V) -> V end,
    PropOut = map(Job,?TEST_PROP),
    MapOut = map(Job,?TEST_MAP),
    JsonOut = map(Job,?TEST_JSON),
    {setup,
     fun setup/0,
    [{"proplist",?_assertEqual(8000, proplists:get_value(<<"age">>,PropOut))},
     {"proplist",?_assertEqual(false, proplists:get_value(<<"earthling">>,PropOut))},
     {"map",?_assertEqual(8000, maps:get(<<"age">>,MapOut))},
     {"map",?_assertEqual(false,maps:get(<<"earthling">>,MapOut))},
     {"json",?_assertEqual(8000, ?MODULE:get(<<"age">>,JsonOut))},
     {"json",?_assertEqual(false,?MODULE:get(<<"earthling">>,JsonOut))}]}.

new_test_() -> 
    %% Depends on default type setting
    Test = [],
    {setup,
     fun setup/0,
    [{"proplist",?_assertEqual([],new(proplist))},
     {"map",?_assertEqual(#{},new(map))},
     {"json",?_assertEqual(<<"{}">>,new(json))},
     {"default",?_assertEqual(Test,new())}]}.

put_test_() -> 
    {setup,
     fun setup/0,
    [{"prop_empty",?_assertEqual([{test,1}],?MODULE:put(test,1,[]))},
     {"prop_replace",?_assertEqual([{test,1}],?MODULE:put(test,1,[{test,2}]))},
     {"map_empty",?_assertEqual(#{test => 1},?MODULE:put(test,1,#{}))},
     {"map_replace",?_assertEqual(#{test => 1},?MODULE:put(test,1,#{test => 2}))},
     {"json_empty",?_assertEqual(<<"{\"test\":1}">>,?MODULE:put(<<"test">>,1,<<"{}">>))},
     {"json_replace",?_assertEqual(<<"{\"test\":1}">>,?MODULE:put(<<"test">>,1,<<"{\"test\":2}">>))}]}.

remove_test_() ->
    PropOut = remove(<<"age">>,?TEST_PROP),
    MapOut =  remove(<<"age">>,?TEST_MAP),
    JsonOut = remove(<<"age">>,?TEST_JSON),
    {setup,
     fun setup/0,
    [{"proplist",?_assertEqual(undefined, proplists:get_value(<<"age">>,PropOut,undefined))},
     {"map",?_assertEqual(undefined, ?MODULE:get(<<"age">>,MapOut))},
     {"json",?_assertEqual(undefined,?MODULE:get(<<"age">>,JsonOut))}]}.

size_test_() -> 
    {setup,
     fun setup/0,
    [{"proplist",?_assertEqual(5,?MODULE:size(?TEST_PROP))},
     {"map",?_assertEqual(5,?MODULE:size(?TEST_MAP))},
     {"json",?_assertEqual(5,?MODULE:size(?TEST_JSON))},
     {"json_list",?_assertEqual(3,?MODULE:size(?TEST_JSON_LIST))}]}.

update_test_() ->
    PropOut = update(<<"age">>,8000,?TEST_PROP),
    MapOut =  update(<<"age">>,8000,?TEST_MAP),
    JsonOut = update(<<"age">>,8000,?TEST_JSON),
    {setup,
     fun setup/0,
    [{"proplist",?_assertEqual(8000, proplists:get_value(<<"age">>,PropOut))},
     {"map",?_assertEqual(8000, maps:get(<<"age">>,MapOut))},
     {"json",?_assertEqual(8000,?MODULE:get(<<"age">>,JsonOut))},
     {"error_prop",?_assertError(badarg,update(<<"not_there">>,true,?TEST_PROP))},
     {"error_map",?_assertError(badarg,update(<<"not_there">>,true,?TEST_MAP))}
     ]}.


values_test_() ->
    Values = lists:sort([<<"Doe">>,<<"John">>,<<"bar">>,true,981,null]),
    {setup,
     fun setup/0,
    [{"proplist",?_assertEqual(Values,lists:sort(values(?TEST_PROP)))},
     {"map",?_assertEqual(Values,lists:sort(values(?TEST_MAP)))},
     {"json",?_assertEqual(Values,lists:sort(values(?TEST_JSON)))}]}.

to_proplist_test_() ->
    {setup,
     fun setup/0,
    [{"proplist",?_assert(is_list(to_proplist(?TEST_PROP)))},
     {"map",?_assert(is_list(to_proplist(?TEST_MAP)))},
     {"json",?_assert(is_list(to_proplist(?TEST_JSON)))}]}.

to_proplist_deep_test_() ->
    TestProp = [{<<"first">>,[{<<"second">>,true}]}],
    TestMap = #{<<"first">> => #{<<"second">> => true}},
    TestJson = <<"{\"first\":{\"second\":true}}">>,
    {setup,
     fun setup/0,
    [{"proplist",?_assertEqual(TestProp,to_proplist(TestProp,deep))},
     {"map",?_assertEqual(TestProp,to_proplist(TestMap,deep))},
     {"json",?_assertEqual(TestProp,to_proplist(TestJson,deep))}]}.

to_map_test_() ->
    {setup,
     fun setup/0,
    [{"proplist",?_assert(is_map(to_map(?TEST_PROP)))},
     {"map",?_assert(is_map(to_map(?TEST_MAP)))},
     {"json",?_assert(is_map(to_map(?CL(?TEST_JSON))))}]}.

to_binary_test_() ->
    {setup,
     fun setup/0,
    [{"proplist",?_assert(is_binary(to_binary(?TEST_PROP)))},
     {"map",?_assert(is_binary(to_binary(?TEST_MAP)))},
     {"json",?_assert(is_binary(to_binary(?TEST_JSON)))}]}.

filter_test_() -> 
    FilterFun = fun(<<"age">>,_) -> true;
                   (_,true) -> true;
                   (_,_) -> false end,
    PropOut = filter(FilterFun,?TEST_PROP),
    MapOut =  filter(FilterFun,?TEST_MAP),
    JsonOut = filter(FilterFun,?TEST_JSON),
    {setup,
     fun setup/0,
    [{"proplist",?_assertEqual(981, proplists:get_value(<<"age">>,PropOut))},
     {"proplist",?_assertEqual(true, proplists:get_value(<<"earthling">>,PropOut))},
     {"map",?_assertEqual(981, maps:get(<<"age">>,MapOut))},
     {"map",?_assertEqual(true,maps:get(<<"earthling">>,MapOut))},
     {"json",?_assertEqual(981, ?MODULE:get(<<"age">>,JsonOut))},
     {"json",?_assertEqual(true,?MODULE:get(<<"earthling">>,JsonOut))}]}.

include_test_() ->
    Json_Ok = <<"{\"name\":\"John\"}">>,
    Prop_Ok = [{<<"surname">>,<<"Doe">>},{<<"foo">>,<<"bar">>}],
    Map_Ok = #{<<"earthling">> => true, <<"age">> => 981},
    {setup,
     fun setup/0,
    [{"proplist",?_assertEqual(Prop_Ok, include([<<"surname">>,<<"foo">>],?TEST_PROP))},
     {"map",?_assertEqual(Map_Ok, include([<<"earthling">>,<<"age">>],?TEST_MAP))},
     {"json",?_assertEqual(Json_Ok, include([<<"name">>],?TEST_JSON))}]}.

exclude_test_() ->
    Json_Ok = <<"{\"name\":\"John\"}">>,
    Prop_Ok = [{<<"name">>,<<"John">>}],
    Map_Ok = #{<<"name">> => <<"John">>},
    ExcFields = [<<"surname">>,<<"foo">>,<<"earthling">>,<<"age">>,<<"empty">>],
    {setup,
     fun setup/0,
    [{"proplist",?_assertEqual(Prop_Ok, exclude(ExcFields,?TEST_PROP))},
     {"map",?_assertEqual(Map_Ok, exclude(ExcFields,?TEST_MAP))},
     {"json",?_assertEqual(Json_Ok, exclude(ExcFields,?TEST_JSON))}]}.


merge_test_() ->
    Json_1 = <<"{\"test\":\"true\"}">>,
    Prop_1 = [{<<"test">>,<<"true">>}],
    Map_1 = #{<<"test">> => <<"true">>},
    JsonOut = merge(Json_1,?TEST_JSON),
    PropOut = merge(Prop_1,?TEST_PROP),
    MapOut = merge(Map_1,?TEST_MAP),
    {setup,
     fun setup/0,
    [{"proplist",?_assertEqual(<<"true">>, ?MODULE:get(<<"test">>,PropOut))},
     {"proplist",?_assertEqual(<<"Doe">>, ?MODULE:get(<<"surname">>,PropOut))},
     {"map",?_assertEqual(<<"true">>, ?MODULE:get(<<"test">>,MapOut))},
     {"map",?_assertEqual(<<"Doe">>, ?MODULE:get(<<"surname">>,MapOut))},
     {"json",?_assertEqual(<<"true">>, ?MODULE:get(<<"test">>,JsonOut))},
     {"json",?_assertEqual(<<"Doe">>, ?MODULE:get(<<"surname">>,JsonOut))}]}.


project_test_() ->
    Data1 = <<"{\"name\":{\"first\":false,\"list\":[{\"any\":1}]}, \"test\":true}">>,
    Data2 = <<"{\"list\":[{\"any\":{\"sub\":true}},{\"any\":{\"sub\":false}}],\"empty\":[]}">>,
    [?_assertEqual([1],project("name:list[*]:any",Data1)),
     ?_assertEqual(true,project("test",Data1)),
     ?_assertEqual(false,project("name:first",Data1)),
     ?_assertEqual([[1],true],project(["name:list[*]:any","test"],Data1)),
     ?_assertEqual([true,false],project("list[*]:any:sub",Data2)),
     ?_assertEqual(true,project("list[1]:any:sub",Data2)),
     ?_assertEqual([<<"first">>,<<"list">>],project("name:$keys$",Data1)),
     ?_assertEqual([[<<"first">>,<<"list">>],[1],false], project(["name:$keys$",
                                                                  "name:list[*]:any",
                                                                  "name:first"
                                                                 ],Data1)),
     ?_assertEqual(nomatch, project(["name:$keys$",
                                     "name:list[*]:any",
                                     "name:not_there"
                                    ],Data1)),
     ?_assertEqual(nomatch,project("list[10]:any:sub",Data2)),
     ?_assertEqual(nomatch,project("unlist[1]:any:sub",Data2)),
     ?_assertEqual(nomatch,project("empty[1]",Data2)),
     ?_assertEqual(nomatch,project("empty[*]",Data2)),
     ?_assertEqual(nomatch,project("unexistant",Data2)),
     ?_assertEqual(nomatch,project("unexistant[*]",Data2))
    ].

parse_path_test_() ->
    Path1 = "name.list[*].any",
    Path2 = <<"name.list[*].any">>,
    Path3 = lists:concat(["name",?JSON_PATH_SEPARATOR,"list[*]",?JSON_PATH_SEPARATOR,"any"]),
    Res1  = ["name","list[*]","any"],
    [?_assertEqual(Res1,parse_path(Path1,".")),
     ?_assertEqual(Res1,parse_path(Path2,".")),
     ?_assertEqual(Res1,parse_path(Path3))
    ].

walk_path_test_() ->
    Path1 = ["name","list[*]","any"],
    Object1 = [{<<"name">>, [{<<"list">>, [[{<<"any">>,1}],[{<<"any">>,2}]]}]}],
    Result1 = [1,2],
    [?_assertEqual(Result1,walk_path(Path1,Object1))
    ].
    

json_lib_throw_test_() ->
    [?_assertException(throw,no_json_library_found,json_binary_decode(<<"{\"test\":true}">>,[])),
     ?_assertException(throw,no_json_library_found,json_binary_encode([{<<"test">>,true}],[]))].



-ifdef(DECODE_TO_MAPS).
%% Some 'adaptation' because JSON conversion changes order between formats
-define(TEST_JSONOUTDIFF,<<"{\"added\":{\"ega\":981,\"newval\":\"one\"},\"changes\":4,\"removed\":{\"age\":981},\"replaced\":{\"surname\":\"Doe\"},\"updated\":{\"surname\":\"DoeDoe\"}}">>).
-else.
-define(TEST_JSONOUTDIFF,<<"{\"added\":{\"ega\":981,\"newval\":\"one\"},\"removed\":{\"age\":981},\"updated\":{\"surname\":\"DoeDoe\"},\"replaced\":{\"surname\":\"Doe\"},\"changes\":4}">>).
-endif.

diff_test_() ->
    PropBefore = [{<<"surname">>,<<"Doe">>}, {<<"age">>,981}, {<<"empty">>,null},{<<"unmod">>,true}],
    PropAfter  = [{<<"surname">>,<<"DoeDoe">>}, {<<"ega">>,981},{<<"newval">>,<<"one">>}, {<<"empty">>,null},{<<"unmod">>,true}],
    PropDiffOut = [{added,[{<<"ega">>,981},{<<"newval">>,<<"one">>}]},
                   {removed,[{<<"age">>,981}]},
                   {updated,[{<<"surname">>,<<"DoeDoe">>}]},
                   {replaced,[{<<"surname">>,<<"Doe">>}]},
                   {changes,4}],

    MapsBefore = #{<<"age">> => 981,<<"empty">> => null,<<"surname">> => <<"Doe">>,<<"unmod">> => true},
    MapsAfter = #{<<"ega">> => 981, <<"empty">> => null, <<"newval">> => <<"one">>, <<"surname">> => <<"DoeDoe">>, <<"unmod">> => true},
    MapsDiffOut = #{added => #{<<"ega">> => 981,<<"newval">> => <<"one">>},
                   removed => #{<<"age">> => 981},
                   replaced => #{<<"surname">> => <<"Doe">>},
                   updated => #{<<"surname">> => <<"DoeDoe">>},
                   changes => 4},

    JsonBefore = <<"{\"surname\":\"Doe\",\"age\":981,\"empty\":null}">>,
    JsonAfter = <<"{\"surname\":\"DoeDoe\",\"ega\":981,\"newval\":\"one\",\"empty\":null}">>,
    JsonDiffOut = ?TEST_JSONOUTDIFF,


    {setup,
     fun setup/0,
    [{"proplist",?_assertEqual(PropDiffOut, diff(PropBefore,PropAfter))},
     {"map",?_assertEqual(MapsDiffOut, diff(MapsBefore,MapsAfter))},
     {"json",?_assertEqual(JsonDiffOut, diff(JsonBefore,JsonAfter))}
    ]}.

equal_test_() -> 
    Json_Ok = <<"{\"name\":\"John\",\"surname\":\"Doe\",\"foo\":\"bar\",\"earthling\":true,\"age\":981,\"other\":null}">>,
    Prop_Ok = [{<<"surname">>,<<"Doe">>},{<<"foo">>,<<"bar">>}, {<<"earthling">>,true}, {<<"name">>,<<"John">>}, {<<"age">>,981}],
    Map_Ok = #{<<"earthling">> => true, <<"age">> => 981, <<"name">> => <<"John">>,  <<"foo">> => <<"bar">>,<<"surname">> => <<"Doe">>,<<"empty">> => null},
    Json_Nook = <<"{\"name\":\"John\",\"surname\":null,\"foo\":\"bar\",\"earthling\":true,\"age\":981,\"other\":null}">>,
    Prop_Nook = [{<<"surname">>,<<"Doe">>},{<<"foo">>,<<"barbara">>}, {<<"earthling">>,true}, {<<"name">>,<<"John">>}, {<<"age">>,981}],
    Map_Nook = #{<<"earthling">> => true, <<"age">> => 981, <<"name">> => <<"John">>,  <<"foo">> => <<"bar">>, <<"empty">> => null},
    {setup,
     fun setup/0,
    [{"map_success",?_assert(equal(Map_Ok,?TEST_MAP))},
     {"prop_success",?_assert(equal(Prop_Ok,?TEST_PROP))},
     {"json_success",?_assert(equal(Json_Ok,?TEST_JSON))},
     {"map_fail",?_assert(not equal(Map_Nook,?TEST_MAP))},
     {"prop_fail",?_assert(not equal(Prop_Nook,?TEST_PROP))},
     {"json_fail",?_assert(not equal(Json_Nook,?TEST_JSON))} ]}.

is_subset_test_() -> 
    Sub_Ok_Json =  <<"{\"surname\":\"Doe\",\"age\":981,\"nothing\":null}">>,
    Sub_Ok_Prop = [{<<"surname">>,<<"Doe">>}, {<<"age">>,981}, {<<"nothing">>,null}],
    Sub_Ok_Map  = #{<<"age">> => 981, <<"surname">> => <<"Doe">>, <<"nothing">> => null},
    Sub_Nook_Json =  <<"{\"suame\":\"Doe\",\"age\":981}">>,
    Sub_Nook_Prop = [{<<"surname">>,<<"Doe">>}, {<<"age">>,91}],
    Sub_Nook_Map  = #{<<"age">> => 981, <<"surname">> => <<"De">>},
    {setup,
     fun setup/0,
    [{"map_success",?_assert(is_subset(Sub_Ok_Map ,?TEST_MAP))},
     {"proplist_success",?_assert(is_subset(Sub_Ok_Prop,?TEST_PROP))},
     {"json_success",?_assert(is_subset(Sub_Ok_Json,?TEST_JSON))},
     {"map_fail",?_assert(not is_subset(Sub_Nook_Map ,?TEST_MAP))},
     {"proplist_fail",?_assert(not is_subset(Sub_Nook_Prop,?TEST_PROP))},
     {"json_fail",?_assert(not is_subset(Sub_Nook_Json,?TEST_JSON))}]}.

is_disjoint_test_() -> 
    Sub_Ok_Json =  <<"{\"suame\":\"Doe\",\"age\":91}">>,
    Sub_Ok_Prop = [{<<"surname">>,<<"De">>}, {<<"ae">>,981}],
    Sub_Ok_Map  = #{<<"ag">> => 981, <<"surname">> => <<"De">>},
    Sub_Nook_Json =  <<"{\"suame\":\"Doe\",\"age\":981}">>,
    Sub_Nook_Prop = [{<<"surname">>,<<"Doe">>}, {<<"age">>,981}],
    Sub_Nook_Map  = #{<<"age">> => 981, <<"surname">> => <<"De">>},
    {setup,
     fun setup/0,
    [{"map_success",?_assert(is_disjoint(Sub_Ok_Map ,?TEST_MAP))},
     {"proplist_success",?_assert(is_disjoint(Sub_Ok_Prop,?TEST_PROP))},
     {"json_success",?_assert(is_disjoint(Sub_Ok_Json,?TEST_JSON))},
     {"map_fail",?_assert(not is_disjoint(Sub_Nook_Map ,?TEST_MAP))},
     {"proplist_fail",?_assert(not is_disjoint(Sub_Nook_Prop,?TEST_PROP))},
     {"json_fail",?_assert(not is_disjoint(Sub_Nook_Json,?TEST_JSON))}]}.

-else.
%%%%% MAP-LESS TEST STUFF GOES HERE 

size_test_() -> 
    {setup,
     fun setup/0,
    [{"proplist",?_assertEqual(5,?MODULE:size(?TEST_PROP))},
     {"json",?_assertEqual(5,?MODULE:size(?TEST_JSON))},
     {"json_list",?_assertEqual(3,?MODULE:size(?TEST_JSON_LIST))}]}.

values_test_() ->
    Values = lists:sort([<<"Doe">>,<<"John">>,<<"bar">>,true,981,null]),
    {setup,
     fun setup/0,
    [{"proplist",?_assertEqual(Values,lists:sort(values(?TEST_PROP)))},
     {"json",?_assertEqual(Values,lists:sort(values(?TEST_JSON)))}]}.

has_key_test_() ->
    {setup,
     fun setup/0,
    [{"proplist_success",?_assert(has_key(<<"surname">>,?TEST_PROP))},
     {"json_success",?_assert(has_key(<<"surname">>,?TEST_JSON))},
     {"proplist_fail",?_assert(not has_key(<<"sname">>,?TEST_PROP))},
     {"json_fail",?_assert(not has_key(<<"sname">>,?TEST_JSON))}]}.

keys_test_() -> 
    Keys = lists:sort([<<"surname">>,<<"name">>,<<"foo">>,<<"earthling">>,<<"age">>,<<"empty">>]),
    {setup,
     fun setup/0,
    [{"proplist",?_assertEqual(Keys,keys(?TEST_PROP))},
     {"json",?_assertEqual(Keys,keys(?TEST_JSON))}]}.

to_proplist_test_() ->
    {setup,
     fun setup/0,
    [{"proplist",?_assert(is_list(to_proplist(?TEST_PROP)))},
     {"json",?_assert(is_list(to_proplist(?TEST_JSON)))}]}.

to_proplist_deep_test_() ->
    TestProp = [{<<"first">>,[{<<"second">>,true}]}],
    TestJson = <<"{\"first\":{\"second\":true}}">>,
    {setup,
     fun setup/0,
    [{"proplist",?_assertEqual(TestProp,to_proplist(TestProp,deep))},
     {"json",?_assertEqual(TestProp,to_proplist(TestJson,deep))}]}.

to_binary_test_() ->
    {setup,
     fun setup/0,
    [{"proplist",?_assert(is_binary(to_binary(?TEST_PROP)))},
     {"json",?_assert(is_binary(to_binary(?TEST_JSON)))}]}.


-endif.

jpp_match_test_() ->
    Object1 = <<"{\"a\":{\"b\":[{\"c\":1},{\"c\":2},{\"c\":3}]}}">>,
    Tst1_1 = {':', [<<"a">>, {'[]', <<"b">>, [1]}, <<"c">>]}, % a:b[1]:c 
    Out1_1 = [1],
    Tst1_2 = {':', [<<"a">>, {'[]', <<"b">>, {'-', 1, 2}}, <<"c">>]}, % a:b[1-2]
    Out1_2 = [1,2],
    Tst1_3 = {':', [<<"a">>, {'[]', <<"b">>, [1,2,3]}]}, % a:b[1,2,3]
    Tst1_3b = {':', [<<"a">>, <<"b">>]}, % a:b[1,2,3]
    Out1_3 = [{<<"c">>,1},{<<"c">>,2},{<<"c">>,3}],
    Tst1_4 = {':', [<<"a">>, {'[]', <<"b">>, []}, <<"c">>]}, % a:b[]:c
    Out1_4 = [1,2,3],
    Tst1_5 = {':', [<<"a">>, {'[]', <<"b">>, [{'-',1,2},3]}, <<"c">>]}, % a:b[1-2,3]:c
    Out1_5 = [1,2,3],

    Object2 = <<"{\"a\" : {\"b\" :{\"any\":{\"c\":1,\"d\":2},\"c\":{\"c\":4,\"d\":5}}}}">>,
    Tst2_1 = {':', [<<"a">>, {'{}', <<"b">>, []}, <<"c">>]}, % a:b{}:c
    Out2_1 = [1,4],
    Tst2_2 = {':', [<<"a">>, {'{}', <<"b">> ,[<<"c">>]}]}, %a:b{c}
    Out2_2 = [{<<"c">>,4},{<<"d">>,5}],
    Tst2_3 = {':', [<<"a">>, <<"b">>, {'$',<<"values">>}, <<"c">>]}, % a:b:$values$:c
    Out2_3 = [1,4],
    Tst2_4 = {':', [<<"a">>, {'{}',<<"b">>,[<<"any">>,<<"c">>]},<<"d">>]}, % a:b{any,c}:d
    Out2_4 = [2,5],
    Tst2_5 = {':',[<<"a">>,<<"b">>,<<"any">>,<<"c">>]},%a:b:any:c
    Out2_5 = 1,

    Object3 = <<"[{\"c\":{\"a\":{\"d\":3}},\"d\":true},{\"c\":{\"b\":{\"d\":2}}}]">>,
    Tst3_1 = {':', [{'[]', '_', []}, {'{}', <<"c">>, {'$',<<"firstChild">>}}, <<"d">>]}, % []:c{$firstChild$}:d
    Out3_1 = [3,2],
    Tst3_2 = {':', [{'[]', '_', [1]}, <<"d">>]}, % [1]:d
    Out3_2 = [true],
    Tst3_3 = {':', [{'[]', '_', []},  <<"c">>, {'$',<<"firstChild">>}, <<"d">>]}, % []:c:$firstChild$:d
    Out3_3 = [3,2],

    Object4 = <<"{\"a\":{\"e\":8},\"c\":{\"e\":1},\"d\":{\"e\":2}}">>,
    Tst4_1 = {':', [{'{}', '_', [<<"c">>, <<"d">>]},<<"e">>]}, % {c,d}:e
    Out4_1 = [1,2],
    Tst4_2 = {':', [{'{}', '_', [{'-',<<"a">>, <<"c">>}]},<<"e">>]}, % {a-c}:e
    Out4_2 = [8,1],
    Tst4_3 = {':',[<<"a">>,{'$',<<"keys">>}]}, % a:$keys$
    Out4_3 = [<<"e">>],
    Tst4_4 = {':',[{'{}',<<"a">>,[{'$',<<"keys">>}]}]}, % a{$keys$}
    Out4_4 = [<<"e">>],
    Tst4_5 = {':',[{'$',<<"keys">>}]}, % $keys$
    Out4_5 = [<<"a">>,<<"c">>,<<"d">>],
    Tst4_6 = {':',[{'[]',{'$',<<"keys">>},[1]}]}, % $keys$[1]
    Out4_6 = [<<"a">>],

    Object5 = <<"{\"a\": {\"b\":[{\"a\":{\"c\":4},\"b\":{\"c\":1}},{\"a\":{\"c\":8},\"b\":{\"c\":2}}]}}">>,
    Tst5_1 = {':', [<<"a">>, {'{}', {'[]', <<"b">>, [{'-', 1, 2}]},[]},<<"c">>]}, % a:b[1-2]{}:c 	
    Out5_1 = [4,1,8,2],
    Tst5_2 = {':', [<<"a">>, {'{}', {'[]', <<"b">>, [{'-', 1, 3}]},[]},<<"c">>]}, % a:b[1-3]{}:c 	
    Tst5_3 = {':', [<<"a">>,<<"z">>]}, % a:z

    Object6 = <<"{\"a\":[{\"b\":1,\"d\":2},{\"b\":3,\"d\":4}],\"b\":[{\"b\":5,\"d\":6},{\"b\":7,\"d\":8}]}">>,
    Tst6_1 = {':',[{'{}',{'[]',{'{}','_',[]},[]},[<<"b">>]}]}, % {}[]{b} 
    Out6_1 = [1,3,5,7],


    {setup,
     fun setup/0,
    [{"form a:b[1]:c",?_assertEqual(Out1_1,jpp_match(Tst1_1,Object1))},
     {"form a:b[1-2]",?_assertEqual(Out1_2,jpp_match(Tst1_2,Object1))},
     {"form a:b[1,2,3]",?_assertEqual(Out1_3,jpp_match(Tst1_3,Object1))},
     {"form a:b",?_assertEqual(Out1_3,jpp_match(Tst1_3b,Object1))},
     {"form a:b[]:c",?_assertEqual(Out1_4,jpp_match(Tst1_4,Object1))},
     {"form a:b[1-2,3]",?_assertEqual(Out1_5,jpp_match(Tst1_5,Object1))},
     {"form a:b{}:c",?_assertEqual(Out2_1,jpp_match(Tst2_1,Object2))},
     {"form a:b{c}",?_assertEqual(Out2_2,lists:sort(jpp_match(Tst2_2,Object2)))},
     {"form a:b:$values$:c}",?_assertEqual(Out2_3,lists:sort(jpp_match(Tst2_3,Object2)))},
     {"form a:b{any,c}:d",?_assertEqual(Out2_4,lists:sort(jpp_match(Tst2_4,Object2)))},
     {"form a:b:any:c", ?_assertEqual(Out2_5, jpp_match(Tst2_5,Object2))},
     {"form []:c{$firstChild$}:d",?_assertEqual(Out3_1,jpp_match(Tst3_1,Object3))},
     {"form [1]:d",?_assertEqual(Out3_2,jpp_match(Tst3_2,Object3))},
     {"form []:c:$firstChild$:d",?_assertEqual(Out3_3,jpp_match(Tst3_3,Object3))},
     {"form {c,d}:e",?_assertEqual(Out4_1,jpp_match(Tst4_1,Object4))},
     {"form {a-c}:e",?_assertEqual(Out4_2,jpp_match(Tst4_2,Object4))},
     {"form a:$keys$",?_assertEqual(Out4_3,jpp_match(Tst4_3,Object4))},
     {"form a{$keys$}",?_assertEqual(Out4_4,jpp_match(Tst4_4,Object4))},
     {"form $keys$",?_assertEqual(Out4_5,jpp_match(Tst4_5,Object4))},
     {"form $keys$[1]",?_assertEqual(Out4_6,jpp_match(Tst4_6,Object4))},
     {"form a:b[1-2]{}:c",?_assertEqual(Out5_1,jpp_match(Tst5_1,Object5))},
     {"nomatch a:b[1-3]{}:c",?_assertEqual(nomatch,jpp_match(Tst5_2,Object5))},
     {"nomatch a:z",?_assertEqual(nomatch,jpp_match(Tst5_3,Object5))},
     {"form {}[]{b}",?_assertEqual(Out6_1,jpp_match(Tst6_1,Object6))}
     ]}.

match_test_() ->
    Object1 = <<"{\"a\":{\"b\":[{\"c\":1},{\"c\":2},{\"c\":3}]}}">>,
    Tst1_1 = <<"a:b[1]:c">>,
    Out1_1 = [1],
    Tst1_2 = <<"a:b[1-2]">>,
    Out1_2 = [{<<"c">>,1},{<<"c">>,2}],
    Tst1_3 = <<"a:b[1,2,3]">>,
    Tst1_3b = <<"a:b[1,2,3]">>,
    Out1_3 = [{<<"c">>,1},{<<"c">>,2},{<<"c">>,3}],
    Tst1_4 = <<"a:b[]:c">>,
    Out1_4 = [1,2,3],
    Tst1_5 = <<"a:b[1-2,3]:c">>,
    Out1_5 = [1,2,3],

    Object2 = <<"{\"a\" : {\"b\" :{\"any\":{\"c\":1,\"d\":2},\"c\":{\"c\":4,\"d\":5}}}}">>,
    Tst2_1 = <<"a:b{}:c">>,
    Out2_1 = [1,4],
    Tst2_2 = <<"a:b{c}">>,
    Out2_2 = [{<<"c">>,4},{<<"d">>,5}],
    Tst2_3 = <<"a:b:$values$:c">>,
    Out2_3 = [1,4],
    Tst2_4 = <<"a:b{any,c}:d">>,
    Out2_4 = [2,5],
    Tst2_5 = <<"a:b:any:c">>,
    Out2_5 = 1,

    Object3 = <<"[{\"c\":{\"a\":{\"d\":3}},\"d\":true},{\"c\":{\"b\":{\"d\":2}}}]">>,
    Tst3_1 = <<"[]:c{$firstChild$}:d">>,
    Out3_1 = [3,2],
    Tst3_2 = <<"[1]:d">>,
    Out3_2 = [true],
    Tst3_3 = <<"[]:c:$firstChild$:d">>,
    Out3_3 = [3,2],

    Object4 = <<"{\"a\":{\"e\":8},\"c\":{\"e\":1},\"d\":{\"e\":2}}">>,
    Tst4_1 = <<"{c,d}:e">>,
    Out4_1 = [1,2],
    Tst4_2 = <<"{a-c}:e">>,
    Out4_2 = [8,1],
    Tst4_3 = <<"a:$keys$">>,
    Out4_3 = [<<"e">>],
    Tst4_4 = <<"a{$keys$}">>,
    Out4_4 = [<<"e">>],
    Tst4_5 = <<"$keys$">>,
    Out4_5 = [<<"a">>,<<"c">>,<<"d">>],
    Tst4_6 = <<"$keys$[1]">>,
    Out4_6 = [<<"a">>],

    Object5 = <<"{\"a\": {\"b\":[{\"a\":{\"c\":4},\"b\":{\"c\":1}},{\"a\":{\"c\":8},\"b\":{\"c\":2}}]}}">>,
    Tst5_1 = <<"a:b[1-2]{}:c">>,
    Out5_1 = [4,1,8,2],
    Tst5_2 = <<"a:b[1-3]{}:c">>,
    Tst5_3 = <<"a:z">>,

    Object6 = <<"{\"a\":[{\"b\":1,\"d\":2},{\"b\":3,\"d\":4}],\"b\":[{\"b\":5,\"d\":6},{\"b\":7,\"d\":8}]}">>,
    Tst6_1 = <<"{}[]{b}">>,
    Out6_1 = [1,3,5,7],

    {setup,
     fun setup/0,
    [{"form a:b[1]:c",?_assertEqual(Out1_1,match(Tst1_1,Object1))},
     {"form a:b[1-2]",?_assertEqual(Out1_2,match(Tst1_2,Object1))},
     {"form a:b[1,2,3]",?_assertEqual(Out1_3,match(Tst1_3,Object1))},
     {"form a:b",?_assertEqual(Out1_3,match(Tst1_3b,Object1))},
     {"form a:b[]:c",?_assertEqual(Out1_4,match(Tst1_4,Object1))},
     {"form a:b[1-2,3]",?_assertEqual(Out1_5,match(Tst1_5,Object1))},
     {"form a:b{}:c",?_assertEqual(Out2_1,match(Tst2_1,Object2))},
     {"form a:b{c}",?_assertEqual(Out2_2,lists:sort(match(Tst2_2,Object2)))},
     {"form a:b:$values$:c}",?_assertEqual(Out2_3,lists:sort(match(Tst2_3,Object2)))},
     {"form a:b{any,c}:d",?_assertEqual(Out2_4,lists:sort(match(Tst2_4,Object2)))},
     {"form a:b:any:c", ?_assertEqual(Out2_5, match(Tst2_5,Object2))},
     {"form []:c{$firstChild$}:d",?_assertEqual(Out3_1,match(Tst3_1,Object3))},
     {"form [1]:d",?_assertEqual(Out3_2,match(Tst3_2,Object3))},
     {"form []:c:$firstChild$:d",?_assertEqual(Out3_3,match(Tst3_3,Object3))},
     {"form {c,d}:e",?_assertEqual(Out4_1,match(Tst4_1,Object4))},
     {"form {a-c}:e",?_assertEqual(Out4_2,match(Tst4_2,Object4))},
     {"form a:$keys$",?_assertEqual(Out4_3,match(Tst4_3,Object4))},
     {"form a{$keys$}",?_assertEqual(Out4_4,match(Tst4_4,Object4))},
     {"form $keys$",?_assertEqual(Out4_5,match(Tst4_5,Object4))},
     {"form $keys$[1]",?_assertEqual(Out4_6,match(Tst4_6,Object4))},
     {"form a:b[1-2]{}:c",?_assertEqual(Out5_1,match(Tst5_1,Object5))},
     {"nomatch a:b[1-3]{}:c",?_assertEqual(nomatch,match(Tst5_2,Object5))},
     {"nomatch a:z",?_assertEqual(nomatch,match(Tst5_3,Object5))},
     {"form {}[]{b}",?_assertEqual(Out6_1,match(Tst6_1,Object6))}]}.

-endif.

