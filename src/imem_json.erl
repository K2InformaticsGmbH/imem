%% -*- coding: utf-8 -*-
-module(imem_json).

%% @doc == JSON operations ==
%% Every operation is valid for:
%% - DataObject as proplist
%% - DataObject as map
%% - DataObject as raw json binary
%% and returns in the same format as the provided data object
%% (except for conversion functions)

-include("imem.hrl").
-include("imem_json.hrl").

% Binary(string) to Json En/Decode interface APIs
-export([encode/1, decode/1, decode/2]).

%% Standard data type operations, inspired by maps operations
-export([ find/2
        , get/2
        , get/3
        , keys/1
        , put/3
        , remove/2
        , size/1
        , values/1
        ]).

%% Conversions
-export([to_proplist/1]).

%% Other Functionality
-export([ eval/2            %% Constructs a data object prescribed by json path (using jpparse)
        , expand_inline/2   %% Recursively expands 'inline_*' references in a JSON object
        , diff/2            %% Lists differences of two JSON objects
        ]).

-safe(all).

%% Unused exports
%% TODO : Review and unciomment/remove retaled code
%% - -export([
%% -          filter/2      %% Only keep key/value pairs based on boolean filter
%% -         , to_proplist/2
%% -         , to_map/1
%% -         , to_binary/1
%% -         , items/1
%% -         , update/3
%% -         , map/2
%% -         , new/0
%% -         , new/1
%% -         , fold/3
%% -         , has_key/2
%% -         , include/2     %% Only include specified keys
%% -         , exclude/2     %% Exclude specified keys
%% -         , merge/2       %% Merge data objects together (join)
%% -         , diff/2        %% Diff between data objects
%% -         , equal/2       %% Check if data objects are equal
%% -         , is_subset/2   %% Check if first data object is a subset of the second
%% -         , is_disjoint/2 %% Check if data objects have no keys in common
%% -         , project/2     %% Create a projection of a json document based on paths
%% -         ]).
 
%% @doc ==============================================================
%% Basic En/Decode interface
%% supports all valid combinations of
%%  libraries: jsx for maps and proplists
%%  erlang release: >= R17 (with maps)
%% ===================================================================
-spec decode(Json :: binary()) -> data_object().
decode(Json) -> decode(Json, []).

-spec decode(Json :: binary(), Opts :: list()) -> data_object().
decode(Json, Opts) -> jsx:decode(Json, lists:usort([strict|Opts])).

-spec encode(data_object()) -> Json :: binary().
encode(Json) -> jsx:encode(Json).
%% ===================================================================

%% ===================================================================
%% Other Exported functions
%% ===================================================================
%% @doc Find the value of a property and return in matching erlang type
%% throw exception if property not found.
-define(JRENUMBR, "(-?(0|[1-9]\\d*)(\.\\d+)?([eE][+-]?\\d+)?)").
-define(JRESTRNG, "(\".*\")").
-define(JRECONST, "(true|false|null)").
-spec find(nonempty_string() | atom() | binary(), jsx:json_term()) ->
    jsx:json_term() | no_return().
find(Property, _DataObject)
  when not (is_atom(Property) orelse
            (is_binary(Property) andalso byte_size(Property) > 0) orelse
            (is_list(Property) andalso length(Property) > 0)) ->
    error(bad_property);
find(_Property, DataObject)
  when not (is_map(DataObject) orelse
            (is_list(DataObject) andalso length(DataObject) > 0) orelse
            (is_binary(DataObject) andalso byte_size(DataObject) > 0)) ->
    error(bad_object);
find(Property, DataObject) when is_atom(Property) ->
    find(atom_to_list(Property), DataObject);
find(Property, DataObject) when is_binary(Property) ->
    find(binary_to_list(Property), DataObject);
find([First|_] = Property, DataObject)
  when is_list(Property), is_binary(DataObject) ->
    ReExp = case {First, lists:last(Property)} of
               {$",$"} -> Property;
               {_,$"} -> [$"|Property];
               {$",_} -> Property++"\"";
               {_,_} -> [$"|Property++"\""]
            end
    ++ ":" ++
        % Duplicate subpattern numbers
        "(?|"?JRENUMBR"|"?JRESTRNG"|"?JRECONST")"
        "[,}\r\n]", % End of object or property TODO newline check

    case re:run(DataObject, ReExp, [{capture, [1], list},ungreedy]) of
        nomatch -> error({not_found, DataObject, ReExp});
        {match, [Got]} ->
            case [Re
                  || Re <- ["^"?JRENUMBR"$", "^"?JRESTRNG"$", "^"?JRECONST"$"],
                     re:run(Got, Re) /= nomatch] of
                ["^"?JRESTRNG"$"] -> re:replace(Got, "^\"|\"$", "", [{return,binary},global]);
                ["^"?JRECONST"$"] -> list_to_existing_atom(Got);
                ["^"?JRENUMBR"$"] -> try jsx:decode(list_to_binary(Got))
                                     catch _:Error -> error({Error, Got}) end;
                [] -> error({badmatch, Got})
            end
    end;
find(Property, DataObject) when is_map(DataObject) ->
    Prop = list_to_binary([Property]),
    case maps:fold(
           fun(K,V,'$not_found') when K == Prop -> V;
              (_K,V,'$not_found') when is_map(V) ->
                   find(Property, V);
              (_K,_V,AccIn) -> AccIn
           end, '$not_found', DataObject) of
        '$not_found' -> error({not_found, Prop, DataObject});
        Value when is_map(Value) -> find(Property, Value);
        [_|_] = Value ->
            lists:foldl(
              fun(#{} = Val, '$not_found') ->
                      find(Property, Val);
                 (_, AccIn) -> AccIn
              end, '$not_found', Value);
        Value -> Value
    end;
find(Property, DataObject) when is_list(DataObject) ->
    Prop = list_to_binary([Property]),
    case lists:foldl(
           fun({K,V},'$not_found') when K == Prop -> V;
              ({_K,V},'$not_found') when is_list(V) -> find(Property, V);
              ({_K,_V}, AccIn) -> AccIn
           end, '$not_found', DataObject) of
        '$not_found' -> error({not_found, Prop, DataObject});
        % Recurse into object
        [{_,_}|_] = Value -> find(Property, Value);
        % Recurse into list
        [_|_] = Value ->
            lists:foldl(
              fun([{_,_}|_] = Val, '$not_found') ->
                      find(Property, Val);
                 (_, AccIn) -> AccIn
              end, '$not_found', Value);
        Value -> Value
    end.

%% @doc Erlang term ordered list of keys used by data object
-spec keys(data_object()) -> list().
keys(DataObject) when is_list(DataObject) ->
    [K || {K,_} <- DataObject];                 % lists:sort(proplists:get_keys(DataObject));
keys(DataObject) when is_map(DataObject) -> 
    maps:keys(DataObject);                      % lists:sort(
keys(DataObject) -> 
    keys(decode(DataObject)).

%% @doc Get function, same as get(Key,DataObject,undefined)
-spec get(key(),data_object()) -> value() | undefined.
get(Key,DataObject) ->
    ?MODULE:get(Key,DataObject,undefined).

%% @doc Return value corresponding to key. If key not present, returns Default.
-spec get(key(),data_object(),Default) -> value() | Default when Default :: term().
get(Key,DataObject,Default) when is_list(DataObject) ->
    get_deep(DataObject, Key, Default, []);
% get(Key,DataObject,Default) when is_list(DataObject) ->
%    proplists:get_value(Key,DataObject,Default);
get(Key,DataObject,Default) when is_map(DataObject) ->
    case maps:find(Key,DataObject) of
        {ok,Value} -> Value;
        error -> Default
    end;
get(Key,DataObject,Default) ->
    ?MODULE:get(Key,decode(DataObject), Default).

-spec get_deep(data_object(),key(),Default, list()) -> list() | Default when Default :: term().
get_deep([], _, _, []) -> undefined;
get_deep([], _, _, Acc) -> Acc;
get_deep([Obj | Rest],Key,Default,Acc) when is_list(Obj) ->
    case proplists:get_value(Key,Obj,Default) of
        Default -> get_deep(Rest,Key,Default,Acc);
        Value -> get_deep(Rest,Key,Default,Acc ++ [Value])
    end;
get_deep(Obj,Key,Default,_) ->
    proplists:get_value(Key,Obj,Default).

%% @doc Add a value to a IMEM Json data object
%% Should Key already exist, old value will be replaced
-spec put(key(), value(), data_object()) -> data_object().
put(Key,Value,DataObject) when is_list(DataObject) ->
    [{Key,Value} | case proplists:delete(Key,DataObject) of
                       [{}] -> [];
                       Rest -> Rest
                   end];
put(Key,Value,DataObject) when is_map(DataObject) ->
    maps:put(Key,Value,DataObject);
put(Key,Value,DataObject) ->
    encode(?MODULE:put(Key,Value,decode(DataObject))).

%% @doc Remove a key from a data object
-spec remove(key(), data_object()) -> data_object().
remove(Key,DataObject) when is_list(DataObject) ->
  proplists:delete(Key,DataObject);
remove(Key,DataObject) when is_map(DataObject) ->
  maps:remove(Key,DataObject);
remove(Key,DataObject) ->
  encode(remove(Key,decode(DataObject))).

%% @doc Size of a data object, ignoring null values
-spec size(data_object()) -> integer().
size(DataObject) when is_list(DataObject) -> 
    length(clean_null_values(DataObject));
size(DataObject) when is_map(DataObject) -> 
    maps:size(clean_null_values(DataObject));
size(DataObject) -> 
 ?MODULE:size(decode(DataObject)).

%% @doc Returns a complete list of values, in arbitrary order, from data object
-spec values(data_object()) -> list().
values(DataObject) when is_list(DataObject) ->
    [V || {_,V} <- DataObject];
values(DataObject) when is_map(DataObject) ->
    maps:values(DataObject);
values(DataObject) -> 
    values(decode(DataObject)).

%% @doc Convert any format of DataObject to proplist
-spec to_proplist(data_object()) -> list().
to_proplist(DataObject) when is_list(DataObject) ->
    DataObject;
to_proplist(DataObject) when is_map(DataObject) ->
    maps:to_list(DataObject);
to_proplist(DataObject) ->
    to_proplist(decode(DataObject)).

%% @doc Projects a parsed Json Path (from jpparse) on a json object
%% and extracts matched values. nomatch is return if no match is found.
%% This function uses imem_json primitives, and as such is format neutral.
-spec eval(JPPTree :: list() | binary() | tuple()
            , [{binary(),data_object()}]) ->    
    {nomatch, NoMatches} % See below for spec of Details

    % Either parsetree/bind list is malformed
    %  or given jppath can not be folded    
    | {error, Details}

    % Happy path return
    | list(value())

    % spec for Details in {error, {nomatch, Details}}
      when
      Details :: {operation_not_supported, any()}
               | {unimplemented, any()}
               | {unbound, any()}
               | {malformed, any()} % this is only for debugging fold function,
                                    % which shouldn't happen otherwise
               ,
      NoMatches :: {path_not_found, any(), any()}
                 | {property_not_found, any(), any()} % While walking sub-objects
                 | {index_out_of_bounds, any(), any()} % While walking sub-lists
               .
eval(JsonPath,Binds)
  when is_binary(JsonPath) orelse is_list(JsonPath) ->
    {ok, Tree} = jpparse:parsetree(JsonPath),
    eval(Tree,Binds);
eval(Tree,[{Name,Object}|_] = Binds)
  when is_binary(Name), is_binary(Object) ->
    case eval(Tree
               , [{N, decode(O)}
                  || {N,O} <- Binds]) of
        {nomatch, Reason} -> {nomatch, Reason};
        {error, Reason} -> {error, Reason};
        MatchedObject -> encode(MatchedObject)
    end;
eval(Tree,Binds) ->
    case jpparse:foldbu(fun jpp_walk/3
                        , Binds
                        , Tree) of
        {error, {nomatch, Reason}} -> {nomatch, Reason};
        {error, Error} -> {error, Error};
        [{Tree, Object}|_] -> Object;
        Malformed -> {error, {malformed, Malformed}}

    end.

-define(TRACE, Pt = Pt, Binds = Binds).
-ifndef(TRACE).
-define(TRACE,
        io:format(user, "[~p] ~p~n PT   ~p~n BIND ~p~n"
                  , [?LINE, _Depth, Pt, Binds])
       ).
-endif.

jpp_walk(_Depth, {'#',Op,Arg} = Pt, Binds) ->
    ?TRACE,
    case proplists:get_value(Arg,Binds) of
        undefined -> exit({unbound, Arg});
        Obj ->
            case Op of
                <<"keys">>      -> [{Pt, ?MODULE:keys(Obj)} | Binds];
                <<"values">>    -> [{Pt, ?MODULE:values(Obj)} | Binds];
                Op -> exit({operation_not_supported
                            , list_to_binary(["#",Op])})
            end
    end;
jpp_walk(_Depth, {'::',_,_Args} = Pt, Binds) ->
    ?TRACE,
    exit({unimplemented, '::'});
jpp_walk(_Depth, {'fun',_,_Args} = Pt, Binds) ->
    ?TRACE,
    exit({unimplemented, 'fun'});
jpp_walk(_Depth, {'[]',L,Idxs} = Pt, Binds) ->
    ?TRACE,
    case proplists:get_value(L,Binds) of
        undefined -> exit({unbound, L});
        List ->
            % Idxs list is processed in reversed order
            % to append to head of building list
            NewBinds = jpp_walk_list(_Depth, List, lists:reverse(Idxs)
                                     , Pt, Binds),
            % TODO: To be rewritten for maps
            case {NewBinds, Idxs, List} of
                {Binds, [], []}         -> [{Pt, []} | Binds];
                {Binds, [], [[_|_]|_]}  -> [{Pt, []} | Binds];
                {Binds, [], _}          -> exit({nomatch
                                                 , {path_not_found
                                                    , Pt, List}});
                {NewBinds, _, _} -> NewBinds
            end
    end;
jpp_walk(_Depth, {'{}',L,Props} = Pt, Binds) ->
    ?TRACE,
    case proplists:get_value(L, Binds) of
        undefined -> exit({unbound, L});
        Obj ->
            % Props list is processed in reversed order
            % to append to head of building list
            NewBinds = jpp_walk_sub_obj(_Depth, Obj, lists:reverse(Props)
                                        , Pt, Binds),
            % TODO: To be rewritten for maps
            case {NewBinds, Props, Obj} of
                {Binds, [], [{_,_}|_]} -> [{Pt, [{}]} | Binds];
                {Binds, [], _} -> exit({nomatch
                                        , {path_not_found, Pt, Obj}});
                {NewBinds, _, _} -> NewBinds
            end
    end;
jpp_walk(_Depth, {':',R,L} = Pt, Binds)
  when is_binary(R) ->
    ?TRACE,
    case proplists:get_value(L,Binds) of
        undefined -> exit({unbound, L});
        Obj ->            
            V = ?MODULE:get(R,Obj),
            if V =:= undefined ->
%io:format(user,"~p not_found ~p ~p~n", [?LINE, R, Obj]),
                   exit({nomatch, {property_not_found, R, Obj}});
               true ->
%io:format("~p got ~p ~p~n", [?LINE, R, V]),
                   [{Pt, V} | Binds]
            end
    end;
jpp_walk(_Depth, Pt, Binds)
  when is_binary(Pt); is_integer(Pt) ->
    ?TRACE,
    Binds.

% TODO: To be rewritten for maps
jpp_walk_list(_Depth, List, Idxs, Pt, Binds) ->
    ?TRACE,
    lists:foldl(
      fun
          (I, _)
            when is_integer(I) andalso
                 ((I =< 0) orelse (I > length(List))) ->
              exit({nomatch, {index_out_of_bound, I, List}});
          (I, BindAcc) when is_integer(I) ->
              case BindAcc of
                  [{Pt, Lst} | RestBindAcc] ->
                      [{Pt, [lists:nth(I,List) | Lst]}
                       | RestBindAcc];
                  _ -> [{Pt, [lists:nth(I,List)]} | BindAcc]
              end;
          % When Index is a not an integer
          % It is assumed that it represents a
          % JSON path in parsed from. Hence
          % its processed independently with
          % existing binds to reduce it to an integer
          (I, BindAcc) ->
              case eval(I, BindAcc) of
                  {M, BindAcc1}
                    when is_integer(M) andalso
                         (M > 0) andalso
                         (M =< length(List)) ->
                      case BindAcc1 of
                          [{I, Lst} | RestBindAcc1] ->
                              [{I, [lists:nth(M,List) | Lst]}
                               | RestBindAcc1];
                          _ -> [{I, [lists:nth(M,List)]}
                                | BindAcc1]
                      end;
                  _ ->
                      exit({nomatch
                            , {path_not_found, I, BindAcc}})
              end
      end
      , Binds
      , Idxs).

% TODO: To be rewritten for maps
jpp_walk_sub_obj(_Depth, Obj, Props, Pt, Binds) ->
    ?TRACE,
    lists:foldl(
      fun
          (P, BindAcc) when is_binary(P) ->
              case ?MODULE:get(P,Obj) of
                  undefined ->
                      exit({nomatch
                            , {property_not_found, P, Obj}});
                  V ->
%io:format(user,"~p got ~p ~p~n", [?LINE, P, V]),
                      case BindAcc of
                          [{Pt, OldBind} | RestBindAcc] ->
                              [{Pt, [{P, V}|OldBind]}
                               | RestBindAcc];
                          _ ->
                              [{Pt, [{P, V}]} | BindAcc]
                      end
              end;
          % When Index is a not a binary
          % it is assumed that it represents a
          % JSON path in parsed from. Hence
          % its processed independently with
          % existing binds to reduce it to binary
          (P, BindAcc) ->
              case eval(P, BindAcc) of
                  {M, BindAcc1} when is_binary(M) ->
                      case ?MODULE:get(M,Obj) of
                          undefined ->
                              exit({nomatch
                                    , {property_not_found
                                       , M, Obj}});
                          V ->
io:format(user,"~p got ~p ~p~n", [?LINE, P, V]),
                              case BindAcc1 of
                                  [{P, OldBind1}
                                   | RestBindAcc1] ->
                                      [{P, [{M, V}|OldBind1]}
                                       | RestBindAcc1];
                                  _ ->
                                      [{P, [{M, V}]}
                                       | BindAcc1]
                              end
                      end;
                  _ ->
                      exit({nomatch
                            , {path_not_found, P, BindAcc}})
              end
      end
      , Binds
      , Props).

-spec clean_null_values(data_object()) -> data_object().
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
    encode(clean_null_values(decode(DataObject))).

%% @doc recursively replaces property names matching 'inline_*' pattern in
%% Root (json object) which corresponding bind from Binds lists and returns the
%% fully expanded object. Used for default lookup and hierarchical overrrides.
%% New Root is re-scanned after each replacement until new head doesn't change
%% anymore. Return types are preserved to type of input Root parameter.
-spec expand_inline(Root :: data_object(),
                    Binds :: [{any(), data_object()}]) -> data_object().
expand_inline(Root, Binds) when is_binary(Root) ->
    encode(expand_inline(decode(Root), Binds));
expand_inline(Root, Binds) ->
    expand_inline(Root, undefined, [if is_map(Root) ->
                    case V of
                        V when is_binary(V) -> {K, decode(V, [return_maps])};
                        V when is_map(V) -> {K, V};
                        [{_,_}|_] -> {K, decode(encode(V), [return_maps])}
                    end;
                true ->
                    case V of
                        V when is_binary(V) -> {K, decode(V)};
                        [{_,_}|_] -> {K, V};
                        V when is_map(V) -> {K, decode(encode(V))}
                    end
             end || {K, V} <- Binds]).

expand_inline(Root, Root, _) -> Root;
expand_inline(Root, _OldRoot, Binds) when is_map(Root) ->
    expand_inline(
      maps:fold(
        fun(K, V, AccIn) ->
                case re:run(K, "^inline_*") of
                    nomatch -> AccIn;
                    _ -> case proplists:get_value(V, Binds, '$not_found') of
                             '$not_found' -> maps:remove(K, AccIn);
                             ValObj -> maps:merge(ValObj, maps:remove(K, AccIn))
                         end
                end
        end, Root, Root),
      Root, Binds);
expand_inline(Root, _OldRoot, Binds) ->
    expand_inline(
      lists:foldl(
        fun({K, V}, AccIn) ->
                case re:run(K, "^inline_*") of
                    nomatch -> AccIn;
                    _ -> case proplists:get_value(V, Binds, '$not_found') of
                             '$not_found' -> lists:keydelete(K, 1, AccIn);
                             ValObj ->
                                 lists:foldl(
                                   fun({ValObjK, ValObjV}, AccInI) ->
                                           case proplists:get_value(ValObjK, AccInI, '$not_found') of
                                               '$not_found' -> [{ValObjK, ValObjV} | AccInI];
                                               _ -> AccInI
                                           end
                                   end, lists:keydelete(K, 1, AccIn), ValObj)
                         end
                end
        end, Root, Root),
      Root, Binds).

%% TODO Code for delete candidate APIs
%% -
%% - %% @doc Standard fold function, returns accumulator. null values are ignored.
%% - %% Fun = fun((K, V, AccIn) -> AccOut)
%% - -spec fold(fun(), Init, data_object()) -> Acc when Init :: term(), Acc :: term().
%% - fold(Fun,Init,DataObject) when is_list(DataObject) -> 
%% -     lists:foldl(fun({K,V},Acc) -> Fun(K,V,Acc) end,
%% -                 Init,
%% -                 proplists:unfold(clean_null_values(DataObject)));
%% - fold(Fun,Init,DataObject) when is_map(DataObject) -> 
%% -     maps:fold(Fun,Init,clean_null_values(DataObject));
%% - fold(Fun,Init,DataObject) -> 
%% -     fold(Fun,Init,decode(DataObject)).
%% -
%% - %% @doc Check if data object has key
%% - -spec has_key(key(), data_object()) -> boolean().
%% - has_key(Key,DataObject) when is_list(DataObject) ->
%% -     proplists:is_defined(Key,DataObject);
%% - has_key(Key,DataObject) when is_map(DataObject) ->
%% -     maps:is_key(Key,DataObject);
%% - has_key(Key,DataObject) -> 
%% -     has_key(Key,decode(DataObject)).
%% -
%% - %% @doc Produces a new data object by calling the function fun F(K, V1) 
%% - %% for every K to value V1 association in data object in arbitrary order. 
%% - %% The function fun F/2 must return the value V2 to be associated with key K 
%% - %% for the new data object. 
%% - %% null values are ignored and not carried over to new data object.
%% - -spec map(fun(), data_object()) -> data_object().
%% - map(Fun,DataObject) when is_list(DataObject) ->
%% -     lists:map(fun({K,V}) -> {K,Fun(K,V)} end,
%% -               proplists:unfold(DataObject));
%% - map(Fun,DataObject) when is_map(DataObject) ->
%% -     maps:map(Fun,clean_null_values(DataObject));
%% - map(Fun,DataObject) ->
%% -     encode(map(Fun,decode(DataObject))).
%% -
%% - %% @doc Create new imem json data object using default type
%% - -spec new() -> data_object().
%% - new() -> new(proplist).
%% - 
%% - %% @doc Create new imen json data object
%% - -spec new(Type) -> data_object() when Type :: json | proplist | map.
%% - new(proplist) -> [];
%% - new(map) -> #{};
%% - new(json) -> <<"{}">>.
%% -
%% - %% @doc Update a key with a new value. If key is not present, 
%% - %% error:badarg exception is raised.
%% - -spec update(key(),value(),data_object()) -> data_object().
%% - update(Key,Value,DataObject) when is_list(DataObject) ->
%% -     case proplists:is_defined(Key,DataObject) of
%% -         true -> [{Key,Value} | proplists:delete(Key,DataObject)];
%% -         false -> erlang:error(badarg)
%% -     end;
%% - update(Key,Value,DataObject) when is_map(DataObject) ->
%% -     %% Maps update raises error:badarg by default
%% -     %% if key is not present
%% -     maps:update(Key,Value,DataObject);
%% - update(Key,Value,DataObject) ->
%% -     encode(update(Key,Value,decode(DataObject))).
%% -
%% - %% @doc Returns a complete list of items (objects) , in arbitrary order, from a data object or array
%% - -spec items(data_object()) -> list().
%% - items(DataObject) when is_list(DataObject) ->
%% -     DataObject;
%% - items(DataObject) when is_map(DataObject) ->
%% -     maps:to_list(DataObject);
%% - items(DataObject) -> 
%% -     items(decode(DataObject)).
%% -
%% - to_proplist(DataObject,deep) when is_list(DataObject)-> DataObject;
%% - to_proplist(DataObject,deep) when is_map(DataObject) ->
%% -     maps:to_list(
%% -         maps:map(fun(_K,_V) when is_map(_V) ->
%% -                     to_proplist(_V,deep);
%% -                 (_K,_V) -> _V end,
%% -              DataObject));
%% - to_proplist(DataObject,deep) when is_binary(DataObject) ->
%% -     to_proplist(decode(DataObject)).
%% -
%% - %% @doc Convert any format of DataObject to map
%% - -spec to_map(data_object()) -> map().
%% - to_map(DataObject) when is_list(DataObject) ->
%% -     maps:from_list(DataObject);
%% - to_map(DataObject) when is_map(DataObject) ->
%% -     DataObject;
%% - to_map(DataObject) ->
%% -     to_map(decode(DataObject)).
%% -
%% - %% @doc Convert any format of DataObject to binary JSON
%% - -spec to_binary(data_object()) -> binary().
%% - to_binary(DataObject) when is_binary(DataObject)-> DataObject;
%% - to_binary(DataObject) -> encode(DataObject).
%% -
%% - %% @doc Builds a new data object with key/value pairs from provided data
%% - %% object according if boolean filter function (fun(K,V) -> true | false) 
%% - %% return true.
%% - -spec filter(fun(),data_object()) -> data_object().
%% - filter(Fun,DataObject) when is_list(DataObject) ->
%% -     lists:foldl(fun({K,V},In) ->
%% -                     case Fun(K,V) of
%% -                         true -> [{K,V}|In];
%% -                         false -> In
%% -                     end end,
%% -                 [],
%% -                 DataObject);
%% - filter(Fun,DataObject) when is_map(DataObject) ->
%% -     maps:fold(fun(K,V,In) ->
%% -                     case Fun(K,V) of
%% -                         true -> maps:put(K,V,In);
%% -                         false -> In
%% -                     end end,
%% -              #{},
%% -              DataObject);
%% - filter(Fun,DataObject) ->
%% -     encode(filter(Fun,decode(DataObject))).
%% - 
%% - %% @doc Build data object with provided keys and associated values from
%% - %% provided data object
%% - -spec include(Keys,data_object()) -> data_object() when Keys :: [key() | Keys].
%% - include(Keys,DataObject) when is_list(DataObject) -> 
%% -     [{K,proplists:get_value(K,DataObject)} || K <- Keys];
%% - include(Keys,DataObject) when is_map(DataObject) -> 
%% -     lists:foldl(fun(K,MapIn) ->
%% -                     maps:put(K,maps:get(K,DataObject),MapIn) end,
%% -                 #{},
%% -                 Keys);
%% - include(Keys,DataObject) ->
%% -     %binary_include(Keys,DataObject).
%% -     encode(include(Keys,decode(DataObject))).
%% - 
%% - %% @doc Build data object with data from provided data object, excluding
%% - %% the elements associated with provided keys
%% - -spec exclude(Keys,data_object()) -> data_object() when Keys :: [key() | Keys].
%% - exclude(Keys,DataObject) when is_list(DataObject) -> 
%% -     [{K,V} || {K,V} <- DataObject, lists:member(K,Keys) =:= false];
%% - exclude(Keys,DataObject) when is_map(DataObject) -> 
%% -     maps:fold(fun(K,V,MapIn) ->
%% -                 case lists:member(K,Keys) of
%% -                     true -> MapIn;
%% -                     false -> maps:put(K,V,MapIn)
%% -                 end end,
%% -              #{},
%% -              DataObject);
%% - exclude(Keys,DataObject) ->
%% -     encode(exclude(Keys,decode(DataObject))).
%% - 
%% - %% @doc Merges two data objects into a data object. 
%% - %% If two keys exists in both data objects  the value in the first data object
%% - %% will be superseded by the value in the second data object, unless the value 
%% - %% of the second data object is null.
%% - -spec merge(data_object(),data_object()) -> data_object().
%% - merge(DataObject1,DataObject2) when is_list(DataObject1), is_list(DataObject2) ->
%% -     CleanObject2 = clean_null_values(DataObject2),
%% -     Object2Keys = proplists:get_keys(CleanObject2),
%% -     CleanObject2 ++ [{K,V} || {K,V} <- DataObject1, lists:member(K,Object2Keys) =:= false];
%% - merge(DataObject1,DataObject2) when is_map(DataObject1), is_map(DataObject2) ->
%% -     maps:merge(DataObject1,clean_null_values(DataObject2));
%% - merge(DataObject1,DataObject2) ->
%% -     encode(merge(decode(DataObject1),decode(DataObject2))).

%% @doc creates a diff data object between the first (v1) and second (v2) provided JSON/map objects.
%% Resulting data object will have the merged root keys of both objects but only for
%% attributes with differences. 
-spec diff(data_object(), data_object()) -> data_object().
diff(Data1, Data2) when is_list(Data1), is_list(Data2) ->
    diff(lists:sort(Data1), lists:sort(Data2),[]);
diff(Data1, undefined) when is_list(Data1) ->
    diff(lists:sort(Data1), []);
diff(Data1, Data2) when is_map(Data1), is_map(Data2) ->
    maps:from_list(diff(maps:to_list(Data1), maps:to_list(Data2)));
diff(Data1, undefined) when is_map(Data1) ->
    maps:from_list(diff(maps:to_list(Data1), []));
diff(Data1, Data2) when is_map(Data1), is_list(Data2) ->
    maps:from_list(diff(maps:to_list(Data1), Data2));
diff(Data1, Data2) when is_map(Data1), is_binary(Data2) ->
    maps:from_list(diff(maps:to_list(Data1), decode(Data2)));
diff(Data1, Data2) when is_list(Data1), is_map(Data2) ->
    diff(Data1, maps:to_list(Data2));
diff(Data1, Data2) when is_list(Data1), is_binary(Data2) ->
    diff(Data1,decode(Data2));
diff(Data1, Data2) when is_binary(Data1), is_binary(Data2) ->
    encode(diff(decode(Data1), decode(Data2)));
diff(Data1, undefined) when is_binary(Data1) ->
    encode(diff(decode(Data1), []));
diff(Data1, Data2) when is_binary(Data1), is_list(Data2) ->
    encode(diff(decode(Data1), Data2));
diff(Data1, Data2) when is_binary(Data1), is_map(Data2) ->
    encode(diff(decode(Data1), maps:to_list(Data2)));
diff(undefined, Data2) when is_list(Data2) ->
    diff([], lists:sort(Data2),[]);
diff(undefined, Data2) when is_map(Data2) ->
    maps:from_list(diff([], maps:to_list(Data2)));
diff(undefined, Data2) when is_binary(Data2) ->
    encode(diff([], decode(Data2)));
diff(undefined, undefined) -> [].

diff([{}], B, Acc) -> diff([], B, Acc);
diff(A, [{}], Acc) -> diff(A, [], Acc);
diff([], [], Acc) -> lists:reverse(Acc);
diff([A|R1], [A|R2], Acc) ->  diff(R1,R2,Acc);
diff([{_,null}|R1], [], Acc) ->  diff(R1, [], Acc);                               % missing equals null
diff([{A,AV}|R1], [], Acc) ->  diff(R1, [], [{A,[{<<"v1">>,AV},{<<"v2">>,null}]} | Acc]);
diff([], [{_,null}|R2], Acc) ->  diff([], R2, Acc);                               % missing equals null
diff([], [{B,BV}|R2], Acc) ->  diff([], R2, [{B,[{<<"v1">>,null},{<<"v2">>,BV}]} | Acc]);
diff([{A,AV}|R1], [{A,BV}|R2], Acc) ->  diff(R1, R2, [{A,[{<<"v1">>,AV},{<<"v2">>,BV}]} | Acc]);
diff([{A,null}|R1], [{B,BV}|R2], Acc) when A < B ->  diff(R1, [{B,BV}|R2], Acc);  % missing equals null
diff([{A,AV}|R1], [{B,BV}|R2], Acc) when A < B ->  diff(R1, [{B,BV}|R2], [{A,[{<<"v1">>,AV},{<<"v2">>,null}]} | Acc]);
diff([{A,AV}|R1], [{B,null}|R2], Acc) when A > B ->  diff([{A,AV}|R1], R2, Acc);  % missing equals null
diff([{A,AV}|R1], [{B,BV}|R2], Acc) when A > B ->  diff([{A,AV}|R1], R2, [{B,[{<<"v1">>,null},{<<"v2">>,BV}]} | Acc]).

%% - 
%% - %% @doc creates a diff data object between the first (old) and second (new) provided data
%% - %% objects. null values are ignored. Resulting data object will have 5 root keys, each containg
%% - %% corresponding key/value pairs. Outgoing format is the same as ingoing format.
%% - %%
%% - %% added : contains key/value pairs who were in data object 2 but not in data object 1
%% - %% removed : contains key/value pairs who were in data object 1 but not in data objec 2
%% - %% updated : containts key/value pairs who are not indentical between data object 1 and 2,
%% - %%           with the values from data object 2
%% - %% replaced : containts key/value pairs who are not indentical between data object 1 and 2,
%% - %%            with the values from data object 1
%% - -spec diff(data_object(), data_object()) -> diff_object().
%% - diff(RawDataObject1, Data2) when is_list(RawDataObject1), is_list(RawDataObject2) ->
%% -     DataObject1 = clean_null_values(RawDataObject1),
%% -     DataObject2 = clean_null_values(RawDataObject2),
%% -     Object1Keys = proplists:get_keys(DataObject1),
%% -     Object2Keys = proplists:get_keys(DataObject2),
%% -     CommonKeys = lists:filter(fun(K) -> lists:member(K,Object1Keys) end, Object2Keys),
%% - 
%% -     UpdatedJob = fun(K,Acc) -> 
%% -                     case {proplists:get_value(K,DataObject1),
%% -                           proplists:get_value(K,DataObject2)} of
%% -                           {__Same,__Same} -> Acc;
%% -                           {__Old,__New}   -> [{K,__Old,__New} | Acc]
%% -                     end end,
%% -     UpdatedTuples = lists:foldl(UpdatedJob,
%% -                                 [],
%% -                                 CommonKeys),
%% - 
%% -     Added = lists:filter(fun({K,_}) -> lists:member(K,Object1Keys) =:= false end, DataObject2),
%% -     Removed = lists:filter(fun({K,_}) -> lists:member(K,Object2Keys) =:= false end, DataObject1),
%% -     Updated = [{K,V} || {K,_,V} <- UpdatedTuples],
%% -     Replaced = [{K,V} || {K,V,_} <- UpdatedTuples],
%% - 
%% -     [{added,Added},
%% -      {removed,Removed},
%% -      {updated,Updated},
%% -      {replaced,Replaced},
%% -      {changes, length(Added) + length(Removed) + length(Updated)}];
%% - diff(RawDataObject1, RawDataObject2) when is_map(RawDataObject1), is_map(RawDataObject2) ->
%% -     DataObject1 = clean_null_values(RawDataObject1),
%% -     DataObject2 = clean_null_values(RawDataObject2),
%% - 
%% -     Object1Keys = maps:keys(DataObject1),
%% -     Object2Keys = maps:keys(DataObject2),
%% -     CommonKeys = lists:filter(fun(K) -> lists:member(K,Object1Keys) end, Object2Keys),
%% - 
%% -     UpdatedJob = fun(K,Acc) -> 
%% -                     case {maps:get(K,DataObject1),
%% -                           maps:get(K,DataObject2)} of
%% -                           {__Same,__Same} -> Acc;
%% -                           {__Old,__New}   -> [{K,__Old,__New} | Acc]
%% -                     end end,
%% -     UpdatedTuples = lists:foldl(UpdatedJob,
%% -                                 [],
%% -                                 CommonKeys),
%% -     Added = lists:foldl(fun(K,MapIn) -> 
%% -                          case lists:member(K,Object1Keys) of
%% -                                 false -> maps:put(K,maps:get(K,DataObject2),MapIn);
%% -                                 true  -> MapIn
%% -                          end end,
%% -                         #{},
%% -                         Object2Keys),
%% -     Removed = lists:foldl(fun(K,MapIn) -> 
%% -                            case lists:member(K,Object2Keys) of
%% -                                   false -> maps:put(K,maps:get(K,DataObject1),MapIn);
%% -                                   true  -> MapIn
%% -                            end end,
%% -                           #{},
%% -                           Object1Keys),
%% -     Updated = lists:foldl(fun({K,_,V},MapIn) -> maps:put(K,V,MapIn) end,
%% -                           #{},
%% -                           UpdatedTuples),
%% -     Replaced = lists:foldl(fun({K,V,_},MapIn) -> maps:put(K,V,MapIn) end,
%% -                            #{},
%% -                            UpdatedTuples),
%% - 
%% -     #{added => Added,
%% -       removed => Removed,
%% -       updated => Updated,
%% -       replaced => Replaced,
%% -       changes =>  maps:size(Added) + maps:size(Removed) + maps:size(Updated)};
%% - diff(DataObject1, DataObject2) ->
%% -     encode(diff(decode(DataObject1),decode(DataObject2))).
%% - 
%% - %% @doc Compare data objects for equality (null values are ignored)
%% - -spec equal(data_object(), data_object()) -> boolean().
%% - equal(DataObject1, DataObject2) when is_list(DataObject1), is_list(DataObject2) -> 
%% -     lists:sort(clean_null_values(DataObject1)) =:= lists:sort(clean_null_values(DataObject2));
%% - equal(DataObject1, DataObject2) when is_map(DataObject1), is_map(DataObject2) ->
%% -     clean_null_values(DataObject1) =:= clean_null_values(DataObject2);
%% - equal(DataObject1, DataObject2) ->
%% -     equal(decode(DataObject1),decode(DataObject2)).
%% - 
%% - 
%% - %% @doc Returns true when every element of data_object1 is also a member of data_object2, otherwise false.
%% - -spec is_subset(data_object(), data_object()) -> boolean().
%% - is_subset(DataObject1, DataObject2) when is_list(DataObject1), is_list(DataObject2) ->
%% -     Is_Member = fun ({_,null}) -> true;
%% -                     ({K1,V1}) -> V1 =:= proplists:get_value(K1,DataObject2) end,
%% -     lists:all(Is_Member,DataObject1);
%% - is_subset(DataObject1, DataObject2) when is_map(DataObject1), is_map(DataObject2) ->
%% -     Is_Member = fun(_,_,false) -> false;
%% -                    (_,null,true) -> true;
%% -                    (K1,V1,true) -> {ok,V1} =:= maps:find(K1,DataObject2)
%% -                    end,
%% -     maps:fold(Is_Member,true,DataObject1);
%% - is_subset(DataObject1, DataObject2) ->
%% -     is_subset(decode(DataObject1), decode(DataObject2)).
%% - 
%% - 
%% - %% @doc Returns true when no element of data_object1 is a member of data_object2, otherwise false.
%% - -spec is_disjoint(data_object(), data_object()) -> boolean().
%% - is_disjoint(DataObject1, DataObject2) when is_list(DataObject1), is_list(DataObject2) ->
%% -     Is_Member = fun({K1,V1}) ->
%% -                     V1 =:= proplists:get_value(K1,DataObject2)
%% -                     end,
%% -     not lists:any(Is_Member,DataObject1);
%% - is_disjoint(DataObject1, DataObject2) when is_map(DataObject1), is_map(DataObject2) ->
%% -     No_Common_Element = fun(_,_,false) -> false;
%% -                         (K1,V1,true) -> {ok,V1} =/= maps:find(K1,DataObject2)
%% -                    end,
%% -     maps:fold(No_Common_Element,true,DataObject1);
%% - is_disjoint(DataObject1, DataObject2) ->
%% -     is_disjoint(decode(DataObject1), decode(DataObject2)).
%% - 
%% - %% @doc Projection of a path, or of multiple paths, from a Json nested object.
%% - %% returns nomatch if path is not found, or if one of the paths is not found.
%% - %%
%% - %% Because projection used imem_json primitives, function is format-neutral.
%% - -spec project(Path,data_object()) -> value() | ListOfValues | nomatch 
%% -            when Path :: list() | binary(),
%% -                 ListOfValues :: [value() | ListOfValues].
%% - project([T|_] = Paths,DataObject) when is_list(T) ->
%% -     All = [walk_path(parse_path(Path),DataObject) || Path <- Paths],
%% -     case lists:any(fun(nomatch) -> true; (_) -> false end,All) of
%% -         true -> nomatch;
%% -         false -> All
%% -     end;
%% - project(Path,DataObject) ->
%% -     walk_path(parse_path(Path),DataObject).
%% - 
%% - %% @doc Tokenizes a path in a series of tokens
%% - -spec parse_path(Path) -> list()
%% -             when Path :: list() | binary().
%% - parse_path(Path)  ->
%% -     parse_path(Path,?JSON_PATH_SEPARATOR).
%% - 
%% - -spec parse_path(Path,list()) -> list()
%% -             when Path :: list() | binary().
%% - parse_path(Path,Separator) when is_list(Path) ->
%% -     string:tokens(Path,Separator);
%% - parse_path(Path,Separator) when is_binary(Path) ->
%% -     parse_path(binary_to_list(Path),Separator).
%% - 
%% - %% @doc Walks a tokenized path to extract the corresponding values
%% - -spec walk_path(list(),data_object()) -> value() | ListOfValues | nomatch
%% -             when ListOfValues :: [value() | ListOfValues].
%% - walk_path([],LastLevel) -> LastLevel;
%% - walk_path(["$keys$"],CurrentLevel) ->
%% -     keys(CurrentLevel);
%% - walk_path([Filter|Tail],CurrentLevel) ->
%% -     {ok,Mask} = re:compile("^(.*)\\[([^\\]]+)\\]$"),
%% -     case re:run(Filter,Mask,[{capture,[2],list}]) of
%% -         nomatch -> NextLevel = ?MODULE:get(list_to_binary(Filter),CurrentLevel),
%% -                    case ?MODULE:get(list_to_binary(Filter),CurrentLevel) of
%% -                     undefined -> nomatch;
%% -                     NextLevel -> walk_path(Tail,NextLevel)
%% -                    end;
%% -         {match,["*"]} -> {match,[Field]} = re:run(Filter,Mask,[{capture,[1],binary}]),
%% -                          case ?MODULE:get(Field,CurrentLevel) of
%% -                             undefined -> nomatch;
%% -                             [] -> nomatch;
%% -                             NextLevels -> lists:flatten([walk_path(Tail,NextLevel) || 
%% -                                             NextLevel <- NextLevels])
%% -                          end;
%% -         {match,[_lstIndex]} -> {match,[Field]} = re:run(Filter,Mask,[{capture,[1],binary}]),
%% -                                Index = list_to_integer(_lstIndex),
%% -                                case ?MODULE:get(Field,CurrentLevel) of
%% -                                 undefined -> nomatch;
%% -                                 [] -> nomatch;
%% -                                 List -> Length = length(List),
%% -                                         if Index > Length -> nomatch;
%% -                                            true -> NextLevel = lists:nth(Index,List),
%% -                                                    walk_path(Tail,NextLevel)
%% -                                         end
%% -                                end
%% -     end.

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

-ifdef(TEST).

-include_lib("eunit/include/eunit.hrl").

%% Testing available when maps are enabled, testing code not duplicated
%% Latest code coverage check: 100%

-define(TEST_JSON, <<"{\"surname\":\"Doe\",\"name\":\"John\",\"foo\":\"bar\","
                     "\"earthling\":true,\"age\":981,\"empty\":null}">>).
-define(TEST_PROP, [{<<"surname">>,<<"Doe">>},{<<"name">>,<<"John">>},
                    {<<"foo">>,<<"bar">>},{<<"earthling">>,true},
                    {<<"age">>,981},{<<"empty">>,null}]).
-define(TEST_LIST_PROP, [[{<<"surname">>,<<"Doe">>},{<<"name">>,<<"John">>}],
                    [{<<"surname">>,<<"Jabe">>},{<<"name">>,<<"John">>}]]).
-define(TEST_JSON_LIST, <<"[{\"surname\":\"Doe\"},{\"surname\":\"Jane\"},"
                          "{\"surname\":\"DoeDoe\"}]">>).
-define(TEST_LACON, <<"[[\"50\",\"LACON\",\"29\"],[\"95\",\"LACON\",\"08\""
                      "]]">>).
-define(TEST_MAP,#{<<"age">> => 981, <<"earthling">> => true,
                   <<"foo">> => <<"bar">>, <<"name">> => <<"John">>,
                   <<"surname">> => <<"Doe">>,<<"empty">> => null}).

%% expand_inline tests
expand_inline_test_() ->
    {inparallel,
     [{Title, ?_assertEqual(Expected, expand_inline(Root, Binds))}
      || {Title,Expected,Root,Binds} <-
         [ % Mixed format tests
           {"map_mixed_bind",
            #{<<"a">>=>1,<<"b">>=>2,<<"c">>=>3,<<"d">>=>4},
            #{<<"a">>=>1,<<"inline_bin">> =>[1],<<"inline_pl">>=>[2],
              <<"inline_map">>=>[3]},
            [{[1], <<"{\"b\":2}">>}, {[2], [{<<"c">>,3}]},
             {[3], #{<<"d">> => 4}}]},
           {"pl_mixed_bind",
            [{<<"d">>,4},{<<"c">>,3},{<<"b">>,2},{<<"a">>,1}],
            [{<<"a">>,1},{<<"inline_bin">>,[1]},{<<"inline_pl">>,[2]},
             {<<"inline_map">>,[3]}],
            [{[1], <<"{\"b\":2}">>}, {[2], [{<<"c">>,3}]},
             {[3], #{<<"d">> => 4}}]},
           {"bin_mixed_bind",
            <<"{\"d\":4,\"c\":3,\"b\":2,\"a\":1}">>,
            <<"{\"a\":1,\"inline_bin\":[1],\"inline_pl\":[2],"
              "\"inline_map\":[3]}">>,
            [{[1], <<"{\"b\":2}">>}, {[2], [{<<"c">>,3}]},
             {[3], #{<<"d">> => 4}}]},

           %
           % expand tests override/add
           %

           % (map)
           {"map_add_override",
            #{<<"a">>=>1,<<"b">>=>2},
            #{<<"a">>=>1,<<"inline_map">>=>[1]},
            [{[1], #{<<"a">>=>3,<<"b">>=>2}}]},
           {"map_inline_inline",
            #{<<"a">>=>1,<<"b">>=>2,<<"c">>=>3},
            #{<<"a">>=>1,<<"inline_map">>=>[1]},
            [{[1], #{<<"b">>=>2,<<"inline_map1">>=>[2]}},
             {[2], #{<<"c">>=>3}}]},

           % (proplists)
           {"pl_add_override",
            [{<<"b">>,2},{<<"a">>,1}],
            [{<<"a">>,1},{<<"inline_map">>,[1]}],
            [{[1], [{<<"a">>,3},{<<"b">>,2}]}]},
           {"pl_inline_inline",
            [{<<"c">>,3},{<<"b">>,2},{<<"a">>,1}],
            [{<<"a">>,1},{<<"inline_map">>,[1]}],
            [{[1], [{<<"b">>,2},{<<"inline_map1">>,[2]}]},
             {[2], [{<<"c">>,3}]}]},

           % (binary)
           {"bin_add_override",
            <<"{\"b\":2,\"a\":1}">>,
            <<"{\"a\":1,\"inline_map\":[1]}">>,
            [{[1], <<"{\"a\":3,\"b\":2}">>}]},
           {"bin_inline_inline",
            <<"{\"c\":3,\"b\":2,\"a\":1}">>,
            <<"{\"a\":1,\"inline_map\":[1]}">>,
            [{[1], <<"{\"b\":2,\"inline_map1\":[2]}">>},
             {[2], <<"{\"c\":3}">>}]}
         ]
     ]
    }.


%% JSON is tested for each function as well even if, basically, it only tests the 
%% JSON to map/proplist conversion each time. This could at least be used as regression
%% testing should JSON data be handled differently.

find_test_() ->
    Tests = [
             {<<"{\"a\":\"b\"}">>,              <<"a">>,    <<"b">>},
             {<<"{\"a\":1}">>,                  a,          1},
             {<<"{\"a\":\"1\",\"b\":\"2\"}">>,  a,          <<"1">>},
             {<<"{\"a\":null}">>,               "a",        null},
             {<<"{\"a\":true}">>,               "a",        true},
             {<<"{\"a\":false}">>,              "a",        false},
             {<<"{\"a\":[{\"a\":1}]}">>,        "a",        1},
             {<<"{\"r\":\"{\\\"a\\\":1}\","
                 "\"b\":{\"a\":-2.0}}">>,       "a",        -2.0}
            ],
    {inparallel,
     [{"find_test_"++integer_to_list(I),
       fun() ->
               PL = decode(B),
               Map = decode(B, [return_maps]),
               ?assertEqual(V, find(K,B)),
               ?assertEqual(V, find(K,PL)),
               ?assertEqual(V, find(K,Map))
       end} || {I,{B,K,V}} <- lists:zip(lists:seq(1,length(Tests)), Tests)]}.

get_test_() ->
    {inparallel
     , [{"proplist_success"
         , ?_assertEqual(<<"Doe">>, ?MODULE:get(<<"surname">>,?TEST_PROP))}
        , {"map_success"
           , ?_assertEqual(<<"Doe">>, ?MODULE:get(<<"surname">>,?TEST_MAP))}
        , {"json_success"
           , ?_assertEqual(<<"Doe">>, ?MODULE:get(<<"surname">>,?TEST_JSON))}
        , {"proplist_fail"
           , ?_assertEqual(undefined, ?MODULE:get(<<"sme">>,?TEST_PROP))}
        , {"map_fail"
           , ?_assertEqual(undefined, ?MODULE:get(<<"sme">>,?TEST_MAP))}
        , {"json_fail"
           , ?_assertEqual(undefined, ?MODULE:get(<<"sme">>,?TEST_JSON))}
        , {"proplist_custom_default"
           , ?_assertEqual(test, ?MODULE:get(<<"sme">>,?TEST_PROP,test))}
        , {"map_custom_default"
           , ?_assertEqual(test, ?MODULE:get(<<"sme">>,?TEST_MAP,test))}
        , {"json_custom_default"
           , ?_assertEqual(test, ?MODULE:get(<<"sme">>,?TEST_JSON,test))}
        , {"list_prop_list"
           , ?_assertEqual([<<"Doe">>,<<"Jabe">>], ?MODULE:get(<<"surname">>, ?TEST_LIST_PROP))}]}.

keys_test_() ->
    Keys = [<<"surname">>,<<"name">>,<<"foo">>
                       ,<<"earthling">>,<<"age">>,<<"empty">>], % lists:sort
    {inparallel
     , [{"proplist", ?_assertEqual(Keys,keys(?TEST_PROP))}
        , {"map", ?_assertEqual(lists:sort(Keys),lists:sort(keys(?TEST_MAP)))}
        , {"json", ?_assertEqual(Keys,keys(?TEST_JSON))}]}.

put_test_() -> 
    {inparallel
     , [{"prop_empty",?_assertEqual([{test,1}],?MODULE:put(test,1,[]))}
        , {"prop_replace"
           ,?_assertEqual([{test,1}],?MODULE:put(test,1,[{test,2}]))}
        , {"map_empty",?_assertEqual(#{test => 1},?MODULE:put(test,1,#{}))}
        , {"map_replace"
           ,?_assertEqual(#{test => 1},?MODULE:put(test,1,#{test => 2}))}
        , {"json_empty"
           ,?_assertEqual(<<"{\"test\":1}">>
                          ,?MODULE:put(<<"test">>,1,<<"{}">>))}
        , {"json_replace"
           ,?_assertEqual(<<"{\"test\":1}">>
                          ,?MODULE:put(<<"test">>,1,<<"{\"test\":2}">>))}
       ]}.

size_test_() -> 
    {inparallel
     , [{"proplist",?_assertEqual(5,?MODULE:size(?TEST_PROP))}
        , {"map",?_assertEqual(5,?MODULE:size(?TEST_MAP))}
        , {"json",?_assertEqual(5,?MODULE:size(?TEST_JSON))}
        , {"json_list",?_assertEqual(3,?MODULE:size(?TEST_JSON_LIST))}]}.


values_test_() ->
    Values = lists:sort([<<"Doe">>,<<"John">>,<<"bar">>,true,981,null]),
    {inparallel
     , [{"proplist",?_assertEqual(Values,lists:sort(values(?TEST_PROP)))}
        , {"map",?_assertEqual(Values,lists:sort(values(?TEST_MAP)))}
        , {"json",?_assertEqual(Values,lists:sort(values(?TEST_JSON)))}]}.

to_proplist_test_() ->
    {inparallel
     , [{"proplist",?_assert(is_list(to_proplist(?TEST_PROP)))}
        , {"map",?_assert(is_list(to_proplist(?TEST_MAP)))}
        , {"json",?_assert(is_list(to_proplist(?TEST_JSON)))}]}.

eval_test_() ->
    Object = <<"
        { \"a1\": 123
        , \"a2\": \"abcd\"
        , \"a3\": [1,2,3]
        , \"a4\": {\"b1\":456}
        , \"a5\": {\"b1\":\"string\"}
        , \"a6\": [ {\"b1\":1}
                  , {\"b1\":2}
                  , {\"b1\":3}]
        }
    ">>,
    Bind = [{<<"root">>, Object}],
    {inparallel
     , [{P,?_assertEqual(
              O, err_to_atom(eval(P,Bind)))}
        || {P,O} <-
           [ % basic hirarchical walk
             {"root{}",             <<"{}">>}
           , {"root[]",             path_not_found}
           , {"root#keys",          <<"[\"a1\",\"a2\",\"a3\",\"a4\","
                                      "\"a5\",\"a6\"]">>}
           , {"root{a6}#keys",      <<"[\"a6\"]">>}
           , {"root{a2,a6}#keys",   <<"[\"a2\",\"a6\"]">>}
           , {"root#values",        <<"[123,\"abcd\",[1,2,3],"
                                      "{\"b1\":456},{\"b1\":\"string\"}"
                                      ",[{\"b1\":1},{\"b1\":2},"
                                      "{\"b1\":3}]]">>}
           , {"root:a6[]",          <<"[]">>}
           , {"root:a6{}",          path_not_found}
           , {"root:a1",            <<"123">>}
           , {"root:a2",            <<"\"abcd\"">>}
           , {"root:a3",            <<"[1,2,3]">>}
           , {"root:a4",            <<"{\"b1\":456}">>}
           , {"root:a4:b1",         <<"456">>}
           , {"root:a5",            <<"{\"b1\":\"string\"}">>}
           , {"root:a5:b1",         <<"\"string\"">>}

           , {"root:a6",            <<"[{\"b1\":1},{\"b1\":2},"
                                      "{\"b1\":3}]">>}
           , {"root:a6[1]",         <<"[{\"b1\":1}]">>}
           , {"root:a6[2]",         <<"[{\"b1\":2}]">>}
           , {"root:a6[3]",         <<"[{\"b1\":3}]">>}
           , {"root:a6[1,3]",       <<"[{\"b1\":1},{\"b1\":3}]">>}
           , {"root:a6[2,3]",       <<"[{\"b1\":2},{\"b1\":3}]">>}
           , {"root:a6[2,2]",       <<"[{\"b1\":2},{\"b1\":2}]">>}
           , {"root{a1}",           <<"{\"a1\":123}">>}
           , {"root{a1,a4}",        <<"{\"a1\":123,\"a4\":"
                                      "{\"b1\":456}}">>}
           , {"root{a4}:a4:b1",     <<"456">>}
           , {"root{a4}:b1",        property_not_found}
           , {"root{a5}:a5:b1",     <<"\"string\"">>}
           , {"root{a4,a5}:b1",     property_not_found}
           ]
       ]
    }.

err_to_atom({nomatch, {Error, _}})      -> Error;
err_to_atom({nomatch, {Error, _, _}})   -> Error;
err_to_atom({error, {Error, _}})        -> Error;
err_to_atom(Else)                       -> Else.

% Write -ve tests seperately

%% TODO : tests for delete candidate APIs
%% - fold_test_() ->
%% -     TestFun = fun(_,_,Acc) -> Acc+1 end,
%% -     {inparallel
%% -      , [{"proplist", ?_assertEqual(5, fold(TestFun,0,?TEST_PROP))}
%% -         , {"map", ?_assertEqual(5, fold(TestFun,0,?TEST_MAP))}
%% -         , {"json", ?_assertEqual(5, fold(TestFun,0,?TEST_JSON))}]}.
%% -
%% - has_key_test_() ->
%% -     {inparallel
%% -      , [{"proplist_success", ?_assert(has_key(<<"surname">>,?TEST_PROP))}
%% -         , {"map_success",?_assert(has_key(<<"surname">>,?TEST_MAP))}
%% -         , {"json_success",?_assert(has_key(<<"surname">>,?TEST_JSON))}
%% -         , {"proplist_fail",?_assert(not has_key(<<"sname">>,?TEST_PROP))}
%% -         , {"map_fail",?_assert(not has_key(<<"sname">>,?TEST_MAP))}
%% -         , {"json_fail",?_assert(not has_key(<<"sname">>,?TEST_JSON))}]}.
%% -
%% - map_map_test_() -> 
%% -     Job = fun(<<"age">>,_) -> 8000;
%% -              (_,true) -> false;
%% -              (_,V) -> V end,
%% -     PropOut = map(Job,?TEST_PROP),
%% -     MapOut = map(Job,?TEST_MAP),
%% -     JsonOut = map(Job,?TEST_JSON),
%% -     {inparallel
%% -      , [{"proplist"
%% -          , ?_assertEqual(8000, proplists:get_value(<<"age">>,PropOut))}
%% -         , {"proplist"
%% -            , ?_assertEqual(false
%% -                            , proplists:get_value(<<"earthling">>,PropOut))}
%% -         , {"map",?_assertEqual(8000, maps:get(<<"age">>,MapOut))}
%% -         , {"map",?_assertEqual(false,maps:get(<<"earthling">>,MapOut))}
%% -         , {"json",?_assertEqual(8000, ?MODULE:get(<<"age">>,JsonOut))}
%% -         , {"json",?_assertEqual(false,?MODULE:get(<<"earthling">>,JsonOut))}
%% -        ]}.
%% - 
%% - new_test_() -> 
%% -     %% Depends on default type setting
%% -     Test = [],
%% -     {inparallel
%% -      , [{"proplist",?_assertEqual([],new(proplist))}
%% -         , {"map",?_assertEqual(#{},new(map))}
%% -         , {"json",?_assertEqual(<<"{}">>,new(json))}
%% -         , {"default",?_assertEqual(Test,new())}]}.

%% - remove_test_() ->
%% -     PropOut = remove(<<"age">>,?TEST_PROP),
%% -     MapOut =  remove(<<"age">>,?TEST_MAP),
%% -     JsonOut = remove(<<"age">>,?TEST_JSON),
%% -     {inparallel
%% -      , [{"proplist"
%% -          ,?_assertEqual(undefined
%% -                         , proplists:get_value(<<"age">>,PropOut,undefined))}
%% -         , {"map",?_assertEqual(undefined, ?MODULE:get(<<"age">>,MapOut))}
%% -         , {"json",?_assertEqual(undefined,?MODULE:get(<<"age">>,JsonOut))}
%% -        ]}.
%% -
%% - update_test_() ->
%% -     PropOut = update(<<"age">>,8000,?TEST_PROP),
%% -     MapOut =  update(<<"age">>,8000,?TEST_MAP),
%% -     JsonOut = update(<<"age">>,8000,?TEST_JSON),
%% -     {inparallel
%% -      , [{"proplist"
%% -          ,?_assertEqual(8000, proplists:get_value(<<"age">>,PropOut))}
%% -         , {"map",?_assertEqual(8000, maps:get(<<"age">>,MapOut))}
%% -         , {"json",?_assertEqual(8000,?MODULE:get(<<"age">>,JsonOut))}
%% -         , {"error_prop"
%% -            ,?_assertError(badarg,update(<<"not_there">>,true,?TEST_PROP))}
%% -         , {"error_map"
%% -            ,?_assertError(badarg,update(<<"not_there">>,true,?TEST_MAP))}]}.
%% - 
%% - to_proplist_deep_test_() ->
%% -     TestProp = [{<<"first">>,[{<<"second">>,true}]}],
%% -     TestMap = #{<<"first">> => #{<<"second">> => true}},
%% -     TestJson = <<"{\"first\":{\"second\":true}}">>,
%% -     {inparallel
%% -      , [{"proplist",?_assertEqual(TestProp,to_proplist(TestProp,deep))}
%% -         , {"map",?_assertEqual(TestProp,to_proplist(TestMap,deep))}
%% -         , {"json",?_assertEqual(TestProp,to_proplist(TestJson,deep))}]}.
%% -
%% - to_map_test_() ->
%% -     {inparallel
%% -      , [{"proplist",?_assert(is_map(to_map(?TEST_PROP)))}
%% -         , {"map",?_assert(is_map(to_map(?TEST_MAP)))}
%% -         , {"json",?_assert(is_map(to_map(clean_null_values(?TEST_JSON))))}]}.
%% -
%% - to_binary_test_() ->
%% -     {inparallel
%% -      , [{"proplist",?_assert(is_binary(to_binary(?TEST_PROP)))}
%% -         , {"map",?_assert(is_binary(to_binary(?TEST_MAP)))}
%% -         , {"json",?_assert(is_binary(to_binary(?TEST_JSON)))}]}.
%% - 
%% - filter_test_() -> 
%% -     FilterFun = fun(<<"age">>,_) -> true;
%% -                    (_,true) -> true;
%% -                    (_,_) -> false end,
%% -     PropOut = filter(FilterFun,?TEST_PROP),
%% -     MapOut =  filter(FilterFun,?TEST_MAP),
%% -     JsonOut = filter(FilterFun,?TEST_JSON),
%% -     {inparallel
%% -      , [{"proplist"
%% -          , ?_assertEqual(981, proplists:get_value(<<"age">>,PropOut))}
%% -         , {"proplist"
%% -            , ?_assertEqual(true
%% -                            , proplists:get_value(<<"earthling">>,PropOut))}
%% -         , {"map",?_assertEqual(981, maps:get(<<"age">>,MapOut))}
%% -         , {"map",?_assertEqual(true,maps:get(<<"earthling">>,MapOut))}
%% -         , {"json",?_assertEqual(981, ?MODULE:get(<<"age">>,JsonOut))}
%% -         , {"json",?_assertEqual(true,?MODULE:get(<<"earthling">>,JsonOut))}
%% -        ]}.
%% - 
%% - include_test_() ->
%% -     Json_Ok = <<"{\"name\":\"John\"}">>,
%% -     Prop_Ok = [{<<"surname">>,<<"Doe">>},{<<"foo">>,<<"bar">>}],
%% -     Map_Ok = #{<<"earthling">> => true, <<"age">> => 981},
%% -     {inparallel
%% -      , [{"proplist"
%% -          , ?_assertEqual(Prop_Ok
%% -                         , include([<<"surname">>,<<"foo">>],?TEST_PROP))}
%% -         , {"map"
%% -            , ?_assertEqual(Map_Ok
%% -                            , include([<<"earthling">>,<<"age">>],?TEST_MAP))}
%% -         , {"json", ?_assertEqual(Json_Ok, include([<<"name">>],?TEST_JSON))}
%% -        ]}.
%% - 
%% - exclude_test_() ->
%% -     Json_Ok = <<"{\"name\":\"John\"}">>,
%% -     Prop_Ok = [{<<"name">>,<<"John">>}],
%% -     Map_Ok = #{<<"name">> => <<"John">>},
%% -     ExcFields = [<<"surname">>,<<"foo">>,<<"earthling">>,<<"age">>,<<"empty">>],
%% -     {inparallel
%% -      , [{"proplist",?_assertEqual(Prop_Ok, exclude(ExcFields,?TEST_PROP))}
%% -         , {"map",?_assertEqual(Map_Ok, exclude(ExcFields,?TEST_MAP))}
%% -         , {"json",?_assertEqual(Json_Ok, exclude(ExcFields,?TEST_JSON))}]}.
%% - 
%% - merge_test_() ->
%% -     Json_1 = <<"{\"test\":\"true\"}">>,
%% -     Prop_1 = [{<<"test">>,<<"true">>}],
%% -     Map_1 = #{<<"test">> => <<"true">>},
%% -     JsonOut = merge(Json_1,?TEST_JSON),
%% -     PropOut = merge(Prop_1,?TEST_PROP),
%% -     MapOut = merge(Map_1,?TEST_MAP),
%% -     {inparallel
%% -      , [{"proplist"
%% -          , ?_assertEqual(<<"true">>, ?MODULE:get(<<"test">>,PropOut))}
%% -         , {"proplist"
%% -            , ?_assertEqual(<<"Doe">>, ?MODULE:get(<<"surname">>,PropOut))}
%% -         , {"map"
%% -            , ?_assertEqual(<<"true">>, ?MODULE:get(<<"test">>,MapOut))}
%% -         , {"map"
%% -            , ?_assertEqual(<<"Doe">>, ?MODULE:get(<<"surname">>,MapOut))}
%% -         , {"json"
%% -            , ?_assertEqual(<<"true">>, ?MODULE:get(<<"test">>,JsonOut))}
%% -         , {"json"
%% -            , ?_assertEqual(<<"Doe">>, ?MODULE:get(<<"surname">>,JsonOut))}
%% -        ]}.
%% - 
%% - project_test_() ->
%% -     Data1 = <<"{\"name\":{\"first\":false,\"list\":[{\"any\":1}]}, \"test\":true}">>,
%% -     Data2 = <<"{\"list\":[{\"any\":{\"sub\":true}},{\"any\":{\"sub\":false}}],\"empty\":[]}">>,
%% -     {inparallel
%% -      , [?_assertEqual([1],project("name:list[*]:any",Data1))
%% -         , ?_assertEqual(true,project("test",Data1))
%% -         , ?_assertEqual(false,project("name:first",Data1))
%% -         , ?_assertEqual([[1],true]
%% -                         , project(["name:list[*]:any","test"],Data1))
%% -         , ?_assertEqual([true,false],project("list[*]:any:sub",Data2))
%% -         , ?_assertEqual(true,project("list[1]:any:sub",Data2))
%% -         , ?_assertEqual([<<"first">>,<<"list">>]
%% -                         , project("name:$keys$",Data1))
%% -         , ?_assertEqual([[<<"first">>,<<"list">>],[1],false]
%% -                         , project(["name:$keys$", "name:list[*]:any"
%% -                                    , "name:first"],Data1))
%% -         , ?_assertEqual(nomatch, project(["name:$keys$", "name:list[*]:any"
%% -                                           , "name:not_there"],Data1))
%% -         , ?_assertEqual(nomatch,project("list[10]:any:sub",Data2))
%% -         , ?_assertEqual(nomatch,project("unlist[1]:any:sub",Data2))
%% -         , ?_assertEqual(nomatch,project("empty[1]",Data2))
%% -         , ?_assertEqual(nomatch,project("empty[*]",Data2))
%% -         , ?_assertEqual(nomatch,project("unexistant",Data2))
%% -         , ?_assertEqual(nomatch,project("unexistant[*]",Data2))
%% -     ]}.
%% - 
%% - diff_test_() ->
%% -     PropBefore = [{<<"surname">>,<<"Doe">>}, {<<"age">>,981}
%% -                   , {<<"empty">>,null},{<<"unmod">>,true}],
%% -     PropAfter  = [{<<"surname">>,<<"DoeDoe">>}, {<<"ega">>,981}
%% -                   , {<<"newval">>,<<"one">>}, {<<"empty">>,null}
%% -                   , {<<"unmod">>,true}],
%% -     PropDiffOut = [{added,[{<<"ega">>,981},{<<"newval">>,<<"one">>}]},
%% -                    {removed,[{<<"age">>,981}]},
%% -                    {updated,[{<<"surname">>,<<"DoeDoe">>}]},
%% -                    {replaced,[{<<"surname">>,<<"Doe">>}]},
%% -                    {changes,4}],
%% - 
%% -     MapsBefore = #{<<"age">> => 981, <<"empty">> => null
%% -                    , <<"surname">> => <<"Doe">>,<<"unmod">> => true},
%% -     MapsAfter = #{<<"ega">> => 981, <<"empty">> => null
%% -                   , <<"newval">> => <<"one">>
%% -                   , <<"surname">> => <<"DoeDoe">>, <<"unmod">> => true},
%% -     MapsDiffOut = #{added => #{<<"ega">> => 981,<<"newval">> => <<"one">>},
%% -                    removed => #{<<"age">> => 981},
%% -                    replaced => #{<<"surname">> => <<"Doe">>},
%% -                    updated => #{<<"surname">> => <<"DoeDoe">>},
%% -                    changes => 4},
%% - 
%% -     JsonBefore = <<"{\"surname\":\"Doe\",\"age\":981,\"empty\":null}">>,
%% -     JsonAfter = <<"{\"surname\":\"DoeDoe\",\"ega\":981,\"newval\":\"one\","
%% -                   "\"empty\":null}">>,
%% -     JsonDiffOut = <<"{\"added\":{\"ega\":981,\"newval\":\"one\"},\"removed\":"
%% -                     "{\"age\":981},\"updated\":{\"surname\":\"DoeDoe\"},"
%% -                     "\"replaced\":{\"surname\":\"Doe\"},\"changes\":4}">>,
%% -     {inparallel
%% -      , [{"proplist",?_assertEqual(PropDiffOut, diff(PropBefore,PropAfter))}
%% -         , {"map",?_assertEqual(MapsDiffOut, diff(MapsBefore,MapsAfter))}
%% -         , {"json",?_assertEqual(JsonDiffOut, diff(JsonBefore,JsonAfter))}]}.
%% - 
%% - equal_test_() -> 
%% -     Json_Ok = <<"{\"name\":\"John\",\"surname\":\"Doe\",\"foo\":\"bar\","
%% -                 "\"earthling\":true,\"age\":981,\"other\":null}">>,
%% -     Prop_Ok = [{<<"surname">>,<<"Doe">>},{<<"foo">>,<<"bar">>}
%% -                , {<<"earthling">>,true}, {<<"name">>,<<"John">>}
%% -                , {<<"age">>,981}],
%% -     Map_Ok = #{<<"earthling">> => true, <<"age">> => 981
%% -                , <<"name">> => <<"John">>, <<"foo">> => <<"bar">>
%% -                , <<"surname">> => <<"Doe">>, <<"empty">> => null},
%% -     Json_Nook = <<"{\"name\":\"John\",\"surname\":null,\"foo\":\"bar\","
%% -                   "\"earthling\":true,\"age\":981,\"other\":null}">>,
%% -     Prop_Nook = [{<<"surname">>,<<"Doe">>},{<<"foo">>,<<"barbara">>}
%% -                  , {<<"earthling">>,true}, {<<"name">>,<<"John">>}
%% -                  , {<<"age">>,981}],
%% -     Map_Nook = #{<<"earthling">> => true, <<"age">> => 981
%% -                  , <<"name">> => <<"John">>,  <<"foo">> => <<"bar">>
%% -                  , <<"empty">> => null},
%% -     {inparallel
%% -      , [{"map_success",?_assert(equal(Map_Ok,?TEST_MAP))}
%% -         , {"prop_success",?_assert(equal(Prop_Ok,?TEST_PROP))}
%% -         , {"json_success",?_assert(equal(Json_Ok,?TEST_JSON))}
%% -         , {"map_fail",?_assert(not equal(Map_Nook,?TEST_MAP))}
%% -         , {"prop_fail",?_assert(not equal(Prop_Nook,?TEST_PROP))}
%% -         , {"json_fail",?_assert(not equal(Json_Nook,?TEST_JSON))}]}.
%% - 
%% - is_subset_test_() -> 
%% -     Sub_Ok_Json =  <<"{\"surname\":\"Doe\",\"age\":981,\"nothing\":null}">>,
%% -     Sub_Ok_Prop = [{<<"surname">>,<<"Doe">>}, {<<"age">>,981}
%% -                    , {<<"nothing">>,null}],
%% -     Sub_Ok_Map  = #{<<"age">> => 981, <<"surname">> => <<"Doe">>
%% -                     , <<"nothing">> => null},
%% -     Sub_Nook_Json =  <<"{\"suame\":\"Doe\",\"age\":981}">>,
%% -     Sub_Nook_Prop = [{<<"surname">>,<<"Doe">>}, {<<"age">>,91}],
%% -     Sub_Nook_Map  = #{<<"age">> => 981, <<"surname">> => <<"De">>},
%% -     {inparallel
%% -      , [{"map_success",?_assert(is_subset(Sub_Ok_Map ,?TEST_MAP))}
%% -         , {"proplist_success",?_assert(is_subset(Sub_Ok_Prop,?TEST_PROP))}
%% -         , {"json_success",?_assert(is_subset(Sub_Ok_Json,?TEST_JSON))}
%% -         , {"map_fail",?_assert(not is_subset(Sub_Nook_Map ,?TEST_MAP))}
%% -         , {"proplist_fail"
%% -            ,?_assert(not is_subset(Sub_Nook_Prop,?TEST_PROP))}
%% -         , {"json_fail",?_assert(not is_subset(Sub_Nook_Json,?TEST_JSON))}]}.
%% - 
%% - is_disjoint_test_() -> 
%% -     Sub_Ok_Json =  <<"{\"suame\":\"Doe\",\"age\":91}">>,
%% -     Sub_Ok_Prop = [{<<"surname">>,<<"De">>}, {<<"ae">>,981}],
%% -     Sub_Ok_Map  = #{<<"ag">> => 981, <<"surname">> => <<"De">>},
%% -     Sub_Nook_Json =  <<"{\"suame\":\"Doe\",\"age\":981}">>,
%% -     Sub_Nook_Prop = [{<<"surname">>,<<"Doe">>}, {<<"age">>,981}],
%% -     Sub_Nook_Map  = #{<<"age">> => 981, <<"surname">> => <<"De">>},
%% -     {inparallel
%% -      , [{"map_success",?_assert(is_disjoint(Sub_Ok_Map ,?TEST_MAP))}
%% -         , {"proplist_success",?_assert(is_disjoint(Sub_Ok_Prop,?TEST_PROP))}
%% -         , {"json_success",?_assert(is_disjoint(Sub_Ok_Json,?TEST_JSON))}
%% -         , {"map_fail",?_assert(not is_disjoint(Sub_Nook_Map ,?TEST_MAP))}
%% -         , {"proplist_fail"
%% -            ,?_assert(not is_disjoint(Sub_Nook_Prop,?TEST_PROP))}
%% -         , {"json_fail",?_assert(not is_disjoint(Sub_Nook_Json,?TEST_JSON))}
%% -        ]}.

%% - to_map_test() ->
%% -     ?assertEqual(?TEST_MAP,to_map(?TEST_JSON)).

%% - map_test_() -> 
%% -     Job = fun(<<"age">>,_) -> 8000;
%% -              (_,true) -> false;
%% -              (_,V) -> V end,
%% -     {inparallel
%% -      , [{"proplist"
%% -          , ?_assertEqual(8000
%% -                          , proplists:get_value(<<"age">>
%% -                                                , map(Job,?TEST_PROP)))}
%% -         , {"proplist"
%% -            , ?_assertEqual(false
%% -                            , proplists:get_value(<<"earthling">>
%% -                                                  ,map(Job,?TEST_PROP)))}
%% -         , {"json"
%% -            , ?_assertEqual(8000, ?MODULE:get(<<"age">>
%% -                                              , map(Job,?TEST_JSON)))}
%% -         , {"json"
%% -            , ?_assertEqual(false, ?MODULE:get(<<"earthling">>
%% -                                               , map(Job,?TEST_JSON)))}]}.
%% - 
%% - parse_path_test_() ->
%% -     Path1 = "name.list[*].any",
%% -     Path2 = <<"name.list[*].any">>,
%% -     Path3 = lists:concat(["name",?JSON_PATH_SEPARATOR
%% -                           ,"list[*]",?JSON_PATH_SEPARATOR,"any"]),
%% -     Res1  = ["name","list[*]","any"],
%% -     {inparallel
%% -      , [?_assertEqual(Res1,parse_path(Path1,"."))
%% -         , ?_assertEqual(Res1,parse_path(Path2,"."))
%% -         , ?_assertEqual(Res1,parse_path(Path3))]}.
%% - 
%% walk_path_test_() ->
%%     Path1 = ["name","list[*]","any"],
%%     Object1 = [{<<"name">>, [{<<"list">>
%%                               , [[{<<"any">>,1}],[{<<"any">>,2}]]}]}],
%%     Result1 = [1,2],
%%     [?_assertEqual(Result1,walk_path(Path1,Object1))].
-endif.
