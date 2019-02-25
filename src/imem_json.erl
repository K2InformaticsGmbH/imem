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
-export([encode/1, encode/2, decode/1, decode/2]).

%% Standard data type operations, inspired by maps operations
-export([ find/2
        , get/2
        , get/3
        , keys/1
        , put/3
        , remove/2
        , size/1
        , values/1
        , find_in_list/3
        ]).

%% Conversions
-export([to_proplist/1]).

%% Other Functionality
-export([ eval/2            %% Constructs a data object prescribed by json path (using jpparse)
        , expand_inline/2   %% Recursively expands 'inline_*' references in a JSON object
        , diff/2            %% Lists differences of two JSON objects
        ]).

-safe(all).

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

-spec encode(data_object(), Opts :: list()) -> Json :: binary().
encode(Json, Opts) -> jsx:encode(Json, Opts).
%% ===================================================================

%% ===================================================================
%% Other Exported functions
%% ===================================================================

%% @doc Scan a list of json objects, find the first occurence with the 
%% given property return the value.
%% Return the Default if property is not found.
-spec find_in_list(string() | atom() | binary(), [jsx:json_term()], jsx:json_term()) -> jsx:json_term().
find_in_list(Property, _DataObjects, _Default)
  when not (is_atom(Property) orelse
            (is_binary(Property) andalso byte_size(Property) > 0) orelse
            (is_list(Property) andalso length(Property) > 0)) ->
    error(bad_property);
find_in_list(_Property, DataObjects, _Default)
  when not is_list(DataObjects) ->
    error(bad_object);
find_in_list(_Property, [], Default)  -> Default;
find_in_list(Property, [DO|DataObjects], Default) when is_binary(DO) ->
    find_in_list(Property, [jsx:decode(DO, [return_maps])|DataObjects], Default);
find_in_list(Property, [DO|DataObjects], Default) when is_list(DO), is_binary(Property) ->
    case lists:keyfind(Property, 1, DO) of
        {_,VB} ->    VB;
        false ->    
            case lists:keyfind(binary_to_list(Property), 1, DO) of
                {_,VL} ->   VL;
                false ->    find_in_list(Property, DataObjects, Default)
            end
    end;
find_in_list(Property, [DO|DataObjects], Default) when is_list(DO), is_list(Property) ->
    case lists:keyfind(Property, 1, DO) of
        {_,VL} ->    VL;
        false ->    
            case lists:keyfind(list_to_binary(Property), 1, DO) of
                {_,VB} ->   VB;
                false ->    find_in_list(Property, DataObjects, Default)
            end
    end;
find_in_list(Property, [DO|DataObjects], Default) when is_list(DO), is_atom(Property) ->
    case lists:keyfind(Property, 1, DO) of
        {_,VA} ->    VA;
        false ->    
            case lists:keyfind(atom_to_list(Property), 1, DO) of
                {_,VL} ->   VL;                
                false ->    
                    case lists:keyfind(list_to_binary(atom_to_list(Property)), 1, DO) of
                        {_,VB} ->   VB;
                        false ->    find_in_list(Property, DataObjects, Default)
                    end
            end
    end;
find_in_list(Property, [DO|DataObjects], Default) when is_map(DO), is_binary(Property) ->
    case maps:is_key(Property, DO) of
        true ->     maps:get(Property, DO);
        false ->
            case maps:is_key(binary_to_list(Property), DO) of 
                true ->    maps:get(binary_to_list(Property), DO);
                false ->   find_in_list(Property, DataObjects, Default)
            end
    end;
find_in_list(Property, [DO|DataObjects], Default) when is_map(DO), is_list(Property) ->
    case maps:is_key(Property, DO) of
        true ->     maps:get(Property, DO);
        false ->
            case maps:is_key(list_to_binary(Property), DO) of 
                true ->    maps:get(list_to_binary(Property), DO);
                false ->   find_in_list(Property, DataObjects, Default)
            end
    end;
find_in_list(Property, [DO|DataObjects], Default) when is_map(DO), is_atom(Property) ->
    case maps:is_key(Property, DO) of
        true ->     maps:get(Property, DO);
        false ->
            case maps:is_key(atom_to_list(Property), DO) of 
                true ->    maps:get(atom_to_list(Property), DO);
                false ->   
                    case maps:is_key(list_to_binary(atom_to_list(Property)), DO) of
                        true -> maps:get(list_to_binary(atom_to_list(Property)), DO);
                        false -> find_in_list(Property, DataObjects, Default)
                    end
            end
    end.
          
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
    case jpparse_fold:foldbu(fun jpp_walk/3
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
                   exit({nomatch, {property_not_found, R, Obj}});
               true ->
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

%% find_in_list tests
find_in_list_test_() ->
    Tests = [
             {<<"surname">>,    ?TEST_LIST_PROP,    njet,       <<"Doe">>   },
             {<<"name">>,       ?TEST_LIST_PROP,    njet,       <<"John">>  },
             {  "surname",      ?TEST_LIST_PROP,    njet,       <<"Doe">>   },
             {  "name",         ?TEST_LIST_PROP,    njet,       <<"John">>  },
             {  surname,        ?TEST_LIST_PROP,    njet,       <<"Doe">>   },
             {  name,           ?TEST_LIST_PROP,    njet,       <<"John">>  },
             {<<"surinam">>,    ?TEST_LIST_PROP,    njet,       njet        },
             {  "surinam",      ?TEST_LIST_PROP,    "njet",     "njet"      },
             {   surinam,       ?TEST_LIST_PROP,    <<"njet">>, <<"njet">>  }
            ],
    {inparallel,
     [{"find_in_list_test_"++integer_to_list(I),
       fun() ->
               ?assertEqual(R, find_in_list(K,L,D)),
               ListOfMaps = [ maps:from_list(E) || E <- L],
               ?assertEqual(R, find_in_list(K,ListOfMaps,D)),
               ListOfJsonStrings = [ jsx:encode(M) || M <- ListOfMaps],
               ?assertEqual(R, find_in_list(K,ListOfJsonStrings,D))
       end} || {I,{K,L,D,R}} <- lists:zip(lists:seq(1,length(Tests)), Tests)]}.


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

-endif.
