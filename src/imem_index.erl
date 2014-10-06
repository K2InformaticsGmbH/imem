%% -*- coding: utf-8 -*-
-module(imem_index).

%% @doc == imem INDEX operations ==

-include("imem.hrl").
-include("imem_meta.hrl").

-export([index_type/1
        ,remove/2       %% (IndexTable,Removes)
        ,insert/2       %% (IndexTable,Inserts)
        ]).


%% ==================================================================
%% value normalising funs
%% ==================================================================
-export([vnf_identity/1             %% identity transformation, no change of value
        ,vnf_lcase_ascii/1          %% lower case ascci, allow empty strings
        ,vnf_lcase_ascii_ne/1       %% lower case ascci non-empty
        ,vnf_integer/1              %% accept integers (convert if necessary) return ?nav on failure 
        ,vnf_float/1                %% accept floats (convert if necessary) return ?nav on failure 
        ]).

%% ==================================================================
%% index filter funs
%% ==================================================================
-export([iff_true/1
        ,iff_binterm_list/1         %% true for binstrings representing a list 
        ,iff_binterm_list_1/1       %% true for binstrings representing a list with one element (root object type) 
        ,iff_list_pattern/2         %% used in generated iff
        ]).

-export([gen_iff_binterm_list_pattern/1      %% used to generate iff fun from key pattern
        ]).

-export([preview/7      %% (IndexTable,ID,Type,SearchStrategies,SearchTerm,Limit,Iff) -> [{Strategy,Key,Value,Stu}]
        ,preview/8      %% (IndexTable,ID,Type,SearchStrategies,SearchTerm,Limit,Iff,Cont) -> [{Strategy,Key,Value,Stu}]
        ]).

-export([binstr_accentfold/1
        ,binstr_to_lower/1
        ,binstr_to_upper/1
        ,binstr_only_ascii/1, binstr_only_ascii/2
        ,binstr_only_valid/1, binstr_only_valid/2
        ,binstr_only_latin1/1, binstr_only_latin1/2
        ,binstr_match_anywhere/2
        ,binstr_match_sub/4
        ,binstr_match_precompile/1
        ]).

-define(BIN_APP,binstr_append_placeholder).
-define(HASH_RANGES,[16#FF,16#7FFFFFFF,16#FFFFFFFF]). %% giving 8(3)/27(6)/32(8) bit/(byte) hashes
-define(SMALLEST_TERM,-1.0e100).


%% ===================================================================
%% Index table maintenance (insert/remove index rows)
%% ===================================================================

%% @doc Remove index entry, called in trigger function upon row remove/update
-spec remove(atom(),list()) -> ok.
remove(_IndexTable,[]) -> ok;
remove(IndexTable,[{ID,ivk,Key,Value}|Items]) ->
    imem_if:delete(IndexTable,{ID,Value,Key}),
    remove(IndexTable,Items);
remove(IndexTable,[{ID,iv_h,Key,Value}|Items]) ->
    case imem_if:read(IndexTable,{ID,Value}) of
        [] ->   
            ?SystemException({"Missing hashmap for",{IndexTable,ID,Value}});
        [#ddIndex{lnk=Hash}] ->     
            imem_if:delete(IndexTable,{ID,Hash,Key})
    end,
    remove(IndexTable,Items);
remove(IndexTable,[{ID,iv_kl,Key,Value}|Items]) ->
    case imem_if:read(IndexTable,{ID,Value}) of
        [] ->   
            ?SystemException({"Missing keylist for",{IndexTable,ID,Value}});
        [#ddIndex{lnk=KL}] ->
            case lists:delete(Key,KL) of
                [] ->   imem_if:delete(IndexTable,{ID,Value});
                NKL ->  imem_if:write(IndexTable,#ddIndex{stu={ID,Value},lnk=NKL})
            end
    end,
    remove(IndexTable,Items);
remove(IndexTable,[{ID,iv_k,_,Value}|Items]) ->
    imem_if:delete(IndexTable,{ID,Value}),
    remove(IndexTable,Items).

%% @doc Insert index entry, called in trigger function upon row insert/update
-spec insert(atom(),list()) -> ok.
insert(_IndexTable,[]) -> ok;
insert(IndexTable,[{ID,ivk,Key,Value}|Items]) ->
    imem_if:write(IndexTable,#ddIndex{stu={ID,Value,Key}}),
    insert(IndexTable,Items);
insert(IndexTable,[{ID,iv_h,Key,Value}|Items]) ->
    Hash = case imem_if:read(IndexTable,{ID,Value}) of
        [] ->                   
            NewHash = new_hash(Value,IndexTable,ID),
            imem_if:write(IndexTable,#ddIndex{stu={ID,Value},lnk=NewHash}),
            NewHash;
        [#ddIndex{lnk=OldHash}] ->    
            OldHash
    end,
    imem_if:write(IndexTable,#ddIndex{stu={ID,Hash,Key}}),
    insert(IndexTable,Items);
insert(IndexTable,[{ID,iv_kl,Key,Value}|Items]) ->
    case imem_if:read(IndexTable,{ID,Value}) of
        [] ->   
            imem_if:write(IndexTable,#ddIndex{stu={ID,Value},lnk=[Key]});
        [#ddIndex{lnk=KL}] ->
            imem_if:write(IndexTable,#ddIndex{stu={ID,Value},lnk=lists:usort([Key|KL])})
    end,
    insert(IndexTable,Items);
insert(IndexTable,[{ID,iv_k,Key,Value}|Items]) ->
    case imem_if:read(IndexTable,{ID,Value}) of
        [] ->                   imem_if:write(IndexTable,#ddIndex{stu={ID,Value},lnk=Key});
        [#ddIndex{lnk=K0}] ->   ?ClientError({"Unique index violation",{IndexTable,ID,Value,K0}})
    end,
    insert(IndexTable,Items).

%% @doc Find unused new hash for a new value in a hashmap, start with small hash range and escalate to bigger ones upon hash collisions
-spec new_hash(term(),atom(),integer()) -> integer().
new_hash(Value,IndexTable,ID) ->
    new_hash(Value,IndexTable,ID,?HASH_RANGES).

new_hash(Value,IndexTable,ID,[]) -> 
    ?SystemException({"Cannot create hash",{IndexTable,ID,Value}});
new_hash(Value,IndexTable,ID,[R|Ranges]) -> 
    Hash = erlang:phash2(Value,R),
    case imem_if:next(IndexTable, {ID,Hash,?SMALLEST_TERM}) of
        '$end_of_table' ->  Hash;
        {ID,Hash,_} ->      new_hash(Value,IndexTable,ID,Ranges);
        _ ->                Hash
    end.

%% ===================================================================
%% Value normalisîng funs
%% ===================================================================

vnf_identity(X) -> [X].

vnf_lcase_ascii(<<"\"\"">>) -> [<<>>]; 
vnf_lcase_ascii(B) when is_binary(B) -> 
    %% unicode_string_only_ascii(string:to_lower(unicode:characters_to_list(B, utf8)));
    [binstr_only_ascii(
        binstr_accentfold(
            binstr_to_lower(B)
            )
        )
    ];
vnf_lcase_ascii(Val) -> 
	% unicode_string_only_ascii(io_lib:format("~p",[Val])).
    BinStr = try io_lib:format("~s",[Val])
             catch error:badarg -> io_lib:format("~p",[Val]) end,
    [binstr_only_ascii(
        binstr_accentfold(
            binstr_to_lower(
                unicode:characters_to_binary(BinStr)
                )
            )
        )
    ].

vnf_lcase_ascii_ne(<<"\"\"">>) -> [?nav]; 
vnf_lcase_ascii_ne(<<>>) -> [?nav];
vnf_lcase_ascii_ne(Text) -> vnf_lcase_ascii(Text).

vnf_integer(I) when is_integer(I) -> [I];
vnf_integer(F) when is_float(F) -> [round(F)];
vnf_integer(A) when is_atom(A) -> [?nav]; 
vnf_integer(B) when is_binary(B) -> 
    case (catch imem_datatype:io_to_integer(B)) of
        I when is_integer(I) -> [I];
        _ ->                    [?nav]
    end;
vnf_integer(L) when is_list(L) ->
    case (catch imem_datatype:io_to_integer(L)) of
        I when is_integer(I) -> [I];
        _ ->                    [?nav]
    end.


vnf_float(I) when is_integer(I) -> [1.0 * I];
vnf_float(F) when is_float(F) -> [F];
vnf_float(A) when is_atom(A) -> [?nav]; 
vnf_float(B) when is_binary(B) -> 
    case (catch imem_datatype:io_to_float(B)) of
        F when is_float(F) ->   [F];
        _ ->                    [?nav]
    end;
vnf_float(L) when is_list(L) -> 
    case (catch imem_datatype:io_to_float(L)) of
        F when is_float(F) ->   [F];
        _ ->                    [?nav]
    end.


%% ===================================================================
%% Index filter funs (decide if an index row candidate should be put)
%% ===================================================================

iff_true({_Key,_Value}) -> true.    %% return true without looking at Key or Value

iff_binterm_list({<<17:8,_/binary>>,_}) -> true;
iff_binterm_list({_,_}) -> false.

iff_binterm_list_1({Key = <<17:8,_/binary>>,_}) -> 
    case (catch imem_datatype:binterm_to_term(Key)) of
        [_] ->  true;
        _ ->    false
    end;
iff_binterm_list_1({_,_}) -> false.


iff_list_pattern(Key,Key) -> true;  %% includes [],[] 
iff_list_pattern(_,['*']) -> true;  %% Key matches zero or more list elements
iff_list_pattern([],_P) -> false;       
iff_list_pattern(_,[]) -> false;       
iff_list_pattern([H|R],[H|P]) ->  iff_list_pattern(R,P);
iff_list_pattern([_|R],['_'|P]) -> iff_list_pattern(R,P);
iff_list_pattern(_,_) -> false.

gen_iff_binterm_list_pattern(__Pattern) when is_list(__Pattern) -> 
    fun({__Key,_}) -> imem_index:iff_list_pattern(imem_datatype:binterm_to_term(__Key),__Pattern) end;
gen_iff_binterm_list_pattern(__Pattern) ->
    ?ClientError({"Expecting a list pattern with optional wildcards '_' and '*'",__Pattern}).


%% ===================================================================
%% Index preview (fast range/full match scan in single index)
%% ===================================================================

%% @doc Preview match scan into an index for finding first "best" matches
-spec preview(atom(),integer(),atom(),list(),term(),integer(),function()) -> list().
preview(IndexTable,ID,Type,SearchStrategies,SearchTerm,Limit,Iff) ->
    case is_regexp_search(SearchStrategies, SearchTerm) of
        true ->
            preview_regexp(IndexTable, ID, Type, SearchTerm, Limit, Iff);
        false ->
            FilteredStrategies = [Strategy || Strategy <- SearchStrategies, Strategy =/= re_match],
            preview_execute(IndexTable, ID, Type, FilteredStrategies, SearchTerm, Limit, Iff, undefined)
    end.
    % [{exact_match,<<"Key0">>,<<"Value0">>,{ID,<<"Value0">>,<<"Key0">>}}
    % ,{head_match,<<"Key1">>,<<"Value1">>,{ID,<<"Value1">>,<<"Key1">>}}
    % ,{body_match,<<"Key2">>,<<"Value2">>,{ID,<<"Value2">>,<<"Key2">>}}
    % ,{split_match,<<"Key3">>,<<"Value3">>,{ID,<<"Value3">>,<<"Key3">>}}
    % ,{re_match,<<"RE_Pattern">>,<<"Value4">>,{ID,<<"Value4">>,<<"Key4">>}}
    % ].

%% @doc Preview match scan into an index for finding first "best" matches
-spec preview(atom(),integer(),atom(),list(),term(),integer(),function(),tuple()) -> list().
preview(_IndexTable,_ID,_Type,_SearchStrategies,_SearchTerm,_Limit,_Iff,_Cont) ->
    [].     %% ToDo: implement continuation search
    % [{exact_match,<<"Key0">>,<<"Value0">>,{ID,<<"Value0">>,<<"Key0">>}}
    % ,{head_match,<<"Key1">>,<<"Value1">>,{ID,<<"Value1">>,<<"Key1">>}}
    % ,{body_match,<<"Key2">>,<<"Value2">>,{ID,<<"Value2">>,<<"Key2">>}}
    % ,{split_match,<<"Key3">>,<<"Value3">>,{ID,<<"Value3">>,<<"Key3">>}}
    % ,{re_match,<<"RE_Pattern">>,<<"Value4">>,{ID,<<"Value4">>,<<"Key4">>}}
    % ].

%% @doc check if the search term should be interpreted as a regular expression.
-spec is_regexp_search(list(), binary()) -> boolean().
is_regexp_search(SearchStrategies, SearchTerm) ->
    case  binary:match(SearchTerm, [<<"*">>, <<"?">>,<<"_">>,<<"%">>]) of
        nomatch -> false;
        _ ->
            case lists:member(re_match, SearchStrategies) of
                true -> true;
                false -> ?ClientError({"Wildcard search is not enabled",SearchTerm})
            end
    end.

preview_regexp(IndexTable, ID, Type, SearchTerm, Limit, Iff) ->
    StartingStu = create_starting_stu(Type, ID, re_match, SearchTerm),
    ReplacedStar = binary:replace(SearchTerm, [<<"*">>], <<"%">>, [global]),
    ReplacedMark = binary:replace(ReplacedStar, [<<"?">>], <<"_">>, [global]),
    Pattern = imem_sql_funs:like_compile(ReplacedMark),
    {atomic, ResultRegexp} =
        imem_if:transaction(fun() -> preview_regexp(IndexTable, ID, Type, Pattern, StartingStu, Limit, Iff) end),
    ResultRegexp.

preview_regexp(_IndexTable, _ID, ivk, _Pattern, _Stu, 0, _Iff) -> [];
preview_regexp(IndexTable, ID, ivk, Pattern, PrevStu, Limit, Iff) ->
    case imem_if:next(IndexTable, PrevStu) of
        '$end_of_table' -> [];
        {ID, Value, Key} = Stu ->
            case imem_sql_funs:re_match(Pattern, Value) andalso Iff({Key, Value}) of
                true ->
                    [build_result_entry(Stu, re_match, Key, Value) |
                     preview_regexp(IndexTable, ID, ivk, Pattern, Stu, Limit-1, Iff)];
                false ->
                    preview_regexp(IndexTable, ID, ivk, Pattern, Stu, Limit, Iff)
            end;
        _ -> []
    end;
preview_regexp(IndexTable, ID, iv_k, Pattern, {ID, Value} = Stu, Limit, Iff) ->
    case imem_if:read(IndexTable, Stu) of
        [#ddIndex{stu = {ID, Value}, lnk = Key} = Entry] ->
            case imem_sql_funs:re_match(Pattern, Value) andalso Iff({Key, Value}) of
                true ->
                    Partial = [build_result_entry(Entry#ddIndex.stu, re_match, Key, Value)];
                false -> Partial = []
            end;
        _ -> Partial = []
    end,
    if
        length(Partial) < Limit ->
            case imem_if:next(IndexTable, Stu) of
                '$end_of_table' -> Partial;
                {ID, _NextValue} = NextStu ->
                    Partial ++ preview_regexp(IndexTable, ID, iv_k, Pattern, NextStu, Limit - length(Partial), Iff);
                _ -> Partial
            end;
        true ->
            Partial
    end;
preview_regexp(IndexTable, ID, iv_kl, Pattern, {ID, Value} = Stu, Limit, Iff) ->
    case imem_if:read(IndexTable, Stu) of
        [#ddIndex{stu = {ID, Value}, lnk = KeyList} = Entry] ->
            case imem_sql_funs:re_match(Pattern, Value) of
                true ->
                    Partial = preview_expand_kl(re_match, Entry#ddIndex.stu, KeyList, Value, Limit, Iff);
                false -> Partial = []
            end;
        _ ->
            Partial = []
    end,
    if
        length(Partial) < Limit ->
            case imem_if:next(IndexTable, Stu) of
                '$end_of_table' -> Partial;
                {ID, _NextValue} = NextStu ->
                    Partial ++ preview_regexp(IndexTable, ID, iv_kl, Pattern, NextStu, Limit - length(Partial), Iff);
                _ -> Partial
            end;
        true ->
            Partial
    end;
preview_regexp(IndexTable, ID, iv_h, Pattern, {ID, Value} = Stu, Limit, Iff) ->
    case imem_if:read(IndexTable, Stu) of
        [#ddIndex{stu = {ID, Value}, lnk = Hash}] ->
            case imem_sql_funs:re_match(Pattern, Value) of
                true ->
                    Partial = preview_expand_hash(re_match, IndexTable, ID, Hash, Value, ?SMALLEST_TERM, Limit, Iff);
                false -> Partial = []
            end;
        _ ->
            Partial = []
    end,
    if
        length(Partial) < Limit ->
            case imem_if:next(IndexTable, Stu) of
                '$end_of_table' -> Partial;
                {ID, _NextValue} = NextStu ->
                    Partial ++ preview_regexp(IndexTable, ID, iv_h, Pattern, NextStu, Limit - length(Partial), Iff);
                _ -> Partial
            end;
        true ->
            Partial
    end.


%% @doc Execute preview in the order defined by the list of searchstrategies.
-spec preview_execute(atom(), integer(), atom(), list(), binary(), integer(), function(), atom()) -> list().
preview_execute(_IndexTable, _ID, _Type, [], _SearchTerm, _Limit, _Iff, _PrevStrategy) -> [];
preview_execute(IndexTable, ID, Type, [exact_match | SearchStrategies], SearchTerm, Limit, Iff, undefined) ->
    {atomic, ResultExact} =
        imem_if:transaction(fun() -> preview_exact(IndexTable, ID, Type, SearchTerm, ?SMALLEST_TERM, Limit, Iff) end),
    ResultExact ++ preview_execute(IndexTable, ID, Type, SearchStrategies, SearchTerm, Limit - length(ResultExact), Iff, exact_match);
preview_execute(IndexTable, ID, Type, [head_match | SearchStrategies], SearchTerm, Limit, Iff, PrevStrategy) ->
    StartingStu = create_starting_stu(Type, ID, head_match, SearchTerm),
    IffAndNotAdded = add_filter_duplicated(PrevStrategy, SearchTerm, Iff),
    {atomic, ResultHead} =
        imem_if:transaction(fun() -> preview_head(IndexTable, ID, Type, SearchTerm, StartingStu, Limit, IffAndNotAdded) end),
    case lists:member(body_match, SearchStrategies) of
        true ->
            ResultHead ++ preview_execute(IndexTable, ID, Type, [body_match], SearchTerm, Limit - length(ResultHead), Iff, head_match);
        false ->
            ResultHead
    end;
preview_execute(IndexTable, ID, Type, [body_match | _SearchStrategies], SearchTerm, Limit, Iff, PrevStrategy) ->
    StartingStu = create_starting_stu(Type, ID, body_match, SearchTerm),
    IffNotAdded = add_filter_duplicated(PrevStrategy, SearchTerm, Iff),
    {atomic, ResultBody} =
        imem_if:transaction(fun() -> preview_body(IndexTable, ID, Type, SearchTerm, StartingStu, Limit, IffNotAdded) end),
    %% Body is the last since including head or exact will duplicate results.
    ResultBody.

preview_exact(_IndexTable, _ID, _Type, _SearchTerm, _Key, 0, _Iff) -> [];
preview_exact(IndexTable, ID, ivk, SearchTerm, Key, Limit, Iff) ->
    case imem_if:next(IndexTable, {ID, SearchTerm, Key}) of
        '$end_of_table' -> [];
        {ID, SearchTerm, NextKey} = Stu ->
            case Iff({NextKey, SearchTerm}) of
                true ->
                    [build_result_entry(Stu, exact_match, NextKey, SearchTerm) |
                     preview_exact(IndexTable, ID, ivk, SearchTerm, NextKey, Limit-1, Iff)];
                false ->
                    preview_exact(IndexTable, ID, ivk, SearchTerm, NextKey, Limit, Iff)
            end;
        _ -> []
    end;
preview_exact(IndexTable, ID, iv_k, SearchTerm, _Key, _Limit, Iff) ->
    case imem_if:read(IndexTable, {ID, SearchTerm}) of
        [#ddIndex{stu = {ID, SearchTerm}, lnk = Key} = Entry] ->
            case Iff({Key, SearchTerm}) of
                true ->
                    [build_result_entry(Entry#ddIndex.stu, exact_match, Key, SearchTerm)];
                false -> []
            end;
        _ -> []
    end;
preview_exact(IndexTable, ID, iv_kl, SearchTerm, _Key, Limit, Iff) ->
    case imem_if:read(IndexTable, {ID, SearchTerm}) of
        [#ddIndex{stu = {ID, SearchTerm}, lnk = KeyList} = Entry] ->
            preview_expand_kl(exact_match, Entry#ddIndex.stu, KeyList, SearchTerm, Limit, Iff);
        _ -> []
    end;
preview_exact(IndexTable, ID, iv_h, SearchTerm, _Key, Limit, Iff) ->
    case imem_if:read(IndexTable, {ID, SearchTerm}) of
        [#ddIndex{stu = {ID, SearchTerm}, lnk = Hash}] ->
            preview_expand_hash(exact_match, IndexTable, ID, Hash, SearchTerm, ?SMALLEST_TERM, Limit, Iff);
        _ -> []
    end.

preview_head(_IndexTable, _ID, _Type, _SearchTerm, _Stu, 0, _Iff) -> [];
preview_head(IndexTable, ID, ivk, SearchTerm, PrevStu, Limit, Iff) ->
    SizeSearch = size(SearchTerm),
    case imem_if:next(IndexTable, PrevStu) of
        '$end_of_table' -> [];
        {ID, <<SearchTerm:SizeSearch/binary, _/binary>> = Value, Key} = Stu ->
            case Iff({Key, Value}) of
                true ->
                    [build_result_entry(Stu, head_match, Key, Value) |
                     preview_head(IndexTable, ID, ivk, SearchTerm, Stu, Limit-1, Iff)];
                false ->
                    preview_head(IndexTable, ID, ivk, SearchTerm, Stu, Limit, Iff)
            end;
        _ -> []
    end;
preview_head(IndexTable, ID, iv_k, SearchTerm, {ID, Value} = Stu, Limit, Iff) ->
    case imem_if:read(IndexTable, Stu) of
        [#ddIndex{stu = {ID, Value}, lnk = Key} = Entry] ->
            case Iff({Key, Value}) of
                true ->
                    Partial = [build_result_entry(Entry#ddIndex.stu, head_match, Key, Value)];
                false -> Partial = []
            end;
        _ -> Partial = []
    end,
    if
        length(Partial) < Limit ->
            SizeSearch = size(SearchTerm),
            case imem_if:next(IndexTable, Stu) of
                '$end_of_table' -> Partial;
                {ID, <<SearchTerm:SizeSearch/binary, _/binary>>} = NextStu ->
                    Partial ++ preview_head(IndexTable, ID, iv_k, SearchTerm, NextStu, Limit - length(Partial), Iff);
                _ -> Partial
            end;
        true ->
            Partial
    end;
preview_head(IndexTable, ID, iv_kl, SearchTerm, {ID, Value} = Stu, Limit, Iff) ->
    case imem_if:read(IndexTable, Stu) of
        [#ddIndex{stu = {ID, Value}, lnk = KeyList} = Entry] ->
            Partial = preview_expand_kl(head_match, Entry#ddIndex.stu, KeyList, Value, Limit, Iff);
        _ ->
            Partial = []
    end,
    if
        length(Partial) < Limit ->
            SizeSearch = size(SearchTerm),
            case imem_if:next(IndexTable, Stu) of
                '$end_of_table' -> Partial;
                {ID, <<SearchTerm:SizeSearch/binary, _/binary>>} = NextStu ->
                    Partial ++ preview_head(IndexTable, ID, iv_kl, SearchTerm, NextStu, Limit - length(Partial), Iff);
                _ -> Partial
            end;
        true ->
            Partial
    end;
preview_head(IndexTable, ID, iv_h, SearchTerm, {ID, Value} = Stu, Limit, Iff) ->
    case imem_if:read(IndexTable, Stu) of
        [#ddIndex{stu = {ID, Value}, lnk = Hash}] ->
            Partial = preview_expand_hash(head_match, IndexTable, ID, Hash, Value, ?SMALLEST_TERM, Limit, Iff);
        _ ->
            Partial = []
    end,
    if
        length(Partial) < Limit ->
            SizeSearch = size(SearchTerm),
            case imem_if:next(IndexTable, Stu) of
                '$end_of_table' -> Partial;
                {ID, <<SearchTerm:SizeSearch/binary, _/binary>>} = NextStu ->
                    Partial ++ preview_head(IndexTable, ID, iv_h, SearchTerm, NextStu, Limit - length(Partial), Iff);
                _ -> Partial
            end;
        true ->
            Partial
    end.


preview_body(_IndexTable, _ID, ivk, _SearchTerm, _Stu, 0, _Iff) -> [];
preview_body(IndexTable, ID, ivk, SearchTerm, PrevStu, Limit, Iff) ->
    case imem_if:next(IndexTable, PrevStu) of
        '$end_of_table' -> [];
        {ID, Value, Key} = Stu ->
            case binary:match(Value, SearchTerm) =/= nomatch andalso Iff({Key, Value}) of
                true ->
                    [build_result_entry(Stu, body_match, Key, Value) |
                     preview_body(IndexTable, ID, ivk, SearchTerm, Stu, Limit-1, Iff)];
                false ->
                    preview_body(IndexTable, ID, ivk, SearchTerm, Stu, Limit, Iff)
            end;
        _ -> []
    end;
preview_body(IndexTable, ID, iv_k, SearchTerm, {ID, Value} = Stu, Limit, Iff) ->
    case imem_if:read(IndexTable, Stu) of
        [#ddIndex{stu = {ID, Value}, lnk = Key} = Entry] ->
            case binary:match(Value, SearchTerm) =/= nomatch andalso Iff({Key, Value}) of
                true ->
                    Partial = [build_result_entry(Entry#ddIndex.stu, body_match, Key, Value)];
                false -> Partial = []
            end;
        _ -> Partial = []
    end,
    if
        length(Partial) < Limit ->
            case imem_if:next(IndexTable, Stu) of
                '$end_of_table' -> Partial;
                {ID, _NextValue} = NextStu ->
                    Partial ++ preview_body(IndexTable, ID, iv_k, SearchTerm, NextStu, Limit - length(Partial), Iff);
                _ -> Partial
            end;
        true ->
            Partial
    end;
preview_body(IndexTable, ID, iv_kl, SearchTerm, {ID, Value} = Stu, Limit, Iff) ->
    case imem_if:read(IndexTable, Stu) of
        [#ddIndex{stu = {ID, Value}, lnk = KeyList} = Entry] ->
            case binary:match(Value, SearchTerm) =/= nomatch of
                true ->
                    Partial = preview_expand_kl(body_match, Entry#ddIndex.stu, KeyList, Value, Limit, Iff);
                false -> Partial = []
            end;
        _ ->
            Partial = []
    end,
    if
        length(Partial) < Limit ->
            case imem_if:next(IndexTable, Stu) of
                '$end_of_table' -> Partial;
                {ID, _NextValue} = NextStu ->
                    Partial ++ preview_body(IndexTable, ID, iv_kl, SearchTerm, NextStu, Limit - length(Partial), Iff);
                _ -> Partial
            end;
        true ->
            Partial
    end;
preview_body(IndexTable, ID, iv_h, SearchTerm, {ID, Value} = Stu, Limit, Iff) ->
    case imem_if:read(IndexTable, Stu) of
        [#ddIndex{stu = {ID, Value}, lnk = Hash}] ->
            case binary:match(Value, SearchTerm) =/= nomatch of
                true ->
                    Partial = preview_expand_hash(body_match, IndexTable, ID, Hash, Value, ?SMALLEST_TERM, Limit, Iff);
                false -> Partial = []
            end;
        _ ->
            Partial = []
    end,
    if
        length(Partial) < Limit ->
            case imem_if:next(IndexTable, Stu) of
                '$end_of_table' -> Partial;
                {ID, _NextValue} = NextStu ->
                    Partial ++ preview_body(IndexTable, ID, iv_h, SearchTerm, NextStu, Limit - length(Partial), Iff);
                _ -> Partial
            end;
        true ->
            Partial
    end.

-spec create_starting_stu(atom(), integer(), atom(), binary()) -> tuple().
create_starting_stu(ivk, ID, head_match, SearchTerm) -> {ID, SearchTerm, ?SMALLEST_TERM};
create_starting_stu(ivk, ID, _MatchType, _SearchTerm) -> {ID, ?SMALLEST_TERM, ?SMALLEST_TERM};
create_starting_stu(_Type, ID, head_match, SearchTerm) -> {ID, SearchTerm};
create_starting_stu(_Type, ID, _MatchType, _SearchTerm) -> {ID, ?SMALLEST_TERM}.

-spec preview_expand_kl(atom(), tuple(), list(), list(), integer(), fun()) -> list().
preview_expand_kl(_Type, _Stu, [], _SearchTerm, _Limit, _Iff) -> [];
preview_expand_kl(_Type, _Stu, _KeyList, _SearchTerm, 0, _Iff) -> [];
preview_expand_kl(Type, Stu, [Key | KeyList], SearchTerm, Limit, Iff) ->
    case Iff({Key, SearchTerm}) of
        true ->
            [build_result_entry(Stu, Type, Key, SearchTerm) |
             preview_expand_kl(Type, Stu, KeyList, SearchTerm, Limit-1, Iff)];
        false ->
            preview_expand_kl(Type, Stu, KeyList, SearchTerm, Limit, Iff)
    end.

-spec preview_expand_hash(atom(), atom(), integer(), integer(), binary(), binary(), integer(), fun()) -> list().
preview_expand_hash(_Type, _IdxTable, _ID, _Hash, _SearchTerm, _Key, 0, _Iff) -> [];
preview_expand_hash(Type, IndexTable, ID, Hash, SearchTerm, Key, Limit, Iff) ->
    case imem_if:next(IndexTable, {ID, Hash, Key}) of
        '$end_of_table' -> [];
        {ID, Hash, NextKey} = Stu->
            case Iff({NextKey, SearchTerm}) of
                true ->
                    [build_result_entry(Stu, Type, NextKey, SearchTerm) |
                     preview_expand_hash(Type, IndexTable, ID, Hash, SearchTerm, NextKey, Limit-1, Iff)];
                false ->
                    preview_expand_hash(Type, IndexTable, ID, Hash, SearchTerm, NextKey, Limit, Iff)
            end;
        _ -> []
    end.

-spec build_result_entry(integer(), atom(), binary(), term()) -> {atom(), binary(), term(), {integer(), term(), binary()}}.
build_result_entry(Stu, MatchType, Key, Value) -> {MatchType, Key, Value, Stu}.

-spec add_filter_duplicated(undefined | exact_match | head_match, binary(), function()) -> function().
add_filter_duplicated(undefined, _SearchTerm, Iff) -> Iff;
add_filter_duplicated(exact_match, SearchTerm, Iff) ->
    fun({Key, Value}) ->
        Value =/= SearchTerm andalso Iff({Key, Value})
    end;
add_filter_duplicated(head_match, SearchTerm, Iff) ->
    SizeSearch = size(SearchTerm),
    fun({Key, Value}) ->
       case Value of
           <<SearchTerm:SizeSearch/binary, _/binary>> -> false;
           _ -> Iff({Key, Value})
       end
    end.



%% ===================================================================
%% Glossary:
%% ===================================================================

%% IndexId: 
%%      ID of the index. (indexes share the same table, ID is used to
%%      differentiate indexes on different fields).
%% Search key: 
%%      Key on which the search gets done
%% Reference key: 
%%      Sometimes used key to store reference
%% Reference: 
%%      ID/Key of the object holding the value in the master table
%% FastLookupNumber:
%%      Plain integer or short hash of a value
%%
%%
%% Index Types:
%% ¯¯¯¯¯¯¯¯¯¯¯¯
%% ivk: default index type
%%          stu =  {IndexId,<<"Value">>,<<"Key">>}
%%          lnk =  0
%%
%% iv_k: unique key index
%%          stu =  {IndexId,<<"UniqueValue">>}
%%          lnk =  <<"Key">>
%%       observation: should crash/throw/error on duplicate value insertion
%%
%% iv_kl: high selectivity index (aka "almost unique")
%%          stu =  {IndexId,<<"AlmostUniqueValue"}
%%          lnk =  [<<"Key1">>,..<<"Keyn">>]            %% usorted list of keys
%%
%% iv_h: low selectivity hash map index 
%%          For the values:
%%              stu =  {IndexId,<<"CommonValue">>}
%%              lnk =  Hash
%%          For the links to the references:
%%              stu =  {IndexId, Hash, <<"Key">>}
%%              lnk =  0
%%
%% ivvk: combined index of 2 fields
%%          stu =  {IndexId,<<"ValueA">>,<<"ValueB">>,Reference}
%%          lnk =  0
%%
%% ivvvk: combined index of 3 fields
%%          stu =  {IndexId,<<"ValueA">>,<<"ValueB">>,<<"ValueB">>,Reference}
%%          lnk =  0
%%
%% How it should be used:
%% ¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯
%% Basically, it's an mnesia-managed orderes set ETS table, where one uses regexp or binary_match
%% operations to iterate on and find matching values and their link back to the objects
%% stored in the master table.
%%
%% It avoids the need to decode raw binary json documents stored in the master table, for
%% faster filtering/searching.
%%
%% It could also be used to provide search-term and/or auto-correction suggestions.
%%
%% Index SHOULD NOT normalize (accent fold, lowercase, ...). That should be left over 
%% to higher level processes (this precludes the use of binary:match/2 for any matching,
%% because case insensitivity can not be guaranteed. Twice as slow regexp will have to be
%% used instead).
%%
%% Suggested implementation:
%% ¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯
%% As a simple_one_for_one gen_server, so index queries can be non-blocking and resolved
%% in parallel, while still being supervised.
%%
%% Index queries could also use the module as a library, having access to all its functionality,
%% but in a sequential, single-threaded way.
%% 
%% Offered functions would abstract different modes of usage, through the use of an
%% environment setting, constant or even global variable.
%%
%%
%% Observations:
%% ¯¯¯¯¯¯¯¯¯¯¯¯¯
%% - imem_index should use imem_if primitives to access data
%%
%% Proposed functionality:
%% ¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯¯
%% case insensitive search: 
%%    - provide IndexId, input string, Limit
%%    - output format:	[ {headmatch, HeadMatchString, HeadMatchResults}
%%						, {anymatch, AnyMatchString, AnyMatchResults}
%%						, {regexpmatch, RegexpMatchString, RegexpMatchResults}
%%						]
%%
%% How it should work:
%%    If input string contains wildcards or regexp-like characters (*?%_)
%%		-> convert to regexp pattern, and perform only a regexp-match. Other result "sets" will be empty.
%% 	  Else
%%    	Should first execute headmatch.
%%		If enough results
%%		  ->	other result "sets" will be empty
%%		Else (not enough results)
%%        -> do anymatch (basic binary_match inside string)

-spec index_type(atom()|binary()|{}) -> atom().
index_type({}) ->                   ivk;     %% parser's default type today, ToDo: remove when parser returns 'undefined'
index_type(A) when is_atom(A) ->    index_type(list_to_binary(atom_to_list(A)));
index_type(<<"undefined">>) ->      ivk;     %% ToDo: default type 'undefined' should come from the parser
index_type(<<"unique">>) ->         iv_k;
index_type(<<"keylist">>) ->        iv_kl;
index_type(<<"hashmap">>) ->        iv_h;
index_type(<<"bintree">>) ->        ivk;     %% might come from the parser in the future
index_type(<<"bitmap">>) ->         iv_b;
index_type(IndexType) ->            ?ClientError({"Index type not supported", IndexType}).  

%% @doc Supports accent folding for all alphabetical characters supported by ISO 8859-15
%% ISO 8859-15 supports the following languages: Albanian, Basque, Breton,
%% Catalan,  Danish,  Dutch,  English, Estonian, Faroese, Finnish, French,
%% Frisian,  Galician,  German,  Greenlandic,  Icelandic,  Irish   Gaelic,
%% Italian,  Latin,  Luxemburgish,  Norwegian, Portuguese, Rhaeto-Romanic,
%% Scottish Gaelic, Spanish, and Swedish.
-spec binstr_accentfold(binary()) -> binary().
binstr_accentfold(Str) when is_binary(Str) ->
   b_convert(Str,<<>>). 

    b_convert(<<>>,A) -> A;
    b_convert(<<195,C,R/binary>>,A) when C >= 128, C =<  133 -> % À Á Â Ã Ä Å
        b_convert(R,<<A/binary,$A>>);   
    b_convert(<<195,C,R/binary>>,A) when C >= 136, C =<  139 -> % È É Ê Ë
        b_convert(R,<<A/binary,$E>>);   
    b_convert(<<195,C,R/binary>>,A) when C >= 140, C =<  143 -> % Ì Í Î Ï 
        b_convert(R,<<A/binary,$I>>);   
    b_convert(<<195,C,R/binary>>,A) when C >= 146, C =<  150; C =:= 152 -> % Ò Ó Ô Õ Ö Ø
        b_convert(R,<<A/binary,$O>>);   
    b_convert(<<195,C,R/binary>>,A) when C >= 153, C =<  156 -> % Ù Ú Û Ü
        b_convert(R,<<A/binary,$U>>);   
    b_convert(<<195,C,R/binary>>,A) when C =:= 157 -> % Ý
        b_convert(R,<<A/binary,$Y>>);   
    b_convert(<<197,C,R/binary>>,A) when C =:= 184 -> % CAPITAL LETTER Y WITH DIAERESIS
        b_convert(R,<<A/binary,$Y>>);   
    b_convert(<<195,C,R/binary>>,A) when C =:= 135 -> % Ç
        b_convert(R,<<A/binary,$C>>); 
    b_convert(<<195,C,R/binary>>,A) when C =:= 145 -> % Ñ
        b_convert(R,<<A/binary,$N>>); 
    b_convert(<<195,C,R/binary>>,A) when C =:= 134 -> % Æ -> AE
        b_convert(R,<<A/binary,$A,$E>>); 
    b_convert(<<195,C,R/binary>>,A) when C =:= 158 -> % Þ -> TH
        b_convert(R,<<A/binary,$T,$H>>); 
    b_convert(<<195,C,R/binary>>,A) when C =:= 144 -> % Ð -> D
        b_convert(R,<<A/binary,$D>>); 
    b_convert(<<197,C,R/binary>>,A) when C =:= 160 -> % S WITH CARON
        b_convert(R,<<A/binary,$S>>); 
    b_convert(<<197,C,R/binary>>,A) when C =:= 189 -> % Z WITH CARON
        b_convert(R,<<A/binary,$Z>>); 
    b_convert(<<197,C,R/binary>>,A) when C =:= 146 -> % OE LIGATURE -> OE
        b_convert(R,<<A/binary,$O,$E>>); 
    
    b_convert(<<195,C,R/binary>>,A) when C >= 160, C =<  165 -> % à á â ã ä å
        b_convert(R,<<A/binary,$a>>);   
    b_convert(<<195,C,R/binary>>,A) when C >= 168, C =<  171 -> % è é ê ë
        b_convert(R,<<A/binary,$e>>);   
    b_convert(<<195,C,R/binary>>,A) when C >= 172, C =<  175 -> % ì í î ï
        b_convert(R,<<A/binary,$i>>);   
    b_convert(<<195,C,R/binary>>,A) when C >= 178, C =<  182; C =:= 184 -> % ò ó ô õ ö ø
        b_convert(R,<<A/binary,$o>>);   
    b_convert(<<195,C,R/binary>>,A) when C >= 185, C =<  188 -> % ù ú û ü
        b_convert(R,<<A/binary,$u>>);   
    b_convert(<<195,C,R/binary>>,A) when C =:= 189; C =:=  191 -> % ý ÿ
        b_convert(R,<<A/binary,$y>>); 
    b_convert(<<195,C,R/binary>>,A) when C =:= 167 -> % ç
        b_convert(R,<<A/binary,$c>>); 
    b_convert(<<195,C,R/binary>>,A) when C =:= 166 -> % æ -> ae
        b_convert(R,<<A/binary,$a,$e>>); 
    b_convert(<<195,C,R/binary>>,A) when C =:= 177 -> % ñ
        b_convert(R,<<A/binary,$n>>); 
    b_convert(<<197,C,R/binary>>,A) when C =:= 161 -> % s WITH CARON
        b_convert(R,<<A/binary,$s>>); 
    b_convert(<<197,C,R/binary>>,A) when C =:= 190 -> % z WITH CARON
        b_convert(R,<<A/binary,$z>>); 
    b_convert(<<195,C,R/binary>>,A) when C =:= 159 -> % ß -> ss
        b_convert(R,<<A/binary,$s,$s>>); 
    b_convert(<<197,C,R/binary>>,A) when C =:= 147 -> % oe LIGATURE -> oe
        b_convert(R,<<A/binary,$o,$e>>); 
    b_convert(<<195,C,R/binary>>,A) when C =:= 190 -> % þ -> th
        b_convert(R,<<A/binary,$t,$h>>); 
    b_convert(<<195,C,R/binary>>,A) when C =:= 176 -> % ð -> d
        b_convert(R,<<A/binary,$d>>); 
    
    b_convert(<<H,R/binary>>,A) -> 
        b_convert(R,<<A/binary,H>>).

%% @doc lowercases all alphabetical characters supported by ISO 8859-15
-spec binstr_to_lower(binary()) -> binary().
binstr_to_lower(Str) when is_binary(Str) ->
    b_lower(Str,<<>>).

    b_lower(<<>>,A) -> A;
    b_lower(<<195,C,R/binary>>,A) when C >= 128, C =<  150; C >= 152, C =< 158-> % Standard range
        NC = C + 32,
        b_lower(R,<<A/binary,195,NC>>);
    b_lower(<<197,C,R/binary>>,A) when C =:= 160; C =:= 146; C =:= 189-> % Non-standard range
        NC = C + 1,
        b_lower(R,<<A/binary,197,NC>>);
    b_lower(<<197,C,R/binary>>,A) when C =:= 184-> % Special case CAPITAL LETTER Y WITH DIAERESIS
        b_lower(R,<<A/binary,195,191>>);
    b_lower(<<C,R/binary>>,A) when C >=  65, C =< 90 -> 
        NC = C + 32,
        b_lower(R,<<A/binary,NC>>);
    b_lower(<<H,R/binary>>,A) -> 
        b_lower(R,<<A/binary,H>>).
 
%% @doc uppercases all alphabetical characters supported by ISO 8859-15
-spec binstr_to_upper(binary()) -> binary().
binstr_to_upper(Str) when is_binary(Str) ->
    b_upper(Str,<<>>).

    b_upper(<<>>,A) -> A;

    b_upper(<<195,C,R/binary>>,A) when C >= 160, C =<  182; C >= 184, C =< 190-> % Standard range
        NC = C - 32,
        b_upper(R,<<A/binary,195,NC>>);
    b_upper(<<197,C,R/binary>>,A) when C =:= 161; C =:= 147; C =:= 190-> % Non-standard range
        NC = C - 1,
        b_upper(R,<<A/binary,197,NC>>);
    b_upper(<<195,C,R/binary>>,A) when C =:= 191-> % Special case CAPITAL LETTER Y WITH DIAERESIS
        b_upper(R,<<A/binary,197,184>>);
    b_upper(<<C,R/binary>>,A) when C >=  97, C =< 122 -> 
        NC = C - 32,
        b_upper(R,<<A/binary,NC>>);
    b_upper(<<H,R/binary>>,A) -> 
        b_upper(R,<<A/binary,H>>).


%% @doc Walks binary string, keeps only valid ascii characters
-spec binstr_only_ascii(binary()) -> binary().
binstr_only_ascii(BinStr) when is_binary(BinStr) ->
    binstr_only_ascii(BinStr,"");
binstr_only_ascii(BinStr) when is_list(BinStr) ->
    binstr_only_ascii(list_to_binary(BinStr),"").
binstr_only_ascii(BinStr,PlaceHolder) ->
    b_afilter(BinStr,PlaceHolder,<<>>).

    b_afilter(<<>>,_,A) -> A;
    % Unprintable characters:0 - 9, 11, 12, 14 - 31, 127
    b_afilter(<<C,R/binary>>,PH,A) when C >= 0, C =<  9;
                                       C =:= 11; C =:= 12;
                                       C >= 14, C =< 31;
                                       C >= 127 ->

        b_vfilter(R,PH,?BIN_APP(A,PH));
    b_afilter(<<C,R/binary>>,PH,A) ->
        b_afilter(R,PH,<<A/binary,C>>).

%% @doc Walks binary string, keeps only valid and displayable) utf8 characters
-spec binstr_only_valid(binary()) -> binary().
binstr_only_valid(Binstr) ->
    binstr_only_valid(Binstr,"").
binstr_only_valid(BinStr, PH) when is_binary(BinStr) ->
    b_vfilter(BinStr,PH,<<>>).

    b_vfilter(<<>>,_,A) -> A;
    % Displayable One-byte UTF8 (== ASCII)
    b_vfilter(<<C,R/binary>>,PH,A) when C =:= 10; % \n
                                       C =:= 13;  % \r
                                       C >= 32, C =<  126 % character range
                                       ->
        b_vfilter(R,PH,<<A/binary,C>>);
    % Two-byte UTF8 192-223, 128-191
    b_vfilter(<<M,C,R/binary>>,PH,A) when M >= 195, M =< 223, C >= 128, C =<  191;
                                          M >= 194, C >= 160, C =< 191 ->
        b_vfilter(R,PH,<<A/binary,M,C>>);

    % Three-byte UTF8 224-239, 128-191, 128-191
    b_vfilter(<<M,C1,C2,R/binary>>,PH,A) when M >= 224, M =< 239,
                                              C1 >= 128, C1 =<  191,
                                              C2 >= 128, C2 =<  191 ->
        b_vfilter(R,PH,<<A/binary,M,C1,C2>>);
   
    % Four-byte UTF8 240-247, 128-191, 128-191, 128-191
    b_vfilter(<<M,C1,C2,C3,R/binary>>,PH,A) when M >= 240, M =< 247,
                                              C1 >= 128, C1 =<  191,
                                              C2 >= 128, C2 =<  191,
                                              C3 >= 128, C3 =<  191 ->
        b_vfilter(R,PH,<<A/binary,M,C1,C2,C3>>);
    % Five-byte UTF8 248-251, 128-191, 128-191, 128-191, 128-191
    b_vfilter(<<M,C1,C2,C3,C4,R/binary>>,PH,A) when M >= 248, M =< 251,
                                              C1 >= 128, C1 =<  191,
                                              C2 >= 128, C2 =<  191,
                                              C3 >= 128, C3 =<  191,
                                              C4 >= 128, C4 =<  191 ->
        b_vfilter(R,PH,<<A/binary,M,C1,C2,C3,C4>>);
    % Six-byte UTF8 252-253, 128-191, 128-191, 128-191, 128-191, 128-191, 
    b_vfilter(<<M,C1,C2,C3,C4,C5,R/binary>>,PH,A) when M >= 252, M =< 253,
                                              C1 >= 128, C1 =<  191,
                                              C2 >= 128, C2 =<  191,
                                              C3 >= 128, C3 =<  191,
                                              C4 >= 128, C4 =<  191,
                                              C5 >= 128, C5 =<  191 ->
        b_vfilter(R,PH,<<A/binary,M,C1,C2,C3,C4,C5>>);
        
    % Everything else (garbage)
    b_vfilter(<<_,R/binary>>,PH,A) ->
        b_vfilter(R,PH,?BIN_APP(A,PH)).

%% @doc Walks binary string, keeps only valid and displayable) utf8 characters
%% also present in the latin1 characterset
-spec binstr_only_latin1(binary()) -> binary().
binstr_only_latin1(Binstr) ->
    binstr_only_latin1(Binstr,"").

binstr_only_latin1(BinStr, PH) when is_binary(BinStr) ->
    b_lfilter(BinStr,PH,<<>>).

    b_lfilter(<<>>,_,A) -> A;
    % Displayable One-byte UTF8 (== ASCII)
    b_lfilter(<<C,R/binary>>,PH,A) when C =:= 10; % \n
                                       C =:= 13;  % \r
                                       C >= 32, C =<  126 % character range
                                       ->
        b_lfilter(R,PH,<<A/binary,C>>);
    % Two-byte UTF8 194-197, 128-191, basic latin1 extension
    b_lfilter(<<M,C,R/binary>>,PH,A) when M >= 195, M =< 197, C >= 128, C =<  191;
                                          M >= 194, C >= 160, C =< 191 ->
        b_lfilter(R,PH,<<A/binary,M,C>>);
       
    % Everything else (garbage)
    b_lfilter(<<_,R/binary>>,PH,A) ->
        b_lfilter(R,PH,?BIN_APP(A,PH)).

binstr_append_placeholder(Binstr,PH) ->
        case PH of
            "" -> <<Binstr/binary>>;
            <<>> -> <<Binstr/binary>>;
            _ -> <<Binstr/binary,PH/binary>>
        end.
        
    

binstr_match_anywhere(Subject,Pattern) when is_binary(Pattern); is_tuple(Pattern) ->
    case binary:match(Subject,Pattern) of
        nomatch -> false;
        {_,_}   -> true
    end;
binstr_match_anywhere(Subject,Pattern) when is_list(Pattern) ->
    binstr_match_anywhere(Subject,list_to_binary(Pattern)).

binstr_match_sub(Subject,Start,Length,Pattern) when is_binary(Pattern); is_tuple(Pattern) ->
    case binary:match(Subject,Pattern,[{scope, {Start,Length}}]) of
        nomatch -> false;
        {_,_}   -> true
    end;
binstr_match_sub(Subject,Start,Length,Pattern) when is_list(Pattern) ->
    binstr_match_sub(Subject,Start,Length,list_to_binary(Pattern)).

binstr_match_precompile(Pattern) ->
    binary:compile_pattern(Pattern).


    
%% ===================================================================
%% TESTS 
%% ===================================================================
-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

setup() -> ok.

teardown(_) -> ok.

string_test_() ->
    {
        setup,
        fun setup/0,
        fun teardown/1,
        {with, [
              fun string_operations/1
              , fun iff_functions/1
        ]}}.    

iff_functions(_) ->
    ?LogDebug("---TEST---~p:iff_functions~n", [?MODULE]),
    ?assertEqual(true, iff_list_pattern([1,2,3],[1,2,3])),
    ?assertEqual(true, iff_list_pattern([1,2,3],[1,'_',3])),
    ?assertEqual(true, iff_list_pattern([1,2,3],[1,2,'_'])),
    ?assertEqual(true, iff_list_pattern([1,2,3],['_','_','_'])),
    ?assertEqual(true, iff_list_pattern([1,2,3],[1,'*'])),
    ?assertEqual(true, iff_list_pattern([1,2,3,4,5],[1,'*'])),
    ?assertEqual(true, iff_list_pattern([1,2,3],['*'])),
    ?assertEqual(true, iff_list_pattern([1,2],['*'])),
    ?assertEqual(true, iff_list_pattern([1],['*'])),
    ?assertEqual(true, iff_list_pattern([],['*'])),
    ?assertEqual(true, iff_list_pattern([1,2,3],['_',2,3])),
    ?assertEqual(true, iff_list_pattern([1,2,3],['_','_',3])),
    ?assertEqual(false, iff_list_pattern([1,2,3],[1,'_'])),
    ?assertEqual(false, iff_list_pattern([2,3],[1,'_'])),
    ?assertEqual(false, iff_list_pattern([2,3],[2,'_','_'])),
    ?assertEqual(false, iff_list_pattern([1,2],['_'])).


string_operations(_) ->
    ?LogDebug("---TEST---~p:string_operations~n", [?MODULE]),
    ?assertEqual([<<"table">>], vnf_lcase_ascii(<<"täble"/utf8>>)),
    ?assertEqual([<<"tuble">>], vnf_lcase_ascii(<<"tüble"/utf8>>)).

binstr_accentfold_test_() ->
    %UpperCaseAcc = <<"À Á Â Ã Ä Å Æ Ç È É Ê Ë Ì Í Î Ï Ð Ñ Ò Ó Ô Õ Ö Ø Ù Ú Û Ü Ý Þ Ÿ Œ Š Ž"/utf8>>,
    UpperCaseRaw = <<195,128,32,195,129,32,195,130,32,195,131,32,195,132,32,195,133,32,195,134,32,
                     195,135,32,195,136,32,195,137,32,195,138,32,195,139,32,195,140,32,195,141,32,
                     195,142,32,195,143,32,195,144,32,195,145,32,195,146,32,195,147,32,195,148,32,
                     195,149,32,195,150,32,195,152,32,195,153,32,195,154,32,195,155,32,195,156,32,
                     195,157,32,195,158,32,197,184,32,197,146,32,197,160,32,197,189>>,
    UpperCaseUnn = <<"A A A A A A AE C E E E E I I I I D N O O O O O O U U U U Y TH Y OE S Z">>,
    %LowerCaseAcc = <<"à á â ã ä å æ ç è é ê ë ì í î ï ð ñ ò ó ô õ ö ø ù ú û ü ý þ ÿ œ š ß ž"/utf8>>,
    LowerCaseRaw = <<195,160,32,195,161,32,195,162,32,195,163,32,195,164,32,195,165,32,195,166,32,
                     195,167,32,195,168,32,195,169,32,195,170,32,195,171,32,195,172,32,195,173,32,
                     195,174,32,195,175,32,195,176,32,195,177,32,195,178,32,195,179,32,195,180,32,
                     195,181,32,195,182,32,195,184,32,195,185,32,195,186,32,195,187,32,195,188,32,
                     195,189,32,195,190,32,195,191,32,197,147,32,197,161,32,195,159,32,197,190>>,
    LowerCaseUnn = <<"a a a a a a ae c e e e e i i i i d n o o o o o o u u u u y th y oe s ss z">>,

    [?_assertEqual(UpperCaseUnn,binstr_accentfold(UpperCaseRaw)),
     ?_assertEqual(LowerCaseUnn,binstr_accentfold(LowerCaseRaw))
    ].

binstr_casemod_test_()->
    %UpperCaseAcc = <<"ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789[]{}À Á Â Ã Ä Å Æ Ç È É Ê Ë Ì Í Î Ï Ð Ñ Ò Ó Ô Õ Ö Ø Ù Ú Û Ü Ý Þ Ÿ"/utf8>>,
    %LowerCaseAcc = <<"abcdefghijklmnopqrstuvwxyz0123456789[]{}à á â ã ä å æ ç è é ê ë ì í î ï ð ñ ò ó ô õ ö ø ù ú û ü ý þ ÿ"/utf8>>,
    UpperCaseRaw = <<65,66,67,68,69,70,71,72,73,74,75,76,77,78,79,80,81,82,83,84,85,86,87,88,89,90,48,49,50,51,52,53,54,55,56,57,91,93,123,125,195,128,32,195,129,32,195,130,32,195,131,32,195,132,32,195,133,32,195,134,32,195,135,32,195,136,32,195,137,32,195,138,32,195,139,32,195,140,32,195,141,32,195,142,32,195,143,32,195,144,32,195,145,32,195,146,32,195,147,32,195,148,32,195,149,32,195,150,32,195,152,32,195,153,32,195,154,32,195,155,32,195,156,32,195,157,32,195,158,32,197,184>>,
    LowerCaseRaw = <<97,98,99,100,101,102,103,104,105,106,107,108,109,110,111,112,113,114,115,116,117,118,119,120,121,122,48,49,50,51,52,53,54,55,56,57,91,93,123,125,195,160,32,195,161,32,195,162,32,195,163,32,195,164,32,195,165,32,195,166,32,195,167,32,195,168,32,195,169,32,195,170,32,195,171,32,195,172,32,195,173,32,195,174,32,195,175,32,195,176,32,195,177,32,195,178,32,195,179,32,195,180,32,195,181,32,195,182,32,195,184,32,195,185,32,195,186,32,195,187,32,195,188,32,195,189,32,195,190,32,195,191>>,
    [?_assertEqual(UpperCaseRaw,binstr_to_upper(LowerCaseRaw)),
     ?_assertEqual(LowerCaseRaw,binstr_to_lower(UpperCaseRaw))].

-define(TL,unicode:characters_to_list).
vnf_lcase_ascii_test_() ->
    [{"empty",?_assertEqual([<<>>],vnf_lcase_ascii(<<"">>))},
     {"from binary0",?_assertEqual([<<"table">>],vnf_lcase_ascii(<<"täble"/utf8>>))},
     {"from binary1",?_assertEqual([<<"tuble">>],vnf_lcase_ascii(<<"tüble"/utf8>>))},
     {"from binary2",?_assertEqual([<<"aaaeeeuu">>],vnf_lcase_ascii(<<"AÀäëéÈüÜ"/utf8>>))},
     {"from list",?_assertEqual([<<"aaaeee">>],vnf_lcase_ascii("AÀäëéÈ"))},
     {"from atom",?_assertEqual([<<"atom">>],vnf_lcase_ascii(aTom))},
     {"from tuple",?_assertEqual([<<"{\"aaaeee\"}">>],vnf_lcase_ascii({"AÀäëéÈ"}))},
     {"from integer",?_assertEqual([<<"12798">>],vnf_lcase_ascii(12798))},
     {"from random",?_assertEqual([<<"g:xr*a\\6r">>],vnf_lcase_ascii(<<71,191,58,192,88,82,194,42,223,65,187,19,92,145,228,248, 26,54,196,114>>))}
     ].

binstr_only_ascii_test_() ->
    [{"form àç90{}",?_assertEqual(<<" 90{}">>,binstr_only_ascii(<<"àç 90{}">>))},
     {"random",?_assertEqual(<<"G:XR*A\\6r">>,binstr_only_ascii(<<71,191,58,192,88,82,194,42,223,65,187,19,92,145,228,248, 26,54,196,114>>))}
    ].

binstr_only_valid_test_() ->
    Random = <<74,94,160,102,193,249,94,21,66,87,242,109,13,107,163,36,165,68,215,
               193,133,58,191,65,41,23,172,79,127,88,215,14,244,33,223,179,217,17,
               86,174,55,29,132,221,124,112,34,14,192,37,153,199,176,212,35,207,115,
               22,41,104,150,48,92,245>>,
    [{"form àç90{}",?_assertEqual(<<"àç 90{}"/utf8>>,binstr_only_valid(<<"àç 90{}"/utf8,138,255>>))},
     {"random",?_assertEqual(<<"J^f^BWm\rk$D:A)OX!ß³V7|p\"%Ç°#s)h0\\">>
                            ,binstr_only_valid(Random))}

    ].

binstr_only_latin1_test_() ->
    Random = <<74,94,160,102,193,249,94,21,66,87,242,109,13,107,163,36,165,68,215,
               193,133,58,191,65,41,23,172,79,127,88,215,14,244,33,223,179,217,17,
               86,174,55,29,132,221,124,112,34,14,192,37,153,199,176,212,35,207,115,
               22,41,104,150,48,92,245>>,
    [{"form àç90{}",?_assertEqual(<<"àç 90{}"/utf8>>,binstr_only_latin1(<<"àç 90{}"/utf8,138,255>>))},
     {"random",?_assertEqual(<<"J^f^BWm\rk$D:A)OX!ß³V7|p\"%Ç°#s)h0\\">>
                            ,binstr_only_latin1(Random))}

    ].
    
-endif.
