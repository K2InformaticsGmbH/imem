%% -*- coding: utf-8 -*-
-module(imem_cmp).                  %% module for data comparisons

-include("imem.hrl").
-include("imem_meta.hrl").

-define(CMP_EQUAL,<<"=">>).         %% for cmp/2 and cmp/3 (Token Comparison)
-define(CMP_NAV,<<>>).              %% for cmp/2 and cmp/3 (one or both sides ?nav, not a value)
-define(CMP_SMALLER_LEFT,<<"<">>).
-define(CMP_SMALLER_RIGHT,<<">">>).
-define(CMP_WHITE_LEFT,<<"w=">>).   %% added whitespace on the left
-define(CMP_WHITE_RIGHT,<<"=w">>).  %% added whitespace on the right
-define(CMP_WHITE,<<"w=w">>).       %% added whitespace on both sides


-define(CMP_EQUAL_KV,<<"==">>).     %% for cmp/2 and cmp/3 (KV-Comparison)
-define(CMP_EQUAL_KEY_SMALLER_LEFT,<<"=<">>).
-define(CMP_EQUAL_KEY_SMALLER_RIGHT,<<"=>">>).
-define(CMP_SMALLER_KEY_LEFT,<<"<*">>).
-define(CMP_SMALLER_KEY_RIGHT,<<">*">>).
-define(CMP_EQUAL_KEY_WHITE_LEFT,<<"=w=">>).   %% added whitespace on the left value
-define(CMP_EQUAL_KEY_WHITE_RIGHT,<<"==w">>).  %% added whitespace on the right value
-define(CMP_EQUAL_KEY_WHITE,<<"=w=w">>).       %% added whitespace on both sides value

-define(CMP_SPACE,32).              %% Stereotype for white space
-define(CMP_CHAR,$@).               %% Stereotype for non-white space
-define(CMP_OPS,"()[]{}+-*/<>=|,").
-define(CMP_NO_SPLIT,["<<", ">>", "<>", "->", "=>", "<=","==","<=",">=","=<","!=","++","--","||", ":=", "=:"]).
-define(CMP_WHITE_SPACE," \t\r\n").

-export([ cmp/2                     %% compare two terms (LeftKey,RightKey)
        , cmp/3                     %% compare two terms (LeftKey,RightKey, Opts)
        , cmp/4                     %% compare two terms with payloads (LeftKey,LeftVal,RightKey,RightVal)
        , cmp/5                     %% compare two terms with payloads (LeftKey,LeftVal,RightKey,RightVal,Opts)
        , cmp_trim/2
        , cmp_white_space/1         %% is the argument a string or a binary containing only white space?
        , norm_white_space/1        %% reduce whitespace to the mminimum without corrupting (Erlang/SQL) code
        ]).

-safe(all).

%% @doc Compare two erlang terms or strings or lists of tokens 
-spec cmp(any(),any()) -> binary().
cmp(L, R) -> cmp(L, R, []). 

%% @doc Compare two erlang terms or strings or lists of tokens with options (not used so far) 
%% Binary parameters are converted to lists and then compared (as lists of characters/bytes).
%% Tuple parameters are split into a key and a value part (last element).
%% For lists of integers (characters/bytes), white space differences are detected as such
%% by considering whitespace relevance for popular programming languages.
%% Result indicates equalness or whitespace equalness or erlang term sort order for difference.
-spec cmp(any(), any(), ddOptions()) -> binary().
cmp(?nav, _, _) -> ?CMP_NAV; 
cmp(_, ?nav, _) -> ?CMP_NAV; 
cmp({AK,AV}, {BK,BV}, Opts) -> cmp(AK, AV, BK, BV, Opts); 
cmp({AK1,AK2,AV}, {BK1,BK2,BV}, Opts) -> cmp({AK1,AK2}, AV, {BK1,BK2}, BV, Opts); 
cmp({AK1,AK2,AK3,AV}, {BK1,BK2,BK3,BV}, Opts) -> cmp({AK1,AK2,AK3}, AV, {BK1,BK2,BK3}, BV, Opts); 
cmp(L, L, _) -> ?CMP_EQUAL; 
cmp(L, R, Opts) when is_binary(L) ->
    cmp(binary_to_list(L), R, Opts);
cmp(L, R, Opts) when is_binary(R) ->
    cmp(L, binary_to_list(R), Opts);
cmp(L, R, Opts) when is_list(L), is_list(R) ->
    Diff = imem_tdiff:diff(L, R, Opts),
    DiffLeft = cmp_trim(Diff, del),
    DiffRight = cmp_trim(Diff, ins),
    DiffBoth = cmp_trim(DiffRight,del),
    case {imem_tdiff:is_eq(DiffLeft), imem_tdiff:is_eq(DiffRight), imem_tdiff:is_eq(DiffBoth)} of
        {false,false,false} when L < R -> ?CMP_SMALLER_LEFT;
        {false,false,false} -> ?CMP_SMALLER_RIGHT;
        {true,_,_} -> ?CMP_WHITE_LEFT;
        {_,true,_} -> ?CMP_WHITE_RIGHT;
        {_,_,true} -> ?CMP_WHITE
    end;
cmp(L, R, _Opts) when L < R -> ?CMP_SMALLER_LEFT;
cmp(_, _, _Opts) -> ?CMP_SMALLER_RIGHT.

cmp_trim(Diff, Dir) -> cmp_trim(?CMP_SPACE, Diff, Dir, []).

cmp_trim(_Last, [], _Dir, Acc) -> lists:reverse(Acc);
cmp_trim(Last, [{Dir,Str}], Dir, Acc) -> 
    case cmp_white_space(Str) of
        true ->     cmp_trim(Last, [], Dir, Acc);
        false ->    cmp_trim(Last, [], Dir, [{Dir,Str}|Acc])
    end;
cmp_trim(Last, [{Dir,Str},{eq,[Next|Chars]}|Diff], Dir, Acc) ->
    %% see if added white space insert/delete is allowed
    case {cmp_white_space(Str),cmp_may_split([Last,Next])} of
        {true, true} -> cmp_trim(lists:last([Next|Chars]), Diff, Dir, [{eq,[Next|Chars]}|Acc]);
        {_,_} ->        cmp_trim(lists:last([Next|Chars]), Diff, Dir, [{eq,[Next|Chars]},{Dir,Str}|Acc])
    end;   
cmp_trim(_Last, [{Chg,Str}|Diff], Dir, Acc) ->
    %% keep equal or opposite piece and move forward
    cmp_trim(lists:last(Str), Diff, Dir, [{Chg,Str}|Acc]).

cmp_white_space(B) when is_binary(B) -> cmp_white_space(binary_to_list(B));
cmp_white_space([]) -> true;
cmp_white_space([Ch|Chrs]) ->
    case lists:member(Ch,?CMP_WHITE_SPACE) of
        false ->    false;
        true ->     cmp_white_space(Chrs)
    end;
cmp_white_space(_NotaList) -> false.

norm_white_space(B) when is_binary(B) -> list_to_binary(norm_white_space(binary_to_list(B)));
norm_white_space(L) when is_list(L) -> norm_ws(L);
norm_white_space(I) -> I.

norm_ws(L) -> norm_ws(?CMP_SPACE, L, []).

norm_ws(_, [], Acc) -> lists:reverse(Acc);
norm_ws(_Last, [Ch|List], Acc) when Ch>?CMP_SPACE ->
    norm_ws(Ch, List, [Ch|Acc]);
norm_ws(Last, [Ch,Next|List], Acc) ->
    IsWhite = lists:member(Ch,?CMP_WHITE_SPACE),
    NoSplit = lists:member([Last,Next],?CMP_NO_SPLIT),
    case {IsWhite,NoSplit} of
        {false,_} ->    norm_ws(Ch, [Next|List], [Ch|Acc]);
        {_,true}  ->    norm_ws(?CMP_SPACE, [Next|List], [?CMP_SPACE|Acc]);
        {true,false} -> 
            LeftWhite = lists:member(Last,?CMP_WHITE_SPACE),
            RightWhite =  lists:member(Next,?CMP_WHITE_SPACE),
            LeftOp = lists:member(Last,?CMP_OPS),
            RightOp =  lists:member(Next,?CMP_OPS),
            case lists:usort([LeftWhite,RightWhite,LeftOp,RightOp]) of
                [false] ->      norm_ws(?CMP_SPACE, [Next|List], [?CMP_SPACE|Acc]);
                [false,true] -> norm_ws(Last, [Next|List], Acc)
            end
    end;
norm_ws(_Last, [Ch], Acc) ->
    case lists:member(Ch,?CMP_WHITE_SPACE) of
        false ->    norm_ws(Ch, [], [Ch|Acc]);
        true  ->    norm_ws(?CMP_SPACE, [], Acc)
    end.

cmp_may_split([Last,Next]) ->
    NoSplit = lists:member([Last,Next],?CMP_NO_SPLIT),
    LeftWhite = lists:member(Last,?CMP_WHITE_SPACE),
    RightWhite =  lists:member(Next,?CMP_WHITE_SPACE),
    LeftOp = lists:member(Last,?CMP_OPS),
    RightOp =  lists:member(Next,?CMP_OPS),
    case {NoSplit,LeftWhite,RightWhite,LeftOp,RightOp} of
         {true   ,_        ,_         ,_     ,_      } -> false;
         {false  ,true     ,_         ,_     ,_      } -> true;   
         {false  ,_        ,true      ,_     ,_      } -> true;   
         {false  ,_        ,_         ,true  ,_      } -> true;   
         {false  ,_        ,_         ,_     ,true   } -> true;
         {false  ,false    ,false     ,false ,false  } -> false
    end. 

-spec cmp(any(), any(), any(), any()) -> binary().
cmp(L, R, LVal, RVal) -> cmp(L, R, LVal, RVal, []).

%% @doc Compare two erlang KV pairs of strings or lists of tokens with options (not used so far) 
%% Binary parameters are converted to lists and then compared (as lists of characters/bytes).
%% For the value part, white space differences are detected as such
%% by considering whitespace relevance for popular programming languages.
%% Result indicates equalness or whitespace equalness or erlang term sort order for difference.
-spec cmp(any(), any(), any(), any(), ddOptions()) -> binary().
cmp(?nav, ?nav, _, _, _) -> ?CMP_NAV; 
cmp(_, _, ?nav, ?nav, _) -> ?CMP_NAV; 
cmp(L, LVal, L, LVal, _) -> ?CMP_EQUAL_KV; 
cmp(L, LVal, R, RVal, Opts) when is_binary(L) ->
    cmp(binary_to_list(L), LVal, R, RVal, Opts);
cmp(L, LVal, R,  RVal, Opts) when is_binary(R) ->
    cmp(L, LVal, binary_to_list(R),  RVal, Opts);
cmp(L, LVal, R,  RVal, Opts) when is_binary(LVal) ->
    cmp(L, binary_to_list(LVal), R,  RVal, Opts);
cmp(L, LVal, R,  RVal, Opts) when is_binary(RVal) ->
    cmp(L, LVal, R,  binary_to_list(RVal), Opts);
cmp(L, LVal, L,  RVal, Opts) when is_list(LVal), is_list(RVal) ->
    Diff = imem_tdiff:diff(LVal, RVal, Opts),
    DiffLeft = cmp_trim(Diff, del),
    DiffRight = cmp_trim(Diff, ins),
    DiffBoth = cmp_trim(DiffRight,del),
    case {imem_tdiff:is_eq(DiffLeft), imem_tdiff:is_eq(DiffRight), imem_tdiff:is_eq(DiffBoth)} of
        {false,false,false} when LVal < RVal -> ?CMP_EQUAL_KEY_SMALLER_LEFT;
        {false,false,false} -> ?CMP_EQUAL_KEY_SMALLER_RIGHT;
        {true,_,_} -> ?CMP_EQUAL_KEY_WHITE_LEFT;
        {_,true,_} -> ?CMP_EQUAL_KEY_WHITE_RIGHT;
        {_,_,true} -> ?CMP_EQUAL_KEY_WHITE
    end;
cmp(L, LVal, L,  RVal, _Opts) when LVal < RVal -> ?CMP_EQUAL_KEY_SMALLER_LEFT;
cmp(L, _LVal, L,  _RVal, _Opts) -> ?CMP_EQUAL_KEY_SMALLER_RIGHT;
cmp(L, _LVal, R, _RVal, _Opts) when L < R -> ?CMP_SMALLER_KEY_LEFT;
cmp(_, _LVal, _, _RVal, _Opts) -> ?CMP_SMALLER_KEY_RIGHT.


%% ----- TESTS ------------------------------------------------
-ifdef(TEST).

-include_lib("eunit/include/eunit.hrl").

cmp_test_() ->
    [ {"CMP_EQUAL1",            ?_assertEqual(?CMP_EQUAL, cmp("123", "123"))}
    , {"CMP_EQUAL2",            ?_assertEqual(?CMP_EQUAL, cmp("A a", "A a"))}
    , {"CMP_EQUAL3",            ?_assertEqual(?CMP_EQUAL, cmp(" \t", " \t"))}
    , {"CMP_EQUAL4",            ?_assertEqual(?CMP_EQUAL, cmp("\r\n", "\r\n"))}
    , {"CMP_EQUAL5",            ?_assertEqual(?CMP_EQUAL, cmp("", ""))}
    , {"CMP_EQUAL6",            ?_assertEqual(?CMP_EQUAL, cmp(undefined, undefined))}
    , {"CMP_EQUAL7",            ?_assertEqual(?CMP_EQUAL, cmp(<<"A text">>, "A text"))}
    , {"CMP_EQUAL8",            ?_assertEqual(?CMP_EQUAL, cmp(9.1234, 9.1234))}

    , {"CMP_EQUAL9",            ?_assertEqual(?CMP_EQUAL_KV, cmp({1,2,3}, {1,2,3}))}

    , {"CMP_SMALLER_LEFT1",     ?_assertEqual(?CMP_SMALLER_LEFT, cmp("", "123"))}
    , {"CMP_SMALLER_LEFT2",     ?_assertEqual(?CMP_SMALLER_LEFT, cmp("ABC", "ADC"))}
    , {"CMP_SMALLER_LEFT3",     ?_assertEqual(?CMP_SMALLER_LEFT, cmp("AB", "ABC"))}
    , {"CMP_SMALLER_LEFT4",     ?_assertEqual(?CMP_SMALLER_LEFT, cmp(12, 13))}
    , {"CMP_SMALLER_LEFT5",     ?_assertEqual(?CMP_SMALLER_LEFT, cmp({1,2}, {0,1,2}))}
    , {"CMP_SMALLER_LEFT6",     ?_assertEqual(?CMP_SMALLER_LEFT, cmp(<<"A Text">>, <<"A t">>))}
    , {"CMP_SMALLER_LEFT7",     ?_assertEqual(?CMP_SMALLER_LEFT, cmp(12, <<"13">>))}

    , {"CMP_SMALLER_LEFT8",     ?_assertEqual(?CMP_EQUAL_KEY_SMALLER_LEFT, cmp({1,2}, {1,3}))}

    , {"CMP_SMALLER_RIGHT1",    ?_assertEqual(?CMP_SMALLER_RIGHT, cmp("AB", "A B"))}
    , {"CMP_SMALLER_RIGHT2",    ?_assertEqual(?CMP_SMALLER_RIGHT, cmp("AB", "A "))}
    , {"CMP_SMALLER_RIGHT3",    ?_assertEqual(?CMP_SMALLER_RIGHT, cmp("AB", "A "))}

    , {"CMP_WHITE_LEFT1",       ?_assertEqual(?CMP_WHITE_LEFT, cmp(" AB", "AB"))}
    , {"CMP_WHITE_LEFT2",       ?_assertEqual(?CMP_WHITE_LEFT, cmp("AB\t", "AB"))}
    , {"CMP_WHITE_LEFT3",       ?_assertEqual(?CMP_WHITE_LEFT, cmp("\rAB", "AB"))}
    , {"CMP_WHITE_LEFT4",       ?_assertEqual(?CMP_WHITE_LEFT, cmp("A \tB ", "A B"))}
    , {"CMP_WHITE_LEFT5",       ?_assertEqual(?CMP_WHITE_LEFT, cmp("A+ B ", "A+B"))}
    , {"CMP_WHITE_LEFT6",       ?_assertEqual(?CMP_WHITE_LEFT, cmp("A( B ", "A(B"))}
    , {"CMP_WHITE_LEFT7",       ?_assertEqual(?CMP_WHITE_LEFT, cmp("{ } ", "{}"))}
    , {"CMP_WHITE_LEFT8",       ?_assertEqual(?CMP_WHITE_LEFT, cmp("A / B ", "A/B"))}
    , {"CMP_WHITE_LEFT9",       ?_assertEqual(?CMP_WHITE_LEFT, cmp("[1, 2 ,3]", "[1,2,3]"))}
 
    , {"CMP_WHITE_RIGHT1",      ?_assertEqual(?CMP_WHITE_RIGHT, cmp("AB", " AB"))}
    , {"CMP_WHITE_RIGHT2",      ?_assertEqual(?CMP_WHITE_RIGHT, cmp("AB", "AB\t"))}
    , {"CMP_WHITE_RIGHT3",      ?_assertEqual(?CMP_WHITE_RIGHT, cmp("AB", "\rAB"))}
    , {"CMP_WHITE_RIGHT4",      ?_assertEqual(?CMP_WHITE_RIGHT, cmp("A B ", "A \tB "))}
    , {"CMP_WHITE_RIGHT5",      ?_assertEqual(?CMP_WHITE_RIGHT, cmp("A+B", "A+ B"))}
    , {"CMP_WHITE_RIGHT6",      ?_assertEqual(?CMP_WHITE_RIGHT, cmp("A/B", "A / B"))}
    , {"CMP_WHITE_RIGHT7",      ?_assertEqual(?CMP_WHITE_RIGHT, cmp("A(B", "A( B"))}
    , {"CMP_WHITE_RIGHT8",      ?_assertEqual(?CMP_WHITE_RIGHT, cmp("[]", "[ ] "))}
    , {"CMP_WHITE_RIGHT9",      ?_assertEqual(?CMP_WHITE_RIGHT, cmp("fun(A,B,C)", "fun(A, B, C)"))}
    , {"CMP_WHITE_RIGHT10",     ?_assertEqual(?CMP_WHITE_RIGHT, cmp("{A,B,C}", "{A, B ,C }"))}

    , {"CMP_WHITE1",            ?_assertEqual(?CMP_WHITE, cmp(" AB", "AB\t"))}
    , {"CMP_WHITE2",            ?_assertEqual(?CMP_WHITE, cmp("AB\t", " AB"))}
    , {"CMP_WHITE3",            ?_assertEqual(?CMP_WHITE, cmp("\rAB", "AB "))}
    , {"CMP_WHITE4",            ?_assertEqual(?CMP_WHITE, cmp("A \tB ", " A B"))}
    , {"CMP_WHITE5",            ?_assertEqual(?CMP_WHITE, cmp("A+ B ", "A +B"))}
    , {"CMP_WHITE6",            ?_assertEqual(?CMP_WHITE, cmp("A / B ", " A/B"))}
    , {"CMP_WHITE7",            ?_assertEqual(?CMP_WHITE, cmp("A( B ", "A (B"))}
    , {"CMP_WHITE8",            ?_assertEqual(?CMP_WHITE, cmp("( ) ", " ()"))}
    , {"CMP_WHITE9",            ?_assertEqual(?CMP_WHITE, cmp("{ } ", " {}"))}
    , {"CMP_WHITE10",           ?_assertEqual(?CMP_WHITE, cmp(" {}", "{ } "))}
    , {"CMP_WHITE11",           ?_assertEqual(?CMP_WHITE, cmp("AB\t", " AB"))}
    , {"CMP_WHITE12",           ?_assertEqual(?CMP_WHITE, cmp(" AB", "AB\t"))}
    , {"CMP_WHITE13",           ?_assertEqual(?CMP_WHITE, cmp("AB ", "\rAB"))}
    , {"CMP_WHITE14",           ?_assertEqual(?CMP_WHITE, cmp(" A B", "A \tB "))}
    , {"CMP_WHITE15",           ?_assertEqual(?CMP_WHITE, cmp("A +B", "A+ B "))}
    , {"CMP_WHITE16",           ?_assertEqual(?CMP_WHITE, cmp(" A/B", "A / B "))}
    , {"CMP_WHITE17",           ?_assertEqual(?CMP_WHITE, cmp("A (B", "A( B "))}
    , {"CMP_WHITE18",           ?_assertEqual(?CMP_WHITE, cmp(" ()", "( ) "))}

    , {"CMP_NAV1",              ?_assertEqual(?CMP_NAV, cmp(?nav, "( ) "))}
    , {"CMP_NAV2",              ?_assertEqual(?CMP_NAV, cmp("ABC", ?nav))}
    , {"CMP_NAV3",              ?_assertEqual(?CMP_NAV, cmp(?nav, ?nav, "K", "V"))}
    , {"CMP_NAV4",              ?_assertEqual(?CMP_NAV, cmp(1, 2, ?nav, ?nav))}

    , {"CMP2_SMALLER_KEY1",     ?_assertEqual(?CMP_SMALLER_KEY_LEFT, cmp(1, "123", 2, "123"))}
    , {"CMP2_SMALLER_KEY2",     ?_assertEqual(?CMP_SMALLER_KEY_LEFT, cmp(null, "123", undefined, "123"))}
    , {"CMP2_SMALLER_KEY3",     ?_assertEqual(?CMP_SMALLER_KEY_RIGHT, cmp("B", "ABC", "A123", "123"))}
    , {"CMP2_SMALLER_KEY4",     ?_assertEqual(?CMP_SMALLER_KEY_RIGHT, cmp("12", "123", 12, "123"))}
    , {"CMP2_SMALLER_KEY5",     ?_assertEqual(?CMP_SMALLER_KEY_RIGHT, cmp(?nav, "123", 12, "123"))}

    , {"CMP2_EQUAL1",            ?_assertEqual(?CMP_EQUAL_KV, cmp(1, "123", 1, "123"))}
    , {"CMP2_EQUAL2",            ?_assertEqual(?CMP_EQUAL_KV, cmp("A", "A a", <<"A">>, "A a"))}
    , {"CMP2_EQUAL3",            ?_assertEqual(?CMP_EQUAL_KV, cmp(3.123, " \t", 3.123, " \t"))}
    , {"CMP2_EQUAL4",            ?_assertEqual(?CMP_EQUAL_KV, cmp(undefined, "\r\n", undefined, "\r\n"))}
    , {"CMP2_EQUAL5",            ?_assertEqual(?CMP_EQUAL_KV, cmp("A", "", "A", ""))}
    , {"CMP2_EQUAL6",            ?_assertEqual(?CMP_EQUAL_KV, cmp("A", undefined, "A", undefined))}
    , {"CMP2_EQUAL7",            ?_assertEqual(?CMP_EQUAL_KV, cmp("A", {1,2,3}, "A", {1,2,3}))}
    , {"CMP2_EQUAL8",            ?_assertEqual(?CMP_EQUAL_KV, cmp("A", <<"A text">>, "A", "A text"))}
    , {"CMP2_EQUAL9",            ?_assertEqual(?CMP_EQUAL_KV, cmp("A", 9.1234, "A", 9.1234))}
    , {"CMP2_EQUAL10",           ?_assertEqual(?CMP_EQUAL_KV, cmp(?nav, 9.1234, ?nav, 9.1234))}
    , {"CMP2_EQUAL11",           ?_assertEqual(?CMP_EQUAL_KV, cmp("A", ?nav, "A", ?nav))}

    , {"CMP2_SMALLER_LEFT1",     ?_assertEqual(?CMP_EQUAL_KEY_SMALLER_LEFT, cmp("A", "", "A", "123"))}
    , {"CMP2_SMALLER_LEFT2",     ?_assertEqual(?CMP_EQUAL_KEY_SMALLER_LEFT, cmp("A", "ABC", "A", "ADC"))}
    , {"CMP2_SMALLER_LEFT3",     ?_assertEqual(?CMP_EQUAL_KEY_SMALLER_LEFT, cmp("A", "AB", "A", "ABC"))}
    , {"CMP2_SMALLER_LEFT4",     ?_assertEqual(?CMP_EQUAL_KEY_SMALLER_LEFT, cmp("A", 12, "A", 13))}
    , {"CMP2_SMALLER_LEFT5",     ?_assertEqual(?CMP_EQUAL_KEY_SMALLER_LEFT, cmp("A", {1,2}, "A", {0,1,2}))}
    , {"CMP2_SMALLER_LEFT6",     ?_assertEqual(?CMP_EQUAL_KEY_SMALLER_LEFT, cmp(123, {1,2}, 123, {1,3}))}
    , {"CMP2_SMALLER_LEFT7",     ?_assertEqual(?CMP_EQUAL_KEY_SMALLER_LEFT, cmp(0, <<"A Text">>, 0, <<"A t">>))}
    , {"CMP2_SMALLER_LEFT8",     ?_assertEqual(?CMP_EQUAL_KEY_SMALLER_LEFT, cmp(null, 12, null, <<"13">>))}
    , {"CMP2_SMALLER_LEFT9",     ?_assertEqual(?CMP_EQUAL_KEY_SMALLER_LEFT, cmp(?nav, 12, ?nav, <<"13">>))}
    , {"CMP2_SMALLER_LEFT10",    ?_assertEqual(?CMP_EQUAL_KEY_SMALLER_LEFT, cmp(null, ?nav, null, <<"13">>))}

    , {"CMP2_SMALLER_RIGHT1",    ?_assertEqual(?CMP_EQUAL_KEY_SMALLER_RIGHT, cmp({1}, "AB", {1}, "A B"))}
    , {"CMP2_SMALLER_RIGHT2",    ?_assertEqual(?CMP_EQUAL_KEY_SMALLER_RIGHT, cmp(null, "AB", null, "A "))}
    , {"CMP2_SMALLER_RIGHT3",    ?_assertEqual(?CMP_EQUAL_KEY_SMALLER_RIGHT, cmp(<<"A">>, "AB", <<"A">>, "A "))}

    , {"CMP2_WHITE_LEFT1",       ?_assertEqual(?CMP_EQUAL_KEY_WHITE_LEFT, cmp([], " AB", [], "AB"))}
    , {"CMP2_WHITE_LEFT2",       ?_assertEqual(?CMP_EQUAL_KEY_WHITE_LEFT, cmp([1], "AB\t", [1], "AB"))}
    , {"CMP2_WHITE_LEFT3",       ?_assertEqual(?CMP_EQUAL_KEY_WHITE_LEFT, cmp([1,null], "\rAB", [1,null], "AB"))}
    , {"CMP2_WHITE_LEFT4",       ?_assertEqual(?CMP_EQUAL_KEY_WHITE_LEFT, cmp({}, "A \tB ", {}, "A B"))}
    , {"CMP2_WHITE_LEFT5",       ?_assertEqual(?CMP_EQUAL_KEY_WHITE_LEFT, cmp({1}, "A+ B ", {1}, "A+B"))}
    , {"CMP2_WHITE_LEFT6",       ?_assertEqual(?CMP_EQUAL_KEY_WHITE_LEFT, cmp({1,2}, "A / B ", {1,2}, "A/B"))}
    , {"CMP2_WHITE_LEFT7",       ?_assertEqual(?CMP_EQUAL_KEY_WHITE_LEFT, cmp({1,2,null}, "A( B ", {1,2,null}, "A(B"))}
    , {"CMP2_WHITE_LEFT8",       ?_assertEqual(?CMP_EQUAL_KEY_WHITE_LEFT, cmp({1,2,3}, "{ } ", {1,2,3}, "{}"))}

    , {"CMP2_WHITE_RIGHT1",      ?_assertEqual(?CMP_EQUAL_KEY_WHITE_RIGHT, cmp("", "AB", "", " AB"))}
    , {"CMP2_WHITE_RIGHT2",      ?_assertEqual(?CMP_EQUAL_KEY_WHITE_RIGHT, cmp("", "AB", "", "AB\t"))}
    , {"CMP2_WHITE_RIGHT3",      ?_assertEqual(?CMP_EQUAL_KEY_WHITE_RIGHT, cmp("", "AB", "", "\rAB"))}
    , {"CMP2_WHITE_RIGHT4",      ?_assertEqual(?CMP_EQUAL_KEY_WHITE_RIGHT, cmp("", "A B ", "", "A \tB "))}
    , {"CMP2_WHITE_RIGHT5",      ?_assertEqual(?CMP_EQUAL_KEY_WHITE_RIGHT, cmp("", "A+B", "", "A+ B"))}
    , {"CMP2_WHITE_RIGHT6",      ?_assertEqual(?CMP_EQUAL_KEY_WHITE_RIGHT, cmp("", "A/B", "", "A / B"))}
    , {"CMP2_WHITE_RIGHT7",      ?_assertEqual(?CMP_EQUAL_KEY_WHITE_RIGHT, cmp("", "A(B", "", "A( B"))}
    , {"CMP2_WHITE_RIGHT8",      ?_assertEqual(?CMP_EQUAL_KEY_WHITE_RIGHT, cmp("", "[]", "", "[ ] "))}

    , {"CMP2_WHITE1",            ?_assertEqual(?CMP_EQUAL_KEY_WHITE, cmp(null, " AB", null, "AB\t"))}
    , {"CMP2_WHITE2",            ?_assertEqual(?CMP_EQUAL_KEY_WHITE, cmp(null, "AB\t", null, " AB"))}
    , {"CMP2_WHITE3",            ?_assertEqual(?CMP_EQUAL_KEY_WHITE, cmp(null, "\rAB", null, "AB "))}
    , {"CMP2_WHITE4",            ?_assertEqual(?CMP_EQUAL_KEY_WHITE, cmp(null, "A \tB ", null, " A B"))}
    , {"CMP2_WHITE5",            ?_assertEqual(?CMP_EQUAL_KEY_WHITE, cmp(null, "A+ B ", null, "A +B"))}
    , {"CMP2_WHITE6",            ?_assertEqual(?CMP_EQUAL_KEY_WHITE, cmp(null, "A / B ", null, " A/B"))}
    , {"CMP2_WHITE7",            ?_assertEqual(?CMP_EQUAL_KEY_WHITE, cmp(null, "A( B ", null, "A (B"))}
    , {"CMP2_WHITE8",            ?_assertEqual(?CMP_EQUAL_KEY_WHITE, cmp(null, "( ) ", null, " ()"))}

    ].

norm_white_space_test_() ->
    [ {"NWS_EQUAL1",            ?_assertEqual("123", norm_white_space("123"))}
    , {"NWS_EQUAL2",            ?_assertEqual("12 3", norm_white_space("12 3"))}
    , {"NWS_EQUAL3",            ?_assertEqual("AB C", norm_white_space("AB C "))}
    , {"NWS_EQUAL4",            ?_assertEqual("AB C", norm_white_space(" AB C"))}
    , {"NWS_EQUAL5",            ?_assertEqual("12. 5", norm_white_space("12. 5"))}
    , {"NWS_EQUAL6",            ?_assertEqual("12.5", norm_white_space("12.5\t"))}
    , {"NWS_EQUAL7",            ?_assertEqual("A/B", norm_white_space("A/B "))}
    , {"NWS_EQUAL8",            ?_assertEqual("A/B", norm_white_space("A / B"))}
    ].

-endif.
