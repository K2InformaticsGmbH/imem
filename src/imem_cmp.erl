%% -*- coding: utf-8 -*-
-module(imem_cmp).                  %% module for data comparisons

-include("imem.hrl").
-include("imem_meta.hrl").

-define(CMP_EQUAL,<<"=">>).         %% for cmp/2 and cmp/3 (Token Comparison)
-define(CMP_EQUAL_STR,$=).          %% for cmp/2 and cmp/3 (Token Comparison)
-define(CMP_NAV,<<>>).              %% for cmp/2 and cmp/3 (one or both sides ?nav, not a value)
-define(CMP_SMALLER_LEFT,<<"<">>).
-define(CMP_SMALLER_RIGHT,<<">">>).
-define(CMP_WHITE_LEFT,<<"w=">>).   %% added whitespace on the left
-define(CMP_WHITE_RIGHT,<<"=w">>).  %% added whitespace on the right
-define(CMP_WHITE,<<"w=w">>).       %% added whitespace on both sides
-define(CMP_CASE,<<"c">>).          %% different casing on both sides (frames earlier result)
-define(CMP_DQUOTE,<<"""">>).       %% dropped double quotes on both sides (frames earlier result)



-define(CMP_SPACE,32).              %% Stereotype for white space
-define(CMP_CHAR,$@).               %% Stereotype for non-white space
-define(CMP_OPS,"()[]{}+-*/<>=|,").
-define(CMP_NO_SPLIT,["<<", ">>", "<>", "->", "=>", "<=","==","<=",">=","=<","!=","++","--","||", ":=", "=:"]).
-define(CMP_WHITESPACE," \t\r\n").

-export([ cmp/2                     %% compare two terms (LeftKey,RightKey)
        , cmp/3                     %% compare two terms (LeftKey,RightKey, Opts)
        , cmp_trim/2
        , cmp_whitespace/1         %% is the argument a string or a binary containing only white space?
        , norm_whitespace/1        %% reduce whitespace to the mminimum without corrupting (Erlang/SQL) code
        , norm_casing/1            %% converts upper case to lower case in binary or string
        , norm_dquotes/1           %% removes double quotes
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
cmp(L, L, _) -> ?CMP_EQUAL; 
cmp(L, R, Opts) when is_binary(L) ->
    cmp(binary_to_list(L), R, Opts);
cmp(L, R, Opts) when is_binary(R) ->
    cmp(L, binary_to_list(R), Opts);
cmp(L, R, _Opts) when is_list(L), is_list(R) ->
    case {norm_dquotes(L),norm_dquotes(R),norm_casing(L),norm_casing(R),norm_casing(norm_dquotes(L)),norm_casing(norm_dquotes(R))} of
        {L1,R1,L2,R2,L3,R3} -> 
            Res0 = cmp_white(L, R, []),
            Res1 = cmp_white(L1, R1, []),
            Res2 = cmp_white(L2, R2, []),
            Res3 = cmp_white(L3, R3, []),
            IsEq0 = lists:member(?CMP_EQUAL_STR,binary_to_list(Res0)),
            IsEq1 = lists:member(?CMP_EQUAL_STR,binary_to_list(Res1)),
            IsEq2 = lists:member(?CMP_EQUAL_STR,binary_to_list(Res2)),
            IsEq3 = lists:member(?CMP_EQUAL_STR,binary_to_list(Res3)),
            case (L3=="a text") of
                true ->
                    io:format(user,"RES0  ~p ~p ~p ~p ~n",[L,R,Res0,IsEq0]),
                    io:format(user,"RES1  ~p ~p ~p ~p ~n",[L1,R1,Res1,IsEq1]),
                    io:format(user,"RES2  ~p ~p ~p ~p ~n",[L2,R2,Res2,IsEq2]),
                    io:format(user,"RES3  ~p ~p ~p ~p ~n",[L3,R3,Res3,IsEq3]);
                false ->
                    ok
            end,
            case {IsEq0,IsEq1,IsEq2,IsEq3} of
                {true,_,_,_} ->             Res0;
                {false,true,_,_} ->         <<?CMP_DQUOTE/binary,Res1/binary,?CMP_DQUOTE/binary>>;
                {false,_,true,_} ->         <<?CMP_CASE/binary,Res2/binary,?CMP_CASE/binary>>;
                {false,_,_,true} ->         <<?CMP_DQUOTE/binary,?CMP_CASE/binary,Res3/binary,?CMP_CASE/binary,?CMP_DQUOTE/binary>>;
                {false,_,_,_} ->            Res0
            end
    end;
cmp(L, R, _Opts) when L < R -> ?CMP_SMALLER_LEFT;
cmp(_, _, _Opts) -> ?CMP_SMALLER_RIGHT.

cmp_white(L, L, _Opts) -> ?CMP_EQUAL;
cmp_white(L, R, Opts) when is_list(L), is_list(R) ->
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
    end.

cmp_trim(Diff, Dir) -> cmp_trim(?CMP_SPACE, Diff, Dir, []).

cmp_trim(_Last, [], _Dir, Acc) -> lists:reverse(Acc);
cmp_trim(Last, [{Dir,Str}], Dir, Acc) -> 
    case cmp_whitespace(Str) of
        true ->     cmp_trim(Last, [], Dir, Acc);
        false ->    cmp_trim(Last, [], Dir, [{Dir,Str}|Acc])
    end;
cmp_trim(Last, [{Dir,Str},{eq,[Next|Chars]}|Diff], Dir, Acc) ->
    %% see if added white space insert/delete is allowed
    case {cmp_whitespace(Str),cmp_may_split([Last,Next])} of
        {true, true} -> cmp_trim(lists:last([Next|Chars]), Diff, Dir, [{eq,[Next|Chars]}|Acc]);
        {_,_} ->        cmp_trim(lists:last([Next|Chars]), Diff, Dir, [{eq,[Next|Chars]},{Dir,Str}|Acc])
    end;   
cmp_trim(_Last, [{Chg,Str}|Diff], Dir, Acc) ->
    %% keep equal or opposite piece and move forward
    cmp_trim(lists:last(Str), Diff, Dir, [{Chg,Str}|Acc]).

cmp_whitespace(B) when is_binary(B) -> cmp_whitespace(binary_to_list(B));
cmp_whitespace([]) -> true;
cmp_whitespace([Ch|Chrs]) ->
    case lists:member(Ch,?CMP_WHITESPACE) of
        false ->    false;
        true ->     cmp_whitespace(Chrs)
    end;
cmp_whitespace(_NotaList) -> false.

norm_whitespace(B) when is_binary(B) -> list_to_binary(norm_whitespace(binary_to_list(B)));
norm_whitespace(L) when is_list(L) -> norm_ws(L);
norm_whitespace(I) -> I.

norm_casing(S) when is_binary(S) -> unicode:characters_to_binary(string:lowercase(unicode:characters_to_list(S, utf8)), unicode, utf8);
norm_casing(S) when is_list(S) -> string:lowercase(S);
norm_casing(S) -> S.

norm_dquotes(S) when is_binary(S) -> string:replace(S,<<$">>,<<>>);
norm_dquotes(S) when is_list(S) -> string:replace(S,[$"],[]);
norm_dquotes(S) -> S.

norm_ws(L) -> norm_ws(?CMP_SPACE, L, []).

norm_ws(_, [], Acc) -> lists:reverse(Acc);
norm_ws(_Last, [Ch|List], Acc) when Ch>?CMP_SPACE ->
    norm_ws(Ch, List, [Ch|Acc]);
norm_ws(Last, [Ch,Next|List], Acc) ->
    IsWhite = lists:member(Ch,?CMP_WHITESPACE),
    NoSplit = lists:member([Last,Next],?CMP_NO_SPLIT),
    case {IsWhite,NoSplit} of
        {false,_} ->    norm_ws(Ch, [Next|List], [Ch|Acc]);
        {_,true}  ->    norm_ws(?CMP_SPACE, [Next|List], [?CMP_SPACE|Acc]);
        {true,false} -> 
            LeftWhite = lists:member(Last,?CMP_WHITESPACE),
            RightWhite =  lists:member(Next,?CMP_WHITESPACE),
            LeftOp = lists:member(Last,?CMP_OPS),
            RightOp =  lists:member(Next,?CMP_OPS),
            case lists:usort([LeftWhite,RightWhite,LeftOp,RightOp]) of
                [false] ->      norm_ws(?CMP_SPACE, [Next|List], [?CMP_SPACE|Acc]);
                [false,true] -> norm_ws(Last, [Next|List], Acc)
            end
    end;
norm_ws(_Last, [Ch], Acc) ->
    case lists:member(Ch,?CMP_WHITESPACE) of
        false ->    norm_ws(Ch, [], [Ch|Acc]);
        true  ->    norm_ws(?CMP_SPACE, [], Acc)
    end.

cmp_may_split([Last,Next]) ->
    NoSplit = lists:member([Last,Next],?CMP_NO_SPLIT),
    LeftWhite = lists:member(Last,?CMP_WHITESPACE),
    RightWhite =  lists:member(Next,?CMP_WHITESPACE),
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
    , {"CMP_EQUAL9",            ?_assertEqual(?CMP_EQUAL, cmp({1,2,3}, {1,2,3}))}

    , {"CMP_SMALLER_LEFT1",     ?_assertEqual(?CMP_SMALLER_LEFT, cmp("", "123"))}
    , {"CMP_SMALLER_LEFT2",     ?_assertEqual(?CMP_SMALLER_LEFT, cmp("ABC", "ADC"))}
    , {"CMP_SMALLER_LEFT3",     ?_assertEqual(?CMP_SMALLER_LEFT, cmp("AB", "ABC"))}
    , {"CMP_SMALLER_LEFT4",     ?_assertEqual(?CMP_SMALLER_LEFT, cmp(12, 13))}
    , {"CMP_SMALLER_LEFT5",     ?_assertEqual(?CMP_SMALLER_LEFT, cmp({1,2}, {0,1,2}))}
    , {"CMP_SMALLER_LEFT6",     ?_assertEqual(?CMP_SMALLER_LEFT, cmp(<<"A Text">>, <<"A t">>))}
    , {"CMP_SMALLER_LEFT7",     ?_assertEqual(?CMP_SMALLER_LEFT, cmp(12, <<"13">>))}

    , {"CMP_SMALLER_LEFT8",     ?_assertEqual(?CMP_SMALLER_LEFT, cmp({1,2}, {1,3}))}

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

    , {"CMP_WHITE_CASE1",       ?_assertEqual(<<"c=c">>, cmp(<<"A Text">>, <<"A text">>))}
    , {"CMP_WHITE_CASE2",       ?_assertEqual(<<"c=wc">>, cmp(<<"a Text">>, <<"A text ">>))}
    , {"CMP_WHITE_CASE3",       ?_assertEqual(<<"cw=wc">>, cmp("a +B", "A+ B "))}
    , {"CMP_WHITE_CASE4",       ?_assertEqual(<<"=">>, cmp_white("a text", "a text", []))}

    , {"CMP_WHITE_QUOTE1",      ?_assertEqual(<<"""=""">>, cmp(<<"A Text">>, <<"A ""Text""">>))}
    , {"CMP_WHITE_QUOTE2",      ?_assertEqual(<<"""c=c""">>, cmp("""a"" +B", "A +B"))}
    , {"CMP_WHITE_QUOTE3",      ?_assertEqual(<<"""cw=wc""">>, cmp("""a"" +B", "A+ B "))}
    , {"CMP_WHITE_QUOTE4",      ?_assertEqual(<<"""cw=c""">>, cmp("""a"" +B", "A+B"))}
    , {"CMP_WHITE_QUOTE5",      ?_assertEqual(<<"""c=wc""">>, cmp("""a""+B", "A+ B"))}
    ].

norm_whitespace_test_() ->
    [ {"NWS_EQUAL1",            ?_assertEqual("123", norm_whitespace("123"))}
    , {"NWS_EQUAL2",            ?_assertEqual("12 3", norm_whitespace("12 3"))}
    , {"NWS_EQUAL3",            ?_assertEqual("AB C", norm_whitespace("AB C "))}
    , {"NWS_EQUAL4",            ?_assertEqual("AB C", norm_whitespace(" AB C"))}
    , {"NWS_EQUAL5",            ?_assertEqual("12. 5", norm_whitespace("12. 5"))}
    , {"NWS_EQUAL6",            ?_assertEqual("12.5", norm_whitespace("12.5\t"))}
    , {"NWS_EQUAL7",            ?_assertEqual("A/B", norm_whitespace("A/B "))}
    , {"NWS_EQUAL8",            ?_assertEqual("A/B", norm_whitespace("A / B"))}
    ].

-endif.
