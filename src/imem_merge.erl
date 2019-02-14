%% -*- coding: utf-8 -*-
-module(imem_merge).

-include("imem.hrl").
-include("imem_meta.hrl").

-define(CMP_EQUAL,<<"=">>).         %% for cmp/2 and cmp/3 (Token Comparison)
-define(CMP_NAV,<<>>).              %% for cmp/2 and cmp/3 (one or both sides ?nav, not a value)
-define(CMP_SMALLER_LEFT,<<"<">>).
-define(CMP_SMALLER_RIGHT,<<">">>).
-define(CMP_WHITE_LEFT,<<"w=">>).   %% added whitespece on the left
-define(CMP_WHITE_RIGHT,<<"=w">>).  %% added whitespace on the right
-define(CMP_WHITE,<<"w=w">>).       %% added whitespace on both sides


-define(CMP_EQUAL_KV,<<"==">>).     %% for cmp/2 and cmp/3 (KV-Comparison)
-define(CMP_EQUAL_KEY_SMALLER_LEFT,<<"=<">>).
-define(CMP_EQUAL_KEY_SMALLER_RIGHT,<<"=>">>).
-define(CMP_SMALLER_KEY_LEFT,<<"<*">>).
-define(CMP_SMALLER_KEY_RIGHT,<<">*">>).
-define(CMP_EQUAL_KEY_WHITE_LEFT,<<"=w=">>).   %% added whitespece on the left value
-define(CMP_EQUAL_KEY_WHITE_RIGHT,<<"==w">>).  %% added whitespace on the right value
-define(CMP_EQUAL_KEY_WHITE,<<"=w=w">>).       %% added whitespace on both sides value

-define(CMP_SPACE,32).              %% Stereotype for white space
-define(CMP_CHAR,$@).               %% Stereotype for non-white space
-define(CMP_OPS,"()[]{}+-*/<>=|,").
-define(CMP_NO_SPLIT,["<<", ">>", "<>", "->", "=>", "<=","==","<=",">=","=<","!=","++","--","||", ":=", "=:"]).
-define(CMP_WHITE_SPACE," \t\r\n").

-export([ merge_diff/3              %% merge two data tables into a bigger one, presenting the differences side by side
        , merge_diff/4              %% merge two data tables into a bigger one, presenting the differences side by side
        ]).

-export([ cmp/2                     %% compare two terms (LeftKey,RightKey)
        , cmp/3                     %% compare two terms (LeftKey,RightKey, Opts)
        , cmp/4                     %% compare two terms with payloads (LeftKey,LeftVal,RightKey,RightVal)
        , cmp/5                     %% compare two terms with payloads (LeftKey,LeftVal,RightKey,RightVal,Opts)
        , diff/2                    %% wrapper adding whitespace and type awareness to tdiff:diff
        , diff/3                    %% wrapper adding whitespace and type awareness to tdiff:diff
        , diff_only/2               %% wrapper adding whitespace and type awareness to tdiff:diff
        , diff_only/3               %% wrapper adding whitespace and type awareness to tdiff:diff
        ]).

-safe([merge_diff]).

%% @doc Generate a diff term by calling tdiff:diff and try to normalize whitespace differences
-spec diff(any(), any()) -> list().
diff(L, R) -> diff(L, R, []).

%% @doc Generate a diff term by calling tdiff:diff with options and try to normalize whitespace differences
-spec diff(any(), any(), list()) -> list().
diff(L, R, Opts) when is_binary(L) -> 
    diff(binary_to_list(L), R, Opts);
diff(L, R, Opts) when is_binary(R) -> 
    diff(L, binary_to_list(R), Opts);
diff(L, R, Opts) when is_list(L), is_list(R), is_list(Opts) -> 
    cmp_norm_whitespace(tdiff:diff(L, R, Opts));
diff(L, R, Opts) -> ?ClientErrorNoLogging({"diff only compares list or binary values",{L,R,Opts}}).

%% @doc Generate a diff term by calling tdiff:diff and try to normalize whitespace differences
%% Then suppress all {eq,_} terms in order to indicate only differences
-spec diff_only(any(), any()) -> list().
diff_only(L, R) -> diff_only(L, R, []).

%% @doc Generate a diff term by calling tdiff:diff with options and try to normalize whitespace differences
%% Then suppress all {eq,_} terms in order to indicate only differences
-spec diff_only(any(), any(), list()) -> list().
diff_only(L, R, Opts) -> 
    cmp_rem_eq(cmp_norm_whitespace(diff(L, R, Opts))).


%% @doc Compare two erlang terms or strings or lists of tokens 
-spec cmp(any(),any()) -> binary().
cmp(L, R) -> cmp(L, R, []). 

%% @doc Compare two erlang terms or strings or lists of tokens with options (not used so far) 
%% Binary parameters are converted to lists and then compared (as lists of characters/bytes).
%% Tuple parameters are split into a key and a value part (last element).
%% For lists of integers (characters/bytes), white space differences are detected as such
%% by considering whitespace relevance for popular programming languages.
%% Result indicates equalness or whitespace equalness or erlang term sort order for difference.
-spec cmp(any(),any(),list()) -> binary().
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
    Diff = cmp_norm_whitespace(tdiff:diff(L, R, Opts)),
    DiffLeft = cmp_trim(Diff, del),
    DiffRight = cmp_trim(Diff, ins),
    DiffBoth = cmp_trim(DiffRight,del),
    case {cmp_eq(DiffLeft), cmp_eq(DiffRight), cmp_eq(DiffBoth)} of
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

cmp_white_space([]) -> true;
cmp_white_space([Ch|Chrs]) ->
    case lists:member(Ch,?CMP_WHITE_SPACE) of
        false ->    false;
        true ->     cmp_white_space(Chrs)
    end;
cmp_white_space(_NotaList) -> false.

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

cmp_norm_whitespace(Diff) -> cmp_norm_whitespace(Diff,[]).

cmp_norm_whitespace([], Acc) -> lists:reverse(Acc);
cmp_norm_whitespace([{del,Str},{eq,WS},{ins,Str}|Diff], Acc) ->
    %% see if we can favor white space changes 
    %% [{del,Str},{eq,WS},{ins,Str}|Diff] -> [{ins,WS},{eq,Str},{del,WS}|Diff]
    case cmp_white_space(WS) of
        true ->     cmp_norm_whitespace([{eq,Str},{del,WS}|Diff],[{ins,WS}|Acc]);
        false ->    cmp_norm_whitespace([{eq,WS},{ins,Str}|Diff], [{del,Str}|Acc])
    end;
cmp_norm_whitespace([{Dir,Str}|Diff], Acc) ->
    %% nothing to do, just forward the term to output
    cmp_norm_whitespace(Diff,[{Dir,Str}|Acc]).

cmp_eq(Diff) -> (cmp_rem_eq(Diff) == []). %% Diff contains only {eq,_} terms

cmp_rem_eq(Diff) -> cmp_rem_eq(Diff,[]). %% Diff wirth {eq,_} terms removed

cmp_rem_eq([], Acc) -> lists:reverse(Acc);
cmp_rem_eq([{ins,Str},{del,Str}|Diff], Acc) -> cmp_rem_eq(Diff, Acc);
cmp_rem_eq([{del,Str},{ins,Str}|Diff], Acc) -> cmp_rem_eq(Diff, Acc);
cmp_rem_eq([{eq,_Str}|Diff], Acc) -> cmp_rem_eq(Diff, Acc);
cmp_rem_eq([D|Diff], Acc) -> cmp_rem_eq(Diff, [D|Acc]). 

-spec cmp(any(), any(), any(), any()) -> binary().
cmp(L, R, LVal, RVal) -> cmp(L, R, LVal, RVal, []).

%% @doc Compare two erlang KV pairs of strings or lists of tokens with options (not used so far) 
%% Binary parameters are converted to lists and then compared (as lists of characters/bytes).
%% For the value part, white space differences are detected as such
%% by considering whitespace relevance for popular programming languages.
%% Result indicates equalness or whitespace equalness or erlang term sort order for difference.
-spec cmp(any(), any(), any(), any(), list()) -> binary().
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
    Diff = cmp_norm_whitespace(tdiff:diff(LVal, RVal, Opts)),
    DiffLeft = cmp_trim(Diff, del),
    DiffRight = cmp_trim(Diff, ins),
    DiffBoth = cmp_trim(DiffRight,del),
    case {cmp_eq(DiffLeft), cmp_eq(DiffRight), cmp_eq(DiffBoth)} of
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


-spec merge_diff(ddTable(), ddTable(), ddTable()) -> ok.
merge_diff(Left, Right, Merged) -> merge_diff(Left, Right, Merged, []).

-spec merge_diff(ddTable(), ddTable(), ddTable(), list()) -> ok.
merge_diff(Left, Right, Merged, Opts) -> merge_diff(Left, Right, Merged, Opts, imem_meta:meta_field_value(user)).

-spec merge_diff(ddTable(), ddTable(), ddTable(), list(), ddEntityId()) -> ok.
merge_diff(Left, Right, Merged, Opts, User) ->
    imem_meta:log_to_db(debug, ?MODULE, merge_diff, [{left, Left}, {right, Right}, {merged, Merged}], "merge_diff table"),
    MySchema = imem_meta:schema(),
    case {imem_meta:qualified_table_name(Left), imem_meta:qualified_table_name(Right), imem_meta:qualified_table_name(Merged)} of
        {{MySchema,L},{MySchema,R},{MySchema,_W}} ->
            case {merge_scan(Left), merge_scan(Right)} of
                {{0,_},_} -> ?ClientError({"Empty table", Left});   % row_count=0
                {_,{0,_}} -> ?ClientError({"Empty table", Right});  % row_count=0
                {{_,Cols},{_,Cols}} -> 
                    imem_if_mnesia:truncate_table(Merged),
                    merge_diff_fill(Cols, Left, Right, Merged, Opts, User);
                {{_,L},{_,R}} -> ?ClientError({"Cannot merge_diff unequal column counts", {L,R}})
            end;
        _ -> ?UnimplementedException({"merge_diff in foreign schema", {Left, Right, Merged}})
    end.

merge_scan(Table) -> merge_scan(Table, imem_meta:dirty_first(Table), {0,0}).

merge_scan(_Table, '$end_of_table', {Rows,Cols}) -> {Rows,Cols};
merge_scan(Table, I, {Rows,3}) ->
    merge_scan(Table, imem_meta:dirty_next(Table,I), {Rows+1,3});
merge_scan(Table, I, {Rows,2}) ->
    [Row] = imem_meta:dirty_read(Table, I),
    case element(5,Row) of
        E3 when E3==?nav; E3==<<>>; E3==[] -> 
            merge_scan(Table, imem_meta:dirty_next(Table,I), {Rows+1,2});
        _ -> 
            merge_scan(Table, imem_meta:dirty_next(Table,I), {Rows+1,3})
    end;
merge_scan(Table, I, {Rows,1}) ->
    [Row] = imem_meta:dirty_read(Table, I),
    case element(5,Row) of
        E3 when E3==?nav; E3==<<>>; E3==[] ->
            case element(4,Row) of
                E2 when E2==?nav; E2==<<>>; E2==[] ->
                    merge_scan(Table, imem_meta:dirty_next(Table,I), {Rows+1,1});
                _ ->
                    merge_scan(Table, imem_meta:dirty_next(Table,I), {Rows+1,2})
            end;
        _ -> 
            merge_scan(Table, imem_meta:dirty_next(Table,I), {Rows+1,3})
    end;
merge_scan(Table, I, {Rows,0}) ->
    [Row] = imem_meta:dirty_read(Table, I),
    case element(5,Row) of
        E3 when E3==?nav; E3==<<>>; E3==[] ->
            case element(4,Row) of
                E2 when E2==?nav; E2==<<>>; E2==[] ->
                    merge_scan(Table, imem_meta:dirty_next(Table,I), {Rows+1,1});
                _ ->  
                    case element(3,Row) of
                        E1 when E1==?nav; E1==<<>>; E1==[] ->
                            merge_scan(Table, imem_meta:dirty_next(Table,I), {Rows+1,0});
                        _ ->        
                            merge_scan(Table, imem_meta:dirty_next(Table,I), {Rows+1,1})
                    end
            end;
        _ -> 
            merge_scan(Table, imem_meta:dirty_next(Table,I), {Rows+1,3})
    end.

merge_diff_fill(Cols, Left, Right, Merged, Opts, User) ->
    LI = imem_meta:dirty_first(Left),
    RI = imem_meta:dirty_first(Right),
    merge_diff_scan(Cols, Left, Right, Merged, Opts, User, LI, RI, [], []).

merge_diff_scan(Cols, _Left, _Right, Merged, Opts, User, '$end_of_table', '$end_of_table', LAcc, RAcc) ->
    merge_write_diff(Cols, Merged, Opts, User, lists:reverse(LAcc), lists:reverse(RAcc));
merge_diff_scan(Cols, Left, Right, Merged, Opts, User, LI, '$end_of_table', [], []) ->
    {K,V} = merge_read(Cols, Left, LI),
    merge_diff_scan(Cols, Left, Right, Merged, Opts, User, imem_meta:dirty_next(Left,LI), '$end_of_table', [{K,V}], []);
merge_diff_scan(Cols, Left, Right, Merged, Opts, User, '$end_of_table', RI, [], []) ->
    {K,V} = merge_read(Cols, Right, RI),
    merge_diff_scan(Cols, Left, Right, Merged, Opts, User, '$end_of_table', imem_meta:dirty_next(Right,RI), [], [{K,V}]);
merge_diff_scan(Cols, Left, Right, Merged, Opts, User, LI, '$end_of_table', [{Key,LV}|Rest], RAcc) ->
    case merge_read(Cols, Left, LI) of
        {Key,V} ->
            merge_diff_scan(Cols, Left, Right, Merged, Opts, User, imem_meta:dirty_next(Left,LI), '$end_of_table', [{Key,V},{Key,LV}|Rest], RAcc);
        {NK,NV} ->
            merge_write_diff(Cols, Merged, Opts, User, lists:reverse([{Key,LV}|Rest]), lists:reverse(RAcc)),
            merge_diff_scan(Cols, Left, Right, Merged, Opts, User, imem_meta:dirty_next(Left,LI), '$end_of_table', [{NK,NV}], [])
    end;
merge_diff_scan(Cols, Left, Right, Merged, Opts, User, '$end_of_table', RI, LAcc, [{Key,RV}|Rest]) ->
    case merge_read(Cols, Right, RI) of
        {Key,V} ->
            merge_diff_scan(Cols, Left, Right, Merged, Opts, User, '$end_of_table', imem_meta:dirty_next(Right,RI), LAcc, [{Key,V},{Key,RV}|Rest]);
        {NK,NV} ->
            merge_write_diff(Cols, Merged, Opts, User, lists:reverse(LAcc), lists:reverse([{Key,RV}|Rest])),
            merge_diff_scan(Cols, Left, Right, Merged, Opts, User, '$end_of_table', imem_meta:dirty_next(Right,RI), [{NK,NV}], [])
    end;
merge_diff_scan(Cols, Left, Right, Merged, Opts, User, LI, RI, [], []) ->
    {LK,LV} = merge_read(Cols, Left, LI),
    {RK,RV} = merge_read(Cols, Right, RI),
    if 
        LK == RK ->
            merge_diff_scan(Cols, Left, Right, Merged, Opts, User, imem_meta:dirty_next(Left,LI), imem_meta:dirty_next(Right,RI), [{LK,LV}], [{RK,RV}]);
        LK < RK ->
            merge_diff_scan(Cols, Left, Right, Merged, Opts, User, imem_meta:dirty_next(Left,LI), RI, [{LK,LV}], []);
        LK > RK ->
            merge_diff_scan(Cols, Left, Right, Merged, Opts, User, LI, imem_meta:dirty_next(right,RI), [], [{RK,RV}])
    end;
merge_diff_scan(Cols, Left, Right, Merged, Opts, User, LI, RI, LAcc, RAcc) ->
    K = merge_key(LAcc, RAcc),
    {LK,LV} = merge_read(Cols, Left, LI),
    {RK,RV} = merge_read(Cols, Right, RI),
    case {(LK==K),(RK==K)} of
        {true,true} ->
            merge_diff_scan(Cols, Left, Right, Merged, Opts, User, imem_meta:dirty_next(Left,LI), imem_meta:dirty_next(Right,RI), [{K,LV}|LAcc], [{K,RV}|RAcc]);
        {true,false} ->
            merge_diff_scan(Cols, Left, Right, Merged, Opts, User, imem_meta:dirty_next(Left,LI), RI, [{K,LV}|LAcc], RAcc);
        {false,true} ->
            merge_diff_scan(Cols, Left, Right, Merged, Opts, User, LI, imem_meta:dirty_next(Right,RI), LAcc, [{K,RV}|RAcc]);
        {false,false} ->
            merge_write_diff(Cols, Merged, Opts, User, lists:reverse(LAcc), lists:reverse(RAcc)),
            if 
                LK == RK ->
                    merge_diff_scan(Cols, Left, Right, Merged, Opts, User, imem_meta:dirty_next(Left,LI), imem_meta:dirty_next(Right,RI), [{LK,LV}], [{RK,RV}]);
                LK < RK ->
                    merge_diff_scan(Cols, Left, Right, Merged, Opts, User, imem_meta:dirty_next(Left,LI), RI, [{LK,LV}], []);
                LK > RK ->
                    merge_diff_scan(Cols, Left, Right, Merged, Opts, User, LI, imem_meta:dirty_next(Right,RI), [], [{RK,RV}])
            end
    end.

merge_key(Acc, []) -> element(1,hd(Acc));
merge_key([], Acc) -> element(1,hd(Acc));
merge_key(Acc, _) -> element(1,hd(Acc)).

merge_read(1, Table, I) ->
    [Row] = imem_meta:dirty_read(Table, I),
    {undefined,element(3,Row)};
merge_read(2, Table, I) ->
    [Row] = imem_meta:dirty_read(Table, I),
    {element(3,Row),element(4,Row)};
merge_read(3, Table, I) ->
    [Row] = imem_meta:dirty_read(Table, I),
    {{element(3,Row),element(4,Row)},element(5,Row)}.

merge_write_diff(_Cols, _Merged, _Opts, _User, [], []) -> ok;
merge_write_diff(Cols, Merged, Opts, User, Acc, []) ->
    K = element(1,hd(Acc)),
    Vals = [V || {_,V} <- Acc],
    Diff = [{del, Vals}],
    merge_write_out(Cols, Merged, Opts, User, K, Diff);
merge_write_diff(Cols, Merged, Opts, User, [], Acc) ->
    K = element(1,hd(Acc)),
    Vals = [V || {_,V} <- Acc],
    Diff = [{ins, Vals}],
    merge_write_out(Cols, Merged, Opts, User, K, Diff);
merge_write_diff(Cols, Merged, Opts, User, [{K,LV}], [{K,RV}]) ->
    merge_write(Cols, Merged, Opts, User, K, [LV], [RV]);
merge_write_diff(Cols, Merged, Opts, User, LAcc, RAcc) ->
    K = element(1,hd(LAcc)),
    LVals = [V || {_,V} <- LAcc],
    RVals = [V || {_,V} <- RAcc],
    Diff = diff(LVals, RVals, Opts),
    %% io:format("Diff = ~p~n",[Diff]),
    merge_write_out(Cols, Merged, Opts, User, K, Diff).

merge_write_out(_Cols, _Merged, _Opts, _User, _K, []) -> ok;
merge_write_out(Cols, Merged, Opts, User, K, [{_,[]}|Diff]) -> 
    merge_write_out(Cols, Merged, Opts, User, K, Diff);
merge_write_out(Cols, Merged, Opts, User, K, [{eq,E}|Diff]) -> 
    merge_write(Cols, Merged, Opts, User, K, E, E),
    merge_write_out(Cols, Merged, Opts, User, K, Diff);
merge_write_out(Cols, Merged, Opts, User, K, [{ins,I},{eq,E}|Diff]) -> 
    merge_write(Cols, Merged, Opts, User, K, ?nav, I),
    merge_write(Cols, Merged, Opts, User, K, E, E),
    merge_write_out(Cols, Merged, Opts, User, K, Diff);
merge_write_out(Cols, Merged, Opts, User, K, [{del,D},{eq,E}|Diff]) -> 
    merge_write(Cols, Merged, Opts, User, K, D, ?nav),
    merge_write(Cols, Merged, Opts, User, K, E, E),
    merge_write_out(Cols, Merged, Opts, User, K, Diff);
merge_write_out(Cols, Merged, Opts, User, K, [{del,[D1,D2|Ds]}|Diff]) -> 
    merge_write(Cols, Merged, Opts, User, K, [D1], ?nav),
    merge_write_out(Cols, Merged, Opts, User, K, [{del,[D2|Ds]}|Diff]);
merge_write_out(Cols, Merged, Opts, User, K, [{ins,[I1,I2|Is]}|Diff]) -> 
    merge_write(Cols, Merged, Opts, User, K, ?nav, [I1]),
    merge_write_out(Cols, Merged, Opts, User, K, [{ins,[I2|Is]}|Diff]);
merge_write_out(Cols, Merged, Opts, User, K, [{del,[D]},{ins,[I|Is]}|Diff]) -> 
    case lists:member($=, binary_to_list(cmp(D,I,Opts))) of
        true ->     % we have white space equality =w or w= or w=w
            merge_write(Cols, Merged, Opts, User, K, [D], [I]),
            merge_write_out(Cols, Merged, Opts, User, K, [{ins,Is}|Diff]);
        false ->
            merge_write(Cols, Merged, Opts, User, K, [D], ?nav),
            merge_write(Cols, Merged, Opts, User, K, ?nav, [I]),
            merge_write_out(Cols, Merged, Opts, User, K, [{ins,Is}|Diff])
    end;
merge_write_out(Cols, Merged, Opts, User, K, [{ins,[I]},{del,[D|Ds]}|Diff]) -> 
    case lists:member($=, binary_to_list(cmp(D,I,Opts))) of
        true ->     % we have white space equality
            merge_write(Cols, Merged, Opts, User, K, [D], [I]),
            merge_write_out(Cols, Merged, Opts, User, K, Diff);
        false ->
            merge_write(Cols, Merged, Opts, User, K, [D], ?nav),
            merge_write(Cols, Merged, Opts, User, K, ?nav, [I]),
            merge_write_out(Cols, Merged, Opts, User, K, [{del,Ds}|Diff])
    end;
merge_write_out(Cols, Merged, Opts, User, K, [{ins,[I1]}]) -> 
    merge_write(Cols, Merged, Opts, User, K, ?nav, [I1]),
    merge_write_out(Cols, Merged, Opts, User, K, []);
merge_write_out(Cols, Merged, Opts, User, K, [{del,[D1]}]) -> 
    merge_write(Cols, Merged, Opts, User, K, [D1], ?nav),
    merge_write_out(Cols, Merged, Opts, User, K, []).

merge_write(_Cols, _Merged, _Opts, _User, _K, [], []) -> ok;
merge_write(_Cols, _Merged, _Opts, _User, _K, ?nav, []) -> ok;
merge_write(_Cols, _Merged, _Opts, _User, _K, [], ?nav) -> ok;
merge_write(_Cols, _Merged, _Opts, _User, _K, ?nav, ?nav) -> ok;
merge_write(1, Merged, Opts, User, K, [L|Ls], [R|Rs]) ->
    Cmp = cmp(L,R,Opts),
    imem_meta:insert(Merged, {imem_meta:physical_table_name(Merged),undefined,L,Cmp,R,<<>>,<<>>,<<>>,<<>>}, User),
    merge_write(1, Merged, Opts, User, K, Ls, Rs);
merge_write(1, Merged, Opts, User, K, [L|Ls], ?nav) ->
    imem_meta:insert(Merged, {imem_meta:physical_table_name(Merged),undefined,L,<<>>,?nav,<<>>,<<>>,<<>>,<<>>}, User),
    merge_write(1, Merged, Opts, User, K, Ls, ?nav);
merge_write(1, Merged, Opts, User, K, ?nav, [R|Rs]) ->
    imem_meta:insert(Merged, {imem_meta:physical_table_name(Merged),undefined,?nav,<<>>,R,<<>>,<<>>,<<>>,<<>>}, User),
    merge_write(1, Merged, Opts, User, K, ?nav, Rs);
merge_write(2, Merged, Opts, User, K, [L|Ls], [R|Rs]) ->
    Cmp = cmp(L,R,Opts),
    imem_meta:insert(Merged, {imem_meta:physical_table_name(Merged),undefined,K,L,Cmp,K,R,<<>>,<<>>}, User),
    merge_write(2, Merged, Opts, User, K, Ls, Rs);
merge_write(2, Merged, Opts, User, K, [L|Ls], ?nav) ->
    imem_meta:insert(Merged, {imem_meta:physical_table_name(Merged),undefined,K,L,<<>>,?nav,?nav,<<>>,<<>>}, User),
    merge_write(2, Merged, Opts, User, K, Ls, ?nav);
merge_write(2, Merged, Opts, User, K, ?nav, [R|Rs]) ->
    imem_meta:insert(Merged, {imem_meta:physical_table_name(Merged),undefined,?nav,?nav,<<>>,K,R,<<>>,<<>>}, User),
    merge_write(2, Merged, Opts, User, K, ?nav, Rs);
merge_write(3, Merged, Opts, User, K, [L|Ls], [R|Rs]) ->
    Cmp = cmp(L,R,Opts),
    imem_meta:insert(Merged, {imem_meta:physical_table_name(Merged),undefined,element(1,K),element(2,K),L,Cmp,element(1,K),element(2,K),R}, User),
    merge_write(3, Merged, Opts, User, K, Ls, Rs);
merge_write(3, Merged, Opts, User, K, [L|Ls], ?nav) ->
    imem_meta:insert(Merged, {imem_meta:physical_table_name(Merged),undefined,element(1,K),element(2,K),L,<<>>,?nav,?nav,?nav}, User),
    merge_write(3, Merged, Opts, User, K, Ls, ?nav);
merge_write(3, Merged, Opts, User, K, ?nav, [R|Rs]) ->
    imem_meta:insert(Merged, {imem_meta:physical_table_name(Merged),undefined,?nav,?nav,?nav,<<>>,element(1,K),element(2,K),R}, User),
    merge_write(3, Merged, Opts, User, K, ?nav, Rs).

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

-endif.
