%% -*- coding: utf-8 -*-
-module(imem_merge).

-include("imem.hrl").
-include("imem_meta.hrl").

-export([ merge_diff/3              %% merge two data tables into a bigger one, presenting the differences side by side
        , merge_diff/4              %% merge two data tables into a bigger one, presenting the differences side by side
        , merge_diff/5              %% merge two data tables into a bigger one, presenting the differences side by side
        , term_diff/4               %% present two terms side-by-side for comparison
        , term_diff/5               %% present two terms side-by-side for comparison
        , term_diff/6               %% present two terms side-by-side for comparison
        ]).

-safe([merge_diff, term_diff]).

load(<<"https://",_/binary>> = LeftUrl, User) -> load_http(LeftUrl, User);
load(<<"http://",_/binary>> = LeftUrl, User) -> load_http(LeftUrl, User);
load(LeftData, _User) -> LeftData.

load_http(Url, User) ->
    try 
        {ok, {_Scheme, _UserInfo, Host, _Port, _Path, _Query}} = http_uri:parse(Url),
        ?Info("Url ~p",[Url]),
        ?Info("User ~p",[User]),
        AuthProfile = imem_client:get_auth_profile(Host, User),
        ?Info("AuthProfile ~p",[AuthProfile]),
        FixedUrl = imem_client:fix_url(Url),
        ?Info("FixedUrl ~p",[FixedUrl]),
        #{body := Body} = imem_client:http(get, FixedUrl, [], AuthProfile, undefined),
        Body
    catch 
        _ -> Url
    end.

-spec term_diff(atom(), term(), atom(), term()) -> list(#ddTermDiff{}).
term_diff(LeftType, LeftData, RightType, RightData) ->
    term_diff(LeftType, LeftData, RightType, RightData, []).

-spec term_diff(atom(), term(), atom(), term(), ddOptions()) -> list(#ddTermDiff{}).
term_diff(LeftType, LeftData, RightType, RightData, Opts) ->
    term_diff(LeftType, LeftData, RightType, RightData, Opts, undefined).

-spec term_diff(atom(), term(), atom(), term(), ddOptions(), ddEntityId()) -> list(#ddTermDiff{}).
term_diff(binstr, LeftData, binstr, RightData, Opts, User) ->
    Left = load(LeftData, User),
    Right = load(RightData, User),
    term_diff_out(Opts, imem_tdiff:diff_binaries(Left, Right, Opts), []);
term_diff(binary, Data, binary, Data, Opts, _User) ->
    [#ddTermDiff{id=1,left=Data,cmp=imem_cmp:cmp(Data,Data,Opts),right=Data}];
term_diff(binary, LeftData, binary, RightData, Opts, _User) ->
    [#ddTermDiff{id=1,left=LeftData,cmp=imem_cmp:cmp(LeftData,RightData,Opts),right=RightData}];
term_diff(LeftType, _LeftData, RightType, _RightData, _Opts, _User) ->
    ?UnimplementedException({"term_diff for unsupported data type", {LeftType, RightType}}).

term_diff_out(_Opts, [], Acc) -> lists:reverse(Acc);
term_diff_out(Opts, [{_,[]}|Diff], Acc) -> 
    term_diff_out(Opts, Diff, Acc);
term_diff_out(Opts, [{eq,E}|Diff], Acc) -> 
    term_diff_out(Opts, Diff, term_diff_add(E, E, Opts, Acc));
term_diff_out(Opts, [{ins,I},{eq,E}|Diff], Acc) -> 
    term_diff_out(Opts, Diff, term_diff_add(E, E, Opts, term_diff_add(?nav, I, Opts, Acc)));
term_diff_out(Opts, [{del,D},{eq,E}|Diff], Acc) -> 
    term_diff_out(Opts, Diff, term_diff_add(E, E, Opts, term_diff_add(D, ?nav, Opts, Acc)));
term_diff_out(Opts, [{del,[D1,D2|Ds]}|Diff], Acc) -> 
    term_diff_out(Opts, [{del,[D2|Ds]}|Diff], term_diff_add([D1], ?nav, Opts, Acc));
term_diff_out(Opts, [{ins,[I1,I2|Is]}|Diff], Acc) -> 
    term_diff_out(Opts, [{ins,[I2|Is]}|Diff], term_diff_add(?nav, [I1], Opts, Acc));
term_diff_out(Opts, [{del,[D]},{ins,[I|Is]}|Diff], Acc) -> 
    case lists:member($=, binary_to_list(imem_cmp:cmp(D,I,Opts))) of
        true ->     % we have white space equality =w or w= or w=w
            term_diff_out(Opts, [{ins,Is}|Diff], term_diff_add([D], [I], Opts, Acc));
        false ->
            term_diff_out(Opts, [{ins,Is}|Diff], term_diff_add(?nav, [I], Opts, term_diff_add([D], ?nav, Opts, Acc)))
    end;
term_diff_out(Opts, [{ins,[I]},{del,[D|Ds]}|Diff], Acc) -> 
    case lists:member($=, binary_to_list(imem_cmp:cmp(D,I,Opts))) of
        true ->     % we have white space equality
            term_diff_out(Opts, Diff, term_diff_add([D], [I], Opts, Acc));
        false ->
            term_diff_out(Opts, [{del,Ds}|Diff], term_diff_add(?nav, [I], Opts, term_diff_add([D], ?nav, Opts, Acc)))
    end;
term_diff_out(Opts, [{ins,[I1]}], Acc) -> 
    term_diff_out(Opts, [], term_diff_add(?nav, [I1], Opts, Acc));
term_diff_out(Opts, [{del,[D1]}], Acc) -> 
    term_diff_out(Opts, [], term_diff_add([D1], ?nav, Opts, Acc)).

term_diff_add([], [], _Opts, Acc) -> Acc;
term_diff_add([], ?nav, _Opts, Acc) -> Acc;
term_diff_add(?nav, [], _Opts, Acc) -> Acc;
term_diff_add([L|LRest], ?nav, Opts, Acc) ->
    term_diff_add(LRest, ?nav, Opts, [#ddTermDiff{id=length(Acc)+1
                                     ,left=list_to_binary(L)}|Acc
                                     ]);
term_diff_add(?nav, [R|RRest], Opts, Acc) ->
    term_diff_add(?nav, RRest, Opts, [#ddTermDiff{id=length(Acc)+1
                                     ,right=list_to_binary(R)}|Acc
                                     ]);
term_diff_add([{L,R}|LRest], [{L,R}|RRest], Opts, Acc) ->
    term_diff_add(LRest, RRest, Opts, [#ddTermDiff{id=length(Acc)+1
                                      ,left=list_to_binary(L)
                                      ,cmp=imem_cmp:cmp(L,R,Opts)
                                      ,right=list_to_binary(R)}|Acc
                                      ]);
term_diff_add([L|LRest], [R|RRest], Opts, Acc) ->
    term_diff_add(LRest, RRest, Opts, [#ddTermDiff{id=length(Acc)+1
                                      ,left=list_to_binary(L)
                                      ,cmp=imem_cmp:cmp(L,R,Opts)
                                      ,right=list_to_binary(R)}|Acc
                                      ]).

-spec merge_diff(ddTable(), ddTable(), ddTable()) -> ok.
merge_diff(Left, Right, Merged) -> 
    merge_diff(Left, Right, Merged, []).

-spec merge_diff(ddTable(), ddTable(), ddTable(), ddOptions()) -> ok.
merge_diff(Left, Right, Merged, Opts) -> 
    merge_diff(Left, Right, Merged, Opts, imem_meta:meta_field_value(user)).

-spec merge_diff(ddTable(), ddTable(), ddTable(), ddOptions(), ddEntityId()) -> ok.
merge_diff(Left, Right, Merged, Opts, User) ->
    imem_meta:log_to_db(debug, ?MODULE, merge_diff, [{left, Left}, {right, Right}, {merged, Merged}], "merge_diff table"),
    MySchema = imem_meta:schema(),
    LeftQname = imem_meta:qualified_table_name(Left),
    RightQname = imem_meta:qualified_table_name(Right),
    MergedQname = imem_meta:qualified_table_name(Merged),
    case {LeftQname, RightQname, MergedQname} of
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
            merge_diff_scan(Cols, Left, Right, Merged, Opts, User, LI, imem_meta:dirty_next(Right,RI), [], [{RK,RV}])
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
    Diff = imem_tdiff:diff(LVals, RVals, Opts),
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
    case lists:member($=, binary_to_list(imem_cmp:cmp(D,I,Opts))) of
        true ->     % we have white space equality =w or w= or w=w
            merge_write(Cols, Merged, Opts, User, K, [D], [I]),
            merge_write_out(Cols, Merged, Opts, User, K, [{ins,Is}|Diff]);
        false ->
            merge_write(Cols, Merged, Opts, User, K, [D], ?nav),
            merge_write(Cols, Merged, Opts, User, K, ?nav, [I]),
            merge_write_out(Cols, Merged, Opts, User, K, [{ins,Is}|Diff])
    end;
merge_write_out(Cols, Merged, Opts, User, K, [{ins,[I]},{del,[D|Ds]}|Diff]) -> 
    case lists:member($=, binary_to_list(imem_cmp:cmp(D,I,Opts))) of
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
    Cmp = imem_cmp:cmp(L,R,Opts),
    imem_meta:insert(Merged, {imem_meta:physical_table_name(Merged),undefined,L,Cmp,R,<<>>,<<>>,<<>>,<<>>}, User),
    merge_write(1, Merged, Opts, User, K, Ls, Rs);
merge_write(1, Merged, Opts, User, K, [L|Ls], ?nav) ->
    imem_meta:insert(Merged, {imem_meta:physical_table_name(Merged),undefined,L,<<>>,?nav,<<>>,<<>>,<<>>,<<>>}, User),
    merge_write(1, Merged, Opts, User, K, Ls, ?nav);
merge_write(1, Merged, Opts, User, K, ?nav, [R|Rs]) ->
    imem_meta:insert(Merged, {imem_meta:physical_table_name(Merged),undefined,?nav,<<>>,R,<<>>,<<>>,<<>>,<<>>}, User),
    merge_write(1, Merged, Opts, User, K, ?nav, Rs);
merge_write(2, Merged, Opts, User, K, [L|Ls], [R|Rs]) ->
    Cmp = imem_cmp:cmp(L,R,Opts),
    imem_meta:insert(Merged, {imem_meta:physical_table_name(Merged),undefined,K,L,Cmp,K,R,<<>>,<<>>}, User),
    merge_write(2, Merged, Opts, User, K, Ls, Rs);
merge_write(2, Merged, Opts, User, K, [L|Ls], ?nav) ->
    imem_meta:insert(Merged, {imem_meta:physical_table_name(Merged),undefined,K,L,<<>>,?nav,?nav,<<>>,<<>>}, User),
    merge_write(2, Merged, Opts, User, K, Ls, ?nav);
merge_write(2, Merged, Opts, User, K, ?nav, [R|Rs]) ->
    imem_meta:insert(Merged, {imem_meta:physical_table_name(Merged),undefined,?nav,?nav,<<>>,K,R,<<>>,<<>>}, User),
    merge_write(2, Merged, Opts, User, K, ?nav, Rs);
merge_write(3, Merged, Opts, User, K, [L|Ls], [R|Rs]) ->
    Cmp = imem_cmp:cmp(L,R,Opts),
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

term_diff_add_test_() ->
    [ {"TD_ADD1",               ?_assertEqual([#ddTermDiff{id=2,left= <<"DEF\n">>,cmp= <<"=w">>,right= <<" DEF\n">>}
                                              ,#ddTermDiff{id=1,left= <<"ABC \n">>,cmp= <<"w=">>,right= <<"ABC\n">>}
                                              ]
                                              , term_diff_add([{"ABC \n", "ABC\n"}, {"DEF\n", " DEF\n"}]
                                                   , [{"ABC \n", "ABC\n"}, {"DEF\n", " DEF\n"}], [], []))}
    ].

term_diff_out_test_() ->
    [ {"TD_OUT1",               ?_assertEqual([#ddTermDiff{id=1,left= <<"ABC \n">>,cmp= <<"w=">>,right= <<"ABC\n">>}
                                              ,#ddTermDiff{id=2,left= <<"DEF\n">>,cmp= <<"=w">>,right= <<" DEF\n">>}
                                              ]
                                              , term_diff_out([], [{eq,[{"ABC \n", "ABC\n"}, {"DEF\n", " DEF\n"}]}], []))}
    ].

term_diff_test_() ->
    [ {"TD_EQUAL1",             ?_assertEqual([#ddTermDiff{id=1,left= <<"ABC">>,cmp= <<"=">>,right= <<"ABC">>}
                                              ]
                                              , term_diff(binstr, <<"ABC">>, binstr, <<"ABC">>, [ignore_whitespace]))}
    , {"TD_EQUAL2",             ?_assertEqual([#ddTermDiff{id=1,left= <<"ABC\n">>,cmp= <<"=">>,right= <<"ABC\n">>}
                                              ,#ddTermDiff{id=2,left= <<"DEF">>,cmp= <<"=">>,right= <<"DEF">>}
                                              ]
                                              , term_diff(binstr, <<"ABC\nDEF">>, binstr, <<"ABC\nDEF">>, [ignore_whitespace]))}
    , {"TD_EQUAL3",             ?_assertEqual([#ddTermDiff{id=1,left= <<"ABC\n">>,cmp= <<"=">>,right= <<"ABC\n">>}
                                              ,#ddTermDiff{id=2,left= <<"DEF\n">>,cmp= <<"=">>,right= <<"DEF\n">>}
                                              ]
                                              , term_diff(binstr, <<"ABC\nDEF\n">>, binstr, <<"ABC\nDEF\n">>, [ignore_whitespace]))}
    , {"TD_WS1",                ?_assertEqual([#ddTermDiff{id=1,left= <<"ABC \n">>,cmp= <<"w=">>,right= <<"ABC\n">>}
                                              ,#ddTermDiff{id=2,left= <<"DEF\n">>,cmp= <<"=w">>,right= <<" DEF\n">>}
                                              ]
                                              , term_diff(binstr, <<"ABC \nDEF\n">>, binstr, <<"ABC\n DEF\n">>, [ignore_whitespace]))}
    , {"TD_DIFF1",              ?_assertEqual([#ddTermDiff{id=1,left= <<"ABC\n">>,cmp= <<"=">>,right= <<"ABC\n">>}
                                              ,#ddTermDiff{id=2,left= <<"XYZ\n">>,cmp= <<>>,right=?nav}
                                              ,#ddTermDiff{id=3,left= <<"DEF\n">>,cmp= <<"=">>,right= <<"DEF\n">>}
                                              ]
                                              , term_diff(binstr, <<"ABC\nXYZ\nDEF\n">>, binstr, <<"ABC\nDEF\n">>, [ignore_whitespace]))}
    , {"TD_SQL_VIEW",           ?_assertEqual([ {ddTermDiff,1,<<"  CREATE OR REPLACE FORCE VIEW sbs0_admin.bad_msisdn (msisdn)\n">>
                                                            ,<<"w=">>
                                                            ,<<"CREATE OR REPLACE FORCE VIEW sbs0_admin.bad_msisdn (msisdn)\n">>}
                                              , {ddTermDiff,2,'$not_a_value', <<>>, <<"BEQUEATH DEFINER\n">>}
                                              , {ddTermDiff,3,<<"AS\n">>, <<"=">>, <<"AS\n">>}
                                              , {ddTermDiff,4,<<"  SELECT MSISDN FROM bad_msisdn_prov\n">>, <<"=w">>, <<"    SELECT MSISDN FROM bad_msisdn_prov\n">>}
                                              , {ddTermDiff,5,<<"UNION\n">>, <<"=w">>, <<"    UNION\n">>}
                                              , {ddTermDiff,6,<<"SELECT MSISDN FROM bad_msisdn_smsc">>, <<"=w">>, <<"    SELECT MSISDN FROM bad_msisdn_smsc">>}
                                              ]
                                              , term_diff(
                                                      binstr
                                                    , <<"  CREATE OR REPLACE FORCE VIEW sbs0_admin.bad_msisdn (msisdn)\nAS\n  SELECT MSISDN FROM bad_msisdn_prov\nUNION\nSELECT MSISDN FROM bad_msisdn_smsc">>
                                                    , binstr
                                                    , <<"CREATE OR REPLACE FORCE VIEW sbs0_admin.bad_msisdn (msisdn)\nBEQUEATH DEFINER\nAS\n    SELECT MSISDN FROM bad_msisdn_prov\n    UNION\n    SELECT MSISDN FROM bad_msisdn_smsc">>
                                                    , [ignore_whitespace]))}
    ].

-endif.
