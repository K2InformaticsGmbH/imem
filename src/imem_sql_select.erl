-module(imem_sql_select).

-include("imem_seco.hrl").

-export([ exec/5
        ]).

exec(SeCo, {select, SelectSections}, Stmt, _Schema, IsSec) ->
    Tables = case lists:keyfind(from, 1, SelectSections) of
        {_, TNames} ->  [imem_sql:table_qname(T) || T <- TNames];
        TError ->       ?ClientError({"Invalid select structure", TError})
    end,
    ColMap = case lists:keyfind(fields, 1, SelectSections) of
        false -> 
            imem_sql:column_map(Tables,[]);
        {_, FieldList} -> 
            imem_sql:column_map(Tables, FieldList);
        CError ->        
            ?ClientError({"Invalid select structure", CError})
    end,
    % WhereMap = case lists:keyfind(where, 1, SelectSections) of
    %     false -> 
    %         [];
    %     {_, BoolTree} -> 
    %         QNames = [{C, imem_sql:field_qname(C)} || C <- CNames],
    %         imem_sql:column_map(
    %             Tables,
    %             [#ddColMap{tag=Tag, oname=O, schema=S, table=T, name=N} || {Tag,{O,{S,T,N}}} <- lists:zip(lists:seq(1,length(QNames)), QNames)]
    %             );
    %     CError ->        
    %         ?ClientError({"Invalid field name", CError})
    % end,
    ColPointers = [{C#ddColMap.tind, C#ddColMap.cind} || C <- ColMap],
    RowFun = fun(X) -> [element(Cind,element(Tind,X))|| {Tind,Cind} <- ColPointers] end,
    MetaIdx = length(Tables) + 1,
    MetaMap = [ N || {_,N} <- lists:usort([{C#ddColMap.cind, C#ddColMap.name} || C <- ColMap, C#ddColMap.tind==MetaIdx])],
   
    MatchHead = '$1',
    Guards = [],    
    Result = '$_',

    MatchSpec = [{MatchHead, Guards, [Result]}],
    JoinSpec = [],                      %% ToDo: e.g. {join type (inner|outer|self, join field element number, matchspec joined table} per join
    Statement = Stmt#statement{
                    tables=Tables, cols=ColMap, meta=MetaMap, rowfun=RowFun,
                    matchspec=MatchSpec, joinspec=JoinSpec
                },
    {ok, StmtRef} = imem_statement:create_stmt(Statement, SeCo, IsSec),
    io:format(user,"Statement : ~p~n", [Stmt]),
    io:format(user,"Tables: ~p~n", [Tables]),
    io:format(user,"Column map: ~p~n", [ColMap]),
    io:format(user,"Meta map: ~p~n", [MetaMap]),
    io:format(user,"MatchSpec: ~p~n", [MatchSpec]),
    io:format(user,"JoinSpec: ~p~n", [JoinSpec]),
    {ok, ColMap, RowFun, StmtRef}.


% exec_tree(ParseTree) ->
%     exec_tree(ParseTree, []).

% exec_tree([], ExecTree) ->
%     ExecTree;
% exec_tree({select, Sections}, ExecTree) ->
%     ok.




%% --Interface functions  (calling imem_if for now, not exported) ---------

if_call_mfa(IsSec,Fun,Args) ->
    case IsSec of
        true -> apply(imem_sec,Fun,Args);
        _ ->    apply(imem_meta, Fun, lists:nthtail(1, Args))
    end.

%% TESTS ------------------------------------------------------------------

-include_lib("eunit/include/eunit.hrl").

setup() -> 
    ?imem_test_setup().

teardown(_SKey) -> 
    catch imem_meta:drop_table(def),
    ?imem_test_teardown().

db_test_() ->
    {
        setup,
        fun setup/0,
        fun teardown/1,
        {with, [
%              fun test_without_sec/1
             fun test_with_sec/1
        ]}
    }.
    
test_without_sec(_) -> 
    test_with_or_without_sec(false).

test_with_sec(_) ->
    test_with_or_without_sec(true).

test_with_or_without_sec(IsSec) ->
    try
        % ClEr = 'ClientError',
        % SeEx = 'SecurityException',
        io:format(user, "----TEST--- ~p ----Security ~p ~n", [?MODULE, IsSec]),
        SKey=case IsSec of
            true ->     ?imem_test_admin_login();
            false ->    none
        end,

        ?assertEqual(ok, imem_sql:exec(SKey, "create table def (col1 integer, col2 char);", 0, "Imem", IsSec)),

        Result0 = if_call_mfa(IsSec,select,[SKey, ddTable, ?MatchAllRecords, 1000]),
        %io:format(user, "ddTable result~n~p~n", [Result0]),
        {List0, true} = Result0,
        AllTableCount = length(List0),
        Result1 = if_call_mfa(IsSec,select,[SKey, all_tables, ?MatchAllKeys]),
        io:format(user, "all_tables result~n~p~n", [Result1]),
        {List1, true} = Result1,
        ?assertEqual(AllTableCount, length(List1)),

        ?assertEqual(ok, insert_range(SKey, 10, "def", "Imem", IsSec)),
 
        {ok, _Clm2, RowFun2, StmtRef2} = imem_sql:exec(SKey, "select col1, user from def where col1 > 5 and col2 <> '8';", 100, "Imem", IsSec),
        ?assertEqual(ok, imem_statement:fetch_recs(SKey, StmtRef2, self(), IsSec)),
        Result2 = receive 
            R2 ->    binary_to_term(R2)
        end,
        {List2, true} = Result2,
        io:format(user, "fetch_recs result~n~p~n", [lists:map(RowFun2,List2)]),
        ?assertEqual(10, length(List2)),            %% ToDo: 4
        ?assertEqual(ok, imem_statement:fetch_recs_async(SKey, StmtRef2, self(), IsSec)),
        Result2a = receive 
            R2a ->    binary_to_term(R2a)
        end,
        {List2a, _} = Result2a,
        io:format(user, "fetch_recs_async result~n~p~n", [lists:map(RowFun2,List2a)]),
        ?assertEqual(Result2, Result2a),           
        %% ?assertEqual(ok, imem_statement:close(SKey, StmtRef2, self())),

        {ok, _Clm3, RowFun3, StmtRef3} = imem_sql:exec(SKey, "select qname from ddTable;", 100, "Imem", IsSec),  %% all_tables
        ?assertEqual(ok, imem_statement:fetch_recs(SKey, StmtRef3, self(), IsSec)),
        Result3 = receive 
            R3 ->    binary_to_term(R3)
        end,
        {List3, true} = Result3,
        io:format(user, "fetch_recs result~n~p~n", [lists:map(RowFun3,List3)]),
        ?assertEqual(AllTableCount, length(List3)),
        ?assertEqual(ok, imem_statement:fetch_recs_async(SKey, StmtRef3, self(), IsSec)),
        Result3a = receive 
            R3a ->    binary_to_term(R3a)
        end,
        {List3a, _} = Result3a,
        io:format(user, "fetch_recs_async result~n~p~n", [lists:map(RowFun3,List3a)]),
        ?assertEqual(Result3, Result3a),           

        ?assertEqual(ok, imem_statement:close(SKey, StmtRef2, self())),
        ?assertEqual(ok, imem_statement:close(SKey, StmtRef3, self())),

        %% ?assertEqual(ok, imem_sql:exec(SKey, "drop table def;", 0, "Imem", IsSec)),

        case IsSec of
            true ->     ?imem_logout(SKey);
            false ->    ok
        end

    catch
        Class:Reason ->  io:format(user, "Exception ~p:~p~n~p~n", [Class, Reason, erlang:get_stacktrace()]),
        ?assert( true == "all tests completed")
    end,
    ok. 



insert_range(_SKey, 0, _TableName, _Schema, _IsSec) -> ok;
insert_range(SKey, N, TableName, Schema, IsSec) when is_integer(N), N > 0 ->
    imem_sql:exec(SKey, "insert into " ++ TableName ++ " values (" ++ integer_to_list(N) ++ ", '" ++ integer_to_list(N) ++ "');", 0, Schema, IsSec),
    insert_range(SKey, N-1, TableName, Schema, IsSec).
