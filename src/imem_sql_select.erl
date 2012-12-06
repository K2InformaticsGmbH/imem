-module(imem_sql_select).

-include("imem_seco.hrl").

-define(DefaultRendering, gui ).         %% gui (strings when necessary) | str (strings) | raw (erlang terms)
-define(DefaultDateFormat, eu ).         %% eu | us | iso | raw
-define(DefaultStrFormat, []).           %% escaping not implemented
-define(DefaultNumFormat, [{prec,2}]).   %% precision, no 

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
    RowFun = case ?DefaultRendering of
        raw ->  imem_datatype:select_rowfun_raw(ColMap);
        str ->  imem_datatype:select_rowfun_str(ColMap, ?DefaultDateFormat, ?DefaultNumFormat, ?DefaultStrFormat);
        gui ->  imem_datatype:select_rowfun_gui(ColMap, ?DefaultDateFormat, ?DefaultNumFormat, ?DefaultStrFormat)
    end,
    MetaIdx = length(Tables) + 1,
    MetaMap = [ N || {_,N} <- lists:usort([{C#ddColMap.cind, C#ddColMap.name} || C <- ColMap, C#ddColMap.tind==MetaIdx])],

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
    % io:format(user,"Statement : ~p~n", [Stmt]),
    % io:format(user,"Tables: ~p~n", [Tables]),
    % io:format(user,"Column map: ~p~n", [ColMap]),
    % io:format(user,"Meta map: ~p~n", [MetaMap]),
    % io:format(user,"MatchSpec: ~p~n", [MatchSpec]),
    % io:format(user,"JoinSpec: ~p~n", [JoinSpec]),
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
              fun test_without_sec/1
            , fun test_with_sec/1
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

        io:format(user, "schema ~p~n", [imem_meta:schema()]),
        io:format(user, "data nodes ~p~n", [imem_meta:data_nodes()]),
        ?assertEqual(true, is_atom(imem_meta:schema())),
        ?assertEqual(true, lists:member({imem_meta:schema(),node()}, imem_meta:data_nodes())),

        SKey=case IsSec of
            true ->     ?imem_test_admin_login();
            false ->    none
        end,

        ?assertEqual(ok, imem_sql:exec(SKey, "
                create table def (
                    col1 integer, 
                    col2 char(2), 
                    col3 date default fun() -> calendar:local_time() end.
                );", 0, "Imem", IsSec)),

        Result0 = if_call_mfa(IsSec,select,[SKey, ddTable, ?MatchAllRecords, 1000]),
        {List0, true} = Result0,
        io:format(user, "ddTable MatchAllRecords (~p)~n~p~n...~n~p~n", [length(List0),hd(List0),lists:last(List0)]),
        AllTableCount = length(List0),

        Result1 = if_call_mfa(IsSec,select,[SKey, all_tables, ?MatchAllKeys]),
        {List1, true} = Result1,
        io:format(user, "all_tables MatchAllKeys (~p)~n~p~n", [length(List1),List1]),
        ?assertEqual(AllTableCount, length(List1)),

        ?assertEqual(ok, insert_range(SKey, 10, "def", "Imem", IsSec)),

        Result2 = if_call_mfa(IsSec,select,[SKey, def, ?MatchAllRecords, 1000]),
        {List2, true} = Result2,
        io:format(user, "def MatchAllRecords (~p)~n~p~n...~n~p~n", [length(List2),hd(List2),lists:last(List2)]),
 
        {ok, _Clm3, RowFun3, StmtRef3} = imem_sql:exec(SKey, "select qname from Imem.ddTable;", 100, "Imem", IsSec),  %% all_tables
        ?assertEqual(ok, imem_statement:fetch_recs_async(SKey, StmtRef3, self(), IsSec)),
        Result3 = receive 
            R3 ->    binary_to_term(R3)
        end,
        {List3, true} = Result3,
        io:format(user, "select qname from Imem.ddTable (~p)~n~p~n...~n~p~n", [length(List3),hd(lists:map(RowFun3,List3)),lists:last(lists:map(RowFun3,List3))]),
        ?assertEqual(AllTableCount, length(List3)),

        ?assertEqual(ok, imem_statement:fetch_recs_async(SKey, StmtRef3, self(), IsSec)),
        Result3a = receive 
            R3a ->    binary_to_term(R3a)
        end,
        {List3a, true} = Result3a,
        io:format(user, "select qname from Imem.ddTable (reread ~p)~n~p~n...~n~p~n", [length(List3a),hd(lists:map(RowFun3,List3a)),lists:last(lists:map(RowFun3,List3a))]),
        ?assertEqual(AllTableCount, length(List3a)),

        {ok, _Clm4, _RowFun4, StmtRef4} = imem_sql:exec(SKey, "select qname from all_tables", 100, "Imem", IsSec),  %% all_tables
        ?assertEqual(ok, imem_statement:fetch_recs_async(SKey, StmtRef4, self(), IsSec)),
        Result4 = receive 
            R4 ->    binary_to_term(R4)
        end,
        {List4, true} = Result4,
        io:format(user, "select qname from all_tables (~p)~n~p~n...~n~p~n", [length(List4),hd(List4),lists:last(List4)]),
        ?assertEqual(AllTableCount, length(List4)),

        {ok, _Clm5, RowFun5, StmtRef5} = imem_sql:exec(SKey, "select col1, col2, col3, user from def", 100, "Imem", IsSec),
        ?assertEqual(ok, imem_statement:fetch_recs_async(SKey, StmtRef5, self(), IsSec)),
        Result5 = receive 
            R5 ->    binary_to_term(R5)
        end,
        {List5, true} = Result5,
        io:format(user, "select col1, col2, col3, user from def (~p)~n~p~n", [length(List5),lists:map(RowFun5,List5)]),
        ?assertEqual(10, length(List5)),            

        ?assertEqual(ok, imem_statement:close(SKey, StmtRef3)),
        ?assertEqual(ok, imem_statement:close(SKey, StmtRef4)),
        ?assertEqual(ok, imem_statement:close(SKey, StmtRef5)),

        ?assertEqual(ok, imem_sql:exec(SKey, "drop table def;", 0, "Imem", IsSec)),

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
    imem_sql:exec(SKey, "insert into " ++ TableName ++ " (col1, col2) values (" ++ integer_to_list(N) ++ ", '" ++ integer_to_list(N) ++ "');", 0, Schema, IsSec),
    insert_range(SKey, N-1, TableName, Schema, IsSec).
