-module(imem_sql_index).

-include("imem_seco.hrl").

-export([ exec/5
        ]).

exec(SKey, {'drop index', {}, TableName}=_ParseTree, _Stmt, _Opts, IsSec) ->
    % ?LogDebug("Drop Index Parse Tree~n~p~n", [_ParseTree]),
    {TableSchema, Tbl} = imem_sql_expr:binstr_to_qname2(TableName),
    Table = if 
        TableSchema =:= undefined ->
            {imem_meta:schema(),list_to_existing_atom(binary_to_list(Tbl))};
        true ->
            {list_to_existing_atom(binary_to_list(TableSchema)),list_to_existing_atom(binary_to_list(Tbl))}
    end,
    if_call_mfa(IsSec, 'drop_index', [SKey,Table]);
exec(SKey, {'drop index', IndexName, TableName}=_ParseTree, _Stmt, _Opts, IsSec) ->
    % ?LogDebug("Drop Index Parse Tree~n~p~n", [_ParseTree]),
    {TableSchema, Tbl} = imem_sql_expr:binstr_to_qname2(TableName),
    {IndexSchema, Index} = imem_sql_expr:binstr_to_qname2(IndexName),
    if 
        not (IndexSchema =:= TableSchema) ->
            ?ClientError({"Index and table are in different schema",{IndexSchema,TableSchema}});
        true -> 
            ok
    end,
    Table = if 
        TableSchema =:= undefined ->
            {imem_meta:schema(),list_to_existing_atom(binary_to_list(Tbl))};
        true ->
            {list_to_existing_atom(binary_to_list(TableSchema)),list_to_existing_atom(binary_to_list(Tbl))}
    end,
    if_call_mfa(IsSec, 'drop_index', [SKey,Table,Index]);

exec(SKey, {'create index', IndexType, IndexName, TableName
            , IndexDefn, NormWithFun, FilterWithFun} = _ParseTree
            , _Stmt, _Opts, IsSec) ->
    ?LogDebug("Create Index Parse Tree~n~p~n", [_ParseTree]),
    MySchema = imem_meta:schema(),
    MySchemaName = ?atom_to_binary(MySchema),
    {TableSchema, Tbl} = imem_sql_expr:binstr_to_qname2(TableName),                
    {IndexSchema, Index} = imem_sql_expr:binstr_to_qname2(IndexName),
    Table = case TableSchema of
        MySchemaName -> list_to_existing_atom(binary_to_list(Tbl)); 
        undefined ->    list_to_existing_atom(binary_to_list(Tbl)); 
        _ ->            ?ClientError({"Cannot create index on foreign schema table", TableSchema})
    end,
    case IndexSchema of
        MySchemaName -> ok; 
        undefined ->    ok; 
        _ ->            ?ClientError({"Cannot create index in foreign schema", IndexSchema})
    end,
    ExistingIndexDefs = case imem_meta:read(ddTable, {MySchema,Table}) of
        [#ddTable{}=D] -> 
            case lists:keysearch(index, 1, D#ddTable.opts) of
                {value,{index, EID}} -> EID;
                false ->                []
            end;
        [] ->
            ?ClientError({"Table dictionary does not exist for",Table})
    end,
    case [EII || EII <- ExistingIndexDefs, EII#ddIdxDef.name == Index] of
        []  ->  ok;
        _ ->    ?ClientError({"Index already exists for table", {IndexName,TableName}})
    end,
    MaxIdx = lists:max([0|[IdxDef#ddIdxDef.id || IdxDef <- ExistingIndexDefs]]),
    Vnf = case NormWithFun of
        {} ->                   (#ddIdxDef{})#ddIdxDef.vnf;
        {norm, NormF} ->        NormF;
        NormWithFun ->          ?ClientError({"Bad norm with function", NormWithFun})
    end,
    Iff = case FilterWithFun of
        {} ->                   (#ddIdxDef{})#ddIdxDef.iff;
        {filter, FilterF} ->    FilterF;
        FilterWithFun ->        ?ClientError({"Bad filter with function", FilterWithFun})
    end,
    NewIdx = #ddIdxDef{id=MaxIdx+1, name=Index, type=imem_index:index_type(IndexType), pl=IndexDefn, vnf=Vnf, iff=Iff},
    if_call_mfa(IsSec, create_or_replace_index, [SKey, Table, [NewIdx|ExistingIndexDefs]]).

%% --Interface functions  (calling imem_if for now, not exported) ---------

if_call_mfa(IsSec,Fun,Args) ->
    case IsSec of
        true -> apply(imem_sec, Fun, Args);
        _ ->    apply(imem_meta, Fun, lists:nthtail(1, Args))
    end.

%% TESTS ------------------------------------------------------------------
-ifdef(TEST).

-include_lib("eunit/include/eunit.hrl").

setup() -> 
    ?imem_test_setup.

teardown(_) -> 
    catch imem_meta:drop_table(idx_index_test),
    catch imem_meta:drop_table(index_test),
    ?imem_test_teardown.

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
        ?LogDebug("---TEST--- ~p ----Security ~p~n", [?MODULE, IsSec]),

        ?LogDebug("schema ~p~n", [imem_meta:schema()]),
        ?LogDebug("data nodes ~p~n", [imem_meta:data_nodes()]),
        ?assertEqual(true, is_atom(imem_meta:schema())),
        ?assertEqual(true, lists:member({imem_meta:schema(),node()}, imem_meta:data_nodes())),

        SKey=?imem_test_admin_login(),

        % Creating and loading some data into index_test table
        catch imem_meta:drop_table(idx_index_test),
        catch imem_meta:drop_table(index_test),
        ?assertEqual(ok
                     , imem_sql:exec(
                         SKey
                         , "create table index_test (col1 integer, col2 binstr not null);"
                         , 0, imem, IsSec)),
        [Meta] = if_call_mfa(IsSec, read, [SKey, ddTable, {imem,index_test}]),
        ?assertEqual(0, if_call_mfa(IsSec, table_size, [SKey, index_test])),
        TableData =
        [ <<"{\"NAME\":\"john0\", \"SURNAME\":\"doe0\", \"AGE\":24}">>
        , <<"{\"NAME\":\"john1\", \"SURNAME\":\"doe1\", \"AGE\":25}">>
        , <<"{\"NAME\":\"john2\", \"SURNAME\":\"doe2\", \"AGE\":26}">>
        , <<"{\"NAME\":\"john3\", \"SURNAME\":\"doe3\", \"AGE\":27}">>
        , <<"{\"NAME\":\"john4\", \"SURNAME\":\"doe4\", \"AGE\":28}">>
        ],
        [if_call_mfa(IsSec, write,[SKey, index_test, {index_test, Id, Data}])
         || {Id, Data} <- lists:zip(lists:seq(1,length(TableData)), TableData)],
        ?assertEqual(length(TableData), if_call_mfa(IsSec, table_size, [SKey, index_test])),

        % Creating index on col1
        ?assertEqual(ok
                     , imem_sql:exec(
                         SKey
                         , "create index i_col1 on index_test (col1);"
                         , 0, imem, IsSec)),
        [Meta1] = if_call_mfa(IsSec, read, [SKey, ddTable, {imem,index_test}]),
        {value, {index, [DdIdx]}} = lists:keysearch(index, 1, Meta1#ddTable.opts),
        ?assertEqual(1, DdIdx#ddIdxDef.id),
        ?assertEqual(<<"i_col1">>, DdIdx#ddIdxDef.name),

        % Creating index on col2
        ?assertEqual(ok
                     , imem_sql:exec(
                         SKey
                         , "create index i_col2 on index_test (col2)"
                           " norm_with fun(X) -> imem_index:vnf_lcase_ascii_ne(X) end.;"
                         , 0, imem, IsSec)),
        [Meta2] = if_call_mfa(IsSec, read, [SKey, ddTable, {imem,index_test}]),
        {value, {index, [DdIdx1, DdIdx]}} = lists:keysearch(index, 1, Meta2#ddTable.opts),
        ?assertEqual(2, DdIdx1#ddIdxDef.id),
        ?assertEqual(<<"i_col2">>, DdIdx1#ddIdxDef.name),

        % Creating index on col2:NAME
        ?assertEqual(ok
                     , imem_sql:exec(
                         SKey
                         , "create index i_col2_name on index_test (col2:NAME);"
                         , 0, imem, IsSec)),
        [Meta3] = if_call_mfa(IsSec, read, [SKey, ddTable, {imem,index_test}]),
        {value, {index, [DdIdx2, DdIdx1, DdIdx]}} =
            lists:keysearch(index, 1, Meta3#ddTable.opts),
        ?assertEqual(3, DdIdx2#ddIdxDef.id),
        ?assertEqual(<<"i_col2_name">>, DdIdx2#ddIdxDef.name),

        % Creating index on col2:SURNAME
        ?assertEqual(ok
                     , imem_sql:exec(
                         SKey
                         , "create index i_col2_surname on index_test (col2:SURNAME)"
                           " filter_with fun(X) -> imem_index:iff_true(X) end.;"
                         , 0, imem, IsSec)),
        [Meta4] = if_call_mfa(IsSec, read, [SKey, ddTable, {imem,index_test}]),
        {value, {index, [DdIdx3, DdIdx2, DdIdx1, DdIdx]}} =
            lists:keysearch(index, 1, Meta4#ddTable.opts),
        ?assertEqual(4, DdIdx3#ddIdxDef.id),
        ?assertEqual(<<"i_col2_surname">>, DdIdx3#ddIdxDef.name),

        % Creating index on col2:AGE
        ?assertEqual(ok
                     , imem_sql:exec(
                         SKey
                         , "create index i_col2_age on index_test (col2:AGE);"
                         , 0, imem, IsSec)),
        [Meta5] = if_call_mfa(IsSec, read, [SKey, ddTable, {imem,index_test}]),
        {value, {index, [DdIdx4, DdIdx3, DdIdx2, DdIdx1, DdIdx]}} =
            lists:keysearch(index, 1, Meta5#ddTable.opts),
        ?assertEqual(5, DdIdx4#ddIdxDef.id),
        ?assertEqual(<<"i_col2_age">>, DdIdx4#ddIdxDef.name),

        % Creating index on col2:NAME and col2:AGE
        ?assertEqual(ok
                     , imem_sql:exec(
                         SKey
                         , "create index i_col2_name_age on index_test (col2:NAME|col2:AGE)"
                           " norm_with fun(X) -> imem_index:vnf_lcase_ascii_ne(X) end."
                           " filter_with fun(X) -> imem_index:iff_true(X) end."
                           ";"
                         , 0, imem, IsSec)),
        [Meta6] = if_call_mfa(IsSec, read, [SKey, ddTable, {imem,index_test}]),
        {value, {index, [DdIdx5, DdIdx4, DdIdx3, DdIdx2, DdIdx1, DdIdx]}} =
            lists:keysearch(index, 1, Meta6#ddTable.opts),
        ?assertEqual(6, DdIdx5#ddIdxDef.id),
        ?assertEqual(<<"i_col2_name_age">>, DdIdx5#ddIdxDef.name),

        % Creating index on col2:SURNAME and col1
        ?assertEqual(ok
                     , imem_sql:exec(
                         SKey
                         , "create index i_col2_surname_col1 on index_test (col2:SURNAME|col1);"
                         , 0, imem, IsSec)),
        [Meta7] = if_call_mfa(IsSec, read, [SKey, ddTable, {imem,index_test}]),
        {value, {index, [DdIdx6, DdIdx5, DdIdx4, DdIdx3, DdIdx2, DdIdx1, DdIdx]}} =
            lists:keysearch(index, 1, Meta7#ddTable.opts),
        ?assertEqual(7, DdIdx6#ddIdxDef.id),
        ?assertEqual(<<"i_col2_surname_col1">>, DdIdx6#ddIdxDef.name),

        % Creating index on all fields
        ?assertEqual(ok
                     , imem_sql:exec(
                         SKey
                         , "create index i_all on index_test"
                           " (col1 | col2 | col2:NAME | col2:SURNAME | col2:AGE);"
                         , 0, imem, IsSec)),
        [Meta8] = if_call_mfa(IsSec, read, [SKey, ddTable, {imem,index_test}]),
        {value, {index, [DdIdx7, DdIdx6, DdIdx5, DdIdx4, DdIdx3, DdIdx2, DdIdx1, DdIdx]}} =
            lists:keysearch(index, 1, Meta8#ddTable.opts),
        ?assertEqual(8, DdIdx7#ddIdxDef.id),
        ?assertEqual(<<"i_all">>, DdIdx7#ddIdxDef.name),

        print_indices(IsSec, SKey, imem, index_test),

        % Creating a duplicate index (negative test)
        ?assertException(throw
                         , {'ClientError'
                            , {"Index already exists for table"
                               , {<<"i_col2_age">>, <<"index_test">>}}
                           }
                         , imem_sql:exec(
                             SKey
                             , "create index i_col2_age on index_test (col2:AGE);"
                             , 0, imem, IsSec)),

        %
        % Dropping indexes in random order
        %

        % Drop index i_col2_name_age
        ?assertEqual(ok
                     , imem_sql:exec(
                         SKey
                         , "drop index i_col2_name_age from index_test;"
                         , 0, imem, IsSec)),
        [Meta9] = if_call_mfa(IsSec, read, [SKey, ddTable, {imem,index_test}]),
        ?assertEqual({value, {index, [DdIdx7, DdIdx6, DdIdx4, DdIdx3, DdIdx2, DdIdx1, DdIdx]}}
                     , lists:keysearch(index, 1, Meta9#ddTable.opts)),

        % Dropping non-exixtant index (negative test)
        ?assertException(throw
                         , {'ClientError'
                            , {"Index does not exist for"
                               , index_test, <<"i_not_exists">>}
                           }
                         , imem_sql:exec(
                             SKey
                             , "drop index i_not_exists from index_test;"
                             , 0, imem, IsSec)),

        % Drop index i_col1
        ?assertEqual(ok
                     , imem_sql:exec(
                         SKey
                         , "drop index i_col1 from index_test;"
                         , 0, imem, IsSec)),
        [Meta10] = if_call_mfa(IsSec, read, [SKey, ddTable, {imem,index_test}]),
        ?assertEqual({value, {index, [DdIdx7, DdIdx6, DdIdx4, DdIdx3, DdIdx2, DdIdx1]}}
                     , lists:keysearch(index, 1, Meta10#ddTable.opts)),

        % Drop index i_col2_surname
        ?assertEqual(ok
                     , imem_sql:exec(
                         SKey
                         , "drop index i_col2_surname from index_test;"
                         , 0, imem, IsSec)),
        [Meta11] = if_call_mfa(IsSec, read, [SKey, ddTable, {imem,index_test}]),
        ?assertEqual({value, {index, [DdIdx7, DdIdx6, DdIdx4, DdIdx2, DdIdx1]}}
                     , lists:keysearch(index, 1, Meta11#ddTable.opts)),

        print_indices(IsSec, SKey, imem, index_test),

        % Drop index i_col2_surname_col1
        ?assertEqual(ok
                     , imem_sql:exec(
                         SKey
                         , "drop index i_col2_surname_col1 from index_test;"
                         , 0, imem, IsSec)),
        [Meta12] = if_call_mfa(IsSec, read, [SKey, ddTable, {imem,index_test}]),
        ?assertEqual({value, {index, [DdIdx7, DdIdx4, DdIdx2, DdIdx1]}}
                     , lists:keysearch(index, 1, Meta12#ddTable.opts)),

        % Drop index i_col2_name
        ?assertEqual(ok
                     , imem_sql:exec(
                         SKey
                         , "drop index i_col2_name from index_test;"
                         , 0, imem, IsSec)),
        [Meta13] = if_call_mfa(IsSec, read, [SKey, ddTable, {imem,index_test}]),
        ?assertEqual({value, {index, [DdIdx7, DdIdx4, DdIdx1]}}
                     , lists:keysearch(index, 1, Meta13#ddTable.opts)),

        % Drop index i_all
        ?assertEqual(ok
                     , imem_sql:exec(
                         SKey
                         , "drop index i_all from index_test;"
                         , 0, imem, IsSec)),
        [Meta14] = if_call_mfa(IsSec, read, [SKey, ddTable, {imem,index_test}]),
        ?assertEqual({value, {index, [DdIdx4, DdIdx1]}}
                     , lists:keysearch(index, 1, Meta14#ddTable.opts)),

        % Dropping previously dropped i_col2_name index (negative test)
        ?assertException(throw
                         , {'ClientError',
                            {"Index does not exist for"
                             , index_test, <<"i_col2_name">>}
                           }
                         , imem_sql:exec(
                             SKey
                             , "drop index i_col2_name from index_test;"
                             , 0, imem, IsSec)),

        print_indices(IsSec, SKey, imem, index_test),

        % Drop index i_col2_age
        ?assertEqual(ok
                     , imem_sql:exec(
                         SKey
                         , "drop index i_col2_age from index_test;"
                         , 0, imem, IsSec)),
        [Meta15] = if_call_mfa(IsSec, read, [SKey, ddTable, {imem,index_test}]),
        ?assertEqual({value, {index, [DdIdx1]}}
                     , lists:keysearch(index, 1, Meta15#ddTable.opts)),

        % Drop index i_col2 (last index)
        ?assertEqual(ok
                     , imem_sql:exec(
                         SKey
                         , "drop index i_col2 from index_test;"
                         , 0, imem, IsSec)),
        [Meta16] = if_call_mfa(IsSec, read, [SKey, ddTable, {imem,index_test}]),
        ?assertEqual(false, lists:keysearch(index, 1, Meta16#ddTable.opts)),
        ok
    catch
        Class:Reason ->  ?LogDebug("Exception ~p:~p~n~p~n", [Class, Reason, erlang:get_stacktrace()]),
        ?assert( true == "all tests completed")
    end,
    ok. 

print_indices(IsSec, SKey, Schema, Table) ->
    [Meta] = if_call_mfa(IsSec, read, [SKey, ddTable, {Schema,Table}]),
    {value, {index, Indices}} =
        lists:keysearch(index, 1, Meta#ddTable.opts),
    ?LogDebug("~nIndices :~n"
          "~s", [lists:flatten(
                   [[" ", binary_to_list(I#ddIdxDef.name), " -> "
                     , string:join(
                         [binary_to_list(element(2, jpparse:string(Pl)))
                          || Pl <- I#ddIdxDef.pl]
                         , " | ")
                     , "\n"]
                    || I <- Indices]
                  )]
         ).

-endif.
