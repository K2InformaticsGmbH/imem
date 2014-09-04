-module(imem_sql_index).

-include("imem_seco.hrl").

-export([ exec/5
        ]).

exec(SKey, {'drop index', {tables, [TableName|Tables]}, Exists, RestrictCascade}=_ParseTree, Stmt, Opts, IsSec) ->
    % ?LogDebug("Drop Table ParseTree~n~p~n", [_ParseTree]),
    QName = imem_meta:qualified_table_name(TableName), 
    % ?LogDebug("Drop Table QName~n~p~n", [QName]),
    if_call_mfa(IsSec, 'drop_table', [SKey,QName]),
    exec(SKey, {'drop table', {tables, Tables}, Exists, RestrictCascade}, Stmt, Opts, IsSec);

exec(SKey, {'create index', IndexType, IndexName, TableName
            , IndexDefn, NormWithFun, FilterWithFun} = _ParseTree
     , _Stmt, _Opts, IsSec) ->
    % ?Info("Parse Tree ~p~n", [_ParseTree]),
    {IndexSchema, Tbl} = imem_sql_expr:binstr_to_qname2(TableName),
    {TableSchema, Index} = imem_sql_expr:binstr_to_qname2(IndexName),
    if not (IndexSchema =:= TableSchema) ->
           ?ClientError({"Index and table are in different schema"
                         , {IndexSchema, TableSchema}});
       true -> ok
    end,
    Table = {imem_meta:schema(), list_to_existing_atom(binary_to_list(Tbl))},
    IndexDefs = case imem_meta:read(ddTable, Table) of
        [#ddTable{}=D] -> 
            case lists:keysearch(index, 1, D#ddTable.opts) of
                {value,{index, ExistingIndexDefs}} -> ExistingIndexDefs;
                false -> 0
            end;
        [] ->
            ?ClientError({"Table dictionary does not exist for",Table})
    end,
    case length([IDf||IDf <- IndexDefs, IDf#ddIdxDef.name =:= Index]) of
        MatchedIndexes when MatchedIndexes > 0 ->
            ?ClientError({"Index already exists in table", IndexName, TableName});
        _ -> ok
    end,
    MaxIdx = lists:max([IdxDef#ddIdxDef.id || IdxDef <- IndexDefs]),
    NewIdx = #ddIdxDef{id = MaxIdx+1
                       , name = Index
                       , pl = IndexDefn},
    NewIdx1 = case NormWithFun of
                  {} ->             NewIdx;
                  {norm, NormF} ->  NewIdx#ddIdxDef{vnf = NormF};
                  NormWithFun ->    ?ClientError({"Bad norm with function", NormWithFun})
              end,
    NewIdx2 = case FilterWithFun of
                    {} ->                   NewIdx1;
                    {filter, FilterF} ->    NewIdx#ddIdxDef{iff = FilterF};
                    FilterWithFun ->        ?ClientError({"Bad filter with function", FilterWithFun})
                end,
    %% TODO: IndexType is not used
    %%       #ddIdxDef.type is hardcoded to ivk
    NewIdx3 = NewIdx2#ddIdxDef{type = ivk},
    create_index(SKey, IndexType, Table, [NewIdx3|IndexDefs], IsSec).

create_index(SKey, IndexType, TableName, [#ddIdxDef{}|_] = IndexDefinitions, IsSec)
  when IndexType =:= {}; IndexType =:= bitmap;
       IndexType =:= keylist; IndexType =:= hashmap ->
    io:format(user,
    %?LogDebug(
              "Create Index ~n"
              "  Type ~p ~n"
              "  Tabl ~p ~n"
              "  Defn ~p ~n"
              , [IndexType
                 , TableName
                 , IndexDefinitions]),
    if_call_mfa(IsSec, create_or_replace_index
                , [SKey, TableName, IndexDefinitions]);
create_index(_SKey, IndexType, TableName, IndexDefinitions, _IsSec) ->
    if not ((IndexType =:= {}) orelse (IndexType =:= bitmap) orelse
            (IndexType =:= keylist) orelse (IndexType =:= hashmap)) ->
           ?ClientError({"Bad index type",IndexType});
       true -> ok
    end,
    if not is_list(IndexDefinitions) ->
           ?ClientError({"Bad index definition",IndexDefinitions});
       true -> ok
    end,
    ?ClientError({"Unknown index definition error", {IndexType, TableName, IndexDefinitions}}).

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
    ?imem_test_setup().

teardown(_) -> 
    catch imem_meta:drop_table(key_test),
    catch imem_meta:drop_table(truncate_test),
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
        ClEr = 'ClientError',
        % SeEx = 'SecurityException',
        ?Info("---TEST--- ~p ----Security ~p~n", [?MODULE, IsSec]),

        ?Info("schema ~p~n", [imem_meta:schema()]),
        ?Info("data nodes ~p~n", [imem_meta:data_nodes()]),
        ?assertEqual(true, is_atom(imem_meta:schema())),
        ?assertEqual(true, lists:member({imem_meta:schema(),node()}, imem_meta:data_nodes())),

        SKey=?imem_test_admin_login(),

        % Creating and loading some data into index_test table
        catch imem_sql:exec(SKey, "drop table index_test;", 0, imem, IsSec),
        ?assertEqual(ok
                     , imem_sql:exec(SKey
                                     , "create table index_test (col1 integer, col2 binstr not null);"
                                     , 0, imem, IsSec)),
        [Meta] = if_call_mfa(IsSec, read, [SKey, ddTable, {imem,index_test}]),
        ?Info("Meta table~n~p~n", [Meta]),
        ?assertEqual(0, if_call_mfa(IsSec, table_size, [SKey, index_test])),
        TableData =
        [
         <<"">>
         , <<"">>
         , <<"">>
         , <<"">>
         , <<"">>
        ],
        [if_call_mfa(IsSec, write,[SKey, index_test, {index_test, Id, Data}])
         || {Id, Data} <- lists:zip(lists:seq(1,length(TableData)), TableData)],

        ?assertEqual(length(TableData), if_call_mfa(IsSec, table_size, [SKey, index_test])),

        ?assertEqual(ok, imem_sql:exec(SKey, "drop table index_test;", 0, imem, IsSec))
    catch
        Class:Reason ->  ?Info("Exception ~p:~p~n~p~n", [Class, Reason, erlang:get_stacktrace()]),
        ?assert( true == "all tests completed")
    end,
    ok. 

-endif.
