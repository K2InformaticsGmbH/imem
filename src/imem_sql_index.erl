-module(imem_sql_index).

-include("imem_seco.hrl").

-export([ exec/5
        ]).

% Functions applied with Common Test
-export([ if_call_mfa/3
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
    Pred = fun(X) -> (atom_to_binary(X,utf8) == Index) end,
    case lists:filter(Pred,imem_meta:table_columns(Table)) of
        [] ->       if_call_mfa(IsSec, 'drop_index', [SKey,Table,Index]);   % Index = external index name
        [Field] ->  if_call_mfa(IsSec, 'drop_index', [SKey,Table,Field])    % Field = internal index atom
    end;    

exec(SKey, {'create index',{},{},TableName,[],{},{}} = _ParseTree, _Stmt, _Opts, IsSec) ->
    % ?LogDebug("Re-Create external Index Parse Tree~n~p~n", [_ParseTree]),
    {TableSchema, Tbl} = imem_sql_expr:binstr_to_qname2(TableName),
    MySchema = imem_meta:schema(),
    MySchemaName = ?atom_to_binary(MySchema),
    Table = case TableSchema of
        MySchemaName -> list_to_existing_atom(binary_to_list(Tbl));
        undefined ->    list_to_existing_atom(binary_to_list(Tbl));
        _ ->            ?ClientError({"Cannot re-create index on foreign schema table", TableSchema})
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
    if_call_mfa(IsSec, create_or_replace_index, [SKey, Table, ExistingIndexDefs]);
exec(SKey, {'create index', {}, IndexName, TableName, [], {}, {}} = _ParseTree, _Stmt, _Opts, IsSec) ->
    % ?LogDebug("Create internal (MNESIA) Index Parse Tree~n~p~n", [_ParseTree]),
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
    Pred = fun(X) -> (atom_to_binary(X,utf8) == Index) end,
    case lists:filter(Pred,imem_meta:table_columns(Table)) of
        [] ->       ?ClientError({"Column does not exist in table",{Index,Tbl}});
        [Field] ->  if_call_mfa(IsSec, 'create_index', [SKey,Table,Field])
    end;
exec(SKey, {'create index', IndexType, IndexName, TableName
            , IndexDefn, NormWithFun, FilterWithFun} = _ParseTree
            , _Stmt, _Opts, IsSec) ->
    % ?LogDebug("Create Index Parse Tree~n~p~n", [_ParseTree]),
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
