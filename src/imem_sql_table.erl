-module(imem_sql_table).

-include("imem_seco.hrl").

-export([ exec/5
        ]).

% Functions applied with Common Test
-export([ if_call_mfa/3
        ]).

exec(_SKey, {'drop table', {tables, []}, _Exists, _RestrictCascade}, _Stmt, _Opts, _IsSec) ->
    exec(_SKey, {'drop table', {tables, []}, _Exists, _RestrictCascade, []}, _Stmt, _Opts, _IsSec);                 % old parser compatibility
exec(_SKey, {'drop table', {tables, Tables}, _Exists, _RestrictCascade}, _Stmt, _Opts, _IsSec) -> 
    exec(_SKey, {'drop table', {tables, Tables}, _Exists, _RestrictCascade, []}, _Stmt, _Opts, _IsSec);             % old parser compatibility

exec(_SKey, {'drop table', {tables, []}, _Exists, _RestrictCascade, _TableTypeOpts}, _Stmt, _Opts, _IsSec) -> ok;   % typed table drops
exec(SKey, {'drop table', {tables, [TableName|Tables]}, Exists, RestrictCascade, TableTypeOpts}=_ParseTree, Stmt, Opts, IsSec) ->
    % ?LogDebug("Drop Table ParseTree~n~p", [_ParseTree]),
    QName = imem_meta:qualified_table_name(TableName), 
    % ?LogDebug("Drop Table QName~n~p~n", [QName]),
    case TableTypeOpts of
        [] ->   if_call_mfa(IsSec, 'drop_table', [SKey,QName]);
        _ ->    if_call_mfa(IsSec, 'drop_table', [SKey,QName,TableTypeOpts])
    end,
    exec(SKey, {'drop table', {tables, Tables}, Exists, RestrictCascade, TableTypeOpts}, Stmt, Opts, IsSec);
exec(SKey, {'truncate table', TableName, {}, {}}=_ParseTree, _Stmt, _Opts, IsSec) ->
    % ?Info("Parse Tree ~p~n", [_ParseTree]),
    if_call_mfa(IsSec, 'truncate_table', [SKey, imem_meta:qualified_table_name(TableName)]);
exec(SKey, {'create table', TableName, Columns, TOpts}=_ParseTree, _Stmt, _Opts, IsSec) ->
    % ?LogDebug("Parse Tree ~p", [_ParseTree]),
    % ?LogDebug("TOpts ~p", [TOpts]),
    create_table(SKey, imem_sql_expr:binstr_to_qname2(TableName), TOpts, Columns, IsSec, []).

create_table(SKey, Table, TOpts, [], IsSec, ColMap) ->
    % ?LogDebug("create_table ~p ~p", [Table,TOpts]),
    if_call_mfa(IsSec, 'create_table', [SKey, Table, lists:reverse(ColMap), TOpts]);        %% {Schema,Table} not converted to atom yet !!!
create_table(SKey, Table, TOpts, [{Name, Type, COpts}|Columns], IsSec, ColMap) when is_binary(Name) ->
    % ?LogDebug("Create table column ~p of type ~p ~p~n",[Name,Type,COpts]),
    {T,L,P} = case Type of
        B when B==<<"decimal">>;B==<<"number">> ->          {decimal,38,0};
        {B,SLen} when B==<<"decimal">>;B==<<"number">> ->   {decimal,binary_to_integer(SLen),0};
        B when is_binary(B) ->      
            case imem_datatype:imem_type(imem_datatype:io_to_term(imem_datatype:strip_squotes(binary_to_list(B)))) of
                Bterm when is_binary(Bterm) ->  ?ClientError({"Unknown datatype",Bterm});
                Term ->                         {Term,undefined,undefined}
            end;
        {<<"float">>,SPrec} ->      {float,undefined,binary_to_integer(SPrec)};
        {<<"timestamp">>,SPrec} ->  {timestamp,undefined,binary_to_integer(SPrec)};
        {Typ,SLen} ->               {imem_datatype:imem_type(binary_to_existing_atom(Typ, utf8)),binary_to_integer(SLen),undefined};
        {Typ,SLen,SPrec} ->         {imem_datatype:imem_type(binary_to_existing_atom(Typ, utf8)),binary_to_integer(SLen),binary_to_integer(SPrec)};
        Else ->                     ?SystemException({"Unexpected parse tree structure",Else})
    end,
    {Default,Opts} = case lists:keyfind(default, 1, COpts) of
        false ->
            case {lists:member('not null', COpts),T} of
                {true,_} ->         {?nav,lists:delete('not null',COpts)};
                {false,binary} ->   {<<>>,COpts};
                {false,binstr} ->   {<<>>,COpts};
                {false,string} ->   {[],COpts};
                {false,list} ->     {[],COpts};
                {false,tuple} ->    {{},COpts};
                {false,map} ->      {#{},COpts};
                {false,_} ->        {undefined,COpts}
            end;
        {_,Bin} ->  
            Str = binary_to_list(Bin),
            FT = case re:run(Str, "fun\\((.*)\\)[ ]*\->(.*)end.", [global, {capture, [1,2], list}]) of
                {match,[[_Params,_Body]]} ->
                    % ?LogDebug("Str ~p~n", [Str]),
                    % ?LogDebug("Params ~p~n", [_Params]),
                    % ?LogDebug("Body ~p~n", [Body]),
                    %  try 
                    %     imem_datatype:io_to_term(Body)
                    % catch _:_ -> 
                        try 
                            imem_datatype:io_to_fun(Str,undefined),
                            Bin
                        catch _:Reason -> 
                            ?ClientError({"Bad default fun",Reason})
                        end;
                    % end;
                nomatch ->  
                    imem_datatype:io_to_term(Str)
            end,
            {FT,lists:keydelete(default, 1, COpts)}
    end,
    C = #ddColumn{  name=Name           %% not converted to atom yet !!!
                  , type=T
                  , len=L
                  , prec=P
                  , default=Default
                  , opts = Opts
                  },
    create_table(SKey, Table, TOpts, Columns, IsSec, [C|ColMap]).

%% --Interface functions  (calling imem_if for now, not exported) ---------

if_call_mfa(IsSec,Fun,Args) ->
    case IsSec of
        true -> apply(imem_sec, Fun, Args);
        _ ->    apply(imem_meta, Fun, lists:nthtail(1, Args))
    end.
