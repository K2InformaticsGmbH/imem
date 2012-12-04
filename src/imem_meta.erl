-module(imem_meta).

-define(META_TABLES,[ddTable]).
-define(META_FIELDS,[user,schema,node,sysdate,systimestamp]).

-include_lib("eunit/include/eunit.hrl").

-include("imem_meta.hrl").

-behavior(gen_server).

-record(state, {
        }).

-export([ start_link/1
        ]).

% gen_server interface (monitoring calling processes)

% gen_server behavior callbacks
-export([ init/1
        , handle_call/3
        , handle_cast/2
        , handle_info/2
        , terminate/2
        , code_change/3
        , format_status/2
        ]).


-export([ drop_meta_tables/0
        ]).

-export([ schema/0
        , schema/1
        , data_nodes/0
        , all_tables/0
        , table_type/1
        , table_columns/1
        , table_size/1
        , check_table/1
        , check_table_record/2
        , check_table_columns/2
        , system_table/1
        , meta_field/1
        , meta_field_info/1
        , meta_field_value/1
        , column_infos/1
        , column_info_items/2
        , subscribe/1
        , unsubscribe/1
        ]).

-export([ add_attribute/2
        , update_opts/2
        , localTimeToSysDate/1
        , nowToSysTimeStamp/1
        , value_cast/8
        ]).

-export([ create_table/4
        , create_table/3
		, drop_table/1
        , read/1            %% read whole table, only use for small tables 
        , read/2            %% read by key
        , select/1          %% contiunation block read
        , select/2          %% select without limit, only use for small result sets
        , select/3          %% select with limit
        , insert/2    
        , write/2           %% write single key
        , delete/2          %% delete rows by key
        , truncate/1        %% truncate table
        ]).

-export([ update_prepare/3          %% stateless creation of update plan from change list
        , update_cursor_prepare/2   %% take change list and generate update plan (stored in state)
        , update_cursor_execute/2   %% take update plan from state and execute it (fetch aborted first)
        , fetch_recs_async/2        %% ToDo: implement proper return of RowFun(), match conditions and joins
        , fetch_close/1
        , exec/3
        , close/1
        ]).

-export([ fetch_start/4
        , update_tables/2  
        ]).


-export([ transaction/1
        , transaction/2
        , transaction/3
        , return_atomic_list/1
        , return_atomic_ok/1
        , return_atomic/1
        ]).

-export([ pretty_type/1
        , string_to_date/1
        , string_to_decimal/3
        , string_to_double/2
        , string_to_ebinary/2
        , string_to_edatetime/1
        , string_to_eipaddr/2
        , string_to_elist/2
        , string_to_enum/2
        , string_to_eterm/1
        , string_to_etimestamp/1
        , string_to_etuple/2
        , string_to_float/2
        , string_to_fun/2
        , string_to_integer/3
        , string_to_number/3
        , string_to_set/2
        , string_to_time/1
        , string_to_timestamp/2
        , string_to_year/1
        ]).


start_link(Params) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, Params, []).

init(_Args) ->
    io:format(user, "~p starting...~n", [?MODULE]),
    Result = try
        catch create_table(ddTable, {record_info(fields, ddTable),?ddTable, #ddTable{}}, [], system),
        check_table(ddTable),
        check_table_record(ddTable, {record_info(fields, ddTable), ?ddTable, #ddTable{}}),

        catch create_table(dual, {record_info(fields, dual),?dual, #dual{}}, [], system),
        check_table(dual),
        check_table_columns(dual, {record_info(fields, dual),?dual, #dual{}}),

        io:format(user, "~p started!~n", [?MODULE]),
        {ok,#state{}}
    catch
        Class:Reason -> io:format(user, "~p failed with ~p:~p~n", [?MODULE,Class,Reason]),
                        {stop, "Insufficient/invalid resources for start"}
    end,
    Result.

handle_call(_Request, _From, State) ->
    {reply, ok, State}.

% handle_cast({stop, Reason}, State) ->
%     {stop,{shutdown,Reason},State};
handle_cast(_Request, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reson, _State) -> ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

format_status(_Opt, [_PDict, _State]) -> ok.


%% ------ META implementation -------------------------------------------------------

system_table(Table) when is_atom(Table) ->
    case lists:member(Table,?META_TABLES) of
        true ->     true;
        false ->    imem_if:system_table(Table) 
    end;
system_table({_,Table}) when is_atom(Table) ->
    case lists:member(Table,?META_TABLES) of
        true ->     true;
        false ->    imem_if:system_table(Table) 
    end.

check_table(Table) when is_atom(Table) ->
    imem_if:table_size(Table).

check_table_record(Table, {Names, Types, DefaultRecord}) when is_atom(Table) ->
    [Table|Defaults] = tuple_to_list(DefaultRecord),
    ColumnInfos = column_infos(Names, Types, Defaults),
    TableColumns = table_columns(Table),    
    if
        Names =:= TableColumns ->
            case imem_if:read(ddTable,{schema(), Table}) of
                [] ->   ?SystemException({"Missing table metadata",Table}); 
                [#ddTable{columns=ColumnInfos}] ->
                    CINames = column_info_items(ColumnInfos, name),
                    CITypes = column_info_items(ColumnInfos, type),
                    CIDefaults = column_info_items(ColumnInfos, default),
                    if
                        (CINames =:= Names) andalso (CITypes =:= Types) andalso (CIDefaults =:= Defaults) ->  
                            ok;
                        true ->                 
                            ?SystemException({"Record does not match table metadata",Table})
                    end   
            end;  
        true ->                 
            ?SystemException({"Record field names do not match table structure",Table})             
    end;
check_table_record(Table, ColumnNames) when is_atom(Table) ->
    TableColumns = table_columns(Table),    
    if
        ColumnNames =:= TableColumns ->
            case imem_if:read(ddTable,{schema(), Table}) of
                [] ->   ?SystemException({"Missing table metadata",Table}); 
                [#ddTable{columns=ColumnInfo}] ->
                    CINames = column_info_items(ColumnInfo, name),
                    if
                        CINames =:= ColumnNames ->  
                            ok;
                        true ->                 
                            ?SystemException({"Record field names do not match table metadata",Table})
                    end          
            end;  
        true ->                 
            ?SystemException({"Record field names do not match table structure",Table})             
    end.

check_table_columns(Table, {Names, Types, DefaultRecord}) when is_atom(Table) ->
    [Table|Defaults] = tuple_to_list(DefaultRecord),
    ColumnInfo = column_infos(Names, Types, Defaults),
    TableColumns = table_columns(Table),    
    MetaInfo = column_infos(Table),    
    if
        Names /= TableColumns ->
            ?SystemException({"Column names do not match table structure",Table});             
        ColumnInfo /= MetaInfo ->
            ?SystemException({"Column info does not match table metadata",Table});
        true ->     ok
    end;
check_table_columns(Table, ColumnInfo) when is_atom(Table) ->
    ColumnNames = column_info_items(ColumnInfo, name),
    TableColumns = table_columns(Table),
    MetaInfo = column_infos(Table),    
    if
        ColumnNames /= TableColumns ->
            ?SystemException({"Column info does not match table structure",Table}) ;
        ColumnInfo /= MetaInfo ->
            ?SystemException({"Column info does not match table metadata",Table});
        true ->     ok                           
    end.

drop_meta_tables() ->
    drop_table(ddTable).     

meta_field(Name) ->
    lists:member(Name,?META_FIELDS).

meta_field_info(sysdate) ->
    #ddColumn{name=sysdate, type='date', length=7, precision=0};
meta_field_info(systimestamp) ->
    #ddColumn{name=systimestamp, type='timestamp', length=20, precision=0};
meta_field_info(schema) ->
    #ddColumn{name=schema, type='eatom', length=10, precision=0};
meta_field_info(node) ->
    #ddColumn{name=node, type='eatom', length=30, precision=0};
meta_field_info(user) ->
    #ddColumn{name=user, type='ebinstr', length=20, precision=0};
meta_field_info(localtime) ->
    #ddColumn{name=localtime, type='edatetime', length=20, precision=0};
meta_field_info(now) ->
    #ddColumn{name=now, type='etimestamp', length=20, precision=0};
meta_field_info(Name) ->
    ?ClientError({"Unknown meta column",Name}). 

meta_field_value(user) ->
    <<"unknown">>; 
meta_field_value(Name) ->
    imem_if:meta_field_value(Name). 

column_info_items(Info, name) ->
    [C#ddColumn.name || C <- Info];
column_info_items(Info, type) ->
    [C#ddColumn.type || C <- Info];
column_info_items(Info, default) ->
    [C#ddColumn.default || C <- Info];
column_info_items(Info, length) ->
    [C#ddColumn.length || C <- Info];
column_info_items(Info, precision) ->
    [C#ddColumn.precision || C <- Info];
column_info_items(Info, opts) ->
    [C#ddColumn.opts || C <- Info];
column_info_items(_Info, Item) ->
    ?ClientError({"Invalid item",Item}).

column_names(Infos)->
    [list_to_atom(lists:flatten(io_lib:format("~p", [N]))) || #ddColumn{name=N} <- Infos].

column_infos(Table) when is_atom(Table)->
            case imem_if:read(ddTable,{schema(), Table}) of
                [] ->                       ?SystemException({"Missing table metadata",Table}); 
                [#ddTable{columns=CI}] ->   CI
            end;  
column_infos(Names) when is_list(Names)->
    [#ddColumn{name=list_to_atom(lists:flatten(io_lib:format("~p", [N])))} || N <- Names].

column_infos(Names, Types, Defaults)->
    [#ddColumn{name=list_to_atom(lists:flatten(io_lib:format("~p", [N]))), type=T, default=D} || {N,T,D} <- lists:zip3(Names, Types, Defaults)].

create_table(Table, {Names, Types, DefaultRecord}, Opts, Owner) ->
    [Table|Defaults] = tuple_to_list(DefaultRecord),
    ColumnInfos = column_infos(Names, Types, Defaults),
    imem_if:create_table(Table, ColumnInfos, Opts),
    imem_if:write(ddTable, #ddTable{qname={schema(),Table}, columns=ColumnInfos, opts=Opts, owner=Owner});
create_table(Table, [#ddColumn{}|_]=ColumnInfos, Opts, Owner) ->
    ColumnNames = column_names(ColumnInfos),
    imem_if:create_table(Table, ColumnNames, Opts),
    imem_if:write(ddTable, #ddTable{qname={schema(),Table}, columns=ColumnInfos, opts=Opts, owner=Owner});
create_table(Table, ColumnNames, Opts, Owner) ->
    ColumnInfos = column_infos(ColumnNames), 
    imem_if:create_table(Table, ColumnNames, Opts),
    imem_if:write(ddTable, #ddTable{qname={schema(),Table}, columns=ColumnInfos, opts=Opts, owner=Owner}).

create_table(Table, {Names, Types, DefaultRecord}, Opts) ->
    [Table|Defaults] = tuple_to_list(DefaultRecord),
    ColumnInfos = column_infos(Names, Types, Defaults),
    imem_if:create_table(Table, ColumnInfos, Opts),
    imem_if:write(ddTable, #ddTable{qname={schema(),Table}, columns=ColumnInfos, opts=Opts});
create_table(Table, [#ddColumn{}|_]=ColumnInfos, Opts) ->
    ColumnNames = column_names(ColumnInfos),
    imem_if:create_table(Table, ColumnNames, Opts),
    imem_if:write(ddTable, #ddTable{qname={schema(),Table}, columns=ColumnInfos, opts=Opts});
create_table(Table, ColumnNames, Opts) ->
    ColumnInfos = column_infos(ColumnNames), 
    imem_if:create_table(Table, ColumnNames, Opts),
    imem_if:write(ddTable, #ddTable{qname={schema(),Table}, columns=ColumnInfos, opts=Opts}).

drop_table({_Schema,Table}) -> 
    drop_table(Table);          %% ToDo: may depend on schema 
drop_table(ddTable) -> 
    imem_if:drop_table(ddTable);
drop_table(Table) -> 
    imem_if:drop_table(Table),
    imem_if:delete(ddTable, {schema(),Table}).

%% one to one from imme_if -------------- HELPER FUNCTIONS ------

schema() ->
    imem_if:schema().

schema(Node) ->
    imem_if:schema(Node).

add_attribute(A, Opts) -> 
    imem_if:add_attribute(A, Opts).

update_opts(T, Opts) ->
    imem_if:update_opts(T, Opts).

localTimeToSysDate(LTime) -> 
    imem_if:localTimeToSysDate(LTime).

nowToSysTimeStamp(Now) -> 
    imem_if:nowToSysTimeStamp(Now).


%% imem_if but security context added --- META INFORMATION ------

data_nodes() ->
    imem_if:data_nodes().

all_tables() ->
    imem_if:all_tables().

table_type({_Schema,Table}) ->
    table_type(Table);          %% ToDo: may depend on schema
table_type(dba_tables) ->
    table_type(ddTable);
table_type(all_tables) ->
    table_type(ddTable);
table_type(user_tables) ->
    table_type(ddTable);
table_type(Table) when is_atom(Table) ->
    imem_if:table_type(Table).

table_columns({_Schema,Table}) ->
    table_columns(Table);       %% ToDo: may depend on schema
table_columns(dba_tables) ->
    table_columns(ddTable);
table_columns(all_tables) ->
    table_columns(ddTable);
table_columns(user_tables) ->
    table_columns(ddTable);
table_columns(Table) ->
    imem_if:table_columns(Table).

table_size({_Schema,Table}) ->
    table_size(Table);          %% ToDo: may depend on schema
table_size(Table) ->
    imem_if:table_size(Table).

exec(Statement, BlockSize, Schema) ->
    imem_sql:exec(none, Statement, BlockSize, Schema, false).   

fetch_recs_async(Pid, Sock) ->
    imem_statement:fetch_recs_async(none, Pid, Sock, false).

fetch_close(Pid) ->
    imem_statement:fetch_close(none, Pid, false).

update_prepare(Tables, ColMap, ChangeList) ->
    imem_statement:update_prepare(false, none, Tables, ColMap, ChangeList).

update_cursor_prepare(Pid, ChangeList) ->
    imem_statement:update_cursor_prepare(none, Pid, false, ChangeList).

update_cursor_execute(Pid, Lock) ->
    imem_statement:update_cursor_execute(none, Pid, false, Lock).

fetch_start(Pid, {_Schema,Table}, MatchSpec, BlockSize) ->
    fetch_start(Pid, Table, MatchSpec, BlockSize);          %% ToDo: may depend on schema
fetch_start(Pid, dba_tables, MatchSpec, BlockSize) ->
    fetch_start(Pid, ddTable, MatchSpec, BlockSize);
fetch_start(Pid, all_tables, MatchSpec, BlockSize) ->
    fetch_start(Pid, ddTable, MatchSpec, BlockSize);
fetch_start(Pid, user_tables, MatchSpec, BlockSize) ->
    fetch_start(Pid, ddTable, MatchSpec, BlockSize);
fetch_start(Pid, Table, MatchSpec, BlockSize) ->
    imem_if:fetch_start(Pid, Table, MatchSpec, BlockSize).

close(Pid) ->
    imem_statement:close(none, Pid).

read({_Schema,Table}) -> 
    read(Table);            %% ToDo: may depend on schema 
read(dba_tables) ->
    read(ddTable);
read(all_tables) -> 
    read(ddTable);
read(user_tables) -> 
    read(ddTable);
read(Table) -> 
    imem_if:read(Table).

read({_Schema,Table}, Key) -> 
    read(Table, Key); 
read(dba_tables, Key) ->
    read(ddTable, Key);
read(all_tables, Key) -> 
    read(ddTable, Key);
read(user_tables, Key) -> 
    read(ddTable, Key);
read(Table, Key) -> 
    imem_if:read(Table, Key).

select({_Schema,Table}, MatchSpec) ->
    select(Table, MatchSpec);           %% ToDo: may depend on schema
select(dba_tables, MatchSpec) ->
    select(ddTable, MatchSpec);
select(all_tables, MatchSpec) ->
    select(ddTable, MatchSpec);
select(user_tables, MatchSpec) ->
    select(ddTable, MatchSpec);
select(Table, MatchSpec) ->
    imem_if:select(Table, MatchSpec).

select(Table, MatchSpec, 0) ->
    select(Table, MatchSpec);
select({_Schema,Table}, MatchSpec, Limit) ->
    select(Table, MatchSpec, Limit);        %% ToDo: may depend on schema
select(dba_tables, MatchSpec, Limit) ->
    select(ddTable, MatchSpec, Limit);
select(all_tables, MatchSpec, Limit) ->
    select(ddTable, MatchSpec, Limit);
select(user_tables, MatchSpec, Limit) ->
    select(ddTable, MatchSpec, Limit);
select(Table, MatchSpec, Limit) ->
    imem_if:select(Table, MatchSpec, Limit).

select(Continuation) ->
    imem_if:select(Continuation).

write({_Schema,Table}, Record) -> 
    write(Table, Record);           %% ToDo: may depend on schema 
write(Table, Record) -> 
    imem_if:write(Table, Record).

insert({_Schema,Table}, Row) ->
    insert(Table, Row);             %% ToDo: may depend on schema
insert(Table, Row) ->
    imem_if:insert(Table, Row).

delete({_Schema,Table}, Key) ->
    delete(Table, Key);             %% ToDo: may depend on schema
delete(Table, Key) ->
    imem_if:delete(Table, Key).

truncate({_Schema,Table}) ->
    truncate(Table);                %% ToDo: may depend on schema
truncate(Table) ->
    imem_if:truncate(Table).

subscribe(EventCategory) ->
    imem_if:subscribe(EventCategory).

unsubscribe(EventCategory) ->
    imem_if:unsubscribe(EventCategory).

update_tables(UpdatePlan, Lock) ->
    update_tables(schema(), UpdatePlan, Lock, []).

update_tables(_MySchema, [], Lock, Acc) ->
    imem_if:update_tables(Acc, Lock);  
update_tables(MySchema, [UEntry|UPlan], Lock, Acc) ->
    update_tables(MySchema, UPlan, Lock, [update_table_name(MySchema, UEntry)|Acc]).

update_table_name(MySchema,[{MySchema,dba_tables,Type}|T]) ->
    [{ddTable,Type}|T];
update_table_name(MySchema,[{MySchema,all_tables,Type}|T]) ->
    [{ddTable,Type}|T];
update_table_name(MySchema,[{MySchema,user_tables,Type}|T]) ->
    [{ddTable,Type}|T];
update_table_name(MySchema,[{MySchema,Table,Type}|T]) ->
    [{Table,Type}|T].

transaction(Function) ->
    imem_if:transaction(Function).

transaction(Function, Args) ->
    imem_if:transaction(Function, Args).

transaction(Function, Args, Retries) ->
    imem_if:transaction(Function, Args, Retries).

return_atomic_list(Result) ->
    imem_if:return_atomic_list(Result). 

return_atomic_ok(Result) -> 
    imem_if:return_atomic_ok(Result).

return_atomic(Result) -> 
    imem_if:return_atomic(Result).

%% ----- DATA TYPES ---------------------------------------------

value_cast(_Item,Old,_Type,_Len,_Prec,_Def,true,_) -> Old;
value_cast(_Item,Old,_Type,_Len,_Prec,_Def,_RO, Old) -> Old;
value_cast(_Item,_Old,_Type,_Len,_Prec,Def,_RO, Def) -> Def;
value_cast(_Item,_Old,_Type,_Len,_Prec,[],_RO, "[]") -> [];
value_cast(_Item,[],_Type,_Len,_Prec,_Def,_RO, "[]") -> [];
value_cast(Item,Old,Type,Len,Prec,Def,_RO,Val) when is_function(Def,0) ->
    value_cast(Item,Old,Type,Len,Prec,Def(),_RO,Val);
value_cast(Item,Old,Type,Len,Prec,Def,_RO,Val) when is_list(Val) ->
    try
        IsString = io_lib:printable_unicode_list(Val),
        if 
            IsString ->
                DefAsStr = lists:flatten(io_lib:format("~p", [Def])),
                OldAsStr = lists:flatten(io_lib:format("~p", [Old])),
                if 
                    (DefAsStr == Val) ->    Def;
                    (OldAsStr == Val) ->    Old;
                    (Type == bigint) ->     string_to_integer(Val,Len,Prec);
                    (Type == blob) ->       string_to_ebinary(Val,Len);
                    (Type == character) ->  string_to_list(Val,map(Len,0,1));
                    (Type == decimal) ->    string_to_decimal(Val,Len,Prec);
                    (Type == date) ->       string_to_date(Val);
                    (Type == double) ->     string_to_double(Val,Prec);
                    (Type == eatom) ->      list_to_atom(Val);
                    (Type == ebinary) ->    string_to_ebinary(Val,Len);
                    (Type == ebinstr) ->    list_to_binary(Val);
                    (Type == edatetime) ->  string_to_edatetime(Val);
                    (Type == efun) ->       string_to_fun(Val,Len);
                    (Type == eipaddr) ->    string_to_eipaddr(Val,Len);
                    (Type == elist) ->      string_to_elist(Val,Len);
                    (Type == enum) ->       string_to_enum(Val,Len);
                    (Type == epid) ->       list_to_pid(Val);
                    (Type == eref) ->       Old;    %% cannot convert back
                    (Type == estring) ->    string_to_list(Val,Len);
                    (Type == etimestamp) -> string_to_etimestamp(Val); 
                    (Type == etuple) ->     string_to_etuple(Val,Len);
                    (Type == float) ->      string_to_float(Val,Prec);
                    (Type == int) ->        string_to_integer(Val,Len,Prec);
                    (Type == integer) ->    string_to_integer(Val,Len,Prec);
                    (Type == longblob) ->   string_to_ebinary(Val,Len);
                    (Type == longtext) ->   string_to_list(Val,map(Len,0,4294967295));
                    (Type == mediumint) ->  string_to_integer(Val,Len,Prec);
                    (Type == mediumtext) -> string_to_list(Val,map(Len,0,16777215));
                    (Type == number) ->     string_to_number(Val,Len,Prec);
                    (Type == numeric) ->    string_to_number(Val,Len,Prec);
                    (Type == set) ->        string_to_set(Val,Len);
                    (Type == smallint) ->   string_to_integer(Val,Len,Prec);
                    (Type == text) ->       string_to_list(Val,map(Len,0,65535));
                    (Type == time) ->       string_to_time(Val);
                    (Type == timestamp) ->  string_to_timestamp(Val,Prec);
                    (Type == tinyint) ->    string_to_integer(Val,Len,Prec);
                    (Type == tinytext) ->   string_to_list(Val,map(Len,0,255));
                    (Type == varchar) ->    string_to_list(Val,map(Len,0,255));
                    (Type == varchar2) ->   string_to_list(Val,map(Len,0,4000));
                    (Type == year) ->       string_to_year(Val);                    
                    true ->                 string_to_eterm(Val)   
                end;
            true -> Val
        end
    catch
        _:{'UnimplementedException',_} ->       ?ClientError({"Unimplemented data type conversion",{Item,{Type,Val}}});
        _:{'ClientError', {Text, Reason}} ->    ?ClientError({Text, {Item,Reason}});
        _:_ ->                                  ?ClientError({"Data conversion format error",{Item,{pretty_type(Type),Val}}})
    end;
value_cast(_Item,_Old,_Type,_Len,_Prec,_Def,_RO,Val) -> Val.    

map(Val,From,To) ->
    if 
        Val == From ->  To;
        true ->         Val
    end.

pretty_type(etuple) -> eTuple;
pretty_type(efun) -> eFun;
pretty_type(ebinary) -> eBinary;
pretty_type(elist) -> eList;
pretty_type(eipaddr) -> eIpaddr;
pretty_type(etimestamp) -> eTimestamp;
pretty_type(edatetime) -> eDatetime;
pretty_type(eatom) -> eAtom;
pretty_type(ebinstr) -> eBinstr;
pretty_type(eref) -> eRef;
pretty_type(epid) -> ePid;
pretty_type(estring) -> eString;
pretty_type(eterm) -> eTerm;
pretty_type(Type) -> Type.

string_to_list(Val,Len) ->
    if 
        Len == 0 ->             Val;
        length(Val) > Len ->    ?ClientError({"Data conversion format error",{string,Len,Val}});
        true ->                 Val
    end.

string_to_integer(Val,Len,Prec) ->
    Value = case string_to_eterm(Val) of
        V when is_integer(V) -> V;
        V when is_float(V) ->   erlang:round(V);
        _ ->                    ?ClientError({"Data conversion format error",{integer,Len,Prec,Val}})
    end,
    Result = if 
        Len == 0 andalso Prec == 0 ->   Value;
        Prec <  0 ->                    erlang:round(erlang:round(math:pow(10, Prec) * Value) * math:pow(10,-Prec));
        true ->                         Value
    end,
    RLen = length(integer_to_list(Result)),
    if 
        Len == 0 ->     Result;
        RLen > Len ->   ?ClientError({"Data conversion format error",{integer,Len,Prec,Val}});
        true ->         Result
    end.

string_to_float(Val,Prec) ->
    Value = case string_to_eterm(Val) of
        V when is_float(V) ->   V;
        V when is_integer(V) -> float(V);
        _ ->                    ?ClientError({"Data conversion format error",{float,Prec,Val}})
    end,
    if 
        Prec == 0 ->    Value;
        true ->         erlang:round(math:pow(10, Prec) * Value) * math:pow(10,-Prec)
    end.

string_to_set(_Val,_Len) -> ?UnimplementedException({}).
string_to_enum(_Val,_Len) -> ?UnimplementedException({}).
string_to_double(_Val,_Prec) -> ?UnimplementedException({}).  %% not in erlang:math ??
string_to_ebinary(_Val,_Len) -> ?UnimplementedException({}).

string_to_date(Val) -> 
    string_to_edatetime(Val).

string_to_time("systime") ->
    string_to_time("localtime");
string_to_time("sysdate") ->
    string_to_time("localtime");
string_to_time("now") ->
    string_to_time("localtime");
string_to_time("localtime") -> 
    {_,Time} = string_to_edatetime("localtime"),
    Time;
string_to_time(Val) -> 
    try
        parse_time(Val)
    catch
        _:_ ->  ?ClientError({"Data conversion format error",{eTime,Val}})
    end.

string_to_timestamp("systime",Prec) ->
    string_to_timestamp("now",Prec);
string_to_timestamp("sysdate",Prec) ->
    string_to_timestamp("now",Prec);
string_to_timestamp("now",Prec) ->
    {Megas,Secs,Micros} = erlang:now(),    
    {Megas,Secs,erlang:round(erlang:round(math:pow(10, Prec-6) * Micros) * erlang:round(math:pow(10,6-Prec)))};  
string_to_timestamp(Val,6) ->
    string_to_etimestamp(Val); 
string_to_timestamp(Val,0) ->
    {Megas,Secs,_} = string_to_etimestamp(Val),
    {Megas,Secs,0};
string_to_timestamp(Val,Prec) when Prec > 0 ->
    {Megas,Secs,Micros} = string_to_etimestamp(Val),
    {Megas,Secs,erlang:round(erlang:round(math:pow(10, Prec-6) * Micros) * erlang:round(math:pow(10,6-Prec)))};
string_to_timestamp(Val,Prec) when Prec < 0 ->
    {Megas,Secs,_} = string_to_etimestamp(Val),
    {Megas,erlang:round(erlang:round(math:pow(10, -Prec) * Secs) * erlang:round(math:pow(10,Prec))),0}.  

string_to_edatetime("today") ->
    {Date,_} = string_to_edatetime("localtime"),
    {Date,{0,0,0}}; 
string_to_edatetime("systime") ->
    string_to_edatetime("localtime"); 
string_to_edatetime("sysdate") ->
    string_to_edatetime("localtime"); 
string_to_edatetime("now") ->
    string_to_edatetime("localtime"); 
string_to_edatetime("localtime") ->
    erlang:localtime(); 
string_to_edatetime(Val) ->
    try 
        case re:run(Val,"[\/\.\-]+",[{capture,all,list}]) of
            {match,["."]} ->    
                case string:tokens(Val, " ") of
                    [Date,Time] ->              {parse_date_eu(Date),parse_time(Time)};
                    [Date] ->                   {parse_date_eu(Date),{0,0,0}}
                end;
            {match,["/"]} ->    
                case string:tokens(Val, " ") of
                    [Date,Time] ->              {parse_date_us(Date),parse_time(Time)};
                    [Date] ->                   {parse_date_us(Date),{0,0,0}}
                end;
            {match,["-"]} ->    
                case string:tokens(Val, " ") of
                    [Date,Time] ->              {parse_date_int(Date),parse_time(Time)};
                    [Date] ->                   {parse_date_int(Date),{0,0,0}}
                end;
            _ ->
                case string:tokens(Val, " ") of
                    [Date,Time] ->              {parse_date_raw(Date),parse_time(Time)};
                    [DT] when length(DT) > 8 -> {Date,Time} = lists:split(8,DT),
                                                {parse_date_raw(Date),parse_time(Time)};
                    [Date] ->                   {parse_date_raw(Date),{0,0,0}}
                end
        end
    catch
        _:_ ->  ?ClientError({"Data conversion format error",{eDatetime,Val}})
    end.    

parse_date_eu(Val) ->
    case string:tokens(Val, ".") of
        [Day,Month,Year] ->     validate_date({parse_year(Year),parse_month(Month),parse_day(Day)});
        _ ->                    ?ClientError({})
    end.    

parse_date_us(Val) ->
    case string:tokens(Val, "/") of
        [Month,Day,Year] ->     validate_date({parse_year(Year),parse_month(Month),parse_day(Day)});
        _ ->                    ?ClientError({})
    end.    

parse_date_int(Val) ->
    case string:tokens(Val, "-") of
        [Year,Month,Day] ->     validate_date({parse_year(Year),parse_month(Month),parse_day(Day)});
        _ ->                    ?ClientError({})
    end.    

parse_date_raw(Val) ->
    case length(Val) of
        8 ->    validate_date({parse_year(lists:sublist(Val,1,4)),parse_month(lists:sublist(Val,5,2)),parse_day(lists:sublist(Val,7,2))});
        6 ->    Year2 = parse_year(lists:sublist(Val,1,2)),
                Year = if 
                   Year2 < 40 ->    2000+Year2;
                   true ->          1900+Year2
                end,     
                validate_date({Year,parse_month(lists:sublist(Val,3,2)),parse_day(lists:sublist(Val,5,2))});
        _ ->    ?ClientError({})
    end.    

parse_year(Val) ->
    case length(Val) of
        4 ->    list_to_integer(Val);
        2 ->    Year2 = parse_year(lists:sublist(Val,1,2)),
                if 
                   Year2 < 40 ->    2000+Year2;
                   true ->          1900+Year2
                end;   
        _ ->    ?ClientError({})
    end.    

parse_month(Val) ->
    case length(Val) of
        1 ->                    list_to_integer(Val);
        2 ->                    list_to_integer(Val);
        _ ->                    ?ClientError({})
    end.    

parse_day(Val) ->
    case length(Val) of
        1 ->                    list_to_integer(Val);
        2 ->                    list_to_integer(Val);
        _ ->                    ?ClientError({})
    end.    

parse_time(Val) ->
    case string:tokens(Val, ":") of
        [H,M,S] ->      {parse_hour(H),parse_minute(M),parse_second(S)};
        [H,M] ->        {parse_hour(H),parse_minute(M),0};
        _ ->            
            case length(Val) of
                6 ->    {parse_hour(lists:sublist(Val,1,2)),parse_minute(lists:sublist(Val,3,2)),parse_minute(lists:sublist(Val,5,2))};
                4 ->    {parse_hour(lists:sublist(Val,1,2)),parse_minute(lists:sublist(Val,3,2)),0};
                2 ->    {parse_hour(lists:sublist(Val,1,2)),0,0};
                0 ->    {0,0,0};
                _ ->    ?ClientError({})
            end
    end.    

parse_hour(Val) ->
    H = list_to_integer(Val),
    if
        H >= 0 andalso H < 25 ->    H;
        true ->                     ?ClientError({})
    end.

parse_minute(Val) ->
    M = list_to_integer(Val),
    if
        M >= 0 andalso M < 60 ->    M;
        true ->                     ?ClientError({})
    end.

parse_second(Val) ->
    S = list_to_integer(Val),
    if
        S >= 0 andalso S < 60 ->    S;
        true ->                     ?ClientError({})
    end.

validate_date(Date) ->
    case calendar:valid_date(Date) of
        true ->     Date;
        false ->    ?ClientError({})
    end.    

string_to_year(Val) -> 
    try     
        parse_year(Val)
    catch
        _:_ ->  ?ClientError({"Data conversion format error",{year,Val}})
    end.    

string_to_etimestamp("sysdate") ->
    erlang:now();
string_to_etimestamp("now") ->
    erlang:now();
string_to_etimestamp(Val) ->
    try     
        case string:tokens(Val, ":") of
            [Megas,Secs,Micros] ->  {list_to_integer(Megas),list_to_integer(Secs),list_to_integer(Micros)};
            _ ->                    ?ClientError({})
        end
    catch
        _:_ ->  ?ClientError({"Data conversion format error",{eTimestamp,Val}})
    end.    

string_to_eipaddr(Val,Len) ->
    Result = try 
        [ip_item(Item) || Item <- string:tokens(Val, ".")]
    catch
        _:_ -> ?ClientError({"Data conversion format error",{eIpaddr,Len,Val}})
    end,
    RLen = length(lists:flatten(Result)),
    if 
        Len == 0 andalso RLen == 4 ->   list_to_tuple(Result);
        Len == 0 andalso RLen == 6 ->   list_to_tuple(Result);
        Len == 0 andalso RLen == 8 ->   list_to_tuple(Result);
        RLen /= Len ->                  ?ClientError({"Data conversion format error",{eIpaddr,Len,Val}});
        true ->                         list_to_tuple(Result)
    end.

ip_item(Val) ->
    case string_to_eterm(Val) of
        V when is_integer(V), V >= 0, V < 256  ->   V;
        _ ->                                        ?ClientError({})
    end.

string_to_number(Val,Len,Prec) ->
    string_to_decimal(Val,Len,Prec). 

string_to_decimal(Val,Len,0) ->         %% use fixed point arithmetic with implicit scaling factor
    string_to_integer(Val,Len,0);  
string_to_decimal(Val,Len,Prec) when Prec > 0 -> 
    Result = erlang:round(math:pow(10, Prec) * string_to_float(Val,0)),
    RLen = length(integer_to_list(Result)),
    if 
        Len == 0 ->     Result;
        RLen > Len ->   ?ClientError({"Data conversion format error",{decimal,Len,Prec,Val}});
        true ->         Result
    end;
string_to_decimal(Val,Len,Prec) ->
    ?ClientError({"Data conversion format error",{decimal,Len,Prec,Val}}).

string_to_elist(Val, 0) -> 
    case string_to_eterm(Val) of
        V when is_list(V) ->    V;
        _ ->                    ?ClientError({"Data conversion format error",{eList,0,Val}})
    end;
string_to_elist(Val,Len) -> 
    case string_to_eterm(Val) of
        V when is_list(V) ->
            if 
                length(V) == Len -> V;
                true ->             ?ClientError({"Data conversion format error",{eList,Len,Val}})
            end;
        _ ->
            ?ClientError({"Data conversion format error",{eTuple,Len,Val}})
    end.

string_to_etuple(Val,0) -> 
    case string_to_eterm(Val) of
        V when is_tuple(V) ->   V;
        _ ->                    ?ClientError({"Data conversion format error",{eTuple,0,Val}})
    end;
string_to_etuple(Val,Len) -> 
    case string_to_eterm(Val) of
        V when is_tuple(V) ->
            if 
                size(V) == Len ->   V;
                true ->             ?ClientError({"Data conversion format error",{eTuple,Len,Val}})
            end;
        _ ->
            ?ClientError({"Data conversion format error",{eTuple,Len,Val}})
    end.

string_to_eterm(Val) -> 
    try
        F = erl_value(Val),
        if 
            is_function(F,0) ->
                ?ClientError({"Data conversion format error",{eTerm,Val}}); 
            true ->                 
                F
        end
    catch
        _:_ -> ?ClientError({})
    end.

erl_value(Val) ->    
    Code = case [lists:last(string:strip(Val))] of
        "." -> Val;
        _ -> Val ++ "."
    end,
    {ok,ErlTokens,_}=erl_scan:string(Code),    
    {ok,ErlAbsForm}=erl_parse:parse_exprs(ErlTokens),    
    {value,Value,_}=erl_eval:exprs(ErlAbsForm,[]),    
    Value.

string_to_fun(Val,Len) ->
    Fun = erl_value(Val),
    if 
        is_function(Fun,Len) -> 
            Fun;
        true ->                 
            ?ClientError({"Data conversion format error",{eFun,Len,Val}})
    end.

%% ----- TESTS ------------------------------------------------

-include_lib("eunit/include/eunit.hrl").

setup() ->
    ?imem_test_setup().

teardown(_) ->
    catch drop_table(meta_table_3),
    catch drop_table(meta_table_2),
    catch drop_table(meta_table_1),
    ?imem_test_teardown().

db_test_() ->
    {
        setup,
        fun setup/0,
        fun teardown/1,
        {with, [
              fun data_types/1
            , fun meta_operations/1
        ]}}.    


data_types(_) ->
    try 
        ClEr = 'ClientError',
        %% SyEx = 'SystemException',    %% difficult to test

        io:format(user, "----TEST--~p:test_data_types~n", [?MODULE]),

        LocalTime = erlang:localtime(),
        {Date,Time} = LocalTime,
        ?assertEqual({{2004,3,1},{0,0,0}}, string_to_edatetime("1.3.2004")),
        ?assertEqual({{2004,3,1},{3,45,0}}, string_to_edatetime("1.3.2004 3:45")),
        ?assertEqual({Date,{0,0,0}}, string_to_edatetime("today")),
        ?assertEqual(LocalTime, string_to_edatetime("sysdate")),
        ?assertEqual(LocalTime, string_to_edatetime("systime")),
        ?assertEqual(LocalTime, string_to_edatetime("now")),
        ?assertEqual({{1888,8,18},{1,23,59}}, string_to_edatetime("18.8.1888 1:23:59")),
        ?assertEqual({{1888,8,18},{1,23,59}}, string_to_edatetime("1888-08-18 1:23:59")),
        ?assertEqual({{1888,8,18},{1,23,59}}, string_to_edatetime("8/18/1888 1:23:59")),
        ?assertEqual({{1888,8,18},{1,23,0}}, string_to_edatetime("8/18/1888 1:23")),
        ?assertEqual({{1888,8,18},{1,0,0}}, string_to_edatetime("8/18/1888 01")),
        ?assertException(throw,{ClEr,{"Data conversion format error",{eDatetime,"8/18/1888 1"}}}, string_to_edatetime("8/18/1888 1")),
        ?assertEqual({{1888,8,18},{0,0,0}}, string_to_edatetime("8/18/1888 ")),
        ?assertEqual({{1888,8,18},{0,0,0}}, string_to_edatetime("8/18/1888")),
        ?assertEqual({1,23,59}, parse_time("01:23:59")),        
        ?assertEqual({1,23,59}, parse_time("1:23:59")),        
        ?assertEqual({1,23,0}, parse_time("01:23")),        
        ?assertEqual({1,23,59}, parse_time("012359")),        
        ?assertEqual({1,23,0}, parse_time("0123")),        
        ?assertEqual({1,0,0}, parse_time("01")),        
        ?assertEqual({0,0,0}, parse_time("")),        
        ?assertEqual({{1888,8,18},{1,23,59}}, string_to_edatetime("18880818012359")),
        ?assertEqual(Time, string_to_time("systime")),
        ?assertEqual(Time, string_to_time("sysdate")),
        ?assertEqual(Time, string_to_time("now")),
        ?assertEqual(Time, string_to_time("localtime")),
        ?assertEqual({12,13,14}, string_to_time("12:13:14")),
        ?assertEqual({0,1,11}, string_to_time("0:1:11")),

        Item = 0,
        OldString = "OldString",
        StringType = estring,
        Len = 3,
        Prec = 1,
        Def = default,
        RO = true,
        RW = false,
        DefFun = fun() -> [{},{}] end,
        ?assertEqual(OldString, value_cast(Item,OldString,StringType,Len,Prec,Def,RO,"NewVal")),

        ?assertEqual(<<"NewVal">>, value_cast(Item,OldString,StringType,Len,Prec,Def,RW,<<"NewVal">>)),
        ?assertEqual([], value_cast(Item,OldString,StringType,Len,Prec,Def,RW,"")),
        ?assertEqual({}, value_cast(Item,OldString,StringType,Len,Prec,Def,RW,{})),
        ?assertEqual([atom,atom], value_cast(Item,OldString,StringType,Len,Prec,Def,RW,[atom,atom])),
        ?assertEqual(12, value_cast(Item,OldString,StringType,Len,Prec,Def,RW,12)),
        ?assertEqual(-3.14, value_cast(Item,OldString,StringType,Len,Prec,Def,RW,-3.14)),

        ?assertEqual(OldString, value_cast(Item,OldString,StringType,Len,Prec,Def,RW,OldString)),
        ?assertException(throw,{ClEr,{"Data conversion format error",{0,{string,3,"NewVal"}}}}, value_cast(Item,OldString,StringType,Len,Prec,Def,RW,"NewVal")),
        ?assertEqual("NewVal", value_cast(Item,OldString,StringType,6,Prec,Def,RW,"NewVal")),
        ?assertEqual("[NewVal]", value_cast(Item,OldString,StringType,8,Prec,Def,RW,"[NewVal]")),
        ?assertEqual(default, value_cast(Item,OldString,StringType,Len,Prec,Def,RW,"default")),

        ?assertEqual([{},{}], value_cast(Item,OldString,StringType,Len,Prec,DefFun,RW,"[{},{}]")),

        ?assertEqual(oldValue, value_cast(Item,oldValue,StringType,Len,Prec,Def,RW,"oldValue")),
        ?assertEqual('OldValue', value_cast(Item,'OldValue',StringType,Len,Prec,Def,RW,"'OldValue'")),
        ?assertEqual(-15, value_cast(Item,-15,StringType,Len,Prec,Def,RW,"-15")),

        IntegerType = integer,
        OldInteger = 17,
        ?assertEqual(OldString, value_cast(Item,OldString,IntegerType,Len,Prec,Def,RW,"OldString")),
        ?assertEqual(OldInteger, value_cast(Item,OldInteger,IntegerType,Len,Prec,Def,RW,"17")),
        ?assertEqual(default, value_cast(Item,OldInteger,IntegerType,Len,Prec,Def,RW,"default")),
        ?assertEqual(18, value_cast(Item,OldInteger,IntegerType,Len,Prec,Def,RW,"18")),
        ?assertEqual(-18, value_cast(Item,OldInteger,IntegerType,Len,Prec,Def,RW,"-18")),
        ?assertEqual(100, value_cast(Item,OldInteger,IntegerType,Len,-2,Def,RW,"149")),
        ?assertEqual(200, value_cast(Item,OldInteger,IntegerType,Len,-2,Def,RW,"150")),
        ?assertEqual(-100, value_cast(Item,OldInteger,IntegerType,4,-2,Def,RW,"-149")),
        ?assertEqual(-200, value_cast(Item,OldInteger,IntegerType,4,-2,Def,RW,"-150")),
        ?assertEqual(-200, value_cast(Item,OldInteger,IntegerType,100,0,Def,RW,"300-500")),
        ?assertEqual(12, value_cast(Item,OldInteger,IntegerType,20,0,Def,RW,"120/10.0")),
        ?assertEqual(12, value_cast(Item,OldInteger,IntegerType,20,0,Def,RW,"120/10.0001")),
        ?assertException(throw, {ClEr,{"Data conversion format error",{0,{integer,3,1,"1234"}}}}, value_cast(Item,OldInteger,IntegerType,Len,Prec,Def,RW,"1234")),
        ?assertException(throw, {ClEr,{"Data conversion format error",{0,{integer,"-"}}}}, value_cast(Item,OldInteger,IntegerType,Len,Prec,Def,RW,"-")),
        ?assertException(throw, {ClEr,{"Data conversion format error",{0,{integer,""}}}}, value_cast(Item,OldInteger,IntegerType,Len,Prec,Def,RW,"")),
        ?assertException(throw, {ClEr,{"Data conversion format error",{0,{integer,3,1,"-100"}}}}, value_cast(Item,OldInteger,IntegerType,Len,Prec,Def,RW,"-100")),
        ?assertException(throw, {ClEr,{"Data conversion format error",{0,{integer,3,1,"9999"}}}}, value_cast(Item,OldInteger,IntegerType,Len,Prec,Def,RW,"9999")),

        FloatType = float,
        OldFloat = -1.2,
        ?assertEqual(8.1, value_cast(Item,OldFloat,FloatType,Len,Prec,Def,RW,"8.1")),
        ?assertEqual(18.0, value_cast(Item,OldFloat,FloatType,Len,Prec,Def,RW,"18")),
        ?assertEqual(1.1, value_cast(Item,OldFloat,FloatType,Len,Prec,Def,RW,"1.12")),
        ?assertEqual(-1.1, value_cast(Item,OldFloat,FloatType,Len,Prec,Def,RW,"-1.14")),
        ?assertEqual(-1.1, value_cast(Item,OldFloat,FloatType,Len,Prec,Def,RW,"-1.1234567")),
        ?assertEqual(-1.12, value_cast(Item,OldFloat,FloatType,Len,2,Def,RW,"-1.1234567")),
        ?assertEqual(-1.123, value_cast(Item,OldFloat,FloatType,Len,3,Def,RW,"-1.1234567")),
        ?assertEqual(-1.1235, value_cast(Item,OldFloat,FloatType,Len,4,Def,RW,"-1.1234567")),
        %% ?assertEqual(-1.12346, value_cast(Item,OldFloat,FloatType,Len,5,Def,RW,"-1.1234567")),  %% fails due to single precision math
        %% ?assertEqual(-1.123457, value_cast(Item,OldFloat,FloatType,Len,6,Def,RW,"-1.1234567")), %% fails due to single precision math
        ?assertEqual(100.0, value_cast(Item,OldFloat,FloatType,Len,-2,Def,RW,"149")),
        ?assertEqual(-100.0, value_cast(Item,OldFloat,FloatType,Len,-2,Def,RW,"-149")),
        ?assertEqual(-200.0, value_cast(Item,OldFloat,FloatType,Len,-2,Def,RW,"-150")),
        %% ?assertEqual(0.6, value_cast(Item,OldFloat,FloatType,Len,Prec,Def,RW,"0.56")),
        %% ?assertEqual(0.6, value_cast(Item,OldFloat,FloatType,Len,Prec,Def,RW,"0.5678")),
        %% ?assertEqual(0.6, value_cast(Item,OldFloat,FloatType,Len,Prec,Def,RW,"0.5678910111")),
        %% ?assertEqual(0.6, value_cast(Item,OldFloat,FloatType,Len,Prec,Def,RW,"0.56789101112131415")),
        ?assertEqual(1234.5, value_cast(Item,OldFloat,FloatType,Len,Prec,Def,RW,"1234.5")),
        %% ?assertEqual(1234.6, value_cast(Item,OldFloat,FloatType,Len,Prec,Def,RW,"1234.56")),
        %% ?assertEqual(1234.6, value_cast(Item,OldFloat,FloatType,Len,Prec,Def,RW,"1234.5678")),
        %% ?assertEqual(1234.6, value_cast(Item,OldFloat,FloatType,Len,Prec,Def,RW,"1234.5678910111")),
        %% ?assertEqual(1234.6, value_cast(Item,OldFloat,FloatType,Len,Prec,Def,RW,"1234.56789101112131415")),

        DecimalType = decimal,
        OldDecimal = -123,
        ?assertEqual(81, value_cast(Item,OldDecimal,DecimalType,Len,Prec,Def,RW,"8.1")),
        ?assertEqual(180, value_cast(Item,OldDecimal,DecimalType,Len,Prec,Def,RW,"18.001")),
        ?assertEqual(1, value_cast(Item,OldDecimal,DecimalType,1,0,Def,RW,"1.12")),
        ?assertException(throw, {ClEr,{"Data conversion format error",{0,{decimal,5,5,"-1.123"}}}}, value_cast(Item,OldDecimal,DecimalType,5,5,Def,RW,"-1.123")),
        ?assertEqual(-112300, value_cast(Item,OldDecimal,DecimalType,7,5,Def,RW,"-1.123")),
        ?assertEqual(-112346, value_cast(Item,OldDecimal,DecimalType,7,5,Def,RW,"-1.1234567")),
        ?assertException(throw, {ClEr,{"Data conversion format error",{0,{decimal,5,Prec,"1234567.89"}}}}, value_cast(Item,OldDecimal,DecimalType,5,Prec,Def,RW,"1234567.89")),

        ETermType = eterm,
        OldETerm = {-1.2,[a,b,c]},
        ?assertEqual(OldETerm, value_cast(Item,OldETerm,ETermType,Len,Prec,Def,RW,OldETerm)),
        ?assertEqual(default, value_cast(Item,OldETerm,ETermType,Len,Prec,Def,RW,"default")),
        ?assertEqual([a,b], value_cast(Item,OldETerm,ETermType,Len,Prec,Def,RW,"[a,b]")),
        ?assertEqual(-1.1234567, value_cast(Item,OldETerm,ETermType,Len,Prec,Def,RW,"-1.1234567")),
        ?assertEqual({[1,2,3]}, value_cast(Item,OldETerm,ETermType,Len,Prec,Def,RW,"{[1,2,3]}")),
        ?assertException(throw, {ClEr,{"Data conversion format error",{0,{eTerm,"[a|]"}}}}, value_cast(Item,OldETerm,ETermType,Len,Prec,Def,RW,"[a|]")),

        ?assertEqual({1,2,3}, value_cast(Item,OldETerm,eTuple,Len,Prec,Def,RW,"{1,2,3}")),
        ?assertEqual({}, value_cast(Item,OldETerm,eTuple,0,Prec,Def,RW,"{}")),
        ?assertEqual({1}, value_cast(Item,OldETerm,eTuple,0,Prec,Def,RW,"{1}")),
        ?assertEqual({1,2}, value_cast(Item,OldETerm,eTuple,0,Prec,Def,RW,"{1,2}")),
        ?assertException(throw, {ClEr,{"Data conversion format error",{0,{eTuple,Len,"[a]"}}}}, value_cast(Item,OldETerm,etuple,Len,Prec,Def,RW,"[a]")),
        ?assertException(throw, {ClEr,{"Data conversion format error",{0,{eTuple,Len,"{a}"}}}}, value_cast(Item,OldETerm,etuple,Len,Prec,Def,RW,"{a}")),

        ?assertEqual([a,b,c], value_cast(Item,OldETerm,elist,Len,Prec,Def,RW,"[a,b,c]")),
        ?assertException(throw, {ClEr,{"Data conversion format error",{0,{eList,Len,"[a]"}}}}, value_cast(Item,OldETerm,elist,Len,Prec,Def,RW,"[a]")),
        ?assertEqual([], value_cast(Item,[],eList,Len,Prec,Def,RW,"[]")),
        ?assertEqual([], value_cast(Item,OldETerm,eList,Len,Prec,[],RW,"[]")),
        ?assertEqual([], value_cast(Item,OldETerm,eList,0,Prec,Def,RW,"[]")),
        ?assertEqual([a], value_cast(Item,OldETerm,eList,0,Prec,Def,RW,"[a]")),
        ?assertEqual([a,b], value_cast(Item,OldETerm,eList,0,Prec,Def,RW,"[a,b]")),

        ?assertEqual({10,132,7,92}, value_cast(Item,OldETerm,eipaddr,0,Prec,Def,RW,"10.132.7.92")),
        ?assertEqual({0,0,0,0}, value_cast(Item,OldETerm,eipaddr,4,Prec,Def,RW,"0.0.0.0")),
        ?assertEqual({255,255,255,255}, value_cast(Item,OldETerm,eipaddr,4,Prec,Def,RW,"255.255.255.255")),
        ?assertException(throw, {ClEr,{"Data conversion format error",{0,{eIpaddr,0,"1.2.3.4.5"}}}}, value_cast(Item,OldETerm,eipaddr,0,0,Def,RW,"1.2.3.4.5")),
        ?assertException(throw, {ClEr,{"Data conversion format error",{0,{eIpaddr,4,"1.2.-1.4"}}}}, value_cast(Item,OldETerm,eipaddr,4,0,Def,RW,"1.2.-1.4")),
        ?assertException(throw, {ClEr,{"Data conversion format error",{0,{eIpaddr,6,"1.256.1.4"}}}}, value_cast(Item,OldETerm,eipaddr,6,0,Def,RW,"1.256.1.4")),
        ?assertException(throw, {ClEr,{"Data conversion format error",{0,{eIpaddr,8,"1.2.1.4"}}}}, value_cast(Item,OldETerm,eipaddr,8,0,Def,RW,"1.2.1.4")),

        Fun = fun(X) -> X*X end,
        io:format(user, "Fun ~p~n", [Fun]),
        Res = value_cast(Item,OldETerm,efun,1,Prec,Def,RW,"fun(X) -> X*X end"),
        io:format(user, "Run ~p~n", [Res]),
        ?assertEqual(Fun(4), Res(4)),

        ?assertEqual(true, true)
    catch
        Class:Reason ->  io:format(user, "Exception ~p:~p~n~p~n", [Class, Reason, erlang:get_stacktrace()]),
        throw ({Class, Reason})
    end,
    ok.

meta_operations(_) ->
    try 
        %% ClEr = 'ClientError',
        %% SyEx = 'SystemException',    %% difficult to test

        io:format(user, "----TEST--~p:test_mnesia~n", [?MODULE]),

        ?assertEqual(true, is_atom(imem_meta:schema())),
        io:format(user, "success ~p~n", [schema]),
        ?assertEqual(true, lists:member({imem_meta:schema(),node()}, imem_meta:data_nodes())),
        io:format(user, "success ~p~n", [data_nodes]),

        io:format(user, "----TEST--~p:test_database_operations~n", [?MODULE]),
        Types1 =    [ #ddColumn{name=a, type=estring, length=10}     %% key
                    , #ddColumn{name=b1, type=estring, length=20}    %% value 1
                    , #ddColumn{name=c1, type=estring, length=30}    %% value 2
                    ],
        Types2 =    [ #ddColumn{name=a, type=integer, length=10}    %% key
                    , #ddColumn{name=b2, type=float, length=8, precision=3}   %% value
                    ],

        ?assertEqual(ok, create_table(meta_table_1, Types1, [])),
        ?assertEqual(ok, create_table(meta_table_2, Types2, [])),

        ?assertEqual(ok, create_table(meta_table_3, {[a,nil],[date,nil],{meta_table_3,undefined,undefined}}, [])),
        io:format(user, "success ~p~n", [create_tables]),

        ?assertEqual(ok, drop_table(meta_table_3)),
        ?assertEqual(ok, drop_table(meta_table_2)),
        ?assertEqual(ok, drop_table(meta_table_1)),
        io:format(user, "success ~p~n", [drop_tables])
    catch
        Class:Reason ->  io:format(user, "Exception ~p:~p~n~p~n", [Class, Reason, erlang:get_stacktrace()]),
        throw ({Class, Reason})
    end,
    ok.
