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
        ]).

-export([ add_attribute/2
        , update_opts/2
        ]).

-export([ create_table/4
        , create_table/3
		, drop_table/1
        , truncate_table/1  %% truncate table
        , read/1            %% read whole table, only use for small tables 
        , read/2            %% read by key
        , select/1          %% contiunation block read
        , select/2          %% select without limit, only use for small result sets
        , select/3          %% select with limit
        , insert/2    
        , write/2           %% write single key
        , delete/2          %% delete rows by key
        ]).

-export([ update_prepare/3          %% stateless creation of update plan from change list
        , update_cursor_prepare/2   %% take change list and generate update plan (stored in state)
        , update_cursor_execute/2   %% take update plan from state and execute it (fetch aborted first)
        , fetch_recs/3
        , fetch_recs_sort/3 
        , fetch_recs_async/2        
        , fetch_recs_async/3        
        , fetch_close/1
        , exec/3
        , close/1
        ]).

-export([ fetch_start/4
        , update_tables/2  
        , subscribe/1
        , unsubscribe/1
        ]).

-export([ transaction/1
        , transaction/2
        , transaction/3
        , return_atomic_list/1
        , return_atomic_ok/1
        , return_atomic/1
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

drop_table({Schema,Table}) ->
    MySchema = schema(),
    case Schema of
        MySchema ->     drop_table(Table);
        OtherSchema ->  ?UnimplementedException({"Drop table in foreign schema",{Schema,Table}})
    end;
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

fetch_recs(Pid, Sock, Timeout) ->
    imem_statement:fetch_recs(none, Pid, Sock, Timeout, false).

fetch_recs_sort(Pid, Sock, Timeout) ->
    imem_statement:fetch_recs_sort(none, Pid, Sock, Timeout, false).

fetch_recs_async(Pid, Sock) ->
    imem_statement:fetch_recs_async(none, Pid, Sock, false).

fetch_recs_async(Pid, Sock, Opts) ->
    imem_statement:fetch_recs_async(none, Pid, Sock, Opts, false).

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
insert(ddTable, Row) ->
    imem_if:insert(ddTable, Row);
insert(Table, Row) when is_list(Row) ->
    case lists:member(?nav,Row) of
        false ->    imem_if:insert(Table, Row);
        true ->     ?ClientError({"Not null constraint violation", {Table,Row}})
    end;
insert(Table, Row) when is_tuple(Row) ->
    case lists:member(?nav,tuple_to_list(Row)) of
        false ->    imem_if:insert(Table, Row);
        true ->     ?ClientError({"Not null constraint violation", {Table,Row}})
    end.

delete({_Schema,Table}, Key) ->
    delete(Table, Key);             %% ToDo: may depend on schema
delete(Table, Key) ->
    imem_if:delete(Table, Key).

truncate_table({_Schema,Table}) ->
    truncate_table(Table);                %% ToDo: may depend on schema
truncate_table(Table) ->
    imem_if:truncate_table(Table).

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
update_table_name(MySchema,[{MySchema,Tab,Type}=Table, Item, Old, New]) ->
    case lists:member(?nav,tuple_to_list(New)) of
        false ->    [{Tab,Type}, Item, Old, New];
        true ->     ?ClientError({"Not null constraint violation", {Item, {Tab,New}}})
    end.

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
              fun meta_operations/1
        ]}}.    

meta_operations(_) ->
    try 
        ClEr = 'ClientError',
        %% SyEx = 'SystemException',    %% difficult to test

        io:format(user, "----TEST--~p:test_mnesia~n", [?MODULE]),

        io:format(user, "schema ~p~n", [imem_meta:schema()]),
        io:format(user, "data nodes ~p~n", [imem_meta:data_nodes()]),
        ?assertEqual(true, is_atom(imem_meta:schema())),
        ?assertEqual(true, lists:member({imem_meta:schema(),node()}, imem_meta:data_nodes())),

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

        ?assertEqual(ok, create_table(meta_table_3, {[a,?nav],[date,undefined],{meta_table_3,?nav,undefined}}, [])),
        io:format(user, "success ~p~n", [create_table_not_null]),

        ?assertEqual(ok, insert(meta_table_3, {{{2000,01,01},{12,45,55}},undefined})),
        ?assertEqual(1, table_size(meta_table_3)),
        ?assertException(throw, {ClEr,{"Not null constraint violation", {meta_table_3,_}}}, insert(meta_table_3, {?nav,undefined})),
        ?assertException(throw, {ClEr,{"Not null constraint violation", {meta_table_3,_}}}, insert(meta_table_3, {{{2000,01,01},{12,45,56}},?nav})),
        io:format(user, "success ~p~n", [not_null_constraint]),
        
        ?assertEqual(ok, update_tables([[{'Imem',meta_table_3,set}, 1, {}, {meta_table_3,{{2000,01,01},{12,45,59}},undefined}]], optimistic)),
        ?assertException(throw, {ClEr,{"Not null constraint violation", {1,{meta_table_3,_}}}}, update_tables([[{'Imem',meta_table_3,set}, 1, {}, {meta_table_3, ?nav, undefined}]], optimistic)),
        ?assertException(throw, {ClEr,{"Not null constraint violation", {1,{meta_table_3,_}}}}, update_tables([[{'Imem',meta_table_3,set}, 1, {}, {meta_table_3,{{2000,01,01},{12,45,59}}, ?nav}]], optimistic)),
        
        ?assertEqual(ok, drop_table(meta_table_3)),
        ?assertEqual(ok, drop_table(meta_table_2)),
        ?assertEqual(ok, drop_table(meta_table_1)),
        io:format(user, "success ~p~n", [drop_tables])
    catch
        Class:Reason ->  io:format(user, "Exception ~p:~p~n~p~n", [Class, Reason, erlang:get_stacktrace()]),
        throw ({Class, Reason})
    end,
    ok.
