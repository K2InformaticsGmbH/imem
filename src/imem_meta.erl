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
        , table_columns/1
        , table_size/1
        , check_table/1
        , check_table_record/2
        , check_table_columns/2
        , system_table/1
        , meta_field/1
        , meta_field_info/1
        , meta_field_value/1
        , column_map/2
        , column_info_items/2
        , column_map_items/2
        , subscribe/1
        , unsubscribe/1
        ]).

-export([ add_attribute/2
        , update_opts/2
        ]).

-export([ create_table/4
        , create_table/3
		, drop_table/1
        , read/1            %% read whole table, only use for small tables 
        , read/2            %% read by key
        , read_block/3      %% read numer of rows after key x      
        , select/1          %% contiunation block read
        , select/2          %% select without limit, only use for small result sets
        , select/3          %% select with limit
        , insert/2    
        , write/2           %% write single key
        , delete/2          %% delete rows by key
        , truncate/1        %% truncate table
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

system_table(Table) ->
    case lists:member(Table,?META_TABLES) of
        true ->     true;
        false ->    imem_if:system_table(Table) 
    end.

check_table(Table) ->
    imem_if:table_size(Table).

check_table_record(Table, {Names, Types, DefaultRecord}) ->
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
check_table_record(Table, ColumnNames) ->
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

check_table_columns(Table, {Names, Types, DefaultRecord}) ->
    [Table|Defaults] = tuple_to_list(DefaultRecord),
    ColumnInfos = column_infos(Names, Types, Defaults),
    TableColumns = table_columns(Table),    
    if
        Names =:= TableColumns ->
            case imem_if:read(ddTable,{schema(), Table}) of
                [] ->   
                    ?SystemException({"Missing table metadata",Table}); 
                [#ddTable{columns=CI}] ->
                    if
                        CI =:= ColumnInfos ->    
                            ok;
                        true ->                 
                            ?SystemException({"Column info does not match table metadata",Table})
                    end          
            end;  
        true ->                 
            ?SystemException({"Column info does not match table structure",Table})             
    end;
check_table_columns(Table, ColumnInfo) ->
    ColumnNames = column_info_items(ColumnInfo, name),
    TableColumns = table_columns(Table),    
    if
        ColumnNames =:= TableColumns ->
            case imem_if:read(ddTable,{schema(), Table}) of
                [] ->   
                    ?SystemException({"Missing table metadata",Table}); 
                [#ddTable{columns=CI}] ->
                    if
                        CI =:= ColumnInfo ->    
                            ok;
                        true ->                 
                            ?SystemException({"Column info does not match table metadata",Table})
                    end          
            end;  
        true ->                 
            ?SystemException({"Column info does not match table structure",Table})             
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
    #ddColumn{name=schema, type='string', length=40, precision=0};
meta_field_info(node) ->
    #ddColumn{name=node, type='string', length=40, precision=0};
meta_field_info(user) ->
    #ddColumn{name=user, type='string', length=40, precision=0};
meta_field_info(Name) ->
    ?ClientError({"Unknown meta column",Name}). 

meta_field_value(user) ->
    "unknown"; 
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

column_map_items(Map, schema) ->
    [C#ddColMap.schema || C <- Map];
column_map_items(Map, table) ->
    [C#ddColMap.table || C <- Map];
column_map_items(Map, name) ->
    [C#ddColMap.name || C <- Map];
column_map_items(Map, qname) ->
    [lists:flatten(io_lib:format("~p.~p.~p", [C#ddColMap.schema,C#ddColMap.table,C#ddColMap.name])) || C <- Map];
column_map_items(Map, qtype) ->
    [lists:flatten(io_lib:format("~p(~p,~p)", [C#ddColMap.type,C#ddColMap.length,C#ddColMap.precision])) || C <- Map];
column_map_items(Map, table_index) ->
    [C#ddColMap.tind || C <- Map];
column_map_items(Map, column_index) ->
    [C#ddColMap.cind || C <- Map];
column_map_items(Map, type) ->
    [C#ddColMap.type || C <- Map];
column_map_items(Map, length) ->
    [C#ddColMap.length || C <- Map];
column_map_items(Map, precision) ->
    [C#ddColMap.length || C <- Map];
column_map_items(_Map, Item) ->
    ?ClientError({"Invalid item",Item}).

column_map([], Columns) ->
    ?ClientError({"Empty table list",Columns});
column_map(Tables, []) ->
    column_map(Tables, [#ddColMap{name='*'}]);    
column_map(Tables, Columns) ->
    column_map(Tables, Columns, 1, [], []).

column_map([{undefined,Table,Alias}|Tables], Columns, Tindex, Lookup, Acc) ->
    column_map([{schema(),Table,Alias}|Tables], Columns, Tindex, Lookup, Acc);
column_map([{Schema,Table,undefined}|Tables], Columns, Tindex, Lookup, Acc) ->
    Cols = case imem_if:read(ddTable,{Schema,Table}) of
        [#ddTable{columns=C}] ->    C;
        [] ->                       ?ClientError({"Table does not exist",{Schema,Table}})
    end,
    L = [{Tindex, Cindex, Schema, Table, Cinfo#ddColumn.name, Cinfo} || {Cindex, Cinfo} <- lists:zip(lists:seq(1,length(Cols)), Cols)],
    column_map(Tables, Columns, Tindex+1, L++Lookup, Acc);
column_map([{Schema,Table,Alias}|Tables], Columns, Tindex, Lookup, Acc) ->
    Cols = case imem_if:read(ddTable,{Schema,Table}) of
        [#ddTable{columns=C}] ->    C;
        [] ->                       ?ClientError({"Table does not exist",{Schema,Table}})
    end,
    L = [{Tindex, Cindex, Schema, Alias, Cinfo#ddColumn.name, Cinfo} || {Cindex, Cinfo} <- lists:zip(lists:seq(1,length(Cols)), Cols)],
    column_map(Tables, Columns, Tindex+1, L++Lookup, Acc);
column_map([], [#ddColMap{schema=undefined, table=undefined, name='*'}=Cmap0|Columns], Tindex, Lookup, Acc) ->
    Cmaps = [ Cmap0#ddColMap{schema=S, table=T, tind=Ti, cind=Ci, type=Type, length=Len, precision=P, name=N} || {Ti, Ci, S, T, N, #ddColumn{type=Type, length=Len, precision=P}} <- Lookup],
    column_map([], Cmaps ++ Columns, Tindex, Lookup, Acc);
column_map([], [#ddColMap{schema=undefined, name='*'}=Cmap0|Columns], Tindex, Lookup, Acc) ->
    column_map([], [Cmap0#ddColMap{schema=schema()}|Columns], Tindex, Lookup, Acc);
column_map([], [#ddColMap{schema=Schema, table=Table, name='*'}=Cmap0|Columns], Tindex, Lookup, Acc) ->
    Cmaps = [ Cmap0#ddColMap{schema=S, table=T, tind=Ti, cind=Ci, type=Type, length=Len, precision=P, name=N} || {Ti, Ci, S, T, N, #ddColumn{type=Type, length=Len, precision=P}} <- Lookup, S==Schema, T==Table],
    column_map([], Cmaps ++ Columns, Tindex, Lookup, Acc);
column_map([], [#ddColMap{schema=Schema, table=Table, name=Name}=Cmap0|Columns], Tindex, Lookup, Acc) ->
    Pred = fun(L) ->
        (Name == element(5, L)) andalso
        ((Table == undefined) or (Table == element(4, L))) andalso
        ((Schema == undefined) or (Schema == element(3, L)))
    end,
    Lmatch = lists:filter(Pred, Lookup),
    Tcount = length(lists:usort([{element(3, X), element(4, X)} || X <- Lmatch])),
    MetaField = meta_field(Name),
    if 
        (Tcount==0) andalso (Schema==undefined) andalso (Table==undefined) andalso MetaField ->
            #ddColumn{type=Type, length=Len, precision=P} = meta_field_info(Name),
            Cmap1 = Cmap0#ddColMap{tind=0, cind=0, type=Type, length=Len, precision=P},
            column_map([], Columns, Tindex, Lookup, [Cmap1|Acc]);                          
        (Tcount==0) andalso (Schema==undefined) andalso (Table==undefined)->  
            ?ClientError({"Unknown column name", Name});
        (Tcount==0) andalso (Schema==undefined)->  
            ?ClientError({"Unknown column name", {Table, Name}});
        (Tcount==0) ->  
            ?ClientError({"Unknown column name", {Schema, Table, Name}});
        (Tcount > 1) andalso (Schema==undefined) andalso (Table==undefined)-> 
            ?ClientError({"Ambiguous column name", Name});
        (Tcount > 1) andalso (Schema==undefined) -> 
            ?ClientError({"Ambiguous column name", {Table, Name}});
        (Tcount > 1) -> 
            ?ClientError({"Ambiguous column name", {Schema, Table, Name}});
        true ->         
            {Ti, Ci, S, T, Name, #ddColumn{type=Type, length=Len, precision=P}} = hd(Lmatch),
            Cmap1 = Cmap0#ddColMap{schema=S, table=T, tind=Ti, cind=Ci, type=Type, length=Len, precision=P},
            column_map([], Columns, Tindex, Lookup, [Cmap1|Acc])
    end;
column_map([], [], _Tindex, _Lookup, Acc) ->
    lists:reverse(Acc);
column_map(Tables, Columns, Tmax, Lookup, Acc) ->
    io:format(user, "column_map error Tables ~p~n", [Tables]),
    io:format(user, "column_map error Columns ~p~n", [Columns]),
    io:format(user, "column_map error Tmax ~p~n", [Tmax]),
    io:format(user, "column_map error Lookup ~p~n", [Lookup]),
    io:format(user, "column_map error Acc ~p~n", [Acc]),
    ?ClientError({"Column map invalid parameter",{Tables,Columns}}).

column_names(Infos)->
    [list_to_atom(lists:flatten(io_lib:format("~p", [N]))) || #ddColumn{name=N} <- Infos].

column_infos(Names)->
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

table_columns(all_tables) ->
    table_columns(ddTable);
table_columns(Table) ->
    imem_if:table_columns(Table).

table_size(Table) ->
    imem_if:table_size(Table).

read(Table, Key) -> 
    imem_if:read(Table, Key).

read(Table) ->
    imem_if:read(Table).

read_block(Table, AfterKey, BlockSize) ->
    imem_if:read_block(Table, AfterKey, BlockSize).    

select(all_tables, MatchSpec) ->
    select(ddTable, MatchSpec);
select(Table, MatchSpec) ->
    imem_if:select(Table, MatchSpec).

select(all_tables, MatchSpec, Limit) ->
    select(ddTable, MatchSpec, Limit);
select(Table, MatchSpec, Limit) ->
    imem_if:select(Table, MatchSpec, Limit).

select(Continuation) ->
    imem_if:select(Continuation).

write(Table, Record) -> 
    imem_if:write(Table, Record).

insert(Table, Row) ->
    imem_if:insert(Table, Row).

delete(Table, Key) ->
    imem_if:delete(Table, Key).

truncate(Table) ->
    imem_if:truncate(Table).

subscribe(EventCategory) ->
    imem_if:subscribe(EventCategory).

unsubscribe(EventCategory) ->
    imem_if:unsubscribe(EventCategory).

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

        ?assertEqual(true, is_atom(imem_meta:schema())),
        io:format(user, "success ~p~n", [schema]),
        ?assertEqual(true, lists:member({imem_meta:schema(),node()}, imem_meta:data_nodes())),
        io:format(user, "success ~p~n", [data_nodes]),

        io:format(user, "----TEST--~p:test_database_operations~n", [?MODULE]),
        Types1 =    [ #ddColumn{name=a, type=string, length=10}     %% key
                    , #ddColumn{name=b1, type=string, length=20}    %% value 1
                    , #ddColumn{name=c1, type=string, length=30}    %% value 2
                    ],
        Types2 =    [ #ddColumn{name=a, type=integer, length=10}    %% key
                    , #ddColumn{name=b2, type=float, length=8, precision=3}   %% value
                    ],

        ?assertEqual(ok, create_table(meta_table_1, Types1, [])),
        ?assertEqual(ok, create_table(meta_table_2, Types2, [])),

        ?assertEqual(ok, create_table(meta_table_3, {[a,nil],[date,nil],{meta_table_3,undefined,undefined}}, [])),
        io:format(user, "success ~p~n", [create_tables]),

        Table1 =    {'Imem', meta_table_1, undefined},
        Table2 =    {undefined, meta_table_2, undefined},
        Table3 =    {undefined, meta_table_3, undefined},
        TableX =    {undefined,meta_table_x, undefined},

        Alias1 =    {undefined, meta_table_1, alias1},
        Alias2 =    {'Imem', meta_table_1, alias2},

        ?assertException(throw, {ClEr, {"Table does not exist", {'Imem', meta_table_x}}}, column_map([TableX], [])),
        io:format(user, "success ~p~n", [table_no_exists]),

        ColsE1=     [ #ddColMap{tag="A1", schema='Imem', table=meta_table_1, name=a}
                    , #ddColMap{tag="A2", name=x}
                    , #ddColMap{tag="A3", name=c1}
                    ],

        ?assertException(throw, {ClEr,{"Unknown column name", x}}, column_map([Table1], ColsE1)),
        io:format(user, "success ~p~n", [unknown_column_name_1]),

        ColsE2=     [ #ddColMap{tag="A1", schema='Imem', table=meta_table_1, name=a}
                    , #ddColMap{tag="A2", table=meta_table_x, name=b1}
                    , #ddColMap{tag="A3", name=c1}
                    ],

        ?assertException(throw, {ClEr,{"Unknown column name", {meta_table_x,b1}}}, column_map([Table1, Table2], ColsE2)),
        io:format(user, "success ~p~n", [unknown_column_name_2]),

        ?assertMatch([_,_,_], column_map([Table1], [])),
        io:format(user, "success ~p~n", [empty_select_columns]),


        ColsA =     [ #ddColMap{tag="A1", schema='Imem', table=meta_table_1, name=a}
                    , #ddColMap{tag="A2", table=meta_table_1, name=b1}
                    , #ddColMap{tag="A3", name=c1}
                    ],

        ?assertException(throw, {ClEr,{"Empty table list", _}}, column_map([], ColsA)),
        io:format(user, "success ~p~n", [empty_table_list]),

        ?assertException(throw, {ClEr,{"Ambiguous column name", a}}, column_map([Table1, Table3], [#ddColMap{name=a}])),
        io:format(user, "success ~p~n", [columns_ambiguous]),

        ?assertMatch([_,_,_], column_map([Table1], ColsA)),
        io:format(user, "success ~p~n", [columns_A]),

        ?assertMatch([_,_,_], column_map([Table1, Table3], ColsA)),
        io:format(user, "success ~p~n", [columns_A_join]),

        ?assertMatch([_,_,_,_,_], column_map([Table1, Table3], [#ddColMap{name='*'}])),
        io:format(user, "success ~p~n", [columns_13_join]),

        ?assertMatch([_,_,_,_,_,_,_], column_map([Table1, Table2, Table3], [#ddColMap{name='*'}])),
        io:format(user, "success ~p~n", [columns_123_join]),

        ?assertMatch([_,_,_,_,_,_,_,_], column_map([Alias1, Alias2, Table3], [#ddColMap{name='*'}])),
        io:format(user, "success ~p~n", [alias_113_join]),

        ?assertMatch([_,_], column_map([Alias1, Alias2, Table3], [#ddColMap{table=meta_table_3, name='*'}])),
        io:format(user, "success ~p~n", [columns_113_star1]),

        ?assertMatch([_,_,_,_], column_map([Alias1, Alias2, Table3], [#ddColMap{table=alias1, name='*'},#ddColMap{table=meta_table_3, name='a'}])),
        io:format(user, "success ~p~n", [columns_alias_1]),

        ?assertMatch([_,_], column_map([Alias1, Alias2, Table3], [#ddColMap{table=alias1, name=a},#ddColMap{table=alias2, name='a'}])),
        io:format(user, "success ~p~n", [columns_alias_2]),

        ?assertMatch([_,_], column_map([Alias1], [#ddColMap{table=alias1, name=a},#ddColMap{name=sysdate}])),
        io:format(user, "success ~p~n", [sysdate]),

        ?assertException(throw, {ClEr,{"Unknown column name", {any,sysdate}}}, column_map([Alias1], [#ddColMap{table=alias1, name=a},#ddColMap{table=any, name=sysdate}])),
        io:format(user, "success ~p~n", [sysdate_reject]),

        ?assertEqual(["'Imem'.alias1.a","undefined.undefined.user"], column_map_items(column_map([Alias1], [#ddColMap{table=alias1, name=a},#ddColMap{name=user}]),qname)),
        io:format(user, "success ~p~n", [user]),

        ?assertEqual(ok, drop_table(meta_table_3)),
        ?assertEqual(ok, drop_table(meta_table_2)),
        ?assertEqual(ok, drop_table(meta_table_1)),
        io:format(user, "success ~p~n", [drop_tables])
    catch
        Class:Reason ->  io:format(user, "Exception ~p:~p~n~p~n", [Class, Reason, erlang:get_stacktrace()]),
        throw ({Class, Reason})
    end,
    ok.
