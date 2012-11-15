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
        , system_table/1
        , meta_field/1
        , meta_field_info/1
        , meta_field_value/1
        , column_map/2
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
        catch create_table(ddTable, record_info(fields, ddTable), [], system),
        check_table(ddTable),
        io:format(user, "~p started!~n", [?MODULE]),
        {ok,#state{}}
    catch
        Class:Reason -> io:format(user, "~p failed with ~p:~p~n", [?MODULE,Class,Reason]),
                        {stop, "Insufficient resources for start"}
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
meta_field_info(Name) ->
    ?ClientError({"Unknown meta column",Name}). 

meta_field_value(Name) ->
    imem_if:meta_field_value(Name). 

column_map(Tables, Columns) ->
    column_map(Tables, Columns, 1, [], []).

column_map([{undefined,Table}|Tables], Columns, Tindex, Lookup, Acc) ->
    column_map([{schema(),Table}|Tables], Columns, Tindex, Lookup, Acc);
column_map([{Schema,Table}|Tables], Columns, Tindex, Lookup, Acc) ->
    #ddTable{columns=Cols} = imem_if:read(ddTable,{Schema,Table}),
    L = [{Tindex, Cindex, Schema, Table, Cinfo#ddColumn.name, Cinfo} || {Cindex, Cinfo} <- lists:zip(lists:seq(1,length(Cols), Cols))],
    column_map(Tables, Columns, Tindex+1, [L|Lookup], Acc);
column_map([], [#ddColMap{schema=undefined, table=undefined, name='*'}=Cmap0|Columns], Tindex, Lookup, Acc) ->
    Cmaps = [ Cmap0#ddColMap{tind=Ti, cind=Ci, type=Type, length=Len, precision=P, name=N} || {Ti, Ci, _S, _T, N, #ddColumn{type=Type, length=Len, precision=P}} <- Lookup],
    column_map([], Cmaps ++ Columns, Tindex, Lookup, Acc);
column_map([], [#ddColMap{schema=undefined, name='*'}=Cmap0|Columns], Tindex, Lookup, Acc) ->
    column_map([], [Cmap0#ddColMap{schema=schema()}|Columns], Tindex, Lookup, Acc);
column_map([], [#ddColMap{schema=Schema, table=Table, name='*'}=Cmap0|Columns], Tindex, Lookup, Acc) ->
    Cmaps = [ Cmap0#ddColMap{tind=Ti, cind=Ci, type=Type, length=Len, precision=P, name=N} || {Ti, Ci, S, T, N, #ddColumn{type=Type, length=Len, precision=P}} <- Lookup, S==Schema, T==Table],
    column_map([], Cmaps ++ Columns, Tindex, Lookup, Acc);
column_map([], [#ddColMap{schema=Schema, table=Table, name=Name}=Cmap0|Columns], Tindex, Lookup, Acc) ->
    Pred = fun(L) ->
        (Name == lists:element(5, L)) andalso
        ((Table == undefined) or (Table == lists:element(4, L))) andalso
        ((Schema == undefined) or (Schema == lists:element(3, L)))
    end,
    Lmatch = lists:filter(Pred, Lookup),
    Tcount = length(lists:usort([{lists:element(3, X), lists:element(4, X)} || X <- Lmatch])),
    MetaField = meta_field(Name),
    if 
        (Tcount==0) andalso (Schema==undefined) andalso (Table==undefined) andalso MetaField ->
            #ddColumn{type=Type, length=Len, precision=P} = meta_field_info(Name),
            Cmap1 = Cmap0#ddColMap{tind=0, cind=0, type=Type, length=Len, precision=P},
            column_map([], Columns, Tindex, Lookup, [Cmap1|Acc]);                          
        (Tcount==0) ->  
            ?ClientError({"Unknown column name", Name});
        (Tcount > 1) -> 
            ?ClientError({"Ambiguous column name", Name});
        true ->         
            {Ti, Ci, S, T, Name, #ddColumn{type=Type, length=Len, precision=P}} = lists:head(Lmatch),
            Cmap1 = Cmap0#ddColMap{schema=S, table=T, tind=Ti, cind=Ci, type=Type, length=Len, precision=P},
            column_map([], Columns, Tindex, Lookup, [Cmap1|Acc])
    end;
column_map([], [], _Tindex, _Lookup, Acc) ->
    Acc.

column_names(ColumnInfos)->
    [list_to_atom(lists:flatten(io_lib:format("~p", [N]))) || #ddColumn{name=N} <- ColumnInfos].

column_infos(ColumnNames)->
    [#ddColumn{name=list_to_atom(lists:flatten(io_lib:format("~p", [N])))} || N <- ColumnNames].

create_table(Table, [#ddColumn{}|_]=ColumnInfos, Opts, Owner) ->
    ColumnNames = column_names(ColumnInfos),
    imem_if:create_table(Table, ColumnNames, Opts),
    imem_if:write(ddTable, #ddTable{qname={schema(),Table}, columns=ColumnInfos, opts=Opts, owner=Owner});
create_table(Table, ColumnNames, Opts, Owner) ->
    ColumnInfos = column_infos(ColumnNames), 
    imem_if:create_table(Table, ColumnNames, Opts),
    imem_if:write(ddTable, #ddTable{qname={schema(),Table}, columns=ColumnInfos, opts=Opts, owner=Owner}).

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
    application:start(imem).

teardown(_) ->
    application:stop(imem).

db_test_() ->
    {
        setup,
        fun setup/0,
        fun teardown/1,
        {with, [
            fun table_operations/1
            %%, fun test_create_account/1
        ]}}.    

table_operations(_) -> 
    ClEr = 'ClientError',
    %% SyEx = 'SystemException',

    io:format(user, "----TEST--~p:test_mnesia~n", [?MODULE]),

    ?assertEqual("Imem", schema()),
    io:format(user, "success ~p~n", [schema]),
    ?assertEqual([{"Imem",node()}], data_nodes()),
    io:format(user, "success ~p~n", [data_nodes]),

    io:format(user, "----TEST--~p:test_database_operations~n", [?MODULE]),
    Types1 =    [ #ddColumn{name=a, type=string, length=10}
                , #ddColumn{name=b1, type=string, length=20}
                , #ddColumn{name=c1, type=string, length=30}
                ],
    Types2 =    [ #ddColumn{name=a, type=integer, length=10}
                , #ddColumn{name=b2, type=integer, length=20}
                , #ddColumn{name=c2, type=integer, length=30}
                ],
    Types3 =    [ #ddColumn{name=a, type=date, length=7}
                , #ddColumn{name=b3, type=date, length=7}
                , #ddColumn{name=c3, type=date, length=7}
                ],
    ?assertEqual(ok, create_table(meta_table_1, Types1, [])),
    ?assertEqual(ok, create_table(meta_table_2, Types2, [])),
    ?assertEqual(ok, create_table(meta_table_3, Types3, [])),
    io:format(user, "success ~p~n", [create_tables]),

%    ?assertEqual(ok, column_map([meta_table_1],[{undefined,'*'}])),

    ?assertEqual(ok, drop_table(meta_table_3)),
    ?assertEqual(ok, drop_table(meta_table_2)),
    ?assertEqual(ok, drop_table(meta_table_1)),
    io:format(user, "success ~p~n", [drop_tables]),
    ok.

