-module(imem_meta).

-define(META_TABLES,[ddTable]).

-include_lib("eunit/include/eunit.hrl").

-include("dd_meta.hrl").

-export([ schema/0
        , schema/1
        , data_nodes/0
        , all_tables/0
        , table_columns/1
        , table_size/1
        , system_table/1
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
		]).


create_table(Table, RecordInfo, Opts, Owner) ->
    case imem_if:create_table(Table, RecordInfo, Opts) of
        {atomic,ok} -> 
            imem_if:write(ddTable, #ddTable{id=Table, recinfo=RecordInfo, opts=Opts, owner=Owner}),
            {atomic,ok};
        Error -> 
            Error
    end.

create_table(Table, RecordInfo, Opts) ->
    case imem_if:create_table(Table, RecordInfo, Opts) of
        {atomic,ok} -> 
            imem_if:write(ddTable, #ddTable{id=Table, recinfo=RecordInfo, opts=Opts, owner=system}),
            {atomic,ok};
        Error -> 
            Error
    end.

drop_table(Table) -> 
    imem_if:drop_table(Table).

%% one to one from imme_if -------------- HELPER FUNCTIONS ------

schema() ->
    imem_if:schema().

schema(Node) ->
    imem_if:schema(Node).

add_attribute(A, Opts) -> 
    imem_if:add_attribute(A, Opts).

update_opts(T, Opts) ->
    imem_if:update_opts(T, Opts).

system_table(Table) ->
    case lists:member(Table,?META_TABLES) of
        true ->     true;
        false ->    imem_if:system_table(Table) 
    end.


%% imem_if but security context added --- META INFORMATION ------

data_nodes() ->
    imem_if:data_nodes().

all_tables() ->
    imem_if:all_tables().

table_columns(TableName) ->
    imem_if:table_columns(TableName).

table_size(TableName) ->
    imem_if:table_size(TableName).


read(Table, Key) -> 
    imem_if:read(Table, Key).

read(TableName) ->
    imem_if:read(TableName).

read_block(TableName, Key, BlockSize) ->
    imem_if:read_block(TableName, Key, BlockSize).    

select(TableName, MatchSpec) ->
    imem_if:select(TableName, MatchSpec).

select(TableName, MatchSpec, Limit) ->
    imem_if:select(TableName, MatchSpec, Limit).

select(Continuation) ->
    imem_if:select(Continuation).

write(Table, Record) -> 
    imem_if:write(Table, Record).

insert(TableName, Row) ->
    imem_if:insert(TableName, Row).

delete(TableName, Key) ->
    imem_if:delete(TableName, Key).    

