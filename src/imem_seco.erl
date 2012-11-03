-module(imem_seco).

-include_lib("eunit/include/eunit.hrl").

-export([ schema/0
        , add_attribute/2
        , update_opts/2
        , find_imem_nodes/2
        , all_tables/1
        , columns/2
        ]).

-export([ authenticate/3
        , login/1
        , change_credentials/3
        , logout/1
        ]).

-export([ create_table/3
        , create_table/4
        , create_cluster_table/4
		, drop_table/2
        , read_all/2
        , select/3
        , read/3                 
        , insert/3    
        , write/3
        , delete/3
		]).

%% one to one from dd_account ------------ AA FUNCTIONS _--------

authenticate(SessionId, Name, Credentials) ->
    dd_account:authenticate(SessionId, Name, Credentials).

login(SeCo) ->
    dd_account:login(SeCo).

change_credentials(SeCo, OldCred, NewCred) ->
    dd_account:change_credentials(SeCo, OldCred, NewCred).

logout(SeCo) ->
    dd_account:logout(SeCo).

%% one to one from imme_if -------------- HELPER FUNCTIONS ------

schema() ->         %% ToDo: move to imem_if and execute there
    [Schema|_]=re:split(filename:basename(mnesia:system_info(directory)),"[.]",[{return,list}]),
    Schema.

add_attribute(A, Opts) -> imem_if:add_attribute(A, Opts).

update_opts(T, Opts) -> imem_if:update_opts(T, Opts).

%% imem_if but security context added --- META INFORMATION ------

find_imem_nodes(_SeCo, Schema) ->
    imem_if:find_imem_nodes(Schema).

all_tables(_SeCo) ->
    imem_if:all_tables().

columns(_SeCo, TableName) ->
    imem_if:columns(TableName).

%% imem_if but security context added --- DATA DEFINITION -------

create_cluster_table(SeCo, TableName, Columns, Opts) ->
    case dd_seco:have_permission(SeCo, create_table) of
        true ->     imem_if:create_cluster_table(TableName, Columns, Opts);
        false ->    {error, {"Create cluster table unauthorized", SeCo}};
        Error ->    Error
    end.

create_table(SeCo, TableName, Columns, Opts) ->
    case dd_seco:have_permission(SeCo, create_table) of
        true ->     imem_if:create_table(TableName, Columns, Opts);
        false ->    {error, {"Create table unauthorized", SeCo}};
        Error ->    Error
    end.

create_table(SeCo, Table, Opts) ->
    case dd_seco:have_permission(SeCo, create_table) of
        true ->     imem_if:create_table(Table, Opts);
        false ->    {error, {"Create table unauthorized", SeCo}};
        Error ->    Error
    end.

drop_table(SeCo, Table) ->
    case dd_seco:have_permission(SeCo, drop_table) of
        true ->     imem_if:drop_table(Table);
        false ->    {error, {"Drop table unauthorized", SeCo}};
        Error ->    Error
    end.

%% imem_if but security context added --- DATA ACCESS CRUD -----

insert(SeCo, TableName, Row) ->
    case dd_seco:have_permission(SeCo, {TableName,insert}) of
        true ->     imem_if:insert(TableName, Row) ;
        false ->    {error, {"Insert unauthorized", SeCo}};
        Error ->    Error
    end.

read(SeCo, TableName, Key) ->
    case dd_seco:have_permission(SeCo, {TableName,select}) of
        true ->     imem_if:read(TableName, Key) ;
        false ->    {error, {"Select unauthorized", SeCo}};
        Error ->    Error
    end.

write(SeCo, TableName, Row) ->
    case dd_seco:have_permission(SeCo, {TableName,insert}) of
        true ->     imem_if:write(TableName, Row) ;
        false ->    {error, {"Insert/update unauthorized", SeCo}};
        Error ->    Error
    end.

delete(SeCo, TableName, Key) ->
    case dd_seco:have_permission(SeCo, {TableName,delete}) of
        true ->     imem_if:delete(TableName, Key) ;
        false ->    {error, {"Delete unauthorized", SeCo}};
        Error ->    Error
    end.

read_all(SeCo, TableName) ->
    case dd_seco:have_permission(SeCo, {TableName,select}) of
        true ->     imem_if:read_all(TableName) ;
        false ->    {error, {"Select unauthorized", SeCo}};
        Error ->    Error
    end.

select(SeCo, TableName, MatchSpec) ->
    case dd_seco:have_permission(SeCo, {TableName,select}) of
        true ->     imem_if:select(TableName, MatchSpec) ;
        false ->    {error, {"Select unauthorized", SeCo}};
        Error ->    Error
    end.

