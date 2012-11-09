-module(imem_meta).

-define(META_TABLES,[ddTable]).

-include_lib("eunit/include/eunit.hrl").

-include("dd_meta.hrl").

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
    try
        catch create_table(ddTable, record_info(fields, ddTable), [], system),
        check_table(ddTable),
        io:format(user, "~p started!~n", [?MODULE])
    catch
        _:_ -> gen_server:cast(self(),{stop, "Insufficient resources for start"}) 
    end,
    {ok,#state{}}.

handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast({stop, Reason}, State) ->
    {stop,{shutdown,Reason},State};
handle_cast(_Request, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reson, _State) -> ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

format_status(_Opt, [_PDict, _State]) -> ok.


check_table(Table) ->
    imem_if:table_size(Table).

drop_meta_tables() ->
    drop_table(ddTable).     


return_ok(ok) -> ok;
return_ok(Error) -> ?SystemException(Error).

return_list(L) when is_list(L) -> L;
return_list(Error) -> ?SystemException(Error).


create_table(Table, RecordInfo, Opts, Owner) ->
    case imem_if:create_table(Table, RecordInfo, Opts) of
        ok -> 
            return_ok(imem_if:write(ddTable, #ddTable{id=Table, recinfo=RecordInfo, opts=Opts, owner=Owner}));
        Error -> 
            ?SystemException(Error)
    end.

create_table(Table, RecordInfo, Opts) ->
    case imem_if:create_table(Table, RecordInfo, Opts) of
        ok -> 
            return_ok(imem_if:write(ddTable, #ddTable{id=Table, recinfo=RecordInfo, opts=Opts, owner=system}));
        Error -> 
            ?SystemException(Error)
    end.

drop_table(Table) -> 
    return_ok(imem_if:drop_table(Table)).

%% one to one from imme_if -------------- HELPER FUNCTIONS ------

schema() ->
    return_list(imem_if:schema()).

schema(Node) ->
    return_list(imem_if:schema(Node)).

add_attribute(A, Opts) -> 
    return_list(imem_if:add_attribute(A, Opts)).

update_opts(T, Opts) ->
    return_list(imem_if:update_opts(T, Opts)).

system_table(Table) ->
    case lists:member(Table,?META_TABLES) of
        true ->     true;
        false ->    imem_if:system_table(Table) 
    end.


%% imem_if but security context added --- META INFORMATION ------

data_nodes() ->
    return_list(imem_if:data_nodes()).

all_tables() ->
    return_list(imem_if:all_tables()).

table_columns(Table) ->
    return_list(imem_if:table_columns(Table)).

table_size(Table) ->
    imem_if:table_size(Table).

read(Table, Key) -> 
    return_list(imem_if:read(Table, Key)).

read(Table) ->
    return_list(imem_if:read(Table)).

read_block(Table, Key, BlockSize) ->
    return_list(imem_if:read_block(Table, Key, BlockSize)).    

select(all_tables, MatchSpec) ->
    select(ddTable, MatchSpec);
select(Table, MatchSpec) ->
    return_list(imem_if:select(Table, MatchSpec)).

select(all_tables, MatchSpec, Limit) ->
    select(ddTable, MatchSpec, Limit);
select(Table, MatchSpec, Limit) ->
    return_list(imem_if:select(Table, MatchSpec, Limit)).

select(Continuation) ->
    return_list(imem_if:select(Continuation)).

write(Table, Record) -> 
    return_ok(imem_if:write(Table, Record)).

insert(Table, Row) ->
    return_ok(imem_if:insert(Table, Row)).

delete(Table, Key) ->
    return_ok(imem_if:delete(Table, Key)).

truncate(Table) ->
    return_ok(imem_if:truncate(Table)).



