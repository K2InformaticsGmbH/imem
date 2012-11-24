-module(imem_statement).

-include("imem_sql.hrl").

%% gen_server
-behaviour(gen_server).
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3]).

-export([ exec/5
        , create_stmt/3
        , fetch_recs/4      %% ToDo: implement proper return of RowFun(), match conditions and joins
%        , fetch/4          %% ToDo: implement plain mnesia fetch for columns in select fields (or in matchspec)
        , read_block/4      %% ToDo: remove
        ]).

-record(state, { statement
               , seco
               }).

exec(SKey, Statement, BlockSize, Schema, IsSec) ->
    imem_sql:exec(SKey, Statement, BlockSize, Schema, IsSec).   %% ToDo: remove this (in imem_sql now)

% statement has its own SKey
fetch_recs(_SKey, Pid, Sock, IsSec) when is_pid(Pid) ->
    gen_server:cast(Pid, {fetch_recs, Sock, IsSec}).

read_block(_SKey, Pid, Sock, IsSec) when is_pid(Pid) ->
    gen_server:cast(Pid, {read_block, Sock, IsSec}).

%% gen_server
create_stmt(Statement, SKey, IsSec) ->
    case IsSec of
        false -> 
            gen_server:start(?MODULE, [Statement], []);
        true ->
            {ok, Pid} = gen_server:start(?MODULE, [Statement], []),            
            NewSKey = imem_sec:clone_seco(SKey, Pid),
            ok = gen_server:call(Pid, {set_seco, NewSKey}),
            {ok, Pid}
    end.


init([Statement]) ->
    {ok, #state{statement=Statement}}.

handle_call({set_seco, SKey}, _From, State) ->    
    {reply,ok,State#state{seco=SKey}}.

handle_cast({fetch_recs, Sock, IsSec}, #state{statement=Stmt, seco=SKey}=State) ->
    #statement{tables=[Table|_], limit=Limit, matchspec=Matchspec} = Stmt,
    {Result, NewState} =
    try
        {Rows, Complete} = if_call_mfa(IsSec, select, [SKey, Table, Matchspec, Limit]),
        {term_to_binary({Rows, Complete}), State}
    catch
        Class:Reason -> {Class, Reason}
    end,
    case Sock of
        Pid when is_pid(Pid)    -> Pid ! Result;
        Sock                    -> gen_tcp:send(Sock, Result)
    end,
    {noreply, NewState};  
handle_cast({read_block, Sock, IsSec}, #state{statement=Stmt, seco=SKey}=State) ->
    #statement{tables=[Table|_], key=Key, block_size=BlockSize} = Stmt,
    {Result, NewState} = 
    try
        case if_call_mfa(IsSec, read_block, [SKey, Table, Key, BlockSize]) of
            {Rows, ?eot} ->   {term_to_binary({Rows, true}), State#state{statement=Stmt#statement{key=?eot}}};  
            {Rows, NewKey} -> {term_to_binary({Rows, false}), State#state{statement=Stmt#statement{key=NewKey}}}
        end
    catch
        Class:Reason -> {Class, Reason}
    end,
    case Sock of
        Pid when is_pid(Pid)    -> Pid ! Result;
        Sock                    -> gen_tcp:send(Sock, Result)
    end,
    {noreply, NewState};  
%handle_cast({read_block, Sock, SKey, IsSec}, #state{statement=Stmt}=State) ->
%    #statement{table=TableName,key=Key,block_size=BlockSize} = Stmt,
%    {NewKey, Rows} = call_mfa(IsSec,read_block,[SKey,TableName, Key, BlockSize]),
%    gen_tcp:send(Sock, term_to_binary({ok, Rows})),
%    {noreply,State#state{statement=Stmt#statement{key=NewKey}}};
handle_cast(_Request, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) -> ok.

code_change(_OldVsn, State, _Extra) -> {ok, State}.


%% --Interface functions  (calling imem_if for now, not exported) ---------

if_call_mfa(IsSec,Fun,Args) ->
    case IsSec of
        true -> apply(imem_sec,Fun,Args);
        _ ->    apply(imem_meta, Fun, lists:nthtail(1, Args))
    end.

%% TESTS ------------------------------------------------------------------

-include_lib("eunit/include/eunit.hrl").

setup() -> 
    ?imem_test_setup().

teardown(_) -> 
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
    
test_without_sec(SKey) -> 
    test_with_or_without_sec(SKey, false).

test_with_sec(SKey) ->
    test_with_or_without_sec(SKey, true).

test_with_or_without_sec(_SKey, IsSec) ->
    try
        % ClEr = 'ClientError',
        % SeEx = 'SecurityException',
        io:format(user, "----TEST--- ~p ----Security ~p ~n", [?MODULE, IsSec])
    catch
        Class:Reason ->  io:format(user, "Exception ~p:~p~n~p~n", [Class, Reason, erlang:get_stacktrace()]),
        ?assert( true == "all tests completed")
    end,
    ok. 

