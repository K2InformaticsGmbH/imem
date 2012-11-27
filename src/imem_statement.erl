-module(imem_statement).

-include("imem_seco.hrl").

%% gen_server
-behaviour(gen_server).
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3]).

-export([ create_stmt/3
        , fetch_recs/4      %% ToDo: implement proper return of RowFun(), match conditions and joins
        , fetch_recs_async/4      %% ToDo: implement proper return of RowFun(), match conditions and joins
%        , fetch/4          %% ToDo: implement plain mnesia fetch for columns in select fields (or in matchspec)
        , read_block/4      %% ToDo: remove
        ]).

-record(state, { statement
               , seco
               , trans_pid
               , reply % TCP socket or Pid
               }).

% statement has its own SKey
fetch_recs(_SKey, Pid, Sock, IsSec) when is_pid(Pid) ->
   gen_server:cast(Pid, {fetch_recs, Sock, IsSec}).

read_block(_SKey, Pid, Sock, IsSec) when is_pid(Pid) ->
    gen_server:cast(Pid, {read_block, Sock, IsSec}).

fetch_recs_async(_SKey, Pid, Sock, IsSec) when is_pid(Pid) ->
    gen_server:cast(Pid, {fetch_recs_async, Sock, IsSec}).

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
    case length(Stmt#statement.tables) of
        1 ->    fetch_recs_single(hd(Stmt#statement.tables), Sock, IsSec, Stmt, SKey, State);
        _ ->    fetch_recs_join(Stmt#statement.tables, Sock, IsSec, Stmt, SKey, State)
    end;
handle_cast({read_block, Sock, IsSec}, #state{statement=Stmt, seco=SKey}=State) ->
    #statement{tables=[{_Schema,Table,_Alias}|_], key=Key, block_size=BlockSize} = Stmt,
    {Result, NewState} = 
    try
        case if_call_mfa(IsSec, read_block, [SKey, Table, Key, BlockSize]) of
            {Rows, ?eot} ->   {term_to_binary({Rows, true}), State#state{statement=Stmt#statement{key=?eot}}};  
            {Rows, NewKey} -> {term_to_binary({Rows, false}), State#state{statement=Stmt#statement{key=NewKey}}}
        end
    catch
        Class:Reason -> {term_to_binary({Class, Reason}),State}
    end,
    case Sock of
        Pid when is_pid(Pid)    -> Pid ! Result;
        Sock                    -> gen_tcp:send(Sock, Result)
    end,
    {noreply, NewState};  
handle_cast({fetch_recs_async, Sock, _IsSec}, #state{statement=Stmt, seco=_SKey, trans_pid=Pid}=State) ->
    #statement{tables=[{_Schema,Table,_Alias}|_], block_size=BlockSize} = Stmt,
    NewTransPid = case Pid of
        undefined ->
            TransPid = imem_if:start_trans(self(), Table, [{'$1', [], ['$_']}], BlockSize),
            TransPid ! next,
            TransPid;
        Pid ->
            Pid ! next,
            Pid
    end,
    {noreply, State#state{reply=Sock,trans_pid=NewTransPid}};  
handle_cast(_Request, State) ->
    {noreply, State}.

handle_info({row, eot}, State) ->
    {noreply, State#state{trans_pid=undefined, reply=undefined}};
handle_info({row, Rows}, #state{trans_pid=Pid, reply=Sock, seco=_SKey}=State) ->
    io:format(user, "received rows ~p~n", Rows),
    case Sock of
        Pid when is_pid(Pid)    -> Pid ! Rows;
        Sock                    -> gen_tcp:send(Sock, Rows)
    end,
    {noreply, State};
handle_info(Info, State) ->
    io:format(user, "imem_statement handle_info shouldn't come here ~p~n", [Info]),
    {noreply, State}.

terminate(_Reason, _State) -> ok.

code_change(_OldVsn, State, _Extra) -> {ok, State}.

wrap(X) -> {X}.

fetch_recs_single({_Schema,Table,_Alias}, Sock, IsSec, Stmt, SKey, State) ->
    #statement{limit=Limit, matchspec=MatchSpec} = Stmt,
    {Result, NewState} =
    try
        {Rows, Complete} = if_call_mfa(IsSec, select, [SKey, Table, MatchSpec, Limit]),
        {term_to_binary({lists:map(fun wrap/1, Rows), Complete}), State}
    catch
        Class:Reason -> {term_to_binary({Class, Reason}),State}
    end,
    case Sock of
        Pid when is_pid(Pid)    -> Pid ! Result;
        Sock                    -> gen_tcp:send(Sock, Result)
    end,
    {noreply, NewState}.

fetch_recs_join([{_Schema,Table,_Alias}|Tables], Sock, IsSec, Stmt, SKey, State) ->
    #statement{limit=Limit, matchspec=MatchSpec, joinspec=JoinSpec} = Stmt,
    {Result, NewState} =
    try
        {Rows, Complete} = if_call_mfa(IsSec, select, [SKey, Table, MatchSpec, Limit]),
        JoinedRecs = fetch_rec_join_run(Rows,Tables,JoinSpec),
        {term_to_binary({JoinedRecs, Complete}), State}
    catch
        Class:Reason -> {term_to_binary({Class, Reason}),State}
    end,
    case Sock of
        Pid when is_pid(Pid)    -> Pid ! Result;
        Sock                    -> gen_tcp:send(Sock, Result)
    end,
    {noreply, NewState}.

fetch_rec_join_run(Rows, Tables, JoinSpec) ->
    fetch_rec_join_run(Rows, Tables, JoinSpec, Tables, JoinSpec, [], []).

fetch_rec_join_run([], _Tables, _JoinSpec, _TS, _JS, FAcc, _RAcc) ->
    FAcc;
fetch_rec_join_run([_|Rows], Tables, Joinspec, [], [], FAcc, RAcc) ->
    fetch_rec_join_run(Rows, Tables, Joinspec, Tables, Joinspec, [list_to_tuple(lists:reverse(RAcc))|FAcc], []);
fetch_rec_join_run([Row|Rows], Tables, JoinSpec, [_Tab|Tabs], [_JS|JSs], FAcc, RAcc) ->
    ?UnimplementedException({"Joins not supported",Tables}),
    fetch_rec_join_run([Row|Rows], Tables, JoinSpec, Tabs, JSs, FAcc, RAcc).



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

