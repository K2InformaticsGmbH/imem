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
        , close/2
        ]).

-record(fetchCtx,               %% state for fetch process
                    { pid       ::pid()
                    , monref    ::any()             %% fetch monitor ref
                    , status    ::atom()            %% undefined | running | aborted
                    , metarec   ::tuple()
                    , blockSize=100 ::integer()     %% could be adaptive
                    , remaining ::integer()         %% rows remaining to be fetched. initialized to Limit and decremented
                    }).

-record(state,                  %% state for statment process, including fetch subprocess
                    { statement
                    , seco=none
                    , fetchCtx=#fetchCtx{}    
                    , reply                  %% TCP socket or Pid
                    }).

%% gen_server -----------------------------------------------------

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

fetch_recs(SKey, Pid, Sock, IsSec) when is_pid(Pid) ->
    gen_server:cast(Pid, {fetch_recs, Sock, IsSec, SKey}).

fetch_recs_async(SKey, Pid, Sock, IsSec) when is_pid(Pid) ->
    gen_server:cast(Pid, {fetch_recs_async, Sock, IsSec, SKey}).

close(SKey, Pid) when is_pid(Pid) ->
    gen_server:cast(Pid, {close, SKey}).

init([Statement]) ->
    {ok, #state{statement=Statement}}.

handle_call({set_seco, SKey}, _From, State) ->    
    {reply,ok,State#state{seco=SKey}}.

handle_cast({fetch_recs, Sock, IsSec, _SKey}, #state{statement=Stmt, seco=SKey}=State) ->
    case length(Stmt#statement.tables) of
        1 ->    fetch_recs_single(hd(Stmt#statement.tables), Sock, IsSec, Stmt, SKey, State);        
        _ ->    ?UnimplementedException({"Joins not supported",Stmt#statement.tables})
    end;
handle_cast({fetch_recs_async, Sock, _IsSec, _SKey}, #state{fetchCtx=#fetchCtx{status=aborted}}=State) ->
    Result = term_to_binary({error,"Fetch aborted"}),
    send_reply_to_client(Sock, Result),
    {noreply, State}; 
handle_cast({fetch_recs_async, Sock, IsSec, _SKey}, #state{statement=Stmt, seco=SKey, fetchCtx=#fetchCtx{pid=Pid}}=State) ->
    #statement{tables=[{_Schema,Table,_Alias}|_], block_size=BlockSize, matchspec=MatchSpec, meta=MetaMap, limit=Limit} = Stmt,
    MetaRec = list_to_tuple([if_call_mfa(IsSec, meta_field_value, [SKey, N]) || N <- MetaMap]),
    NewTransCtx = case Pid of
        undefined ->
            case if_call_mfa(IsSec, fetch_start, [SKey, self(), Table, MatchSpec, BlockSize]) of
                TransPid when is_pid(TransPid) ->
                    MonitorRef = erlang:monitor(process, TransPid), 
                    TransPid ! next,
                    #fetchCtx{pid=TransPid, monref=MonitorRef, status=running, metarec=MetaRec, blockSize=BlockSize, remaining=Limit};
                Error ->    
                    ?SystemException({"Cannot spawn async fetch process",Error})
            end;
        Pid ->
            Pid ! next,
            #fetchCtx{metarec=MetaRec}
    end,
    {noreply, State#state{reply=Sock,fetchCtx=NewTransCtx}};  
handle_cast({close, _SKey}, State) ->
    % io:format(user, "~p - received close in state ~p~n", [?MODULE, State]),
    {stop, normal, State}; 
handle_cast(Request, State) ->
    io:format(user, "~p - received unsolicited cast ~p~nin state ~p~n", [?MODULE, Request, State]),
    {noreply, State}.

handle_info({row, ?eot}, State) ->
    % io:format(user, "~p - received end of table in state ~p~n", [?MODULE, State]),
    {noreply, State#state{fetchCtx=#fetchCtx{}, reply=undefined}};
handle_info({row, Rows}, #state{reply=Sock, fetchCtx=FetchCtx0, statement=Stmt}=State) ->
    #fetchCtx{metarec=MetaRec, blockSize=BlockSize, remaining=Remaining0}=FetchCtx0,
    % io:format(user, "received rows ~p~n", [Rows]),
    RowsRead=length(Rows),
    {Result, Sent} = case length(Stmt#statement.tables) of
        1 ->    Wrap = fun(X) -> {X, MetaRec} end,
                if  
                    ((RowsRead < Remaining0) andalso (RowsRead < BlockSize)) ->
                        {{lists:map(Wrap, Rows), true}, RowsRead};
                    RowsRead < Remaining0 ->
                        {{lists:map(Wrap, Rows), false}, RowsRead};
                    RowsRead == Remaining0 ->
                        {{lists:map(Wrap, Rows), true}, RowsRead};
                    Remaining0 > 0 ->
                        {ResultRows,Rest} = lists:split(Remaining0, Rows),
                        LastKey = lists:nthtail(length(ResultRows)-1, ResultRows),
                        Pred = fun(X) -> (X==LastKey) end,
                        ResultTail = lists:takewhile(Pred, Rest),
                        {{lists:map(Wrap, ResultRows ++ ResultTail), true}, length(ResultRows) + length(ResultTail)};
                    Remaining0 =< 0 ->
                        {{[], true}, 0}
                end;
        _ ->    ?UnimplementedException({"Joins not supported",Stmt#statement.tables})
    end,
    % io:format(user, "sending rows ~p~n", [Result]),
    send_reply_to_client(Sock, term_to_binary(Result)),
    FetchCtx1=FetchCtx0#fetchCtx{remaining=Remaining0-Sent},
    {noreply, State#state{fetchCtx=FetchCtx1}};
handle_info({'DOWN', _Ref, process, _Pid, _Reason}, #state{reply=undefined}=State) ->
    % io:format(user, "~p - received expected exit info for monitored pid ~p ref ~p reason ~p~n", [?MODULE, Pid, Ref, Reason]),
    {noreply, State#state{fetchCtx=#fetchCtx{}}}; 
handle_info({'DOWN', Ref, process, Pid, Reason}, State) ->
    io:format(user, "~p - received unexpected exit info for monitored pid ~p ref ~p reason ~p~n", [?MODULE, Pid, Ref, Reason]),
    {noreply, State#state{fetchCtx=#fetchCtx{pid=undefined, monref=undefined, status=aborted}}};
handle_info(Info, State) ->
    io:format(user, "~p - received unsolicited info ~p~nin state ~p~n", [?MODULE, Info, State]),
    {noreply, State}.

terminate(_Reason, #state{fetchCtx=#fetchCtx{pid=Pid, monref=undefined}}) -> 
    io:format(user, "~p - terminating monitor not found~n", [?MODULE]),
    catch Pid ! abort, 
    ok;
terminate(_Reason, #state{fetchCtx=#fetchCtx{pid=Pid, monref=MonitorRef}}) ->
    io:format(user, "~p - terminating after demonitor~n", [?MODULE]),
    erlang:demonitor(MonitorRef, [flush]),
    catch Pid ! abort, 
    ok.

code_change(_OldVsn, State, _Extra) -> {ok, State}.

send_reply_to_client(SockOrPid, Result) ->
    case SockOrPid of
        Pid when is_pid(Pid)    -> Pid ! Result;
        Sock                    -> gen_tcp:send(Sock, Result)
    end.

fetch_recs_single({_Schema,Table,_Alias}, Sock, IsSec, Stmt, SKey, State) ->
    #statement{limit=Limit, matchspec=MatchSpec, meta=MetaMap} = Stmt,
    {Result, NewState} =
    try
        MetaRec = case IsSec of
            false ->    list_to_tuple([imem_meta:meta_field_value(N) || N <- MetaMap]);
            true ->     list_to_tuple([imem_sec:meta_field_value(SKey, N) || N <- MetaMap])
        end,
        Wrap = fun(X) -> {X, MetaRec} end,
        {Rows, Complete} = if_call_mfa(IsSec, select, [SKey, Table, MatchSpec, Limit]),
        {term_to_binary({lists:map(Wrap, Rows), Complete}), State}
    catch
        Class:Reason -> {term_to_binary({Class, Reason}),State}
    end,
    case Sock of
        Pid when is_pid(Pid)    -> Pid ! Result;
        Sock                    -> gen_tcp:send(Sock, Result)
    end,
    {noreply, NewState}.


% fetch_recs_join([{_Schema,Table,_Alias}|Tables], Sock, IsSec, Stmt, SKey, State) ->
%     #statement{limit=Limit, matchspec=MatchSpec, joinspec=JoinSpec} = Stmt,
%     {Result, NewState} =
%     try
%         {Rows, Complete} = if_call_mfa(IsSec, select, [SKey, Table, MatchSpec, Limit]),
%         JoinedRecs = fetch_rec_join_run(Rows,Tables,JoinSpec),
%         {term_to_binary({JoinedRecs, Complete}), State}
%     catch
%         Class:Reason -> {term_to_binary({Class, Reason}),State}
%     end,
%     case Sock of
%         Pid when is_pid(Pid)    -> Pid ! Result;
%         Sock                    -> gen_tcp:send(Sock, Result)
%     end,
%     {noreply, NewState}.

% fetch_rec_join_run(Rows, Tables, JoinSpec) ->
%     fetch_rec_join_run(Rows, Tables, JoinSpec, Tables, JoinSpec, [], []).

% fetch_rec_join_run([], _Tables, _JoinSpec, _TS, _JS, FAcc, _RAcc) ->
%     FAcc;
% fetch_rec_join_run([_|Rows], Tables, Joinspec, [], [], FAcc, RAcc) ->
%     fetch_rec_join_run(Rows, Tables, Joinspec, Tables, Joinspec, [list_to_tuple(lists:reverse(RAcc))|FAcc], []);
% fetch_rec_join_run([Row|Rows], Tables, JoinSpec, [_Tab|Tabs], [_JS|JSs], FAcc, RAcc) ->
%     ?UnimplementedException({"Joins not supported",Tables}),
%     fetch_rec_join_run([Row|Rows], Tables, JoinSpec, Tabs, JSs, FAcc, RAcc).



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

