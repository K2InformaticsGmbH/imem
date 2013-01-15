-module(imem_statement).

-include("imem_seco.hrl").
-include("imem_sql.hrl").

%% gen_server
-behaviour(gen_server).
-export([ init/1
        , handle_call/3
        , handle_cast/2
        , handle_info/2
        , terminate/2
        , code_change/3
        ]).

-export([ update_prepare/5          %% stateless creation of update plan from change list
        , update_cursor_prepare/4   %% stateful creation of update plan (stored in state)
        , update_cursor_execute/4   %% stateful execution of update plan (fetch aborted first)
        , fetch_recs/5              %% simulation of synchronous fetch
        , fetch_recs_sort/5         %% simulation of synchronous fetch followed by a lists:sort
        , fetch_recs_async/4        %% async streaming fetch
        , fetch_recs_async/5        %% async streaming fetch with options ({tail_mode,)
        , fetch_close/3
        , close/2
        ]).

-export([ create_stmt/3
        ]).

-record(fetchCtx,               %% state for fetch process
                    { pid       ::pid()
                    , monref    ::any()             %% fetch monitor ref
                    , status    ::atom()            %% undefined | waiting | fetching | aborted
                    , metarec   ::tuple()
                    , blockSize=100 ::integer()     %% could be adaptive
                    , remaining ::integer()         %% rows remaining to be fetched. initialized to Limit and decremented
                    , opts = [] ::list()            %% fetch options like {tail_mode,true}
                    , tailSpec  ::any()             %% compiled matchspec for master table condition (bound with MetaRec) 
                    }).

-record(state,                  %% state for statment process, including fetch subprocess
                    { statement
                    , seco=none
                    , fetchCtx=#fetchCtx{}    
                    , reply                 %% reply destination TCP socket or Pid
                    , updPlan = []          %% bulk execution plan (table updates/inserts/deletes)
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

fetch_recs(SKey, Pid, Sock, Timeout, IsSec) when is_pid(Pid) ->
    gen_server:cast(Pid, {fetch_recs_async, IsSec, SKey, Sock,[]}),
    Result = try
        case receive 
            R ->    R
        after Timeout ->
            io:format(user, "~p - fetch_recs timeout ~p~n", [?MODULE, Timeout]),
            gen_server:call(Pid, {fetch_close, IsSec, SKey}), 
            ?ClientError({"Fetch timeout, increase timeout and retry",Timeout})
        end of
            {Pid,{List, true}} ->   List;
            {Pid,{List, false}} ->  
                io:format(user, "~p - fetch_recs too much data~n", [?MODULE]),
                gen_server:call(Pid, {fetch_close, IsSec, SKey}), 
                ?ClientError({"Too much data, increase block size or receive in streaming mode",length(List)});
            Error ->
                io:format(user, "~p - fetch_recs bad async receive ~p~n", [?MODULE, Error]),
                gen_server:call(Pid, {fetch_close, IsSec, SKey}),                
                ?SystemException({"Bad async receive",Error})            
        end
    after
        gen_server:call(Pid, {fetch_close, IsSec, SKey})
    end,
    Result.

fetch_recs_sort(SKey, Pid, Sock, Timeout, IsSec) when is_pid(Pid) ->
    lists:sort(fetch_recs(SKey, Pid, Sock, Timeout, IsSec)).

fetch_recs_async(SKey, Pid, Sock, IsSec) ->
    fetch_recs_async(SKey, Pid, Sock, [], IsSec).

fetch_recs_async(SKey, Pid, Sock, Opts, IsSec) when is_pid(Pid) ->
    gen_server:cast(Pid, {fetch_recs_async, IsSec, SKey, Sock, Opts}).

fetch_close(SKey, Pid, IsSec) when is_pid(Pid) ->
    gen_server:call(Pid, {fetch_close, IsSec, SKey}).

update_cursor_prepare(SKey, Pid, IsSec, ChangeList) when is_pid(Pid) ->
    case gen_server:call(Pid, {update_cursor_prepare, IsSec, SKey, ChangeList}) of
        ok ->   ok;
        Error-> throw(Error)
    end.

update_cursor_execute(SKey, Pid, IsSec, none) when is_pid(Pid) ->
    case gen_server:call(Pid, {update_cursor_execute, IsSec, SKey, none}) of
        ok ->       ok;
        Error ->    throw(Error)
    end; 
update_cursor_execute(SKey, Pid, IsSec, optimistic) when is_pid(Pid) ->
    case gen_server:call(Pid, {update_cursor_execute, IsSec, SKey, optimistic}) of
        ok ->       ok;
        Error ->    throw(Error)
    end.

close(SKey, Pid) when is_pid(Pid) ->
    gen_server:cast(Pid, {close, SKey}).

init([Statement]) ->
    imem_meta:log_to_db(debug,?MODULE,init,[],Statement#statement.stmt_str),
    {ok, #state{statement=Statement}}.

handle_call({set_seco, SKey}, _From, State) ->    
    {reply,ok,State#state{seco=SKey}};
handle_call({update_cursor_prepare, IsSec, _SKey, ChangeList}, _From, #state{statement=Stmt, seco=SKey}=State) ->
    {Reply, UpdatePlan1} = try
        {ok, update_prepare(IsSec, SKey, Stmt#statement.tables, Stmt#statement.cols, ChangeList)}
    catch
        _:Reason ->  {Reason, []}
    end,
    {reply, Reply, State#state{updPlan=UpdatePlan1}};  
handle_call({update_cursor_execute, IsSec, _SKey, Lock}, _From, #state{seco=SKey, fetchCtx=FetchCtx0, updPlan=UpdatePlan}=State) ->
    Reply = try 
        case FetchCtx0#fetchCtx.monref of
            undefined ->    ok;
            MonitorRef ->   kill_fetch(MonitorRef, FetchCtx0#fetchCtx.pid)
        end,
        if_call_mfa(IsSec,update_tables,[SKey, UpdatePlan, Lock]) 
    catch
        _:Reason ->  Reason
    end,
    % io:format(user, "~p - update_cursor_execute result ~p~n", [?MODULE, Reply]),
    FetchCtx1 = FetchCtx0#fetchCtx{monref=undefined, status=aborted, metarec=undefined},
    {reply, Reply, State#state{fetchCtx=FetchCtx1}};
handle_call({fetch_close, _IsSec, _SKey}, _From, #state{statement=Stmt,fetchCtx=#fetchCtx{status=tailing}}=State) ->
    imem_meta:log_to_db(debug,?MODULE,handle_call,[{from,_From},{status,tailing}],"fetch_close unsubscribe"),
    unsubscribe(Stmt),
    {reply, ok, State#state{fetchCtx=#fetchCtx{}}};
handle_call({fetch_close, _IsSec, _SKey}, _From, #state{fetchCtx=#fetchCtx{pid=undefined, monref=undefined}}=State) ->
    imem_meta:log_to_db(debug,?MODULE,handle_call,[{from,_From},{status,undefined}],"fetch_close ignored"),
    {reply, ok, State#state{fetchCtx=#fetchCtx{}}};
handle_call({fetch_close, _IsSec, _SKey}, _From, #state{fetchCtx=#fetchCtx{pid=Pid, monref=MonitorRef, status=Status}}=State) ->
    imem_meta:log_to_db(debug,?MODULE,handle_call,[{from,_From},{status,Status}],"fetch_close kill"),
    kill_fetch(MonitorRef, Pid), 
    {reply, ok, State#state{fetchCtx=#fetchCtx{}}}.

handle_cast({fetch_recs_async, _IsSec, _SKey, Sock, _Opts}, #state{fetchCtx=#fetchCtx{status=aborted}}=State) ->
    imem_meta:log_to_db(warning,?MODULE,handle_cast,[{sock,Sock},{opts,_Opts},{status,aborted}],"fetch_recs_async rejected"),
    send_reply_to_client(Sock, {error,"Fetch aborted, execute fetch_close before refetch"}),
    {noreply, State}; 
handle_cast({fetch_recs_async, IsSec, _SKey, Sock, Opts}, #state{statement=Stmt, seco=SKey, fetchCtx=FetchCtx0}=State) ->
    #statement{tables=[{_Schema,Table,_Alias}|_], block_size=BlockSize, matchspec={MatchSpec0,Binds}, meta=MetaFields, limit=Limit} = Stmt,
    imem_meta:log_to_db(debug,?MODULE,handle_cast,[{sock,Sock},{opts,Opts},{status,FetchCtx0#fetchCtx.status}],"fetch_recs_async"),
    MetaRec = list_to_tuple([if_call_mfa(IsSec, meta_field_value, [SKey, N]) || N <- MetaFields]),
    % io:format(user,"Table : ~p~n", [Table]),
    % io:format(user,"MetaRec : ~p~n", [MetaRec]),
    % io:format(user,"Binds : ~p~n", [Binds]),
    [{MatchHead, Guards0, [Result]}] = MatchSpec0,
    % io:format(user,"Guards before bind : ~p~n", [Guards0]),
    Guards1 = case Guards0 of
        [] ->       [];
        [Guard0] -> [imem_sql:simplify_matchspec(select_bind(MetaRec, Guard0, Binds))]
    end,
    % io:format(user,"Guards after  bind : ~p~n", [Guards1]),
    MatchSpec = [{MatchHead, Guards1, [Result]}],
    TailSpec = ets:match_spec_compile(MatchSpec),
    FetchCtx1 = case FetchCtx0#fetchCtx.pid of
        undefined ->
            case if_call_mfa(IsSec, fetch_start, [SKey, self(), Table, MatchSpec, BlockSize, Opts]) of
                TransPid when is_pid(TransPid) ->
                    MonitorRef = erlang:monitor(process, TransPid),
                    TransPid ! next,
                    % io:format(user, "~p - fetch opts ~p~n", [?MODULE,Opts]), 
                    #fetchCtx{pid=TransPid, monref=MonitorRef, status=waiting, metarec=MetaRec, blockSize=BlockSize, remaining=Limit, opts=Opts, tailSpec=TailSpec};
                Error ->    
                    ?SystemException({"Cannot spawn async fetch process",Error})
            end;
        Pid ->
            Pid ! next,
            % io:format(user, "~p - fetch opts ~p~n", [?MODULE,Opts]), 
            FetchCtx0#fetchCtx{metarec=MetaRec, opts=Opts}
    end,
    {noreply, State#state{reply=Sock,fetchCtx=FetchCtx1}};  
handle_cast({close, _SKey}, State) ->
    imem_meta:log_to_db(debug,?MODULE,handle_cast,[],"close statement"),
    % io:format(user, "~p - received close in state ~p~n", [?MODULE, State]),
    {stop, normal, State}; 
handle_cast(Request, State) ->
    imem_meta:log_to_db(error,?MODULE,handle_cast,[{request,Request},{state,State}],"receives unsolicited cast"),
    io:format(user, "~p - receives unsolicited cast ~p~nin state ~p~n", [?MODULE, Request, State]),
    {noreply, State}.

handle_info({row, ?eot}, #state{reply=Sock,fetchCtx=FetchCtx0}=State) ->
    % io:format(user, "~p - received end of table in status ~p~n", [?MODULE,FetchCtx0#fetchCtx.status]),
    % io:format(user, "~p - received end of table in state~n~p~n", [?MODULE,State]),
    case FetchCtx0#fetchCtx.status of
        fetching ->
            io:format(user, "~p - late end of table received in state~n~p~n", [?MODULE, State]),        
            imem_meta:log_to_db(warning,?MODULE,handle_info,[{row, ?eot}],"eot"),
            send_reply_to_client(Sock, {[],true}),  
            handle_fetch_complete(State);
        _ ->
            io:format(user, "~p - unexpected end of table received in state~n~p~n", [?MODULE, State]),        
            imem_meta:log_to_db(warning,?MODULE,handle_info,[{row, ?eot}],"eot"),
            {noreply, State}
    end;        
handle_info({mnesia_table_event,{write,Record,_ActivityId}}, #state{reply=Sock,fetchCtx=FetchCtx0,statement=Stmt}=State) ->
    imem_meta:log_to_db(debug,?MODULE,handle_info,[{mnesia_table_event,write}],"tail write"),
    %io:format(user, "~p - received mnesia subscription event ~p ~p~n", [?MODULE, write, Record]),
    #fetchCtx{status=Status,metarec=MetaRec,remaining=Remaining0,tailSpec=TailSpec}=FetchCtx0,
    case Status of
        tailing ->
            case ets:match_spec_run([Record],TailSpec) of
                [] ->  
                    {noreply, State};
                [Rec] ->       
                    case length(Stmt#statement.tables) of
                        1 ->    
                            Wrap = fun(X) -> {X, MetaRec} end,
                            if  
                                Remaining0 > 1 ->
                                    send_reply_to_client(Sock, {lists:map(Wrap, [Rec]),false}),
                                    {noreply, State#state{fetchCtx=FetchCtx0#fetchCtx{remaining=Remaining0-1}}};
                                true ->
                                    send_reply_to_client(Sock, {lists:map(Wrap, [Rec]),true}),
                                    unsubscribe(Stmt),
                                    {noreply, State#state{fetchCtx=#fetchCtx{}}}
                            end;
                        _N ->    
                            case join_rows([Rec], FetchCtx0, Stmt) of
                                [] ->
                                    {noreply, State};
                                Result ->    
                                    Complete = (Remaining0 =< length(Result)),
                                    % io:format(user, "sending rows ~p~n", [Result]),
                                    send_reply_to_client(Sock, {Result, Complete}),
                                    case Complete of
                                        true -> 
                                            unsubscribe(Stmt),
                                            {noreply, State#state{fetchCtx=#fetchCtx{}}};
                                        false ->
                                            {noreply, State#state{fetchCtx=FetchCtx0#fetchCtx{remaining=Remaining0-length(Result)}}}
                                    end
                            end
                    end
            end;
        _ ->
            {noreply, State}
    end;
handle_info({mnesia_table_event,{delete_object, _OldRecord, _ActivityId}}, State) ->
    imem_meta:log_to_db(debug,?MODULE,handle_info,[{mnesia_table_event,delete_object}],"tail delete"),
    % io:format(user, "~p - received mnesia subscription event ~p ~p~n", [?MODULE, delete_object, _OldRecord]),
    {noreply, State};
handle_info({mnesia_table_event,{delete, {_Tab, _Key}, _ActivityId}}, State) ->
    imem_meta:log_to_db(debug,?MODULE,handle_info,[{mnesia_table_event,delete}],"tail delete"),
    % io:format(user, "~p - received mnesia subscription event ~p ~p~n", [?MODULE, delete, {_Tab, _Key}]),
    {noreply, State};
handle_info({row, Rows0}, #state{reply=Sock, fetchCtx=FetchCtx0, statement=Stmt}=State) ->
    #fetchCtx{metarec=MetaRec,remaining=Remaining0,status=Status}=FetchCtx0,
    % io:format(user, "~p - received ~p rows~n", [?MODULE, length(Rows)]),
    % io:format(user, "~p - received rows~n~p~n", [?MODULE, Rows]),
    {Rows1,Complete} = case {Status,Rows0} of
        {waiting,[?sot,?eot|R]} ->
            imem_meta:log_to_db(debug,?MODULE,handle_info,[{row,length(R)}],"data complete"),     
            {R,true};
        {waiting,[?sot|R]} ->            
            imem_meta:log_to_db(debug,?MODULE,handle_info,[{row,length(R)}],"data first"),     
            {R,false};
        {fetching,[?eot|R]} ->
            imem_meta:log_to_db(debug,?MODULE,handle_info,[{row,length(R)}],"data complete"),     
            {R,true};
        {fetching,[?sot,?eot|R]} ->
            imem_meta:log_to_db(warning,?MODULE,handle_info,[{row,length(R)}],"data transaction restart"),     
            handle_fetch_complete(State);
        {fetching,[?sot|R]} ->
            imem_meta:log_to_db(warning,?MODULE,handle_info,[{row,length(R)}],"data transaction restart"),     
            handle_fetch_complete(State);
        {fetching,R} ->            
            imem_meta:log_to_db(debug,?MODULE,handle_info,[{row,length(R)}],"data"),     
            {R,false};
        {_,R} ->            
            imem_meta:log_to_db(error,?MODULE,handle_info,[{row,length(R)}],"data"),     
            {R,false}        
    end,   
    Result = case length(Stmt#statement.tables) of
        1 ->    
            Wrap = fun(X) -> {X, MetaRec} end,
            RowsRead=length(Rows1),
            if  
                RowsRead < Remaining0 ->
                    lists:map(Wrap, Rows1);
                RowsRead == Remaining0 ->
                    lists:map(Wrap, Rows1);
                Remaining0 > 0 ->
                    {ResultRows,Rest} = lists:split(Remaining0, Rows1),
                    LastKey = lists:nthtail(length(ResultRows)-1, ResultRows),
                    Pred = fun(X) -> (X==LastKey) end,
                    ResultTail = lists:takewhile(Pred, Rest),
                    lists:map(Wrap, ResultRows ++ ResultTail);
                Remaining0 =< 0 ->
                    []
            end;
        _ ->
            join_rows(Rows1, FetchCtx0, Stmt)
    end,
    case is_number(Remaining0) of
        true ->
            % io:format(user, "sending rows ~p~n", [Result]),
            case Remaining0 =< length(Result) of
                true ->     
                    send_reply_to_client(Sock, {Result, true}),
                    handle_fetch_complete(State);
                false ->    
                    send_reply_to_client(Sock, {Result, Complete}),
                    FetchCtx1 = FetchCtx0#fetchCtx{remaining=Remaining0-length(Result),status=fetching},
                    if
                        Complete ->
                            handle_fetch_complete(State#state{fetchCtx=FetchCtx1});
                        true ->
                            {noreply, State#state{fetchCtx=FetchCtx1}}
                    end
            end;
        false ->
            io:format(user, "receiving rows ~n~p~n", [Rows0]),
            io:format(user, "in unexpected state ~n~p~n", [State]),
            {noreply, State}
    end;
handle_info({'DOWN', _Ref, process, _Pid, _Reason}, #state{reply=undefined}=State) ->
    % io:format(user, "~p - received expected exit info for monitored pid ~p ref ~p reason ~p~n", [?MODULE, _Pid, _Ref, _Reason]),
    {noreply, State#state{fetchCtx=#fetchCtx{}}}; 
handle_info({'DOWN', Ref, process, Pid, Reason}, State) ->
    io:format(user, "~p - received unexpected exit info for monitored pid ~p ref ~p reason ~p~n", [?MODULE, Pid, Ref, Reason]),
    {noreply, State#state{fetchCtx=#fetchCtx{pid=undefined, monref=undefined, status=aborted}}};
handle_info(Info, State) ->
    io:format(user, "~p - received unsolicited info ~p~nin state ~p~n", [?MODULE, Info, State]),
    {noreply, State}.

handle_fetch_complete(#state{reply=Sock,fetchCtx=FetchCtx0,statement=Stmt}=State)->
    #fetchCtx{pid=Pid,monref=MonitorRef,opts=Opts}=FetchCtx0,
    kill_fetch(MonitorRef, Pid),
    % io:format(user, "~p - fetch opts ~p~n", [?MODULE,Opts]), 
    case lists:member({tail_mode,true},Opts) of
        false ->
            {noreply, State#state{fetchCtx=#fetchCtx{},reply=undefined}};
        true ->     
            {_Schema,Table,_Alias} = hd(Stmt#statement.tables),
            case catch if_call_mfa(false,subscribe,[none,{table,Table,simple}]) of
                ok ->
                    % io:format(user, "~p - Subscribed to table changes ~p~n", [?MODULE, Table]),    
                    {noreply, State#state{fetchCtx=FetchCtx0#fetchCtx{status=tailing}}};
                Error ->
                    io:format(user, "~p - Cannot subscribe to table changes~n~p~n", [?MODULE, {Table,Error}]),    
                    send_reply_to_client(Sock, {'SystemException', {"Cannot subscribe to table changes",{Table,Error}}}),
                    {noreply, State#state{fetchCtx=#fetchCtx{},reply=undefined}}
            end    
    end.

terminate(_Reason, #state{fetchCtx=#fetchCtx{pid=Pid, monref=undefined}}) -> 
    % io:format(user, "~p - terminating monitor not found~n", [?MODULE]),
    catch Pid ! abort, 
    ok;
terminate(_Reason, #state{statement=Stmt,fetchCtx=#fetchCtx{status=tailing}}) -> 
    % io:format(user, "~p - terminating tail_mode~n", [?MODULE]),
    unsubscribe(Stmt),
    ok;
terminate(_Reason, #state{fetchCtx=#fetchCtx{pid=Pid, monref=MonitorRef}}) ->
    % io:format(user, "~p - demonitor and terminate~n", [?MODULE]),
    kill_fetch(MonitorRef, Pid), 
    ok.

code_change(_OldVsn, State, _Extra) -> {ok, State}.

unsubscribe(Stmt) ->
    {_Schema,Table,_Alias} = hd(Stmt#statement.tables),
    catch if_call_mfa(false,unsubscribe,[none,{table,Table,simple}]).

kill_fetch(MonitorRef, Pid) ->
    catch erlang:demonitor(MonitorRef, [flush]),
    catch Pid ! abort. 

select_bind(_MetaRec, Guard, []) -> Guard;
select_bind(MetaRec, Guard0, [{Tag,Ti,Ci}|Binds]) ->
    Guard1 = select_bind(MetaRec, Guard0, {Tag,Ti,Ci}),
    select_bind(MetaRec, Guard1, Binds);
select_bind(MetaRec, {Op,Tag}, {Tag,_,Ci}) ->   {Op,element(Ci,MetaRec)};
select_bind(MetaRec, {Op,A}, {Tag,Ti,Ci}) ->    {Op,select_bind(MetaRec,A,{Tag,Ti,Ci})};
select_bind(MetaRec, {Op,Tag,B}, {Tag,_,Ci}) -> 
    case element(Ci,MetaRec) of
        {{_,_,_},{_,_,_}} = DT ->
            offset_datetime(Op,DT,B);
        {Mega,Sec,Micro} ->
            offset_timestamp(Op,{Mega,Sec,Micro},B);
        Other ->
            {Op,Other,B}
    end;
select_bind(MetaRec, {Op,A,Tag}, {Tag,_,Ci}) -> 
    case element(Ci,MetaRec) of
        {{_,_,_},{_,_,_}} = DT ->
            offset_datetime(Op,DT,A);
        {Mega,Sec,Micro} ->
            offset_timestamp(Op,{Mega,Sec,Micro},A);
        Other ->
            {Op,A,Other}
    end;
select_bind(MetaRec, {Op,A,B}, {Tag,Ti,Ci}) ->
    BA=select_bind(MetaRec,A,{Tag,Ti,Ci}),
    BB=select_bind(MetaRec,B,{Tag,Ti,Ci}),
    case lists:member(Op,?ComparisonOperators) of
        true ->     comparison_bind(Op,BA,BB);
        false ->    {Op,BA,BB}
    end;
select_bind(_, A, _) ->             A.

offset_datetime('-', DT, Offset) ->
    offset_datetime('+', DT, -Offset);
offset_datetime('+', {{Y,M,D},{HH,MI,SS}}, Offset) ->
    GregSecs = calendar:datetime_to_gregorian_seconds({{Y,M,D},{HH,MI,SS}}),  %% for local time we should use calendar:local_time_to_universal_time_dst(DT)
    calendar:gregorian_seconds_to_datetime(GregSecs + round(Offset*86400.0)); %% calendar:universal_time_to_local_time(
offset_datetime(OP, DT, Offset) ->
    ?ClientError({"Illegal datetime offset operation",{OP,DT,Offset}}).

offset_timestamp('+', TS, Offset) when Offset < 0.0 -> 
    offset_timestamp('-', TS, -Offset);    
offset_timestamp('-', TS, Offset) when Offset < 0.0 -> 
    offset_timestamp('+', TS, -Offset);    
offset_timestamp(_, TS, Offset) when Offset < 5.787e-12 -> 
    TS;
offset_timestamp('+', {Mega,Sec,Micro}, Offset) ->
    NewMicro = Micro + round(Offset*8.64e10),
    NewSec = Sec + NewMicro div 1000000,
    NewMega = Mega + NewSec div 1000000,
    {NewMega, NewSec rem 1000000, NewMicro rem 1000000};    
offset_timestamp('-', {Mega,Sec,Micro}, Offset) ->
    NewMicro = Micro - round(Offset*8.64e10) + Sec * 1000000 + Mega * 1000000000000,
    Mi = NewMicro rem 1000000,
    NewSec = (NewMicro-Mi) div 1000000, 
    Se = NewSec rem 1000000,
    NewMega = (NewSec-Se) div 1000000,
    {NewMega, Se, Mi};    
offset_timestamp(OP, TS, Offset) ->
    ?ClientError({"Illegal timestamp offset operation",{OP,TS,Offset}}).

join_rows(Rows, FetchCtx0, Stmt) ->
    #fetchCtx{metarec=MetaRec, blockSize=BlockSize, remaining=Remaining0}=FetchCtx0,
    Tables = tl(Stmt#statement.tables),
    JoinSpec = Stmt#statement.joinspec,
    % io:format(user, "Join Tables: ~p~n", [Tables]),
    % io:format(user, "Join Specs: ~p~n", [JoinSpec]),
    join_rows(Rows, MetaRec, BlockSize, Remaining0, Tables, JoinSpec, []).

join_rows([], _, _, _, _, _, Acc) -> Acc;                              %% lists:reverse(Acc);
join_rows(_, _, _, Remaining, _, _, Acc) when Remaining < 1 -> Acc;    %% lists:reverse(Acc);
join_rows([Row|Rows], MetaRec, BlockSize, Remaining, Tables, JoinSpec, Acc) ->
    Rec = erlang:make_tuple(length(Tables)+2, undefined, [{1,Row},{2+length(Tables),MetaRec}]),
    JAcc = join_row([Rec], BlockSize, 2, Tables, JoinSpec),
    join_rows(Rows, MetaRec, BlockSize, Remaining-length(JAcc), Tables, JoinSpec, JAcc++Acc).

join_row(Recs, _BlockSize, _, [], []) -> Recs;
join_row(Recs0, BlockSize, T, [{_S,Table,_A}|Tabs], [JS|JSpecs]) ->
    Recs1 = [join_table(Rec, BlockSize, T, Table, JS) || Rec <- Recs0],
    join_row(lists:flatten(Recs1), BlockSize, T+1, Tabs, JSpecs).

join_table(Rec, BlockSize, T, Table, {MatchSpec,[]}) ->
    case imem_meta:select(Table, MatchSpec, BlockSize) of
        {[], true} ->   
            [];
        {L, true} ->
            [setelement(T, Rec, I) || I <- L]
    end;
join_table(Rec, BlockSize, T, Table, {MatchSpec0,[{Tag,Ti,Ci}|Binds]}) ->
    [{MatchHead, [Guard0], [Result]}] = MatchSpec0,
    % io:format(user, "Rec used for bind ~p~n", [Rec]),
    % io:format(user, "Join guard before bind ~p~n", [Guard0]),
    Guard1 = imem_sql:simplify_matchspec(join_bind(Rec, Guard0, {Tag,Ti,Ci})),
    % io:format(user, "Join guard after  bind ~p~n", [Guard1]),
    join_table(Rec, BlockSize, T, Table, {[{MatchHead, [Guard1], [Result]}], Binds}).

join_bind(Rec, {Op,Tag}, {Tag,Ti,Ci}) ->    {Op,element(Ci,element(Ti,Rec))};
join_bind(Rec, {Op,A}, {Tag,Ti,Ci}) ->      {Op,join_bind(Rec,A,{Tag,Ti,Ci})};
join_bind(Rec, {Op,Tag,B}, {Tag,Ti,Ci}) ->  
    case element(Ci,element(Ti,Rec)) of
        {{_,_,_},{_,_,_}} = DT ->
            offset_datetime(Op,DT,B);
        {Mega,Sec,Micro} ->
            offset_timestamp(Op,{Mega,Sec,Micro},B);
        Other ->
            {Op,Other,B}
    end;
join_bind(Rec, {Op,A,Tag}, {Tag,Ti,Ci}) ->  
    case element(Ci,element(Ti,Rec)) of
        {{_,_,_},{_,_,_}} = DT ->
            offset_datetime(Op,DT,A);
        {Mega,Sec,Micro} ->
            offset_timestamp(Op,{Mega,Sec,Micro},A);
        Other ->
            {Op,A,Other}
    end;
join_bind(Rec, {Op,A,B}, {Tag,Ti,Ci}) ->
    BA=join_bind(Rec,A,{Tag,Ti,Ci}),
    BB=join_bind(Rec,B,{Tag,Ti,Ci}),
    case lists:member(Op,?ComparisonOperators) of
        true ->     comparison_bind(Op,BA,BB);
        false ->    {Op,BA,BB}
    end;
join_bind(_, A, _) ->               A.

comparison_bind(Op,A,B) ->
    AW = case A of
        {Ma,Sa,Microa} when is_integer(Ma), is_integer(Sa), is_integer(Microa) -> {const,A}; 
        {{Ya,Mona,Da},{_,_,_}} when is_integer(Ya), is_integer(Mona), is_integer(Da) -> {const,A};
        A when size(A) =< 3 -> A;
        A when is_tuple(A) -> {const,A};
        A -> A
    end,
    BW = case B of
        {Mb,Sb,Microb} when is_integer(Mb), is_integer(Sb), is_integer(Microb) -> {const,B};
        {{Yb,Monb,Db},{_,_,_}} when is_integer(Yb), is_integer(Monb), is_integer(Db) -> {const,B};
        B when size(B) =< 3 -> B;
        B when is_tuple(B) -> {const,B};
        B -> B
    end,
    {Op,AW,BW}.   

send_reply_to_client(SockOrPid, Result) ->
    NewResult = {self(),Result},
    imem_server:send_resp(NewResult, SockOrPid).

update_prepare(IsSec, SKey, Tables, ColMap, ChangeList) ->
    TableTypes = [{Schema,Table,if_call_mfa(IsSec,table_type,[SKey,{Schema,Table}])} || {Schema,Table,_Alias} <- Tables],
    % io:format(user, "~p - received change list~n~p~n", [?MODULE, ChangeList]),
    %% transform a ChangeList
        % [1,nop,{{def,"2","'2'"},{}},"2"],                     %% no operation on this line
        % [5,ins,{},"99"],                                      %% insert {def,"99", undefined}
        % [3,del,{{def,"5","'5'"},{}},"5"],                     %% delete {def,"5","'5'"}
        % [4,upd,{{def,"12","'12'"},{}},"112"]                  %% update {def,"12","'12'"} to {def,"112","'12'"}
    %% into an UpdatePlan                                       {table} = {Schema,Table,Type}
        % [1,{table},{def,"2","'2'"},{def,"2","'2'"}],          %% no operation on this line
        % [5,{table},{},{def,"99", undefined}],                 %% insert {def,"99", undefined}
        % [3,{table},{def,"5","'5'"},{}],                       %% delete {def,"5","'5'"}
        % [4,{table},{def,"12","'12'"},{def,"112","'12'"}]      %% failing update {def,"12","'12'"} to {def,"112","'12'"}
    UpdPlan = update_prepare(IsSec, SKey, TableTypes, ColMap, ChangeList, []),
    %io:format(user, "~p - prepared table changes~n~p~n", [?MODULE, UpdPlan]),
    UpdPlan.

update_prepare(_IsSec, _SKey, _Tables, _ColMap, [], Acc) -> Acc;
update_prepare(_IsSec, _SKey, [{Schema,Table,bag}|_], _ColMap, _CList, _Acc) ->
    ?UnimplementedException({"Bag table cursor update not supported", {Schema,Table}});
update_prepare(IsSec, SKey, Tables, ColMap, [[Item,nop,Recs|_]|CList], Acc) ->
    Action = [hd(Tables), Item, element(1,Recs), element(1,Recs)],     
    update_prepare(IsSec, SKey, Tables, ColMap, CList, [Action|Acc]);
update_prepare(IsSec, SKey, Tables, ColMap, [[Item,del,Recs|_]|CList], Acc) ->
    Action = [hd(Tables), Item, element(1,Recs), {}],     
    update_prepare(IsSec, SKey, Tables, ColMap, CList, [Action|Acc]);
update_prepare(IsSec, SKey, Tables, ColMap, [[Item,upd,Recs|Values]|CList], Acc) ->
    % io:format(user, "~p - ColMap~n~p~n", [?MODULE, ColMap]),
    if  
        length(Values) > length(ColMap) ->      ?ClientError({"Too many values",{Item,Values}});        
        length(Values) < length(ColMap) ->      ?ClientError({"Too few values",{Item,Values}});        
        true ->                                 ok    
    end,            
    ValMap = lists:usort(
        [{Ci,imem_datatype:value_to_db(Item,element(Ci,element(1,Recs)),T,L,P,D,false,Value), R} || 
            {#ddColMap{tind=Ti, cind=Ci, type=T, length=L, precision=P, default=D, readonly=R},Value} 
            <- lists:zip(ColMap,Values), Ti==1]),    
    % io:format(user, "~p - value map~n~p~n", [?MODULE, ValMap]),
    IndMap = lists:usort([Ci || {Ci,_,_} <- ValMap]),
    % io:format(user, "~p - ind map~n~p~n", [?MODULE, IndMap]),
    ROViol = [{element(Ci,element(1,Recs)),NewVal} || {Ci,NewVal,R} <- ValMap, R==true, element(Ci,element(1,Recs)) /= NewVal],   
    % io:format(user, "~p - key change~n~p~n", [?MODULE, ROViol]),
    if  
        length(ValMap) /= length(IndMap) ->     ?ClientError({"Contradicting column update",{Item,ValMap}});        
        length(ROViol) /= 0 ->                  ?ClientError({"Cannot update readonly field",{Item,hd(ROViol)}});        
        true ->                                 ok    
    end,            
    NewRec = lists:foldl(fun({Ci,Value,_},Rec) -> setelement(Ci,Rec,Value) end, element(1,Recs), ValMap),    
    Action = [hd(Tables), Item, element(1,Recs), NewRec],     
    update_prepare(IsSec, SKey, Tables, ColMap, CList, [Action|Acc]);
update_prepare(IsSec, SKey, [{_,Table,_}|_]=Tables, ColMap, CList, Acc) ->
    ColInfo = if_call_mfa(IsSec, column_infos, [SKey, Table]),    
    DefRec = list_to_tuple([Table|if_call_mfa(IsSec,column_info_items, [SKey, ColInfo, default])]),    
    % io:format(user, "~p - default record ~p~n", [?MODULE, DefRec]),     
    update_prepare(IsSec, SKey, Tables, ColMap, DefRec, CList, Acc);
update_prepare(_IsSec, _SKey, _Tables, _ColMap, [CLItem|_], _Acc) ->
    ?ClientError({"Invalid format of change list", CLItem}).

update_prepare(IsSec, SKey, Tables, ColMap, DefRec, [[Item,ins,_|Values]|CList], Acc) ->
    if  
        length(Values) > length(ColMap) ->      ?ClientError({"Too many values",{Item,Values}});        
        length(Values) < length(ColMap) ->      ?ClientError({"Not enough values",{Item,Values}});        
        true ->                                 ok    
    end,            
    ValMap = lists:usort(
        [{Ci,imem_datatype:value_to_db(Item,?nav,T,L,P,D,false,Value)} || 
            {#ddColMap{tind=Ti, cind=Ci, type=T, length=L, precision=P, default=D},Value} 
            <- lists:zip(ColMap,Values), Ti==1]),    
    IndMap = lists:usort([Ci || {Ci,_} <- ValMap]),
    HasKey = lists:member(2,IndMap),
    if 
        length(ValMap) /= length(IndMap) ->     ?ClientError({"Contradicting column insert",{Item,ValMap}});
        HasKey /= true  ->                      ?ClientError({"Missing key column",{Item,ValMap}});
        true ->                                 ok
    end,
    Rec = lists:foldl(
            fun({Ci,Value},Rec) ->
                if 
                    erlang:is_function(Value,0) -> 
                        setelement(Ci,Rec,Value());
                    true ->                 
                        setelement(Ci,Rec,Value)
                end
            end, 
            DefRec, ValMap),
    Action = [hd(Tables), Item, {}, Rec],     
    update_prepare(IsSec, SKey, Tables, ColMap, CList, [Action|Acc]).

% update_bag(IsSec, SKey, Table, ColMap, [C|CList]) ->
%     ?UnimplementedException({"Cursor update not supported for bag tables",Table}).

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

teardown(_SKey) -> 
    catch imem_meta:drop_table(def),
    ?imem_test_teardown().

db_test_() ->
    {timeout, 20000, 
        {
            setup,
            fun setup/0,
            fun teardown/1,
            {with, [
                  fun test_without_sec/1
                , fun test_with_sec/1
            ]}
        }
    }.
    
test_without_sec(_) -> 
    test_with_or_without_sec(false).

test_with_sec(_) ->
    test_with_or_without_sec(true).

test_with_or_without_sec(IsSec) ->
    try
        ClEr = 'ClientError',
        % SeEx = 'SecurityException',
        io:format(user, "----TEST--- ~p ----Security ~p ~n", [?MODULE, IsSec]),

        io:format(user, "schema ~p~n", [imem_meta:schema()]),
        io:format(user, "data nodes ~p~n", [imem_meta:data_nodes()]),
        ?assertEqual(true, is_atom(imem_meta:schema())),
        ?assertEqual(true, lists:member({imem_meta:schema(),node()}, imem_meta:data_nodes())),

        ?assertEqual([],receive_all()),

        SKey=case IsSec of
            true ->     ?imem_test_admin_login();
            false ->    none
        end,

        Sql1 = "create table def (col1 varchar2(10), col2 integer);",
        io:format(user, "Sql: ~p~n", [Sql1]),
        ?assertEqual(ok, imem_sql:exec(SKey, Sql1, 0, "Imem", IsSec)),
        ?assertEqual(ok, insert_range(SKey, 15, def, 'Imem', IsSec)),
        TableRows1 = lists:sort(if_call_mfa(IsSec,read,[SKey, def])),
        [Meta] = if_call_mfa(IsSec, read, [SKey, ddTable, {'Imem',def}]),
        io:format(user, "Meta table~n~p~n", [Meta]),
        io:format(user, "original table~n~p~n", [TableRows1]),

        Sql2 = "select col1, col2 from def;",
        io:format(user, "Query: ~p~n", [Sql1]),
        {ok, _Clm2, _RowFun2, StmtRef2} = imem_sql:exec(SKey, Sql2, 4, "Imem", IsSec),
        ?assertEqual(ok, imem_statement:fetch_recs_async(SKey, StmtRef2, self(), IsSec)),
        Result2a = receive 
            R2a ->    R2a
        end,
        {StmtRef2, {List2a, false}} = Result2a,
        ?assertEqual(4, length(List2a)),           
        %% ChangeList2 = [[OP,ID] ++ L || {OP,ID,L} <- lists:zip3([nop, ins, del, upd], [1,2,3,4], lists:map(RowFun2,List2a))],
        %% io:format(user, "change list~n~p~n", [ChangeList2]),
        ChangeList2 = [
        [1,nop,{{def,"2",2},{}},"2",2],         %% no operation on this line
        [5,ins,{},"99","undefined"],            %% insert {def,"99", undefined}
        [3,del,{{def,"5",5},{}},"5",5],         %% delete {def,"5","'5'"}
        [4,upd,{{def,"12",12},{}},"112",12]     %% update {def,"12","'12'"} to {def,"112","'12'"}
        ],
        ?assertException(throw,{ClEr,{"Cannot update readonly field",{4,{"12","112"}}}}, update_cursor_prepare(SKey, StmtRef2, IsSec, ChangeList2)),
        TableRows2 = lists:sort(if_call_mfa(IsSec,read,[SKey, def])),
        io:format(user, "unchanged table~n~p~n", [TableRows2]),
        ?assertEqual(TableRows1, TableRows2),

        ChangeList3 = [
        [1,nop,{{def,"2",2},{}},"2",2],         %% no operation on this line
        [5,ins,{},"99", "undefined"],           %% insert {def,"99", undefined}
        [3,del,{{def,"5",5},{}},"5",5],         %% delete {def,"5",5}
        [4,upd,{{def,"12",12},{}},"12",12],     %% nop update {def,"12",12}
        [6,upd,{{def,"10",10},{}},"10","110"]   %% update {def,"10",10} to {def,"10",110}
        ],
        ExpectedRows3 = [
        {def,"2",2},                            %% no operation on this line
        {def,"99",undefined},                   %% insert {def,"99", undefined}
        {def,"10",110},                         %% update {def,"10",10} to {def,"10",110}
        {def,"12",12}                           %% nop update {def,"12",12}
        ],
        RemovedRows3 = [
        {def,"5",5}                             %% delete {def,"5",5}
        ],

        ?assertEqual(ok, update_cursor_prepare(SKey, StmtRef2, IsSec, ChangeList3)),
        ?assertEqual(ok, update_cursor_execute(SKey, StmtRef2, IsSec, optimistic)),        
        TableRows3 = lists:sort(if_call_mfa(IsSec,read,[SKey, def])),
        io:format(user, "changed table~n~p~n", [TableRows3]),
        [?assert(lists:member(R,TableRows3)) || R <- ExpectedRows3],
        [?assertNot(lists:member(R,TableRows3)) || R <- RemovedRows3],

        ?assertEqual(ok, if_call_mfa(IsSec,truncate_table,[SKey, def])),
        ?assertEqual(0,imem_meta:table_size(def)),
        ?assertEqual(ok, insert_range(SKey, 5, def, 'Imem', IsSec)),
        ?assertEqual(5,imem_meta:table_size(def)),

        ?assertEqual(ok, close(SKey, StmtRef2)),

        Sql3 = "select col1, col2 from def where col2 <= 5;",
        io:format(user, "Query: ~p~n", [Sql3]),
        {ok, _Clm3, _RowFun3, StmtRef3} = imem_sql:exec(SKey, Sql3, 100, 'Imem', IsSec),
        try
            ?assertEqual(ok, imem_statement:fetch_recs_async(SKey, StmtRef3, self(), [{tail_mode,true}], IsSec)),
            Result3a = receive_all(),
            ?assertEqual(1, length(Result3a)),
            [{_,{List3a,true}}] = Result3a,
            ?assertEqual(5, length(List3a)),
            ?assertEqual(ok, insert_range(SKey, 10, def, 'Imem', IsSec)),
            ?assertEqual(10,imem_meta:table_size(def)),
            Result3b = receive_all(),
            ?assertEqual(5, length(Result3b)),           
            ?assertEqual(ok, fetch_close(SKey, StmtRef3, IsSec)),
            ?assertEqual(ok, insert_range(SKey, 5, def, 'Imem', IsSec)),
            ?assertEqual([], receive_all())        
        after
            ?assertEqual(ok, close(SKey, StmtRef3))
        end,

        ?assertEqual(ok, if_call_mfa(IsSec,truncate_table,[SKey, def])),
        ?assertEqual(0,imem_meta:table_size(def)),
        ?assertEqual(ok, insert_range(SKey, 5, def, 'Imem', IsSec)),
        ?assertEqual(5,imem_meta:table_size(def)),

        Sql4 = "select t1.col1, t2.col2 from def t1, def t2 where t2.col1 = t1.col1 and t2.col2 <= 5;",
        io:format(user, "Query: ~p~n", [Sql4]),
        {ok, _Clm4, _RowFun4, StmtRef4} = imem_sql:exec(SKey, Sql4, 100, 'Imem', IsSec),
        try
            ?assertEqual(ok, imem_statement:fetch_recs_async(SKey, StmtRef4, self(), [{tail_mode,true}], IsSec)),
            [{_,{List4a,true}}] = receive_all(),
            ?assertEqual(5, length(List4a)),
            ?assertEqual(ok, insert_range(SKey, 10, def, 'Imem', IsSec)),
            ?assertEqual(10,imem_meta:table_size(def)),
            Result4b = receive_all(),
            ?assertEqual(5, length(Result4b)),           
            ?assertEqual(ok, fetch_close(SKey, StmtRef4, IsSec)),
            ?assertEqual(ok, insert_range(SKey, 5, def, 'Imem', IsSec)),
            ?assertEqual([], receive_all())        
        after
            ?assertEqual(ok, close(SKey, StmtRef4))
        end,

        ?assertEqual(ok, if_call_mfa(IsSec,truncate_table,[SKey, def])),
        ?assertEqual(0,imem_meta:table_size(def)),
        ?assertEqual(ok, insert_range(SKey, 10, def, 'Imem', IsSec)),
        ?assertEqual(10,imem_meta:table_size(def)),

        Sql5 = "select col1 from def;",
        io:format(user, "Query: ~p~n", [Sql5]),
        {ok, _Clm5, _RowFun5, StmtRef5} = imem_sql:exec(SKey, Sql5, 5, 'Imem', IsSec),
        try
            ?assertEqual(ok, imem_statement:fetch_recs_async(SKey, StmtRef5, self(), [], IsSec)),
            [{_,{List5a,false}}] = receive_all(),
            ?assertEqual(5, length(List5a)),
            io:format(user, "trying to insert one row before fetch complete~n", []),
            ?assertEqual(ok, insert_range(SKey, 1, def, 'Imem', IsSec)),
            io:format(user, "completed insert one row before fetch complete~n", []),
            ?assertEqual(ok, imem_statement:fetch_recs_async(SKey, StmtRef5, self(), [{tail_mode,true}], IsSec)),
            [{_,{List5b,true}}] = receive_all(),
            ?assertEqual(5, length(List5b)),
            ?assertEqual(ok, insert_range(SKey, 1, def, 'Imem', IsSec)),
            [{_,{List5c,false}}] = receive_all(),
            ?assertEqual(1, length(List5c)),
            ?assertEqual(ok, insert_range(SKey, 1, def, 'Imem', IsSec)),
            ?assertEqual(ok, insert_range(SKey, 11, def, 'Imem', IsSec)),
            ?assertEqual(11,imem_meta:table_size(def)),
            Result5d = receive_all(),
            ?assertEqual(12, length(Result5d)),
            ?assertEqual(ok, fetch_close(SKey, StmtRef5, IsSec)),
            ?assertEqual(ok, insert_range(SKey, 5, def, 'Imem', IsSec)),
            ?assertEqual([], receive_all())        
        after
            ?assertEqual(ok, close(SKey, StmtRef5))
        end,


        ?assertEqual(ok, imem_sql:exec(SKey, "drop table def;", 0, 'Imem', IsSec)),

        ?assertEqual({{2000,1,29},{12,13,14}}, offset_datetime('+', {{2000,1,28},{12,13,14}}, 1.0)),
        ?assertEqual({{2000,1,27},{12,13,14}}, offset_datetime('-', {{2000,1,28},{12,13,14}}, 1.0)),
        ?assertEqual({{2000,1,28},{12,13,14}}, offset_datetime('+', {{2000,1,28},{12,13,14}}, 1.0e-10)),
        ?assertEqual({{2000,1,28},{12,13,14}}, offset_datetime('-', {{2000,1,28},{12,13,14}}, 1.0e-10)),
        ?assertEqual({{2000,1,28},{11,13,14}}, offset_datetime('-', {{2000,1,28},{12,13,14}}, 1.0/24.0)),
        ?assertEqual({{2000,1,28},{12,12,14}}, offset_datetime('-', {{2000,1,28},{12,13,14}}, 1.0/24.0/60.0)),
        ?assertEqual({{2000,1,28},{12,13,13}}, offset_datetime('-', {{2000,1,28},{12,13,14}}, 1.0/24.0/3600.0)),
        
        ENow = erlang:now(),
        ?assertEqual(ENow, offset_timestamp('+', offset_timestamp('+', ENow, 1.0),-1.0)),
        ?assertEqual(ENow, offset_timestamp('+', offset_timestamp('-', ENow, 1.0),1.0)),
        ?assertEqual(ENow, offset_timestamp('+', offset_timestamp('-', ENow, 0.1),0.1)),
        ?assertEqual(ENow, offset_timestamp('+', offset_timestamp('-', ENow, 0.01),0.01)),
        ?assertEqual(ENow, offset_timestamp('+', offset_timestamp('-', ENow, 0.001),0.001)),
        ?assertEqual(ENow, offset_timestamp('+', offset_timestamp('-', ENow, 0.0001),0.0001)),
        ?assertEqual(ENow, offset_timestamp('+', offset_timestamp('-', ENow, 0.00001),0.00001)),
        ?assertEqual(ENow, offset_timestamp('+', offset_timestamp('-', ENow, 0.000001),0.000001)),
        ?assertEqual(ENow, offset_timestamp('+', offset_timestamp('-', ENow, 1.0e-6),1.0e-6)),
        ?assertEqual(ENow, offset_timestamp('+', offset_timestamp('-', ENow, 1.0e-7),1.0e-7)),
        ?assertEqual(ENow, offset_timestamp('+', offset_timestamp('-', ENow, 1.0e-8),1.0e-8)),
        ?assertEqual(ENow, offset_timestamp('+', offset_timestamp('-', ENow, 1.0e-9),1.0e-9)),
        ?assertEqual(ENow, offset_timestamp('+', offset_timestamp('-', ENow, 1.0e-10),1.0e-10)),
        ?assertEqual(ENow, offset_timestamp('+', offset_timestamp('-', ENow, 1.0e-11),1.0e-11)),
        ?assertEqual(ENow, offset_timestamp('+', offset_timestamp('-', ENow, 1.0e-12),1.0e-12)),

        io:format(user, "ErlangNow: ~p~n", [ENow]),
        OneSec = 1.0/86400.0,
        io:format(user, "Now-  1us: ~p~n", [offset_timestamp('-', ENow, 0.000001 * OneSec)]),
        io:format(user, "Now- 10us: ~p~n", [offset_timestamp('-', ENow, 0.00001 * OneSec)]),
        io:format(user, "Now-100us: ~p~n", [offset_timestamp('-', ENow, 0.0001 * OneSec)]),
        io:format(user, "Now-  1ms: ~p~n", [offset_timestamp('-', ENow, 0.001 * OneSec)]),
        io:format(user, "Now- 10ms: ~p~n", [offset_timestamp('-', ENow, 0.01 * OneSec)]),
        io:format(user, "Now-100ms: ~p~n", [offset_timestamp('-', ENow, 0.1 * OneSec)]),
        io:format(user, "Now-   1s: ~p~n", [offset_timestamp('-', ENow, OneSec)]),
        io:format(user, "Now-  10s: ~p~n", [offset_timestamp('-', ENow, 10.0*OneSec)]),
        io:format(user, "Now- 100s: ~p~n", [offset_timestamp('-', ENow, 100.0*OneSec)]),
        io:format(user, "Now-1000s: ~p~n", [offset_timestamp('-', ENow, 1000.0*OneSec)]),

        case IsSec of
            true ->     ?imem_logout(SKey);
            false ->    ok
        end

    catch
        Class:Reason ->  io:format(user, "Exception ~p:~p~n~p~n", [Class, Reason, erlang:get_stacktrace()]),
        ?assert( true == "all tests completed")
    end,
    ok. 

receive_all() ->
    receive_all([]).

receive_all(Acc) ->    
    case receive 
            R ->    io:format(user, "~p got:~n~p~n", [erlang:now(),R]),
                    R
        after 50 ->
            stop
        end of
        stop ->     lists:reverse(Acc);
        Result ->   receive_all([Result|Acc])
    end.

insert_range(_SKey, 0, _Table, _Schema, _IsSec) -> ok;
insert_range(SKey, N, Table, Schema, IsSec) when is_integer(N), N > 0 ->
    if_call_mfa(IsSec, write,[SKey,Table,{Table,integer_to_list(N),N}]),
    insert_range(SKey, N-1, Table, Schema, IsSec).


    % {M1,S1,Mic1} = erlang:now(),
    % {M2,S2,Mic2} = erlang:now(),
    % Count = length(Result),
    % Delta = Mic2 - Mic1 + 1000000 * ((S2-S1) + 1000000 * (M2-M1)),
    % Message = io_lib:format("fetch_recs latency per record: ~p usec",[Delta div Count]),
    % imem_meta:log_to_db(debug,?MODULE,fetch_recs,[{rec_count,Count},{fetch_duration,Delta}], Message),
