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
        , receive_raw/0
        , receive_raw/1
        , receive_list/2
        , receive_list/3
        , result_lists/2
        , result_tuples/2
        ]).

-record(fetchCtx,               %% state for fetch process
                    { pid       ::pid()
                    , monref    ::any()             %% fetch monitor ref
                    , status    ::atom()            %% undefined | waiting | fetching | done | tailing | aborted
                    , metarec   ::tuple()
                    , blockSize=100 ::integer()     %% could be adaptive
                    , remaining ::integer()         %% rows remaining to be fetched. initialized to Limit and decremented
                    , opts = [] ::list()            %% fetch options like {tail_mode,true}
                    , tailSpec  ::any()             %% compiled matchspec for master table condition (bound with MetaRec) 
                    , filter    ::any()             %% filter specification {Guard,Binds}
                    }).

-record(state,                  %% state for statment process, including fetch subprocess
                    { statement
                    , isSec =false          :: boolean()
                    , seco=none             :: integer()
                    , fetchCtx=#fetchCtx{}         
                    , reply                                 %% reply destination TCP socket or Pid
                    , updPlan = []          :: list()       %% bulk execution plan (table updates/inserts/deletes)
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

fetch_recs(SKey, #stmtResult{stmtRef=Pid}, Sock, Timeout, IsSec) ->
    fetch_recs(SKey, Pid, Sock, Timeout, IsSec);
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
            {Pid,{error, {'SystemException', Reason}}} ->
                io:format(user, "~p - fetch_recs exception ~p ~p~n", [?MODULE, 'SystemException', Reason]),
                gen_server:call(Pid, {fetch_close, IsSec, SKey}),                
                ?SystemException(Reason);            
            {Pid,{error, {'ClientError', Reason}}} ->
                io:format(user, "~p - fetch_recs exception ~p ~p~n", [?MODULE, 'ClientError', Reason]),
                gen_server:call(Pid, {fetch_close, IsSec, SKey}),                
                ?ClientError(Reason);            
            {Pid,{error, {'ClientError', Reason}}} ->
                io:format(user, "~p - fetch_recs exception ~p ~p~n", [?MODULE, 'ClientError', Reason]),
                gen_server:call(Pid, {fetch_close, IsSec, SKey}),                
                ?ClientError(Reason);            
            Error ->
                io:format(user, "~p - fetch_recs bad async receive~n~p~n", [?MODULE, Error]),
                gen_server:call(Pid, {fetch_close, IsSec, SKey}),                
                ?SystemException({"Bad async receive",Error})            
        end
    after
        gen_server:call(Pid, {fetch_close, IsSec, SKey})
    end,
    Result.

fetch_recs_sort(SKey, #stmtResult{stmtRef=Pid}, Sock, Timeout, IsSec) ->
    fetch_recs_sort(SKey, Pid, Sock, Timeout, IsSec);
fetch_recs_sort(SKey, Pid, Sock, Timeout, IsSec) when is_pid(Pid) ->
    lists:sort(fetch_recs(SKey, Pid, Sock, Timeout, IsSec)).

fetch_recs_async(SKey, #stmtResult{stmtRef=Pid}, Sock, IsSec) ->
    fetch_recs_async(SKey, Pid, Sock, IsSec);
fetch_recs_async(SKey, Pid, Sock, IsSec) ->
    fetch_recs_async(SKey, Pid, Sock, [], IsSec).

fetch_recs_async(SKey, #stmtResult{stmtRef=Pid}, Sock, Opts, IsSec) ->
    fetch_recs_async(SKey, Pid, Sock, Opts, IsSec);
fetch_recs_async(SKey, Pid, Sock, Opts, IsSec) when is_pid(Pid) ->
    gen_server:cast(Pid, {fetch_recs_async, IsSec, SKey, Sock, Opts}).

fetch_close(SKey,  #stmtResult{stmtRef=Pid}, IsSec) ->
    fetch_close(SKey, Pid, IsSec);
fetch_close(SKey, Pid, IsSec) when is_pid(Pid) ->
    gen_server:call(Pid, {fetch_close, IsSec, SKey}).

update_cursor_prepare(SKey, #stmtResult{stmtRef=Pid}, IsSec, ChangeList) ->
    update_cursor_prepare(SKey, Pid, IsSec, ChangeList);
update_cursor_prepare(SKey, Pid, IsSec, ChangeList) when is_pid(Pid) ->
    case gen_server:call(Pid, {update_cursor_prepare, IsSec, SKey, ChangeList}) of
        ok ->   ok;
        Error-> throw(Error)
    end.

update_cursor_execute(SKey, #stmtResult{stmtRef=Pid}, IsSec, none) ->
    update_cursor_prepare(SKey, Pid, IsSec, none);
update_cursor_execute(SKey, Pid, IsSec, none) when is_pid(Pid) ->
    case gen_server:call(Pid, {update_cursor_execute, IsSec, SKey, none}) of
        ok ->       ok;
        Error ->    throw(Error)
    end; 
update_cursor_execute(SKey, #stmtResult{stmtRef=Pid}, IsSec, optimistic) ->
    update_cursor_execute(SKey, Pid, IsSec, optimistic);
update_cursor_execute(SKey, Pid, IsSec, optimistic) when is_pid(Pid) ->
    case gen_server:call(Pid, {update_cursor_execute, IsSec, SKey, optimistic}) of
        ok ->       ok;
        Error ->    throw(Error)
    end.

close(SKey, #stmtResult{stmtRef=Pid}) ->
    close(SKey, Pid);
close(SKey, Pid) when is_pid(Pid) ->
    gen_server:cast(Pid, {close, SKey}).

init([Statement]) ->
    imem_meta:log_to_db(debug,?MODULE,init,[],Statement#statement.stmtStr),
    {ok, #state{statement=Statement}}.

handle_call({set_seco, SKey}, _From, State) ->    
    {reply,ok,State#state{seco=SKey, isSec=true}};
handle_call({update_cursor_prepare, IsSec, _SKey, ChangeList}, _From, #state{statement=Stmt, seco=SKey}=State) ->
    {Reply, UpdatePlan1} = try
        {ok, update_prepare(IsSec, SKey, Stmt#statement.tables, Stmt#statement.colMaps, ChangeList)}
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
handle_call({fetch_close, _IsSec, _SKey}, _From, #state{statement=Stmt,fetchCtx=#fetchCtx{pid=Pid, monref=MonitorRef, status=Status}}=State) ->
    imem_meta:log_to_db(debug,?MODULE,handle_call,[{from,_From},{status,Status}],"fetch_close"),
    case Status of
        undefined ->    ok;                             % close is ignored
        done ->         ok;                             % normal close after completed fetch
        fetching ->     kill_fetch(MonitorRef, Pid);    % client stops fetch 
        tailing ->      unsubscribe(Stmt);              % client stops tail mode
        aborted ->      ok                              % client acknowledges abort
    end,
    {reply, ok, State#state{fetchCtx=#fetchCtx{}}}.     % client may restart the fetch now

handle_cast({fetch_recs_async, _IsSec, _SKey, Sock, _Opts}, #state{fetchCtx=#fetchCtx{status=done}}=State) ->
    % io:format(user, "fetch_recs_async called in status done~n", []),
    imem_meta:log_to_db(warning,?MODULE,handle_cast,[{sock,Sock},{opts,_Opts},{status,done}],"fetch_recs_async rejected"),
    send_reply_to_client(Sock, {error,{'ClientError',"Fetch is completed, execute fetch_close before fetching from start again"}}),
    {noreply, State}; 
handle_cast({fetch_recs_async, _IsSec, _SKey, Sock, _Opts}, #state{fetchCtx=#fetchCtx{status=aborted}}=State) ->
    % io:format(user, "fetch_recs_async called in status aborted~n", []),
    imem_meta:log_to_db(warning,?MODULE,handle_cast,[{sock,Sock},{opts,_Opts},{status,aborted}],"fetch_recs_async rejected"),
    send_reply_to_client(Sock, {error,{'SystemException',"Fetch is aborted, execute fetch_close before fetching from start again"}}),
    {noreply, State}; 
handle_cast({fetch_recs_async, _IsSec, _SKey, Sock, _Opts}, #state{fetchCtx=#fetchCtx{status=tailing}}=State) ->
    % io:format(user, "fetch_recs_async called in status tailing~n", []),
    imem_meta:log_to_db(warning,?MODULE,handle_cast,[{sock,Sock},{opts,_Opts},{status,tailing}],"fetch_recs_async rejected"),
    send_reply_to_client(Sock, {error,{'ClientError',"Fetching in tail mode, execute fetch_close before fetching from start again"}}),
    {noreply, State}; 
handle_cast({fetch_recs_async, IsSec, _SKey, Sock, Opts}, #state{statement=Stmt, seco=SKey, fetchCtx=FetchCtx0}=State) ->
    % io:format(user, "fetch_recs_async called in status ~p~n", [FetchCtx0#fetchCtx.status]),
    #statement{tables=[{_Schema,Table,_Alias}|_], blockSize=BlockSize, mainSpec=MainSpec, metaFields=MetaFields} = Stmt,
    #scanSpec{sspec=SSpec0,sbinds=SBinds,fguard=FGuard,mbinds=MBinds,fbinds=FBinds,limit=Limit} = MainSpec,
    imem_meta:log_to_db(debug,?MODULE,handle_cast,[{sock,Sock},{opts,Opts},{status,FetchCtx0#fetchCtx.status}],"fetch_recs_async"),
    % io:format(user,"Table  : ~p~n", [Table]),
    % io:format(user,"SBinds : ~p~n", [SBinds]),
    % io:format(user,"MBinds : ~p~n", [MBinds]),
    % io:format(user,"FGuard : ~p~n", [FGuard]),    
    MetaRec = list_to_tuple([if_call_mfa(IsSec, meta_field_value, [SKey, N]) || N <- MetaFields]),
    % io:format(user,"MetaRec: ~p~n", [MetaRec]),
    [{SHead, SGuards0, [Result]}] = SSpec0,
    % io:format(user,"SGuards before bind : ~p~n", [SGuards0]),
    SGuards1 = case SGuards0 of
        [] ->       [];
        [SGuard0]-> [imem_sql:simplify_guard(select_bind(MetaRec, SGuard0, SBinds))]
    end,
    % io:format(user,"SGuards after meta bind : ~p~n", [SGuards1]),
    SSpec = [{SHead, SGuards1, [Result]}],
    TailSpec = ets:match_spec_compile(SSpec),
    FBound = select_bind(MetaRec, FGuard, MBinds),
    % io:format(user,"FBound : ~p~n", [FBound]),
    Filter = make_filter_fun(1,FBound, FBinds),
    SkipFetch = lists:member({fetch_mode,skip},Opts),
    case {SkipFetch,FetchCtx0#fetchCtx.pid} of
        {true,undefined} ->     %% skip fetch
            FetchSkip = #fetchCtx{status=undefined,metarec=MetaRec,blockSize=BlockSize,remaining=Limit,opts=Opts,filter=Filter,tailSpec=TailSpec},
            handle_fetch_complete(State#state{reply=Sock,fetchCtx=FetchSkip}); 
        {false,undefined} ->    %% start fetch
            case if_call_mfa(IsSec, fetch_start, [SKey, self(), Table, SSpec, BlockSize, Opts]) of
                TransPid when is_pid(TransPid) ->
                    MonitorRef = erlang:monitor(process, TransPid),
                    TransPid ! next,
                    % io:format(user, "~p - fetch opts ~p~n", [?MODULE,Opts]), 
                    FetchStart = #fetchCtx{pid=TransPid,monref=MonitorRef,status=waiting,metarec=MetaRec,blockSize=BlockSize,remaining=Limit,opts=Opts,filter=Filter,tailSpec=TailSpec},
                    {noreply, State#state{reply=Sock,fetchCtx=FetchStart}}; 
                Error ->    
                    ?SystemException({"Cannot spawn async fetch process",Error})
            end;
        {true,_Pid} ->          %% skip ongoing fetch (and possibly go to tail_mode)
            FetchSkipRemaining = FetchCtx0#fetchCtx{metarec=MetaRec,opts=Opts,filter=Filter,tailSpec=TailSpec},
            handle_fetch_complete(State#state{reply=Sock,fetchCtx=FetchSkipRemaining}); 
        {false,Pid} ->          %% fetch next block
            Pid ! next,
            % io:format(user, "~p - fetch opts ~p~n", [?MODULE,Opts]),
            FetchContinue = FetchCtx0#fetchCtx{metarec=MetaRec,opts=Opts,filter=Filter,tailSpec=TailSpec}, 
            {noreply, State#state{reply=Sock,fetchCtx=FetchContinue}}  
    end;
handle_cast({close, _SKey}, State) ->
    imem_meta:log_to_db(debug,?MODULE,handle_cast,[],"close statement"),
    % io:format(user, "~p - received close in state ~p~n", [?MODULE, State]),
    {stop, normal, State}; 
handle_cast(Request, State) ->
    io:format(user, "~p - receives unsolicited cast ~p~nin state ~p~n", [?MODULE, Request, State]),
    imem_meta:log_to_db(error,?MODULE,handle_cast,[{request,Request},{state,State}],"receives unsolicited cast"),
    {noreply, State}.

handle_info({row, ?eot}, #state{reply=Sock,fetchCtx=FetchCtx0}=State) ->
    % io:format(user, "~p - received end of table in status ~p~n", [?MODULE,FetchCtx0#fetchCtx.status]),
    % io:format(user, "~p - received end of table in state~n~p~n", [?MODULE,State]),
    case FetchCtx0#fetchCtx.status of
        fetching ->
            imem_meta:log_to_db(warning,?MODULE,handle_info,[{row, ?eot},{status, fetching}],"eot"),
            send_reply_to_client(Sock, {[],true}),  
            io:format(user, "~p - late end of table received in state~n~p~n", [?MODULE, State]),        
            handle_fetch_complete(State);
        _ ->
            io:format(user, "~p - unexpected end of table received in state~n~p~n", [?MODULE, State]),        
            imem_meta:log_to_db(warning,?MODULE,handle_info,[{row, ?eot}],"eot"),
            {noreply, State}
    end;        
handle_info({mnesia_table_event,{write,Record,_ActivityId}}, #state{reply=Sock,fetchCtx=FetchCtx0,statement=Stmt}=State) ->
    % imem_meta:log_to_db(debug,?MODULE,handle_info,[{mnesia_table_event,write}],"tail write"),
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
                                Remaining0 =< 1 ->
                                    send_reply_to_client(Sock, {lists:map(Wrap, [Rec]),true}),
                                    unsubscribe(Stmt),
                                    {noreply, State#state{fetchCtx=FetchCtx0#fetchCtx{status=done}}};
                                true ->
                                    send_reply_to_client(Sock, {lists:map(Wrap, [Rec]),tail}),
                                    {noreply, State#state{fetchCtx=FetchCtx0#fetchCtx{remaining=Remaining0-1}}}
                            end;
                        _N ->    
                            case join_rows([Rec], FetchCtx0, Stmt) of
                                [] ->
                                    {noreply, State};
                                Result ->    
                                    if 
                                        (Remaining0 =< length(Result)) ->
                                            send_reply_to_client(Sock, {Result, true}),
                                            unsubscribe(Stmt),
                                            {noreply, State#state{fetchCtx=FetchCtx0#fetchCtx{status=done}}};
                                        true ->
                                            send_reply_to_client(Sock, {Result, tail}),
                                            {noreply, State#state{fetchCtx=FetchCtx0#fetchCtx{remaining=Remaining0-length(Result)}}}
                                    end
                            end
                    end
            end;
        _ ->
            {noreply, State}
    end;
handle_info({mnesia_table_event,{delete_object, _OldRecord, _ActivityId}}, State) ->
    % imem_meta:log_to_db(debug,?MODULE,handle_info,[{mnesia_table_event,delete_object}],"tail delete"),
    % io:format(user, "~p - received mnesia subscription event ~p ~p~n", [?MODULE, delete_object, _OldRecord]),
    {noreply, State};
handle_info({mnesia_table_event,{delete, {_Tab, _Key}, _ActivityId}}, State) ->
    % imem_meta:log_to_db(debug,?MODULE,handle_info,[{mnesia_table_event,delete}],"tail delete"),
    % io:format(user, "~p - received mnesia subscription event ~p ~p~n", [?MODULE, delete, {_Tab, _Key}]),
    {noreply, State};
handle_info({row, Rows0}, #state{reply=Sock, isSec=IsSec, seco=SKey, fetchCtx=FetchCtx0, statement=Stmt}=State) ->
    #fetchCtx{metarec=MetaRec,remaining=Remaining0,status=Status,filter=Filter, opts=Opts}=FetchCtx0,
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
        {fetching,[?sot,?eot|_R]} ->
            imem_meta:log_to_db(warning,?MODULE,handle_info,[{row,length(_R)}],"data transaction restart"),     
            handle_fetch_complete(State);
        {fetching,[?sot|_R]} ->
            imem_meta:log_to_db(warning,?MODULE,handle_info,[{row,length(_R)}],"data transaction restart"),     
            handle_fetch_complete(State);
        {fetching,R} ->            
            imem_meta:log_to_db(debug,?MODULE,handle_info,[{row,length(R)}],"data"),     
            {R,false};
        {BadStatus,R} ->            
            imem_meta:log_to_db(error,?MODULE,handle_info,[{status,BadStatus},{row,length(R)}],"data"),     
            {R,false}        
    end,   
    Result = case {length(Stmt#statement.tables),Filter} of
        {1,true} ->
            Wrap = fun(X) -> {X, MetaRec} end,
            Rows=length(Rows1),
            if  
                Rows < Remaining0 ->
                    lists:map(Wrap, Rows1);     
                Rows == Remaining0 ->
                    lists:map(Wrap, Rows1);     
                Remaining0 > 0 ->
                    {ResultRows,Rest} = lists:split(Remaining0, Rows1),
                    LastKey = element(2,lists:last(ResultRows)),
                    Pred = fun(X) -> (element(2,X)==LastKey) end,
                    ResultTail = lists:takewhile(Pred, Rest),   
                    %% ToDo: may need to read more blocks to completely read this key in a bag
                    lists:map(Wrap, ResultRows ++ ResultTail);
                Remaining0 =< 0 ->
                    []
            end;
        {1,Filter} ->
            Wrap = fun(X) -> {X, MetaRec} end,
            Rows2 = lists:map(Wrap, Rows1),
            Rows3 = lists:filter(Filter,Rows2),
            Rows=length(Rows3),
            if  
                Rows < Remaining0 ->
                    Rows3;
                Rows == Remaining0 ->
                    Rows3;
                Remaining0 > 0 ->
                    {ResultRows,Rest} = lists:split(Remaining0, Rows3),
                    LastKey = element(2,element(1,lists:last(ResultRows))),
                    Pred = fun(X) -> (element(2,element(1,X))==LastKey) end,
                    ResultTail = lists:takewhile(Pred, Rest),
                    %% ToDo: may need to read more blocks to completely read this key in a bag
                    ResultRows ++ ResultTail;
                Remaining0 =< 0 ->
                    []
            end;
        {_,_} ->
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
                    PushMode = lists:member({fetch_mode,push},Opts),
                    if
                        Complete ->
                            handle_fetch_complete(State#state{fetchCtx=FetchCtx1});
                        PushMode ->
                            gen_server:cast(self(),{fetch_recs_async, IsSec, SKey, Sock, Opts}),
                            {noreply, State#state{fetchCtx=FetchCtx1}};
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
    % io:format(user, "~p - fetch complete, opts ~p~n", [?MODULE,Opts]), 
    case lists:member({tail_mode,true},Opts) of
        false ->
            % io:format(user, "~p - fetch complete no tail ~p~n", [?MODULE,Opts]), 
            {noreply, State#state{fetchCtx=FetchCtx0#fetchCtx{status=done}}};           
        true ->     
            {_Schema,Table,_Alias} = hd(Stmt#statement.tables),
            io:format(user, "~p - fetch complete, switching to tail_mode~p~n", [?MODULE,Opts]), 
            case  catch if_call_mfa(false,subscribe,[none,{table,Table,simple}]) of
                ok ->
                    io:format(user, "~p - Subscribed to table changes ~p~n", [?MODULE, Table]),    
                    {noreply, State#state{fetchCtx=FetchCtx0#fetchCtx{status=tailing}}};
                Error ->
                    io:format(user, "~p - Cannot subscribe to table changes~n~p~n", [?MODULE, {Table,Error}]),    
                    imem_meta:log_to_db(error,?MODULE,handle_fetch_complete,[{table,Table},{error,Error},{sock,Sock}],"Cannot subscribe to table changes"),
                    send_reply_to_client(Sock, {error,{'SystemException',{"Cannot subscribe to table changes",{Table,Error}}}}),
                    {noreply, State#state{fetchCtx=FetchCtx0#fetchCtx{status=done}}}   
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

kill_fetch(undefined, undefined) -> ok;
kill_fetch(MonitorRef, Pid) ->
    catch erlang:demonitor(MonitorRef, [flush]),
    catch Pid ! abort. 

select_bind(_MetaRec, Guard, []) -> Guard;
select_bind(MetaRec, Guard0, [B|Binds]) ->
    Guard1 = imem_sql:simplify_guard(select_bind_one(MetaRec, Guard0, B)),
    select_bind(MetaRec, Guard1, Binds).

select_bind_one(MetaRec, {Op,Tag}, {Tag,_,Ci}) ->   {Op,element(Ci,MetaRec)};
select_bind_one(MetaRec, {Op,A}, {Tag,Ti,Ci}) ->    {Op,select_bind(MetaRec,A,{Tag,Ti,Ci})};
select_bind_one(MetaRec, {Op,Tag,B}, {Tag,_,Ci}) -> 
    case element(Ci,MetaRec) of
        {{_,_,_},{_,_,_}} = DT ->
            offset_datetime(Op,DT,B);
        {Mega,Sec,Micro} ->
            offset_timestamp(Op,{Mega,Sec,Micro},B);
        Other ->
            {Op,Other,B}
    end;
select_bind_one(MetaRec, {Op,A,Tag}, {Tag,_,Ci}) -> 
    case element(Ci,MetaRec) of
        {{_,_,_},{_,_,_}} = DT ->
            offset_datetime(Op,DT,A);
        {Mega,Sec,Micro} ->
            offset_timestamp(Op,{Mega,Sec,Micro},A);
        Other ->
            {Op,A,Other}
    end;
select_bind_one(MetaRec, {Op,A,B}, {Tag,Ti,Ci}) ->
    BA=select_bind_one(MetaRec,A,{Tag,Ti,Ci}),
    BB=select_bind_one(MetaRec,B,{Tag,Ti,Ci}),
    case lists:member(Op,?ComparisonOperators) of
        true ->     comparison_bind(Op,BA,BB);
        false ->    {Op,BA,BB}
    end;
select_bind_one(_, A, _) -> A.

join_bind(_Rec, Guard, []) -> Guard;
join_bind(Rec, Guard0, [B|Binds]) ->
    Guard1 = imem_sql:simplify_guard(join_bind_one(Rec, Guard0, B)),
    join_bind(Rec, Guard1, Binds).

join_bind_one(Rec, {Op,Tag}, {Tag,Ti,Ci}) ->    {Op,element(Ci,element(Ti,Rec))};
join_bind_one(Rec, {Op,A}, {Tag,Ti,Ci}) ->      {Op,join_bind_one(Rec,A,{Tag,Ti,Ci})};
join_bind_one(Rec, {Op,Tag,B}, {Tag,Ti,Ci}) ->  
    case element(Ci,element(Ti,Rec)) of
        {{_,_,_},{_,_,_}} = DT ->
            offset_datetime(Op,DT,B);
        {Mega,Sec,Micro} ->
            offset_timestamp(Op,{Mega,Sec,Micro},B);
        Other ->
            {Op,Other,B}
    end;
join_bind_one(Rec, {Op,A,Tag}, {Tag,Ti,Ci}) ->  
    case element(Ci,element(Ti,Rec)) of
        {{_,_,_},{_,_,_}} = DT ->
            offset_datetime(Op,DT,A);
        {Mega,Sec,Micro} ->
            offset_timestamp(Op,{Mega,Sec,Micro},A);
        Other ->
            {Op,A,Other}
    end;
join_bind_one(Rec, {Op,A,B}, {Tag,Ti,Ci}) ->
    BA=join_bind_one(Rec,A,{Tag,Ti,Ci}),
    BB=join_bind_one(Rec,B,{Tag,Ti,Ci}),
    case lists:member(Op,?ComparisonOperators) of
        true ->     comparison_bind(Op,BA,BB);
        false ->    {Op,BA,BB}
    end;
join_bind_one(_, A, _) ->               A.

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

make_filter_fun(_Ti, true, _FBinds)  ->
    fun(_X) -> true end;
make_filter_fun(Ti, {'is_member', {const,A}, {const,B}}, FBinds) ->
    make_filter_fun(Ti,{'is_member', A, B}, FBinds);
make_filter_fun(Ti, {'is_member', {const,A}, B}, FBinds) ->
    make_filter_fun(Ti,{'is_member', A, B}, FBinds);
make_filter_fun(Ti, {'is_member', A, {const,B}}, FBinds) ->
    make_filter_fun(Ti, {'is_member', A, B}, FBinds);
make_filter_fun(Ti, {'is_member', A, '$_'}, FBinds) ->
    ABind = lists:keyfind(A,1,FBinds),
    case ABind of 
        false ->        
            fun(X1) -> 
                lists:member(A,tl(tuple_to_list(element(Ti,X1))))
            end;
        {A,ATi,ACi} ->  
            fun(X2) ->
                lists:member(element(ACi,element(ATi,X2)),tl(tuple_to_list(element(Ti,X2))))
            end
    end;
make_filter_fun(_Ti, {'is_member', A, B}, FBinds)  ->
    ABind = lists:keyfind(A,1,FBinds),
    BBind = lists:keyfind(B,1,FBinds),
    case {ABind,BBind} of 
        {false,false} ->        
            fun(_X) -> 
                if 
                    is_list(B) ->   lists:member(A,B);
                    is_tuple(B) ->  lists:member(A,tuple_to_list(B));
                    true ->         false
                end
            end;
        {false,{B,BTi,BCi}} ->
            fun(X1) ->
                Bbound = element(BCi,element(BTi,X1)),
                if 
                    is_list(Bbound) ->  lists:member(A,Bbound);
                    is_tuple(Bbound) -> lists:member(A,tuple_to_list(Bbound));
                    true ->             false
                end
            end;
        {{A,ATi,ACi},false} ->  
            fun(X2) ->
                if 
                    is_list(B) ->  lists:member(element(ACi,element(ATi,X2)),B);
                    is_tuple(B) -> lists:member(element(ACi,element(ATi,X2)),tuple_to_list(B));
                    true ->             false
                end
            end;
        {{A,XTi,XCi},{B,YTi,YCi}} ->  
            fun(X3) ->
                Ybound = element(YCi,element(YTi,X3)), 
                if 
                    is_list(Ybound) ->  lists:member(element(XCi,element(XTi,X3)),Ybound);
                    is_tuple(Ybound) -> lists:member(element(XCi,element(XTi,X3)),tuple_to_list(Ybound));
                    true ->             false
                end
            end
    end;
make_filter_fun(Ti,FGuard, FBinds) ->
    ?UnimplementedException({"Illegal filter",{Ti, FGuard, FBinds}}).

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
    JoinSpecs = Stmt#statement.joinSpecs,
    % io:format(user, "Join Tables: ~p~n", [Tables]),
    % io:format(user, "Join Specs: ~p~n", [JoinSpecs]),
    join_rows(Rows, MetaRec, BlockSize, Remaining0, Tables, JoinSpecs, []).

join_rows([], _, _, _, _, _, Acc) -> Acc;                              %% lists:reverse(Acc);
join_rows(_, _, _, Remaining, _, _, Acc) when Remaining < 1 -> Acc;    %% lists:reverse(Acc);
join_rows([Row|Rows], MetaRec, BlockSize, Remaining, Tables, JoinSpecs, Acc) ->
    Rec = erlang:make_tuple(length(Tables)+2, undefined, [{1,Row},{2+length(Tables),MetaRec}]),
    JAcc = join_row([Rec], BlockSize, 2, Tables, JoinSpecs),
    join_rows(Rows, MetaRec, BlockSize, Remaining-length(JAcc), Tables, JoinSpecs, JAcc++Acc).

join_row(Recs, _BlockSize, _Ti, [], []) -> Recs;
join_row(Recs0, BlockSize, Ti, [{_S,Table,_A}|Tabs], [JS|JSpecs]) ->
    Recs1 = case lists:member(Table,?DataTypes) of
        true ->  [join_virtual(Rec, BlockSize, Ti, Table, JS) || Rec <- Recs0];
        false -> [join_table(Rec, BlockSize, Ti, Table, JS) || Rec <- Recs0]
    end,
    join_row(lists:flatten(Recs1), BlockSize, Ti+1, Tabs, JSpecs).

join_table(Rec, _BlockSize, Ti, Table, #scanSpec{sspec=SSpec,sbinds=SBinds,fguard=FGuard,mbinds=MBinds,fbinds=FBinds,limit=Limit}) ->
    % io:format(user, "Rec used for join bind ~p~n", [Rec]),
    [{MatchHead, [Guard0], [Result]}] = SSpec,
    Guard1 = join_bind(Rec, Guard0, SBinds),
    MaxSize = Limit+1000,
    % io:format(user, "Join guard after bind : ~p~n", [Guard1]),
    case imem_meta:select(Table, [{MatchHead, [Guard1], [Result]}], MaxSize) of
        {[], true} ->   [];
        {L, true} ->
            case FGuard of
                true -> 
                    [setelement(Ti, Rec, I) || I <- L];
                _ ->
                    MboundGuard = join_bind(Rec, FGuard, MBinds),
                    % io:format(user, "Join guard after MBind : ~p~n", [MboundGuard]),
                    Filter = make_filter_fun(Ti, MboundGuard, FBinds),
                    Recs = [setelement(Ti, Rec, I) || I <- L],
                    lists:filter(Filter,Recs)
            end;
        {_, false} ->   ?ClientError({"Too much data in join intermediate result", MaxSize});
        Error ->        ?SystemException({"Unexpected join intermediate result", Error})
    end.

join_virtual(Rec, _BlockSize, Ti, Table, #scanSpec{sspec=SSpec,sbinds=SBinds,fguard=FGuard,mbinds=MBinds,limit=Limit}) ->
    [{_,[SGuard],_}] = SSpec,
    MaxSize = Limit+1000,
    case join_bind(Rec, SGuard, SBinds) of
        true ->
            case FGuard of
                true ->
                    ?UnimplementedException({"Unsupported virtual join filter guard", FGuard}); 
                _ ->
                    % io:format(user, "Rec used for join bind ~p~n", [Rec]),
                    % io:format(user, "MBinds used for join bind ~p~n", [MBinds]),
                    case join_bind(Rec, FGuard, MBinds) of
                        {is_member,Tag, '$_'} when is_atom(Tag) ->
                            Items = element(1,Rec),
                            % io:format(user, "generate_virtual table ~p from ~p~n~p~n", [Table,'$_',Items]),
                            Virt = generate_virtual(Table,tl(tuple_to_list(Items)),MaxSize),
                            % io:format(user, "Generated virtual table ~p~n~p~n", [Table,Virt]),
                            [setelement(Ti, Rec, {Table,I}) || I <- Virt];
                        {is_member,Tag, Items} when is_atom(Tag) ->
                            % io:format(user, "generate_virtual table ~p from~n~p~n", [Table,Items]),
                            Virt = generate_virtual(Table,Items,MaxSize),
                            % io:format(user, "Generated virtual table ~p~n~p~n", [Table,Virt]),
                            [setelement(Ti, Rec, {Table,I}) || I <- Virt];
                        BadFG ->
                            ?UnimplementedException({"Unsupported virtual join bound filter guard",BadFG})
                    end
            end;
        BadJG ->
            ?UnimplementedException({"Unsupported virtual join guard",BadJG})
    end.

generate_virtual(Table, Items, MaxSize) when is_tuple(Items) ->
    generate_virtual(Table, tuple_to_list(Items), MaxSize);

generate_virtual(atom=Table, Items, MaxSize) when is_list(Items) ->
    generate_limit_check(Table, length(Items), MaxSize),
    Pred = fun(X) -> is_atom(X) end,
    lists:filter(Pred,Items);
generate_virtual(atom, Item, _) when is_atom(Item)-> [Item];
generate_virtual(atom, _, _) -> [];

generate_virtual(binary=Table, Items, MaxSize) when is_binary(Items) ->
    generate_limit_check(Table, byte_size(Items), MaxSize),
    [list_to_binary([B]) || B <- binary_to_list(Items)];
generate_virtual(binary, _, _) -> [];

generate_virtual(binstr=Table, Items, MaxSize) when is_binary(Items) ->
    generate_limit_check(Table, byte_size(Items), MaxSize),
    String = binary_to_list(Items),
    case io_lib:printable_unicode_list(String) of
        true ->     String;
        false ->    []
    end;

generate_virtual(boolean=Table, Items, MaxSize) when is_list(Items) ->
    generate_limit_check(Table, length(Items), MaxSize),
    Pred = fun(X) -> is_boolean(X) end,
    lists:filter(Pred,Items);
generate_virtual(boolean, Item, _) when is_boolean(Item)-> [Item];
generate_virtual(boolean, _, _) -> [];

generate_virtual(datetime=Table, Items, MaxSize) when is_list(Items) ->
    generate_limit_check(Table, length(Items), MaxSize),
    Pred = fun
        ({{_,_,_},{_,_,_}}) -> true;
        (_) -> false
    end,
    lists:filter(Pred,Items);
generate_virtual(datetime, {{_,_,_},{_,_,_}}=Item, _) -> 
    [Item];
generate_virtual(datetime, _, _) -> [];

generate_virtual(decimal=Table, Items, MaxSize) when is_list(Items) ->
    generate_limit_check(Table, length(Items), MaxSize),
    Pred = fun(X) -> is_integer(X) end,
    lists:filter(Pred,Items);
generate_virtual(decimal, Item, _) when is_integer(Item)-> [Item];
generate_virtual(decimal, _, _) -> [];

generate_virtual(float=Table, Items, MaxSize) when is_list(Items) ->
    generate_limit_check(Table, length(Items), MaxSize),
    Pred = fun(X) -> is_float(X) end,
    lists:filter(Pred,Items);
generate_virtual(float, Item, _) when is_float(Item)-> [Item];
generate_virtual(float, _, _) -> [];

generate_virtual('fun'=Table, Items, MaxSize) when is_list(Items) ->
    generate_limit_check(Table, length(Items), MaxSize),
    Pred = fun(X) -> is_function(X) end,
    lists:filter(Pred,Items);
generate_virtual('fun', Item, _) when is_function(Item)-> [Item];
generate_virtual('fun', _, _) -> [];

generate_virtual(integer=Table, Items, MaxSize) when is_list(Items) ->
    generate_limit_check(Table, length(Items), MaxSize),
    Pred = fun(X) -> is_integer(X) end,
    lists:filter(Pred,Items);
generate_virtual(integer, Item, _) when is_integer(Item)-> [Item];
generate_virtual(integer, _, _) -> [];

generate_virtual(ipaddr=Table, Items, MaxSize) when is_list(Items) ->
    generate_limit_check(Table, length(Items), MaxSize),
    Pred = fun
        ({A,B,C,D}) when is_integer(A), is_integer(B), is_integer(C), is_integer(D) -> true;
        (_) -> false
    end,                                %% ToDo: IpV6
    lists:filter(Pred,Items);
generate_virtual(ipaddr, {A,B,C,D}=Item, _) when is_integer(A), is_integer(B), is_integer(C), is_integer(D) -> [Item];
generate_virtual(ipaddr, _, _) -> [];      %% ToDo: IpV6

generate_virtual(list=Table, Items, MaxSize) when is_list(Items) ->
    generate_limit_check(Table, length(Items), MaxSize),
    Pred = fun(X) -> is_list(X) end,
    lists:filter(Pred,Items);

generate_virtual(timestamp=Table, Items, MaxSize) when is_list(Items) ->
    generate_limit_check(Table, length(Items), MaxSize),
    Pred = fun
        ({Meg,Sec,Mic}) when is_number(Meg), is_integer(Sec), is_integer(Mic) -> true;
        (_) -> false
    end,
    lists:filter(Pred,Items);
generate_virtual(timestamp, {Meg,Sec,Mic}=Item, _) when is_number(Meg), is_integer(Sec), is_integer(Mic) -> 
    [Item];
generate_virtual(timestamp, _, _) -> [];

generate_virtual(tuple=Table, Items, MaxSize) when is_list(Items) ->
    generate_limit_check(Table, length(Items), MaxSize),
    Pred = fun(X) -> is_tuple(X) end,
    lists:filter(Pred,Items);
generate_virtual(tuple, Item, _) when is_tuple(Item)-> [Item];
generate_virtual(tuple, _, _) -> [];

generate_virtual(pid=Table, Items, MaxSize) when is_list(Items) ->
    generate_limit_check(Table, length(Items), MaxSize),
    Pred = fun(X) -> is_pid(X) end,
    lists:filter(Pred,Items);
generate_virtual(pid, Item, _) when is_pid(Item)-> [Item];
generate_virtual(pid, _, _) -> [];

generate_virtual(ref=Table, Items, MaxSize) when is_list(Items) ->
    generate_limit_check(Table, length(Items), MaxSize),
    Pred = fun(X) -> is_reference(X) end,
    lists:filter(Pred,Items);
generate_virtual(ref, Item, _) when is_reference(Item)-> [Item];
generate_virtual(ref, _, _) -> [];

generate_virtual(string=Table, Items, MaxSize) when is_list(Items) ->
    generate_limit_check(Table, length(Items), MaxSize),
    case io_lib:printable_unicode_list(Items) of
        true ->     Items;
        false ->    []
    end;

generate_virtual(term=Table, Items, MaxSize) when is_list(Items) ->
    generate_limit_check(Table, length(Items), MaxSize),
    Items;
generate_virtual(term, Item, _) ->
    [Item];

generate_virtual(userid=Table, Items, MaxSize) when is_list(Items) ->
    generate_limit_check(Table, length(Items), MaxSize),
    Pred = fun(X) -> is_integer(X) end,
    lists:filter(Pred,Items);               %% ToDo: filter in imem_account
generate_virtual(userid, Item, _) when is_integer(Item)-> 
    [Item];                                 %% ToDo: filter in imem_account
generate_virtual(userid, _, _) -> [];

generate_virtual(Table, Items, _MaxSize) -> 
    ?UnimplementedException({"Unsupported virtual table generation",{Table,Items}}).

generate_limit_check(Table, CurSize, MaxSize) when CurSize > MaxSize -> 
    ?ClientError({"Too much data for virtual table generation",{Table,CurSize,MaxSize}});
generate_limit_check(_,_,_) -> ok. 

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
            {#ddColMap{tind=Ti, cind=Ci, type=T, len=L, prec=P, default=D, readonly=R},Value} 
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
            {#ddColMap{tind=Ti, cind=Ci, type=T, len=L, prec=P, default=D},Value} 
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

receive_raw() ->
    receive_raw(50, []).

receive_raw(Timeout) ->
    receive_raw(Timeout, []).

receive_raw(Timeout,Acc) ->    
    case receive 
            R ->    io:format(user, "~p got:~n~p~n", [erlang:now(),R]),
                    R
        after Timeout ->
            stop
        end of
        stop ->     lists:reverse(Acc);
        Result ->   receive_raw(Timeout,[Result|Acc])
    end.

receive_list(StmtResult, Complete) ->
    receive_list(StmtResult,Complete,50,[]).

receive_list(StmtResult, Complete, Timeout) ->
    receive_list(StmtResult, Complete, Timeout,[]).

receive_list(#stmtResult{stmtRef=StmtRef,rowFun=RowFun}=StmtResult,Complete,Timeout,Acc) ->    
    case receive
            R ->    % io:format(user, "~p got:~n~p~n", [erlang:now(),R]),
                    R
        after Timeout ->
            stop
        end of
        stop ->     
            Unchecked = case Acc of
                [] ->                       
                    [{StmtRef,[],Complete}];
                [{StmtRef,{_,Complete}}|_] -> 
                    lists:reverse(Acc);
                [{StmtRef,{L1,C1}}|_] ->      
                    throw({bad_complete,{StmtRef,{L1,C1}}});
                Res ->                      
                    throw({bad_receive,lists:reverse(Res)})
            end,
            case lists:usort([element(1, SR) || SR <- Unchecked]) of
                [StmtRef] ->                
                    List = lists:flatten([element(1,element(2, T)) || T <- Unchecked]),
                    RT = result_tuples(List,RowFun),
                    if 
                        length(RT) =< 10 ->
                            io:format(user, "Received  : ~p~n", [RT]);
                        true ->
                            io:format(user, "Received  : ~p items [~p,~p,~p]~n", [length(RT),hd(RT), '...', lists:last(RT)])
                    end,            
                    RT;
                StmtRefs ->
                    throw({bad_stmtref,lists:delete(StmtRef, StmtRefs)})
            end;
        Result ->   
            receive_list(StmtResult,Complete,Timeout,[Result|Acc])
    end.

result_lists(List,RowFun) when is_list(List), is_function(RowFun) ->  
    lists:map(RowFun,List).

result_tuples(List,RowFun) when is_list(List), is_function(RowFun) ->  
    [list_to_tuple(R) || R <- lists:map(RowFun,List)].

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

        ?assertEqual([],receive_raw()),

        SKey=case IsSec of
            true ->     ?imem_test_admin_login();
            false ->    none
        end,


    %% test table def

        ?assertEqual(ok, imem_sql:exec(SKey, 
                "create table def (
                    col1 varchar2(10), 
                    col2 integer
                );"
                , 0, 'Imem', IsSec)),

        ?assertEqual(ok, insert_range(SKey, 15, def, 'Imem', IsSec)),

        TableRows1 = lists:sort(if_call_mfa(IsSec,read,[SKey, def])),
        [Meta] = if_call_mfa(IsSec, read, [SKey, ddTable, {'Imem',def}]),
        io:format(user, "Meta table~n~p~n", [Meta]),
        io:format(user, "original table~n~p~n", [TableRows1]),


        SR0 = exec(SKey,query0, 15, IsSec, "select col1, col2 from def;"),
        try
            ?assertEqual(ok, fetch_async(SKey,SR0,[],IsSec)),
            List0 = receive_list(SR0,true),
            ?assertEqual(15, length(List0)),
            ?assertEqual([], receive_raw())
        after
            ?assertEqual(ok, close(SKey, SR0))
        end,

        SR1 = exec(SKey,query1, 4, IsSec, "select col1, col2 from def;"),
        ?assertEqual(ok, fetch_async(SKey,SR1,[],IsSec)),
        List1a = receive_list(SR1,false),
        ?assertEqual(4, length(List1a)),
        ?assertEqual([], receive_raw()),
        ?assertEqual(ok, fetch_async(SKey,SR1,[],IsSec)),
        List1b = receive_list(SR1,false),
        ?assertEqual(4, length(List1b)),
        ?assertEqual([], receive_raw()),
        ?assertEqual(ok, fetch_async(SKey,SR1,[],IsSec)),
        List1c = receive_list(SR1,false),
        ?assertEqual(4, length(List1c)),
        ?assertEqual(ok, fetch_async(SKey,SR1,[],IsSec)),
        List1d = receive_list(SR1,true),
        ?assertEqual(3, length(List1d)),
        ?assertEqual([], receive_raw()),

        %% ChangeList2 = [[OP,ID] ++ L || {OP,ID,L} <- lists:zip3([nop, ins, del, upd], [1,2,3,4], lists:map(RowFun2,List2a))],
        %% io:format(user, "change list~n~p~n", [ChangeList2]),
        ChangeList2 = [
        [1,nop,{{def,"2",2},{}},"2",2],         %% no operation on this line
        [5,ins,{},"99","undefined"],            %% insert {def,"99", undefined}
        [3,del,{{def,"5",5},{}},"5",5],         %% delete {def,"5","'5'"}
        [4,upd,{{def,"12",12},{}},"112",12]     %% update {def,"12","'12'"} to {def,"112","'12'"}
        ],
        ?assertException(throw,{ClEr,{"Cannot update readonly field",{4,{"12","112"}}}}, 
            update_cursor_prepare(SKey, SR1, IsSec, ChangeList2)
        ),
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

        ?assertEqual(ok, update_cursor_prepare(SKey, SR1, IsSec, ChangeList3)),
        ?assertEqual(ok, update_cursor_execute(SKey, SR1, IsSec, optimistic)),        
        TableRows3 = lists:sort(if_call_mfa(IsSec,read,[SKey, def])),
        io:format(user, "changed table~n~p~n", [TableRows3]),
        [?assert(lists:member(R,TableRows3)) || R <- ExpectedRows3],
        [?assertNot(lists:member(R,TableRows3)) || R <- RemovedRows3],

        ?assertEqual(ok, if_call_mfa(IsSec,truncate_table,[SKey, def])),
        ?assertEqual(0,imem_meta:table_size(def)),
        ?assertEqual(ok, insert_range(SKey, 5, def, 'Imem', IsSec)),
        ?assertEqual(5,imem_meta:table_size(def)),

        ?assertEqual(ok, close(SKey, SR1)),

        SR2 = exec(SKey,query2, 100, IsSec, 
            "select col1, col2 
             from def 
             where col2 <= 5;"
        ),
        try
            ?assertEqual(ok, fetch_async(SKey,SR2,[{tail_mode,true}],IsSec)),
            List2a = receive_list(SR2,true),
            ?assertEqual(5, length(List2a)),
            ?assertEqual([], receive_raw()),
            ?assertEqual(ok, insert_range(SKey, 10, def, 'Imem', IsSec)),
            ?assertEqual(10,imem_meta:table_size(def)),  %% unchanged, all updates
            List2b = receive_list(SR2,tail),
            ?assertEqual(5, length(List2b)),             %% 10 updates, 5 filtered with TailFun()           
            ?assertEqual(ok, fetch_close(SKey, SR2, IsSec)),
            ?assertEqual(ok, insert_range(SKey, 5, def, 'Imem', IsSec)),
            ?assertEqual([], receive_raw())        
        after
            ?assertEqual(ok, close(SKey, SR2))
        end,

        ?assertEqual(ok, if_call_mfa(IsSec,truncate_table,[SKey, def])),
        ?assertEqual(0,imem_meta:table_size(def)),
        ?assertEqual(ok, insert_range(SKey, 5, def, 'Imem', IsSec)),
        ?assertEqual(5,imem_meta:table_size(def)),

        SR3 = exec(SKey,query3, 2, IsSec, 
            "select t1.col1, t2.col2 
             from def t1, def t2 
             where t2.col1 = t1.col1 
             and t2.col2 <= 5;"
        ),
        try
            ?assertEqual(ok, fetch_async(SKey,SR3,[{tail_mode,true}],IsSec)),
            List3a = receive_list(SR3,false),
            ?assertEqual(2, length(List3a)),
            ?assertEqual(ok, fetch_async(SKey,SR3,[{fetch_mode,push},{tail_mode,true}],IsSec)),
            List3b = receive_list(SR3,true),
            ?assertEqual(3, length(List3b)),
            ?assertEqual(ok, insert_range(SKey, 10, def, 'Imem', IsSec)),
            ?assertEqual(10,imem_meta:table_size(def)),
            List3c = receive_list(SR3,tail),
            ?assertEqual(5, length(List3c)),           
            ?assertEqual(ok, fetch_close(SKey, SR3, IsSec)),
            ?assertEqual(ok, insert_range(SKey, 5, def, 'Imem', IsSec)),
            ?assertEqual([], receive_raw())        
        after
            ?assertEqual(ok, close(SKey, SR3))
        end,

        ?assertEqual(ok, if_call_mfa(IsSec,truncate_table,[SKey, def])),
        ?assertEqual(0,imem_meta:table_size(def)),
        ?assertEqual(ok, insert_range(SKey, 10, def, 'Imem', IsSec)),
        ?assertEqual(10,imem_meta:table_size(def)),

        SR4 = exec(SKey,query4, 5, IsSec, "select col1 from def;"),
        try
            ?assertEqual(ok, fetch_async(SKey,SR4,[],IsSec)),
            List4a = receive_list(SR4,false),
            ?assertEqual(5, length(List4a)),
            io:format(user, "trying to insert one row before fetch complete~n", []),
            ?assertEqual(ok, insert_range(SKey, 1, def, 'Imem', IsSec)),
            io:format(user, "completed insert one row before fetch complete~n", []),
            ?assertEqual(ok, fetch_async(SKey,SR4,[{tail_mode,true}],IsSec)),
            List4b = receive_list(SR4,true),
            ?assertEqual(5, length(List4b)),
            ?assertEqual(ok, insert_range(SKey, 1, def, 'Imem', IsSec)),
            List4c = receive_list(SR4,tail),
            ?assertEqual(1, length(List4c)),
            ?assertEqual(ok, insert_range(SKey, 1, def, 'Imem', IsSec)),
            ?assertEqual(ok, insert_range(SKey, 11, def, 'Imem', IsSec)),
            ?assertEqual(11,imem_meta:table_size(def)),
            List4d = receive_list(SR4,tail),
            ?assertEqual(12, length(List4d)),
            io:format(user, "12 tail rows received in single packets~n", []),
            ?assertEqual(ok, fetch_async(SKey,SR4,[],IsSec)),
            Result4e = receive_raw(),
            io:format(user, "reject received ~p~n", [Result4e]),
            [{StmtRef4, {error, {ClEr,Reason4e}}}] = Result4e, 
            ?assertEqual("Fetching in tail mode, execute fetch_close before fetching from start again",Reason4e),
            ?assertEqual(StmtRef4,SR4#stmtResult.stmtRef),
            ?assertEqual(ok, fetch_close(SKey, SR4, IsSec)),
            ?assertEqual(ok, insert_range(SKey, 5, def, 'Imem', IsSec)),
            ?assertEqual([], receive_raw())
        after
            ?assertEqual(ok, close(SKey, SR4))
        end,

        SR5 = exec(SKey,query5, 100, IsSec, "select name(qname) from all_tables"),
        try
            ?assertEqual(ok, fetch_async(SKey, SR5, [], IsSec)),
            List5a = receive_list(SR5,true),
            ?assert(lists:member({"Imem.def"},List5a)),
            ?assert(lists:member({"Imem.ddTable"},List5a)),
            io:format(user, "first read success (async)~n", []),
            ?assertEqual(ok, fetch_async(SKey, SR5, [], IsSec)),
            [{StmtRef5, {error, Reason5a}}] = receive_raw(),
            ?assertEqual({'ClientError',
                "Fetch is completed, execute fetch_close before fetching from start again"},
                Reason5a),
            ?assertEqual(StmtRef5,SR5#stmtResult.stmtRef),
            ?assertEqual(ok, fetch_close(SKey, SR5, IsSec)),
            ?assertEqual(ok, fetch_async(SKey, SR5, [], IsSec)),
            List5b = receive_list(SR5,true),
            ?assertEqual(List5a,List5b),
            io:format(user, "second read success (async)~n", []),
            ?assertException(throw,
                {'ClientError',"Fetch is completed, execute fetch_close before fetching from start again"},
                fetch_recs_sort(SKey, SR5, self(), 1000, IsSec)
            ),
            ?assertEqual(ok, fetch_close(SKey, SR5, IsSec)), % actually not needed here, fetch_recs does it
            List5c = fetch_recs_sort(SKey, SR5, self(), 1000, IsSec),
            ?assertEqual(length(List5b), length(List5c)),
            ?assertEqual(lists:sort(List5b), lists:sort(result_tuples(List5c,SR5#stmtResult.rowFun))),
            io:format(user, "third read success (sync)~n", [])
        after
            ?assertEqual(ok, close(SKey, SR4))
        end,

        RowCount6 = imem_meta:table_size(def),
        SR6 = exec(SKey,query6, 3, IsSec, "select col1 from def;"),
        try
            ?assertEqual(ok, fetch_async(SKey, SR6, [{fetch_mode,push}], IsSec)),
            List6a = receive_list(SR6,true),
            ?assertEqual(RowCount6, length(List6a)),
            ?assertEqual([], receive_raw()),
            ?assertEqual(ok, insert_range(SKey, 5, def, 'Imem', IsSec)),
            ?assertEqual([], receive_raw())            
        after
            ?assertEqual(ok, close(SKey, SR6))
        end,

        SR7 = exec(SKey,query7, 3, IsSec, "select col1 from def;"),
        try
            ?assertEqual(ok, fetch_async(SKey, SR7, [{fetch_mode,skip},{tail_mode,true}], IsSec)),
            ?assertEqual([], receive_raw()),
            ?assertEqual(ok, insert_range(SKey, 5, def, 'Imem', IsSec)),
            List7a = receive_list(SR7,tail),
            ?assertEqual(5, length(List7a)),
            ?assertEqual([], receive_raw()),
            ?assertEqual(ok, fetch_close(SKey, SR7, IsSec)),
            ?assertEqual(ok, insert_range(SKey, 5, def, 'Imem', IsSec)),
            ?assertEqual([], receive_raw())
        after
            ?assertEqual(ok, close(SKey, SR7))
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

        F0 = make_filter_fun(1,true, []),
        ?assertEqual(true,F0(1)),        
        ?assertEqual(true,F0([])),        
        F1 = make_filter_fun(1,{'is_member', a, [a,b]}, []),
        ?assertEqual(true,F1(1)),        
        ?assertEqual(true,F1([])),        
        F2 = make_filter_fun(1,{'is_member', a, '$3'}, [{'$3',1,3}]),
        ?assertEqual(false,F2({{1,2,[3,4,5]}})),        
        ?assertEqual(true, F2({{1,2,[c,a,d]}})),        
        F3 = make_filter_fun(1,{'is_member', '$2', '$3'}, [{'$2',1,2},{'$3',1,3}]),
        ?assertEqual(true, F3({{1,d,[c,a,d]}})),        
        ?assertEqual(true, F3({{1,c,[c,a,d]}})),        
        ?assertEqual(true, F3({{1,a,[c,a,d]}})),        
        ?assertEqual(true, F3({{1,3,[3,4,5]}})),        
        ?assertEqual(false,F3({{1,2,[3,4,5]}})),        
        ?assertEqual(false,F3({{1,a,[3,4,5]}})),        
        ?assertEqual(false,F3({{1,[a],[3,4,5]}})),        
        ?assertEqual(false,F3({{1,3,[]}})),        

        F4 = make_filter_fun(1,{'is_member', a, '$_'}, []),
        ?assertEqual(true, F4({{1,a,[c,a,d]}})),        
        ?assertEqual(false, F4({{1,d,[c,a,d]}})),        

        case IsSec of
            true ->     ?imem_logout(SKey);
            false ->    ok
        end

    catch
        Class:Reason ->  io:format(user, "Exception ~p:~p~n~p~n", [Class, Reason, erlang:get_stacktrace()]),
        ?assert( true == "all tests completed")
    end,
    ok. 


insert_range(_SKey, 0, _Table, _Schema, _IsSec) -> ok;
insert_range(SKey, N, Table, Schema, IsSec) when is_integer(N), N > 0 ->
    if_call_mfa(IsSec, write,[SKey,Table,{Table,integer_to_list(N),N}]),
    insert_range(SKey, N-1, Table, Schema, IsSec).

exec(SKey,Id, BS, IsSec, Sql) ->
    io:format(user, "~p : ~s~n", [Id,Sql]),
    {RetCode, StmtResult} = imem_sql:exec(SKey, Sql, BS, 'Imem', IsSec),
    ?assertEqual(ok, RetCode),
    #stmtResult{stmtCols=StmtCols} = StmtResult,
    io:format(user, "Statement Cols : ~p~n", [StmtCols]),
    [?assert(is_binary(SC#stmtCol.alias)) || SC <- StmtCols],
    StmtResult.

fetch_async(SKey, StmtResult, Opts, IsSec) ->
    ?assertEqual(ok, fetch_recs_async(SKey, StmtResult#stmtResult.stmtRef, self(), Opts, IsSec)).

    % {M1,S1,Mic1} = erlang:now(),
    % {M2,S2,Mic2} = erlang:now(),
    % Count = length(Result),
    % Delta = Mic2 - Mic1 + 1000000 * ((S2-S1) + 1000000 * (M2-M1)),
    % Message = io_lib:format("fetch_recs latency per record: ~p usec",[Delta div Count]),
    % imem_meta:log_to_db(debug,?MODULE,fetch_recs,[{rec_count,Count},{fetch_duration,Delta}], Message),
