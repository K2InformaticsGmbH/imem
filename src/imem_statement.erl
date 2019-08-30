-module(imem_statement).

-define(VALID_FETCH_OPTS, [fetch_mode, tail_mode, params]).

-include("imem_seco.hrl").
-include("imem_sql.hrl").

-define(CALL_TIMEOUT(__Method), ?GET_CONFIG(callTimeout,[__Method],10000,"gen_server call timeout in msec.")).

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
        , filter_and_sort/5         %% apply FilterSpec and SortSpec and return new SQL and SortFun
        , filter_and_sort/6         %% apply FilterSpec and SortSpec and return new SQL and SortFun
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
        , receive_recs/2
        , receive_recs/3
        , recs_sort/2
        , result_tuples/2
        ]).

% Functions applied with Common Test
-export([ if_call_mfa/3
        , table_name/1          %% table name from physical_name={Schema,Name} or cluster_name={Node,Schema,Name}
        ]).

-record(fetchCtx,               %% state for fetch process
                    { pid       ::pid()
                    , monref    ::any()             %% fetch monitor ref
                    , status    ::atom()            %% undefined | waiting | fetching | done | tailing | aborted
                    , metarec   ::tuple()
                    , blockSize=100 ::integer()     %% could be adaptive
                    , rownum    ::undefined|integer %% next rownum to be used, if any, element(1,MetaRec)
                    , remaining=0   ::integer()     %% rows remaining to be fetched. initialized to Limit and decremented
                    , opts = [] ::list()            %% fetch options like {tail_mode,true}
                    , tailSpec  ::any()             %% compiled matchspec for master table condition (bound with MetaRec) 
                    , filter    ::any()             %% filter specification {Guard,Binds}
                    , recName   ::atom()
                    }).

-record(state,                  %% state for statment process, including fetch subprocess
                    { statement
                    , parmonref = undefined :: reference()  %% parent monitor reference (DOWN -> kill statement)
                    , isSec =false          :: boolean()
                    , seco=none             :: integer()
                    , fetchCtx=#fetchCtx{}         
                    , reply                                 %% reply destination TCP socket or Pid
                    , updPlan = []          :: list()       %% bulk execution plan (table updates/inserts/deletes)
                    }).

%% gen_server -----------------------------------------------------

create_stmt(Statement, SKey, IsSec) ->
    % Node = element(1,hd(Statement#statement.tables)),
    Node = node(),  % ToDo: switch to remove lock to local execution
    case {IsSec,node()} of
        {false,Node} ->     % meta, local
            gen_server:start(?MODULE, [Statement,self()], [{spawn_opt, [{fullsweep_after, 0}]}]);
        {false,_} ->        % meta, remote
            rpc:call( Node, gen_server, start
                    , [?MODULE, [Statement,self()], [{spawn_opt, [{fullsweep_after, 0}]}]]
            );
        {true,Node} ->      % sec, local
            {ok, Pid} = gen_server:start(?MODULE, [Statement,self()], []),
            NewSKey = imem_sec:clone_seco(SKey, Pid),
            ok = gen_server:call(Pid, {set_seco, NewSKey}),
            {ok, Pid};
        {true,_} ->         % sec, remote
            {ok, Pid} = rpc:call( Node, gen_server, start
                                , [?MODULE, [Statement,self()], [{spawn_opt, [{fullsweep_after, 0}]}]]
                                ),
            NewSKey = imem_sec:clone_seco(SKey, Pid),
            ok = rpc:call(gen_server,call,[Pid, {set_seco, NewSKey}]),
            {ok, Pid}
    end.

fetch_recs(SKey, Pid, Sock, Timeout, IsSec) ->
    fetch_recs(SKey, Pid, Sock, Timeout, [], IsSec).

fetch_recs(SKey, #stmtResults{stmtRefs=StmtRefs}, Sock, Timeout, Opts, IsSec) ->
    fetch_recs(SKey, StmtRefs, Sock, Timeout, Opts, IsSec);
fetch_recs(SKey, StmtRefs, Sock, Timeout, Opts, IsSec) when is_list(StmtRefs) ->
     [fetch_recs(SKey, StmtRef, Sock, Timeout, Opts, IsSec) || StmtRef <- StmtRefs];
fetch_recs(SKey, Pid, Sock, Timeout, Opts, IsSec) when is_pid(Pid) ->
    gen_server:cast(Pid, {fetch_recs_async, IsSec, SKey, Sock, Opts}),
    Result = try
        case receive 
            R ->    R
        after Timeout ->
            ?Debug("fetch_recs timeout ~p~n", [Timeout]),
            gen_server:call(Pid, {fetch_close, IsSec, SKey}), 
            ?ClientError({"Fetch timeout, increase timeout and retry",Timeout})
        end of
            {_, {Pid,{List, true}}} ->   List;
            {_, {Pid,{List, false}}} ->  
                ?Debug("fetch_recs too much data~n", []),
                gen_server:call(Pid, {fetch_close, IsSec, SKey}), 
                ?ClientError({"Too much data, increase block size or receive in streaming mode",length(List)});
            {_, {Pid,{error, {'SystemException', Reason}}}} ->
                ?Debug("fetch_recs exception ~p ~p~n", ['SystemException', Reason]),
                gen_server:call(Pid, {fetch_close, IsSec, SKey}),                
                ?SystemException(Reason);            
            {_, {Pid,{error, {'ClientError', Reason}}}} ->
                ?Debug("fetch_recs exception ~p ~p~n", ['ClientError', Reason]),
                gen_server:call(Pid, {fetch_close, IsSec, SKey}),                
                ?ClientError(Reason);            
            {_, {Pid,{error, {'UnimplementedException', Reason}}}} ->
                ?Debug("fetch_recs exception ~p ~p~n", ['UnimplementedException', Reason]),
                gen_server:call(Pid, {fetch_close, IsSec, SKey}),                
                ?UnimplementedException(Reason);
            Error ->
                ?Debug("fetch_recs bad async receive~n~p~n", [Error]),
                gen_server:call(Pid, {fetch_close, IsSec, SKey}),                
                ?SystemException({"Bad async receive",Error})            
        end
    after
        gen_server:call(Pid, {fetch_close, IsSec, SKey})
    end,
    Result.

fetch_recs_sort(SKey, #stmtResults{stmtRefs=StmtRefs, sortFun=SortFun}, Sock, Timeout, IsSec) ->
     recs_sort(lists:flatten([fetch_recs(SKey, StmtRef, Sock, Timeout, IsSec) || StmtRef <- StmtRefs]), SortFun);
fetch_recs_sort(SKey, StmtRefs, Sock, Timeout, IsSec) when is_list(StmtRefs) ->
     lists:sort(lists:flatten([fetch_recs(SKey, StmtRef, Sock, Timeout, IsSec) || StmtRef <- StmtRefs]));
fetch_recs_sort(SKey, Pid, Sock, Timeout, IsSec) when is_pid(Pid) ->
    lists:sort(fetch_recs(SKey, Pid, Sock, Timeout, IsSec)).

recs_sort(Recs, SortFun) ->
    % ?LogDebug("Recs:~n~p~n",[Recs]),
    List = [{SortFun(X),setelement(1,X,{}),X} || X <- Recs],
    % ?LogDebug("List:~n~p~n",[List]),
    [Rs || {_, _, Rs} <- lists:sort(List)].

fetch_recs_async(SKey, StmtResult, Sock, IsSec) ->
     fetch_recs_async(SKey, StmtResult, Sock, [], IsSec).

fetch_recs_async(SKey, #stmtResults{stmtRefs=StmtRefs}, Sock, Opts, IsSec) ->
     fetch_recs_async(SKey, StmtRefs, Sock, Opts, IsSec);
fetch_recs_async(SKey, StmtRefs, Sock, Opts, IsSec) when is_list(StmtRefs)->
     [fetch_recs_async(SKey, StmtRef, Sock, Opts, IsSec) || StmtRef <- StmtRefs];
fetch_recs_async(SKey, Pid, Sock, Opts, IsSec) when is_pid(Pid) ->
    case [{M,V} || {M,V} <- Opts, true =/= lists:member(M, ?VALID_FETCH_OPTS)] of
        [] -> gen_server:cast(Pid, {fetch_recs_async, IsSec, SKey, Sock, Opts});
        InvalidOpt -> ?ClientError({"Invalid option for fetch", InvalidOpt})
    end.

fetch_close(SKey, #stmtResults{stmtRefs=StmtRefs}, IsSec) ->
     fetch_close(SKey,  StmtRefs, IsSec);
fetch_close(SKey,  StmtRefs, IsSec) when is_list(StmtRefs) ->
     [fetch_close(SKey, StmtRef, IsSec) || StmtRef <- StmtRefs];
fetch_close(SKey, Pid, IsSec) when is_pid(Pid) ->
    gen_server:call(Pid, {fetch_close, IsSec, SKey}).

filter_and_sort(SKey, StmtResult, FilterSpec, SortSpec, IsSec) ->
    filter_and_sort(SKey, StmtResult, FilterSpec, SortSpec, [], IsSec).

filter_and_sort(SKey, #stmtResults{stmtRefs=StmtRefs}, FilterSpec, SortSpec, Cols, IsSec) ->
    filter_and_sort(SKey, StmtRefs, FilterSpec, SortSpec, Cols, IsSec); 
filter_and_sort(SKey,  StmtRefs, FilterSpec, SortSpec, Cols, IsSec) when is_list(StmtRefs) ->
     [filter_and_sort(SKey, hd(StmtRefs), FilterSpec, SortSpec, Cols, IsSec)];
filter_and_sort(SKey, Pid, FilterSpec, SortSpec, Cols, IsSec) when is_pid(Pid) ->
    gen_server:call(Pid, {filter_and_sort, IsSec, FilterSpec, SortSpec, Cols, SKey}).

update_cursor_prepare(SKey, #stmtResults{stmtRefs=StmtRefs}, IsSec, ChangeList) ->
    update_cursor_prepare(SKey, StmtRefs, IsSec, ChangeList);
update_cursor_prepare(SKey, StmtRefs, IsSec, ChangeList) when is_list(StmtRefs) ->
    [update_cursor_prepare(SKey, StmtRef, IsSec, ChangeList) || StmtRef <- StmtRefs];
update_cursor_prepare(SKey, Pid, IsSec, ChangeList) when is_pid(Pid) ->
    case gen_server:call(Pid, {update_cursor_prepare, IsSec, SKey, ChangeList},?CALL_TIMEOUT(update_cursor_prepare)) of
        ok ->   ok;
        Error-> throw(Error)
    end.

update_cursor_execute(SKey, #stmtResults{stmtRefs=StmtRefs}, IsSec, Lock) ->
    [update_cursor_execute(SKey, StmtRef, IsSec, Lock) || StmtRef <- StmtRefs];
update_cursor_execute(SKey, StmtRefs, IsSec, Lock) when is_list(StmtRefs) ->
    [update_cursor_execute(SKey, StmtRef, IsSec, Lock) || StmtRef <- StmtRefs];
update_cursor_execute(SKey, Pid, IsSec, Lock) when is_pid(Pid) ->
    update_cursor_exec(SKey, Pid, IsSec, Lock).

update_cursor_exec(SKey, Pid, IsSec, Lock) when Lock==none;Lock==optimistic ->
    Result = gen_server:call(Pid, {update_cursor_execute, IsSec, SKey, Lock},?CALL_TIMEOUT(update_cursor_execute)),
    % ?Debug("update_cursor_execute ~p~n", [Result]),
    case Result of
        KeyUpd when is_list(KeyUpd) ->  KeyUpd;
        Error ->                        throw(Error)
    end.

close(SKey, #stmtResults{stmtRefs=StmtRefs}) ->
    close(SKey, StmtRefs);
close(SKey, StmtRefs) when is_list(StmtRefs) ->
    [close(SKey, StmtRef) || StmtRef <- StmtRefs];
close(SKey, Pid) when is_pid(Pid) ->
    gen_server:cast(Pid, {close, SKey}).

init([Statement,ParentPid]) ->
    imem_meta:log_to_db(info,?MODULE,init,[],Statement#statement.stmtStr),
    {ok, #state{statement=Statement, parmonref=erlang:monitor(process, ParentPid)}}.

handle_call({set_seco, SKey}, _From, State) ->
    ?IMEM_SKEY_PUT(SKey), % store internal SKey in statement process, may be needed to authorize join functions
    % ?LogDebug("Putting SKey ~p to process dict of statement ~p",[SKey,self()]),
    {reply,ok,State#state{seco=SKey, isSec=true}};
handle_call({update_cursor_prepare, IsSec, _SKey, ChangeList}, _From, #state{statement=Stmt, seco=SKey}=State) ->
    STT = ?TIMESTAMP,
    {Reply, UpdatePlan1} = try
        {ok, update_prepare(IsSec, SKey, Stmt#statement.tables, Stmt#statement.colMap, ChangeList)}
    catch
        _:Reason ->  
            imem_meta:log_to_db(error, ?MODULE, handle_call, [{reason,Reason}, {changeList,ChangeList}], "update_cursor_prepare error"),
            {{error,Reason}, []}
    end,
    imem_meta:log_slow_process(?MODULE, update_cursor_prepare, STT, 100, 4000, [{table,hd(Stmt#statement.tables)},{rows,length(ChangeList)}]),    
    {reply, Reply, State#state{updPlan=UpdatePlan1}};  
handle_call({update_cursor_execute, IsSec, _SKey, Lock}, _From, #state{seco=SKey, fetchCtx=FetchCtx0, updPlan=UpdatePlan, statement=Stmt}=State) ->
    #fetchCtx{metarec=MR}=FetchCtx0,
    % ?Debug("UpdateMetaRec ~p~n", [MR]),
    STT = ?TIMESTAMP,
    Reply = try 
        % case FetchCtx0#fetchCtx.monref of
        %     undefined ->    ok;
        %     MonitorRef ->   kill_fetch(MonitorRef, FetchCtx0#fetchCtx.pid)
        % end,
        % ?Debug("UpdatePlan ~p~n", [UpdatePlan]),
        KeyUpdateRaw = if_call_mfa(IsSec,update_tables,[SKey, UpdatePlan, Lock]),
        % ?Debug("KeyUpdateRaw ~p~n", [KeyUpdateRaw]),
        case length(Stmt#statement.tables) of
            1 ->    
                WrapOut = fun({Tag,X}) -> {Tag,?MetaMain(MR, X)} end,
                lists:map(WrapOut, KeyUpdateRaw);
            _ ->            
                LastIdx = ?TableIdx(length(Stmt#statement.tables)),     %% position of last tuple
                WrapOut = fun
                    ({Tag,{}}) ->
                        R0 = erlang:make_tuple(LastIdx, undefined, [{?MetaIdx,MR},{?MainIdx,{}}]), 
                        {Tag,R0};                        
                    ({Tag,X}) ->
                        case join_rows([?MetaMain(MR, X)], FetchCtx0, Stmt) of
                            [] ->
                                R1 = erlang:make_tuple(LastIdx, undefined, [{?MetaIdx,MR},{?MainIdx,X}]), 
                                {Tag,R1};
                            [R2|_] ->
                                {Tag,R2}
                        end
                end,
                % ?Debug("map KeyUpdateRaw ~p~n", [KeyUpdateRaw]),
                lists:map(WrapOut, KeyUpdateRaw)
        end
    catch
        _:Reason ->
            imem_meta:log_to_db(error,?MODULE,handle_call,[{reason,Reason},{updPlan,UpdatePlan}],"update_cursor_execute error"),
            {error,Reason,erlang:get_stacktrace()}
    end,
    % ?Debug("update_cursor_execute result ~p~n", [Reply]),
    imem_meta:log_slow_process(?MODULE, update_cursor_execute, STT, 100, 4000, [{table, hd(Stmt#statement.tables)}, {rows, length(UpdatePlan)}]),    
    {reply, Reply, State};    
handle_call({filter_and_sort, _IsSec, FilterSpec, SortSpec, Cols0, _SKey}, _From, #state{statement=Stmt}=State) ->
    STT = ?TIMESTAMP,
    #statement{stmtParse={select,SelectSections}, colMap=ColMap, fullMap=FullMap} = Stmt,
    {_, WhereTree} = lists:keyfind(where, 1, SelectSections),
    % ?LogDebug("SelectSections~n~p~n", [SelectSections]),
    % ?LogDebug("ColMap~n~p~n", [ColMap]),
    % ?LogDebug("FullMap~n~p~n", [FullMap]),
    Reply = try
        NewSortFun = imem_sql_expr:sort_spec_fun(SortSpec, FullMap, ColMap),
        % ?Info("NewSortFun ~p~n", [NewSortFun]),
        OrderBy = imem_sql_expr:sort_spec_order(SortSpec, FullMap, ColMap),
        % ?Info("OrderBy ~p~n", [OrderBy]),
        Filter =  imem_sql_expr:filter_spec_where(FilterSpec, ColMap, WhereTree),
        % ?Info("Filter ~p~n", [Filter]),
        Cols1 = case Cols0 of
            [] ->   lists:seq(1,length(ColMap));
            _ ->    Cols0
        end,
        AllFields = imem_sql_expr:column_map_items(ColMap, ptree),
        % ?Info("AllFields ~p~n", [AllFields]),
        NewFields =  [lists:nth(N,AllFields) || N <- Cols1],
        % ?Info("NewFields ~p~n", [NewFields]),
        NewSections0 = lists:keyreplace('fields', 1, SelectSections, {'fields',NewFields}),
        NewSections1 = lists:keyreplace('where', 1, NewSections0, {'where',Filter}),
        % ?Info("NewSections1~n~p~n", [NewSections1]),
        NewSections2 = lists:keyreplace('order by', 1, NewSections1, {'order by',OrderBy}),
        % ?Info("NewSections2~n~p~n", [NewSections2]),
        NewSql = sqlparse_fold:top_down(sqlparse_format_flat,{select,NewSections2}, []),     % sql_box:flat_from_pt({select,NewSections2}),
        % ?Info("NewSql~n~p~n", [NewSql]),
        {ok, NewSql, NewSortFun}
    catch
        _:Reason ->
            imem_meta:log_to_db(error,?MODULE,handle_call,[{reason,Reason},{filter_spec,FilterSpec},{sort_spec,SortSpec},{cols,Cols0}],"filter_and_sort error"),
            {error,Reason}
    end,
    % ?LogDebug("replace_sort result ~p~n", [Reply]),
    imem_meta:log_slow_process(?MODULE, filter_and_sort, STT, 100, 4000, [{table, hd(Stmt#statement.tables)}, {filter_spec, FilterSpec}, {sort_spec, SortSpec}]),    
    {reply, Reply, State};
handle_call({fetch_close, _IsSec, _SKey}, _From, #state{statement=Stmt,fetchCtx=FetchCtx0}=State) ->
    % imem_meta:log_to_db(debug,?MODULE,handle_call,[{from,_From},{status,Status}],"fetch_close"),
    #fetchCtx{pid=Pid, monref=MonitorRef, status=Status} = FetchCtx0,
    case Status of
        undefined ->    ok;                             % close is ignored
        done ->         ok;                             % normal close after completed fetch
        waiting ->      kill_fetch(MonitorRef, Pid);    % client stops fetch 
        fetching ->     kill_fetch(MonitorRef, Pid);    % client stops fetch 
        tailing ->      unsubscribe(Stmt);              % client stops tail mode
        aborted ->      ok                              % client acknowledges abort
    end,
    FetchCtx1 = FetchCtx0#fetchCtx{pid=undefined, monref=undefined, metarec=undefined, status=undefined},   
    {reply, ok, State#state{fetchCtx=FetchCtx1}}.      % client may restart the fetch now

handle_cast({fetch_recs_async, _IsSec, _SKey, Sock, _Opts}, #state{fetchCtx=#fetchCtx{status=done}}=State) ->
    % ?Debug("fetch_recs_async called in status done~n", []),
    imem_meta:log_to_db(warning,?MODULE,handle_cast,[{sock,Sock},{opts,_Opts},{status,done}],"fetch_recs_async rejected"),
    send_reply_to_client(Sock, {error,{'ClientError',"Fetch is completed, execute fetch_close before fetching from start again"}}),
    {noreply, State}; 
handle_cast({fetch_recs_async, _IsSec, _SKey, Sock, _Opts}, #state{fetchCtx=#fetchCtx{status=aborted}}=State) ->
    % ?Debug("fetch_recs_async called in status aborted~n", []),
    imem_meta:log_to_db(warning,?MODULE,handle_cast,[{sock,Sock},{opts,_Opts},{status,aborted}],"fetch_recs_async rejected"),
    send_reply_to_client(Sock, {error,{'SystemException',"Fetch is aborted, execute fetch_close before fetching from start again"}}),
    {noreply, State}; 
handle_cast({fetch_recs_async, _IsSec, _SKey, Sock, _Opts}, #state{fetchCtx=#fetchCtx{status=tailing}}=State) ->
    % ?Debug("fetch_recs_async called in status tailing~n", []),
    imem_meta:log_to_db(warning,?MODULE,handle_cast,[{sock,Sock},{opts,_Opts},{status,tailing}],"fetch_recs_async rejected"),
    send_reply_to_client(Sock, {error,{'ClientError',"Fetching in tail mode, execute fetch_close before fetching from start again"}}),
    {noreply, State}; 
handle_cast({fetch_recs_async, IsSec, _SKey, Sock, Opts}, #state{statement=Stmt, seco=SKey, fetchCtx=FetchCtx0}=State) ->
    % ?LogDebug("fetch_recs_async called in status ~p~n", [FetchCtx0#fetchCtx.status]),
    % ?LogDebug("fetch_recs_async called with Stmt~n~p~n", [Stmt]),
    #statement{tables=[Table|JTabs], blockSize=BlockSize, mainSpec=MainSpec, metaFields=MetaFields, stmtParams=Params0} = Stmt,
    % imem_meta:log_to_db(debug,?MODULE,handle_cast,[{sock,Sock},{opts,Opts},{status,FetchCtx0#fetchCtx.status}],"fetch_recs_async"),
    case {lists:member({fetch_mode,skip},Opts), FetchCtx0#fetchCtx.pid} of
        {Skip,undefined} ->      %% {SkipFetch, Pid} = {true|false, uninitialized} -> skip fetch
            Params1 = case lists:keyfind(params, 1, Opts) of
                false ->    Params0;    %% from statement exec, only used on first fetch
                {_, P} ->   P           %% from fetch_recs, only used on first fetch
            end,
            RecName = try imem_meta:table_record_name(Table)
                catch {'ClientError', {"Table does not exist", _}} -> undefined
            end,
            RowNum = case MetaFields of
                [<<"rownum">>|_] -> 1;
                _ ->                undefined
            end, 
            MR = imem_sql:meta_rec(IsSec,SKey,MetaFields,Params1,FetchCtx0#fetchCtx.metarec),
            % ?LogDebug("Meta Rec: ~p~n", [MR]),
            % ?LogDebug("Main Spec before meta bind:~n~p~n", [MainSpec]),
            case Skip of
                true ->
                    {_SSpec,TailSpec,FilterFun} = imem_sql_expr:bind_scan(?MainIdx,{MR},MainSpec),
                    % ?LogDebug("Tail Spec after meta bind:~n~p~n", [TailSpec]),
                    % ?LogDebug("Filter Fun after meta bind:~n~p~n", [FilterFun]),
                    FetchSkip = #fetchCtx{status=undefined,metarec=MR,blockSize=BlockSize
                                         ,rownum=RowNum,remaining=MainSpec#scanSpec.limit
                                         ,opts=Opts,tailSpec=TailSpec
                                         ,filter=FilterFun,recName=RecName},
                    handle_fetch_complete(State#state{reply=Sock,fetchCtx=FetchSkip}); 
                false ->
                    try 
                        {TailSpec,FilterFun,FetchStartResult} = case imem_meta:is_virtual_table(Table) of 
                            false ->    
                                {SS,TS,FF} = imem_sql_expr:bind_scan(?MainIdx,{MR},MainSpec),
                                % ?LogDebug("Scan Spec after meta bind:~n~p", [SS]),
                                % ?LogDebug("Tail Spec after meta bind:~n~p", [TS]),
                                % ?LogDebug("Filter Fun after meta bind:~n~p", [FF]),
                                {TS,FF,if_call_mfa(IsSec, fetch_start, [SKey, self(), Table, SS, BlockSize, Opts])};
                            true ->     
                                {[{_,[SG],_}],TS,FF} = imem_sql_expr:bind_virtual(?MainIdx,{MR},MainSpec),
                                % ?LogDebug("Virtual Tail Spec after meta bind:~n~p", [TS]),
                                % ?LogDebug("Virtual Filter Fun after meta bind:~n~p", [FF]),
                                % ?LogDebug("Virtual Scan SGuard after meta bind:~n~p", [SG]),
                                SGuard = imem_sql_expr:to_guard(SG),
                                % ?LogDebug("Virtual Scan SGuard after meta bind:~n~p", [SGuard]),
                                Rows = [{RecName,I,K} ||{I,K} <- generate_virtual_data(RecName,{MR},SGuard,RowNum)],
                                % ?LogDebug("Generated virtual scan data ~p~n~p", [Table,Rows]),
                                {TS,FF,if_call_mfa(IsSec, fetch_start_virtual, [SKey, self(), Table, Rows, BlockSize, RowNum, Opts])}
                        end,
                        get_select_permissions(IsSec,SKey, [Table|JTabs]),
                        % ?LogDebug("Select permission granted for : ~p", [Table]),
                        case FetchStartResult of
                            TransPid when is_pid(TransPid) ->
                                MonitorRef = erlang:monitor(process, TransPid),
                                TransPid ! next,
                                % ?LogDebug("fetch opts ~p~n", [Opts]),
                                RecName = try imem_meta:table_record_name(Table)
                                          catch {'ClientError', {"Table does not exist", _TableName}} -> undefined
                                          end,
                                FetchStart = #fetchCtx{pid=TransPid,monref=MonitorRef,status=waiting
                                                      ,metarec=MR,blockSize=BlockSize
                                                      ,rownum=RowNum,remaining=MainSpec#scanSpec.limit
                                                      ,opts=Opts,tailSpec=TailSpec,filter=FilterFun
                                                      ,recName=RecName},
                                {noreply, State#state{reply=Sock,fetchCtx=FetchStart}}; 
                            Error ->
                                send_reply_to_client(Sock, {error, {"Cannot spawn async fetch process", Error}}),
                                FetchAborted1 = #fetchCtx{pid=undefined, monref=undefined, status=aborted},
                                {noreply, State#state{reply=Sock,fetchCtx=FetchAborted1}}
                        end
                    catch
                        _:Err ->
                            send_reply_to_client(Sock, {error, Err}),
                            FetchAborted2 = #fetchCtx{pid=undefined, monref=undefined, status=aborted},
                            {noreply, State#state{reply=Sock,fetchCtx=FetchAborted2}}
                    end
            end;
        {true,_Pid} ->          %% skip ongoing fetch (and possibly go to tail_mode)
            MR = imem_sql:meta_rec(IsSec,SKey,MetaFields,Params0,FetchCtx0#fetchCtx.metarec),
            % ?LogDebug("Meta Rec: ~p~n", [MR]),
            % ?LogDebug("Main Spec before meta bind:~n~p~n", [MainSpec]),
            {_SSpec,TailSpec,FilterFun} = imem_sql_expr:bind_scan(?MainIdx,{MR},MainSpec),
            % ?LogDebug("Tail Spec after meta bind:~n~p~n", [TailSpec]),
            % ?LogDebug("Filter Fun after meta bind:~n~p~n", [FilterFun]),
            FetchSkipRemaining = FetchCtx0#fetchCtx{metarec=MR,opts=Opts,tailSpec=TailSpec,filter=FilterFun},
            handle_fetch_complete(State#state{reply=Sock,fetchCtx=FetchSkipRemaining}); 
        {false,Pid} ->          %% fetch next block
            MR = imem_sql:meta_rec(IsSec,SKey,MetaFields,Params0,FetchCtx0#fetchCtx.metarec),
            % ?LogDebug("Meta Rec: ~p~n", [MR]),
            % ?LogDebug("Main Spec before meta bind:~n~p~n", [MainSpec]),
            {_SSpec,TailSpec,FilterFun} = imem_sql_expr:bind_scan(?MainIdx,{MR},MainSpec),
            % ?LogDebug("Tail Spec after meta bind:~n~p~n", [TailSpec]),
            % ?LogDebug("Filter Fun after meta bind:~n~p~n", [FilterFun]),
            Pid ! next,
            % ?LogDebug("fetch opts ~p~n", [Opts]),
            FetchContinue = FetchCtx0#fetchCtx{metarec=MR,opts=Opts,tailSpec=TailSpec,filter=FilterFun}, 
            {noreply, State#state{reply=Sock,fetchCtx=FetchContinue}}  
    end;
handle_cast({close, _SKey}, State) ->
    % imem_meta:log_to_db(debug,?MODULE,handle_cast,[],"close statement"),
    ?Info("received close for ~p", [element(2,State#state.statement)]),
    {stop, normal, State}; 
handle_cast(Request, State) ->
    ?Debug("received unsolicited cast ~p~nin state ~p~n", [Request, State]),
    imem_meta:log_to_db(error,?MODULE,handle_cast,[{request,Request},{state,State}],"receives unsolicited cast"),
    {noreply, State}.

handle_info({row, ?eot}, #state{reply=Sock,fetchCtx=FetchCtx0}=State) ->
    % ?Debug("received end of table in fetch status ~p~n", [FetchCtx0#fetchCtx.status]),
    ?Info("received end of table in state~n~p~n", [State]),
    case FetchCtx0#fetchCtx.status of
        fetching ->
            imem_meta:log_to_db(warning,?MODULE,handle_info,[{row, ?eot},{status,fetching},{sock,Sock}],"eot"),
            send_reply_to_client(Sock, {[],true}),  
            % ?Debug("late end of table received in state~n~p~n", [State]),
            handle_fetch_complete(State);
        _ ->
            ?Debug("unexpected end of table received in state~n~p~n", [State]),        
            imem_meta:log_to_db(warning,?MODULE,handle_info,[{row, ?eot}],"eot"),
            {noreply, State}
    end;        
handle_info({mnesia_table_event, {write, _Table, {schema, _TableName, _TableNewProperties}, _OldRecords, _ActivityId}}, State) ->
    % imem_meta:log_to_db(debug,?MODULE,handle_info,[{mnesia_table_event,write, schema}],"tail delete"),
    ?Info("received mnesia subscription event ~p ~p ~p~n", [write_schema, _TableName, _TableNewProperties]),
    {noreply, State};
handle_info({mnesia_table_event,{write,_Table,R0,_OldRecords,_ActivityId}},#state{reply=Sock}=State) ->
    case process_tail_row(R0, State) of
        {[], NewState} -> {noreply, NewState};
        {Result, NewState} ->
            send_reply_to_client(Sock, Result),
            {noreply, NewState}
    end;
handle_info({mnesia_table_event,{delete, schema, _What, _SchemaInfo, _ActivityId}}, State) ->
    {noreply, State};
handle_info({mnesia_table_event,{delete, _Table, _What, DelRows, _ActivityId}}, State) ->
    NewState = process_tail_delete_rows(DelRows, State),
    {noreply, NewState};
handle_info({row, Rows0}, #state{reply=Sock, isSec=IsSec, seco=SKey, fetchCtx=FetchCtx0, statement=Stmt}=State) ->
    #fetchCtx{metarec=MR0,rownum=RowNum,remaining=Rem0,status=Status,filter=FilterFun, opts=Opts}=FetchCtx0,
    {Rows1,Complete} = case {Status,Rows0} of
        {waiting,[?sot,?eot|R]} ->
            ?Info("received ~p rows for ~p data complete", [length(Rows0)-2,element(2,Stmt)]),
            {R,true};
        {waiting,[?sot|R]} ->            
            ?Info("received ~p rows for ~p data first", [length(Rows0)-1,element(2,Stmt)]),
            {R,false};
        {fetching,[?eot|R]} ->
            ?Info("received ~p rows for ~p data complete", [length(Rows0)-1,element(2,Stmt)]),
            {R,true};
        {fetching,[?sot,?eot|_R]} ->
            imem_meta:log_to_db(warning,?MODULE,handle_info,[{row,length(_R)}],"data transaction restart"),     
            ?Info("received ~p rows for ~p data transaction restart", [length(Rows0)-2,element(2,Stmt)]),
            handle_fetch_complete(State);
        {fetching,[?sot|_R]} ->
            imem_meta:log_to_db(warning,?MODULE,handle_info,[{row,length(_R)}],"data transaction restart"),     
            ?Info("received ~p rows for ~p data transaction restart", [length(Rows0)-1,element(2,Stmt)]),
            handle_fetch_complete(State);
        {fetching,R} ->            
            % imem_meta:log_to_db(debug,?MODULE,handle_info,[{row,length(R)}],"data"),     
            ?Info("received ~p rows for ~p", [length(Rows0),element(2,Stmt)]),
            {R,false};
        {BadStatus,R} ->            
            imem_meta:log_to_db(error,?MODULE,handle_info,[{status,BadStatus},{row,length(R)}],"data"),     
            ?Info("received ~p rows for ~p bad status", [length(Rows0),element(2,Stmt)]),
            {R,false}        
    end,   
    % ?Info("Filtering ~p rows with filter ~p~n", [length(Rows1),FilterFun]),
    MR1 = case RowNum of
        undefined -> MR0;
        _ ->         setelement(?RownumIdx,MR0,RowNum)
    end,
    Wrap = fun(X) -> ?MetaMain(MR1, X) end,
    Result = case {length(Stmt#statement.tables),FilterFun,RowNum} of
        {_,false,_} ->
            [];
        {1,true,undefined} ->
            take_remaining(Rem0, lists:map(Wrap, Rows1));
        {1,_,undefined} ->
            take_remaining(Rem0, lists:filter(FilterFun,lists:map(Wrap, Rows1)));
        {_,true,undefined} ->
            join_rows(lists:map(Wrap, Rows1), FetchCtx0, Stmt);
        {_,_,undefined} ->
            join_rows(lists:filter(FilterFun,lists:map(Wrap, Rows1)), FetchCtx0, Stmt);
        {1,true,FirstRN} ->
            update_row_num(MR1, FirstRN, take_remaining(Rem0, lists:map(Wrap, Rows1)));
        {_,true,FirstRN} ->
            JoinedRows = join_rows(lists:map(Wrap, Rows1), FetchCtx0, Stmt),
            update_row_num(MR1, FirstRN, JoinedRows);
        {_,_,FirstRN} ->
            JoinedRows = join_rows(lists:filter(FilterFun,lists:map(Wrap,Rows1)), FetchCtx0, Stmt),
            update_row_num(MR1, FirstRN, JoinedRows)
    end,
    case is_number(Rem0) of
        true ->
            % ?Debug("sending rows ~p~n", [Result]),
            case length(Result) >= Rem0 of
                true ->     
                    % ?LogDebug("send~n~p~n", [{Result, true}]),
                    send_reply_to_client(Sock, {Result, true}),
                    handle_fetch_complete(State);
                false ->    
                    % ?LogDebug("send~n~p~n", [{Result, Complete}]),
                    send_reply_to_client(Sock, {Result, Complete}),
                    FetchCtx1 = case RowNum of
                        undefined ->    FetchCtx0#fetchCtx{remaining=Rem0-length(Result),status=fetching};
                        _ ->            FetchCtx0#fetchCtx{rownum=RowNum+length(Result),remaining=Rem0-length(Result),status=fetching}
                    end,
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
            ?Debug("receiving rows ~n~p~n", [Rows0]),
            ?Debug("in unexpected state ~n~p~n", [State]),
            {noreply, State}
    end;
handle_info({'DOWN', PMR, process, _Pid, normal}, #state{parmonref=PMR}=State) ->
    % ?Debug("received normal exit from parent ~p ref ~p", [_Pid, PMR]),
    {stop, normal, State};
handle_info({'DOWN', PMR, process, _Pid, Reason}, #state{parmonref=PMR}=State) ->
    ?Info("received unexpected exit info from parent ~p ref ~p reason ~p", [_Pid, PMR, Reason]),
    {stop, {shutdown,Reason}, State};
handle_info({'DOWN', _Ref, process, _Pid, _Reason}, #state{reply=undefined,fetchCtx=FetchCtx0}=State) ->
    % ?Debug("received expected exit info from cursor process ~p ref ~p reason ~p~n", [_Pid, _Ref, _Reason]),
    FetchCtx1 = FetchCtx0#fetchCtx{monref=undefined, status=undefined},   
    {noreply, State#state{fetchCtx=FetchCtx1}};
handle_info({'DOWN', Ref, process, Pid, {aborted, {no_exists,{_TableName,_}}=Reason}}, State) ->
    handle_info({'DOWN', Ref, process, Pid, Reason}, State);
handle_info({'DOWN', Ref, process, _Pid, {no_exists,{TableName,_}}=_Reason}, #state{reply=Sock,fetchCtx=#fetchCtx{monref=Ref}}=State) ->
    ?Info("received unexpected exit info from cursor process ~p ref ~p reason ~p", [_Pid, Ref, _Reason]),
    send_reply_to_client(Sock, {error, {'ClientError', {"Table does not exist", TableName}}}),
    {noreply, State#state{fetchCtx=#fetchCtx{pid=undefined, monref=undefined, status=aborted}}};
handle_info(_Info, State) ->
    ?Warn("received unsolicited info ~p ~nin state ~p~n", [_Info, State]),
    {noreply, State}.

handle_fetch_complete(#state{reply=Sock,fetchCtx=FetchCtx0,statement=Stmt}=State)->
    #fetchCtx{pid=Pid,monref=MonitorRef,opts=Opts}=FetchCtx0,
    kill_fetch(MonitorRef, Pid),
    % ?Debug("fetch complete, opts ~p~n", [Opts]), 
    case lists:member({tail_mode,true},Opts) of
        false ->
            % ?Debug("fetch complete no tail ~p~n", [Opts]), 
            {noreply, State#state{fetchCtx=FetchCtx0#fetchCtx{status=done}}};           
        true ->     
            Table = table_name(hd(Stmt#statement.tables)),
            % ?Debug("fetch complete, switching to tail_mode~p~n", [Opts]), 
            case  catch if_call_mfa(false,subscribe,[none,{table, Table, detailed}]) of
                ok ->
                    % ?Debug("Subscribed to table changes ~p~n", [Table]),    
                    {noreply, State#state{fetchCtx=FetchCtx0#fetchCtx{status=tailing}}};
                Error ->
                    ?Debug("Cannot subscribe to table changes~n~p~n", [{Table,Error}]),    
                    imem_meta:log_to_db(error,?MODULE,handle_fetch_complete,[{table,Table},{error,Error},{sock,Sock}],"Cannot subscribe to table changes"),
                    send_reply_to_client(Sock, {error,{'SystemException',{"Cannot subscribe to table changes",{Table,Error}}}}),
                    {noreply, State#state{fetchCtx=FetchCtx0#fetchCtx{status=done}}}   
            end    
    end.

table_name({_,TN}) -> TN;
table_name({_,_,TN}) -> TN.

terminate(_Reason, #state{fetchCtx=#fetchCtx{pid=Pid, monref=undefined}}) -> 
    % ?Debug("terminating monitor not found~n", []),
    catch Pid ! abort, 
    ok;
terminate(_Reason, #state{statement=Stmt,fetchCtx=#fetchCtx{status=tailing}}) -> 
    % ?Debug("terminating tail_mode~n", []),
    unsubscribe(Stmt),
    ok;
terminate(_Reason, #state{fetchCtx=#fetchCtx{pid=Pid, monref=MonitorRef}}) ->
    % ?Debug("demonitor and terminate~n", []),
    kill_fetch(MonitorRef, Pid), 
    ok.

code_change(_OldVsn, State, _Extra) -> {ok, State}.


get_select_permissions(false, _, _) -> true;
get_select_permissions(true, _, []) -> true;
get_select_permissions(true, SKey, [Table|JTabs]) ->
    case imem_meta:is_virtual_table(Table) of
        true ->     
            get_select_permissions(true, SKey, JTabs);
        false ->
            case imem_sec:have_table_permission(SKey, Table, select) of
                false ->   ?SecurityException({"Select unauthorized", {Table,SKey}});
                true ->    get_select_permissions(true, SKey, JTabs)
        end
    end.

unsubscribe(Stmt) ->
    Table = table_name(hd(Stmt#statement.tables)),
    case catch if_call_mfa(false,unsubscribe,[none,{table,Table,detailed}]) of
        ok ->       ok;
        {ok,_} ->   ok;
        Error ->
                    ?Debug("Cannot unsubscribe table changes~n~p~n", [{Table,Error}]),    
                    imem_meta:log_to_db(error,?MODULE,unsubscribe,[{table,Table},{error,Error}],"Cannot unsubscribe table changes")
    end.

kill_fetch(undefined, undefined) -> ok;
kill_fetch(MonitorRef, Pid) ->
    catch erlang:demonitor(MonitorRef, [flush]),
    catch Pid ! abort. 

take_remaining(Remaining, Rows) ->            
    RowC = length(Rows),
    if  
        RowC == 0 ->            [];
        RowC < Remaining ->     Rows;
        RowC == Remaining ->    Rows;
        Remaining =< 0 ->       [];
        Remaining > 0 ->
            {ResultRows,_Rest} = lists:split(Remaining, Rows),
            % LastKey = element(?KeyIdx,element(?MainIdx,lists:last(ResultRows))),    %% 2=KeyPosInRecord 2=MainTablePosInRecs
            % Pred = fun(X) -> (element(?KeyIdx,element(?MainIdx,X))==LastKey) end,   %% 2=KeyPosInRecord 2=MainTablePosInRecs
            % ResultTail = lists:takewhile(Pred, Rest),                               %% Try to give all rows for lasr key in a bag
            % %% TODO: may need to read more blocks to completely read this key in a bag
            % ResultRows ++ ResultTail
            ResultRows
    end.

process_tail_delete_rows([], State) -> State;
process_tail_delete_rows([R0 | Rest], #state{reply=Sock}=State) ->
    case process_tail_row(R0, State) of
        {[], NewState} -> process_tail_delete_rows(Rest, NewState);
        {Result, NewState} ->
            send_reply_to_client(Sock, {delete, Result}),
            process_tail_delete_rows(Rest, NewState)
    end.


process_tail_row(R0,#state{isSec=IsSec,seco=SKey,fetchCtx=FetchCtx0,statement=Stmt}=State) ->
    % imem_meta:log_to_db(debug,?MODULE,handle_info,[{mnesia_table_event,write}],"tail write"),
    % ?LogDebug("received mnesia subscription event ~p ~p~n", [write, Record]),
    #fetchCtx{status=Status,metarec=MR0,rownum=RowNum,remaining=Rem0,filter=FilterFun,tailSpec=TailSpec,recName=RecName}=FetchCtx0,
    MR1 = imem_sql:meta_rec(IsSec,SKey,Stmt#statement.metaFields,[],MR0),    %% Params cannot change any more after first fetch
    MR2 = case RowNum of
        undefined -> MR1;
        _ ->         setelement(?RownumIdx,MR1,RowNum)
    end,
    R1 = erlang:setelement(?RecIdx, R0, RecName),  %% Main Tail Record (record name recovered)
    % ?LogDebug("processing tail : record / tail / filter~n~p~n~p~n~p~n", [R1,TailSpec,FilterFun]),
    RawRecords = case {Status,TailSpec,FilterFun} of
        {tailing,false,_} ->        [];
        {tailing,_,false} ->        [];
        {tailing,undefined,_} ->    ?SystemException({"Undefined tail function for tail record",R1});
        {tailing,_,undefined} ->    ?SystemException({"Undefined filter function for tail record",R1});
        {tailing,true,true} ->      [?MetaMain(MR2, R1)];
        {tailing,true,_} ->         lists:filter(FilterFun, [?MetaMain(MR2, R1)]);
        {tailing,_,true} ->
            case ets:match_spec_run([R1], TailSpec) of
                [] ->   [];
                [R2] -> [?MetaMain(MR2, R2)]
            end;
        {tailing,_,_} ->
            case ets:match_spec_run([R1], TailSpec) of
                [] ->   [];
                [R2] -> lists:filter(FilterFun, [?MetaMain(MR2, R2)])
            end;
        STF ->
            imem_meta:log_to_db(warning,?MODULE,handle_info,[{status,STF},{rec,R1}],"Unexpected status in tail event"),
            []
    end,
    case {RawRecords,length(Stmt#statement.tables),RowNum} of
        {[],_,_} ->   %% nothing to be returned
            {[], State};
        {_,1,undefined} ->    %% single table select, avoid join overhead
            if
                Rem0 =< 1 ->
                    % ?LogDebug("send~n~p~n", [{RawRecords,true}]),
                    unsubscribe(Stmt),
                    {{RawRecords,true}, State#state{fetchCtx=FetchCtx0#fetchCtx{status=done}}};
                true ->
                    % ?LogDebug("send~n~p~n", [{RawRecords,tail}]),
                    {{RawRecords,tail}, State#state{fetchCtx=FetchCtx0#fetchCtx{remaining=Rem0-1}}}
            end;
        {_,1,_FirstRN} ->    %% single table select, avoid join overhead but update RowNum meta field
            if
                Rem0 =< 1 ->
                    % ?LogDebug("send~n~p~n", [{RawRecords,true}]),
                    unsubscribe(Stmt),
                    {{RawRecords,true}, State#state{fetchCtx=FetchCtx0#fetchCtx{status=done}}};
                true ->
                    % ?LogDebug("send~n~p~n", [{RawRecords,tail}]),
                    {{RawRecords,tail}, State#state{fetchCtx=FetchCtx0#fetchCtx{rownum=RowNum+1,remaining=Rem0-1}}}
            end;
        {_,_,undefined} ->        %% join raw result of main scan with remaining tables
            case join_rows(RawRecords, FetchCtx0, Stmt) of
                [] ->
                    {[], State};
                Result ->
                    if
                        (Rem0 =< length(Result)) ->
                            % ?LogDebug("send~n~p~n", [{Result,true}]),
                            unsubscribe(Stmt),
                            {{Result,true}, State#state{fetchCtx=FetchCtx0#fetchCtx{status=done}}};
                        true ->
                            % ?LogDebug("send~n~p~n", [{Result,tail}]),
                            {{Result,tail}, State#state{fetchCtx=FetchCtx0#fetchCtx{remaining=Rem0-length(Result)}}}
                    end
            end;
        {_,_,_} ->        %% join raw result of main scan with remaining tables, correct RowNum
            case join_rows(RawRecords, FetchCtx0, Stmt) of
                [] ->
                    {[], State};
                JoinedRows ->
                    if
                        (Rem0 =< length(JoinedRows)) ->
                            % ?LogDebug("send~n~p~n", [{Result,true}]),
                            unsubscribe(Stmt),
                            {{update_row_num(MR2, RowNum, JoinedRows),true}, State#state{fetchCtx=FetchCtx0#fetchCtx{status=done}}};
                        true ->
                            % ?LogDebug("send~n~p~n", [{Result,tail}]),
                            {{update_row_num(MR2, RowNum, JoinedRows),tail}, State#state{fetchCtx=FetchCtx0#fetchCtx{rownum=RowNum+length(JoinedRows),remaining=Rem0-length(JoinedRows)}}}
                    end
            end
    end.

update_row_num(MR, FirstRN, Rows) ->
    UpdRN = fun({R,X}) -> setelement(?MetaIdx, X, setelement(?RownumIdx, MR, R)) end,
    lists:map(UpdRN, zip_row_num(FirstRN,Rows)).

zip_row_num(FirstRN,Rows) ->    
    lists:zip(lists:seq(FirstRN, FirstRN+length(Rows)-1), Rows).

join_rows(MetaAndMainRows, FetchCtx0, Stmt) ->
    #fetchCtx{blockSize=BlockSize, remaining=RemainingRowQuota}=FetchCtx0,
    JoinTables = tl(Stmt#statement.tables),  %% {Schema,Name,Alias} for each table to join
    JoinSpecs = Stmt#statement.joinSpecs,
    % ?Info("Join Tables: ~p~n", [JoinTables]),
    % ?Info("Join Specs: ~p~n", [JoinSpecs]),
    join_rows(MetaAndMainRows, BlockSize, RemainingRowQuota, JoinTables, JoinSpecs, []).

join_rows([], _, _, _, _, Acc) -> Acc;                              %% lists:reverse(Acc);
join_rows(_, _, RRowQuota, _, _, Acc) when RRowQuota < 1 -> Acc;    %% lists:reverse(Acc);
join_rows([Row|Rows], BlockSize, RRowQuota, JoinTables, JoinSpecs, Acc) ->
    Rec = erlang:make_tuple(length(JoinTables)+2, undefined, [{?MetaIdx,element(?MetaIdx,Row)},{?MainIdx,element(?MainIdx,Row)}]),
    JAcc = join_row([Rec], BlockSize, ?MainIdx+1, JoinTables, JoinSpecs),
    join_rows(Rows, BlockSize, RRowQuota-length(JAcc), JoinTables, JoinSpecs, Acc++JAcc).

join_row(Recs, _BlockSize, _Ti, [], []) -> Recs;
join_row(Recs0, BlockSize, Ti, [{_S,JoinTable}|Tabs], [JS|JSpecs]) ->
    %% Ti = tuple index, points to the tuple position, starts with main table index for each row
    Recs1 = case lists:member(JoinTable,?DataTypes) of
        true ->  [join_virtual(Rec, BlockSize, Ti, JoinTable, JS) || Rec <- Recs0];
        false -> [join_table(Rec, BlockSize, Ti, JoinTable, JS) || Rec <- Recs0]
    end,
    join_row(lists:flatten(Recs1), BlockSize, Ti+1, Tabs, JSpecs).

join_table(Rec, _BlockSize, Ti, Table, #scanSpec{limit=Limit}=JoinSpec) ->
    % ?LogDebug("Join ~p table ~p~n", [Ti,Table]),
    % ?LogDebug("Rec used for join bind~n~p~n", [Rec]),
    {SSpec,_TailSpec,FilterFun} = imem_sql_expr:bind_scan(Ti,Rec,JoinSpec),
    % ?Info("SSpec for join~n~p~n", [SSpec]),
    % ?Info("TailSpec for join~n~p~n", [_TailSpec]),
    % ?Info("FilterFun for join~n~p~n", [FilterFun]),
    MaxSize = Limit+1000,   %% TODO: Move away from single shot join fetch, use async block fetch here as well.
    case imem_meta:select(Table, SSpec, MaxSize) of
        {[], true} ->   [];
        {L, true} ->
            % ?LogDebug("scan result for join~n~p~n", [L]),
            case FilterFun of
                true ->     [setelement(Ti, Rec, I) || I <- L];
                false ->    [];
                Filter ->   Recs = [setelement(Ti, Rec, I) || I <- L],
                            lists:filter(Filter,Recs)
            end;
        {_, false} ->   ?ClientError({"Too much data in join intermediate result", MaxSize});
        Error ->        ?SystemException({"Unexpected join intermediate result", Error})
    end.

join_virtual(Rec, _BlockSize, Ti, Table, #scanSpec{limit=Limit}=JoinSpec) ->
    {SSpec1,_TailSpec,FilterFun} = imem_sql_expr:bind_virtual(Ti,Rec,JoinSpec),
    [{_,[SGuard1],_}] = SSpec1,
    % ?LogDebug("Virtual join table (~p) ~p~n", [Ti,Table]),
    % ?LogDebug("Virtual join spec (~p) ~p~n", [Ti,JoinSpec]),
    % ?LogDebug("Rec used for join bind (~p)~n~p~n", [Ti,Rec]),
    % ?LogDebug("Virtual join scan spec (~p)~n~p~n", [Ti,SSpec1]),
    % ?LogDebug("Virtual join guard bound (~p)~n~p~n", [Ti,SGuard1]),
    MaxSize = Limit+1000,   %% TODO: Move away from single shot join fetch, use async block fetch here as well.
    Virt = generate_virtual_data(Table,Rec,imem_sql_expr:to_guard(SGuard1),MaxSize),
    % ?LogDebug("Generated virtual data ~p~n~p", [Table,Virt]),
    Recs = [setelement(Ti, Rec, {Table,I,K}) || {I,K} <- Virt],
    % ?LogDebug("Generated records ~p~n~p", [Table,Recs]),
    case FilterFun of
        true ->     Recs;
        false ->    [];
        Filter ->   lists:filter(Filter,Recs)
    end.

generate_virtual_data(_Table,_Rec,false,_MaxSize) -> 
    [];
generate_virtual_data(boolean,_Rec,true,_MaxSize) ->
    [{false,<<"false">>},{true,<<"true">>}];
generate_virtual_data(_Table,_Rec,true,_MaxSize) ->
    ?ClientError({"Invalid virtual filter guard", true});
generate_virtual_data(Table,Rec,{is_member,Tag,'$_'},MaxSize) when is_atom(Tag) ->
    Items = element(?MainIdx,Rec),
    generate_virtual(Table,tl(tuple_to_list(Items)),MaxSize);
generate_virtual_data(Table,Rec,{'and',{is_member,Tag,'$_'},_},MaxSize) when is_atom(Tag) ->
    Items = element(?MainIdx,Rec), 
    generate_virtual(Table,tl(tuple_to_list(Items)),MaxSize);
generate_virtual_data(Table,Rec,{'and',{'and',{is_member,Tag,'$_'},_},_},MaxSize) when is_atom(Tag) ->
    Items = element(?MainIdx,Rec), 
    generate_virtual(Table,tl(tuple_to_list(Items)),MaxSize);
generate_virtual_data(Table,_Rec,{is_member,Tag, Items},MaxSize) when is_atom(Tag) ->
    generate_virtual(Table,Items,MaxSize);
generate_virtual_data(Table,_Rec,{'and',{is_member,Tag, Items},_},MaxSize) when is_atom(Tag) ->
    generate_virtual(Table,Items,MaxSize);
generate_virtual_data(tuple,_Rec,{'==',Tag,{const,Val}},MaxSize) when is_atom(Tag),is_tuple(Val) ->
    generate_virtual(tuple,{const,Val},MaxSize);
generate_virtual_data(Table,_Rec,{'==',Tag,Val},MaxSize) when is_atom(Tag) ->
    generate_virtual(Table,Val,MaxSize);
generate_virtual_data(Table,_Rec,{'==',Val,Tag},MaxSize) when is_atom(Tag) ->
    generate_virtual(Table,Val,MaxSize);
generate_virtual_data(Table,_Rec,{'or',{'==',Tag,V1},OrEqual},MaxSize) when is_atom(Tag) ->
    generate_virtual(Table,[V1|vals_from_or_equal(Tag,OrEqual)],MaxSize);
% generate_virtual_data(Table,_Rec,{'and',{'or',{'==',Tag,V1},OrEqual},_},MaxSize) when is_atom(Tag) ->
%     generate_virtual(Table,[V1|vals_from_or_equal(Tag,OrEqual)],MaxSize);
% generate_virtual_data(Table,_Rec,{'and',{'and',{'or',{'==',Tag,V1},OrEqual},_},_},MaxSize) when is_atom(Tag) ->
%     generate_virtual(Table,[V1|vals_from_or_equal(Tag,OrEqual)],MaxSize);
generate_virtual_data(integer,_Rec,{'and',{'>=',Tag,Min},{'=<',Tag,Max}},MaxSize) ->
    generate_virtual_integers(Min,Max,MaxSize);
generate_virtual_data(integer,_Rec,{'and',{'>',Tag,Min},{'=<',Tag,Max}},MaxSize) ->
    generate_virtual_integers(Min+1,Max,MaxSize);
generate_virtual_data(integer,_Rec,{'and',{'>=',Tag,Min},{'<',Tag,Max}},MaxSize) ->
    generate_virtual_integers(Min,Max-1,MaxSize);
generate_virtual_data(integer,_Rec,{'and',{'>',Tag,Min},{'<',Tag,Max}},MaxSize) ->
    generate_virtual_integers(Min+1,Max-1,MaxSize);
generate_virtual_data(integer,_Rec,SGuard,_MaxSize) ->
    ?UnimplementedException({"Unsupported virtual integer bound filter guard",SGuard});
generate_virtual_data(Table,_Rec,SGuard,_MaxSize) ->
    ?UnimplementedException({"Unsupported virtual join bound filter guard",{Table,SGuard}}).

vals_from_or_equal(Tag,OrEqual) ->
    vals_from_or_equal(Tag,OrEqual,[]).

vals_from_or_equal(Tag, {'==',Tag,V}, Acc) -> 
    lists:reverse([V|Acc]);
vals_from_or_equal(Tag, {'or',{'==',Tag,V},OrEqual}, Acc) -> 
    vals_from_or_equal(Tag, OrEqual, [V|Acc]);
vals_from_or_equal(_, SGuard, _Acc) -> 
    ?UnimplementedException({"Unsupported virtual in() filter guard",SGuard}).

generate_virtual_integers(Min,Max,MaxSize) when (Max>=Min),(MaxSize>Max-Min) ->
    [{I,imem_datatype:integer_to_io(I)} || I <- lists:seq(Min,Max)];
generate_virtual_integers(Min,Max,_MaxSize) ->
    ?UnimplementedException({"Unsupported virtual integer table size", Max-Min+1}).

generate_virtual(list=Table, {list,Items}, MaxSize)  ->
    generate_virtual(Table, Items, MaxSize);
% generate_virtual(Table, {const,Items}, MaxSize) when is_tuple(Items) ->
%     generate_virtual(Table, tuple_to_list(Items), MaxSize);

generate_virtual(tuple, {const,I}, _) when is_tuple(I) ->
    [{I,imem_datatype:term_to_io(I)}];

generate_virtual(_, [], _) -> [];

generate_virtual(_, <<>>, _) -> [];

generate_virtual(atom=Table, Items, MaxSize) when is_list(Items) ->
    generate_limit_check(Table, length(Items), MaxSize),
    Pred = fun(X) -> is_atom(X) end,
    [{I,imem_datatype:atom_to_io(I)} || I <- lists:filter(Pred,Items)];
generate_virtual(atom, I, _) when is_atom(I)-> [{I,imem_datatype:atom_to_io(I)}];
generate_virtual(atom, _, _) -> [];

generate_virtual(binary=Table, Items, MaxSize) when is_list(Items) ->
    generate_limit_check(Table, length(Items), MaxSize),
    [{I,imem_datatype:binary_to_io(I)} || I <- Items];
generate_virtual(binary=Table, Items, MaxSize) when is_binary(Items) ->
    generate_limit_check(Table, byte_size(Items), MaxSize),
    [{list_to_binary([I]),list_to_binary([I])} || I <- binary_to_list(Items)];
generate_virtual(binary, _, _) -> [];

generate_virtual(binstr=Table, Items, MaxSize) when is_list(Items) ->
    generate_limit_check(Table, length(Items), MaxSize),
    [{I,imem_datatype:binstr_to_io(I)} || I <- Items];
generate_virtual(binstr=Table, Items, MaxSize) when is_binary(Items) ->
    generate_limit_check(Table, byte_size(Items), MaxSize),
    String = binary_to_list(Items),
    case io_lib:printable_unicode_list(String) of
        true ->     [{<<S>>,<<S>>} || S <- String];
        false ->    []
    end;
generate_virtual(binstr, _, _) -> [];

generate_virtual(boolean=Table, Items, MaxSize) when is_list(Items) ->
    generate_limit_check(Table, length(Items), MaxSize),
    Pred = fun(X) -> is_boolean(X) end,
    [{I,imem_datatype:boolean_to_io(I)} || I <- lists:filter(Pred,Items)];
generate_virtual(boolean, I, _) when is_boolean(I)-> [{I,imem_datatype:boolean_to_io(I)}];
generate_virtual(boolean, _, _) -> [];

generate_virtual(datetime=Table, Items, MaxSize) when is_list(Items) ->
    generate_limit_check(Table, length(Items), MaxSize),
    Pred = fun
        ({{_,_,_},{_,_,_}}) -> true;
        (_) -> false
    end,
    [{I,imem_datatype:datetime_to_io(I)} || I <- lists:filter(Pred,Items)];
generate_virtual(datetime, {{_,_,_},{_,_,_}}=I, _) -> 
    [{I,imem_datatype:datetime_to_io(I)}];
generate_virtual(datetime, _, _) -> [];

generate_virtual(decimal=Table, Items, MaxSize) when is_list(Items) ->  % TODO: check function
    generate_limit_check(Table, length(Items), MaxSize),
    Pred = fun(X) -> is_integer(X) end,
    [{I,imem_datatype:integer_to_io(I)} || I <- lists:filter(Pred,Items)];
generate_virtual(decimal, I, _) when is_integer(I)-> [{I,imem_datatype:integer_to_io(I)}];
generate_virtual(decimal, _, _) -> [];

generate_virtual(float=Table, Items, MaxSize) when is_list(Items) ->
    generate_limit_check(Table, length(Items), MaxSize),
    Pred = fun(X) -> is_float(X) end,
    [{I,imem_datatype:float_to_io(I)} || I <- lists:filter(Pred,Items)];
generate_virtual(float, I, _) when is_float(I)-> [{I,imem_datatype:float_to_io(I)}];
generate_virtual(float, _, _) -> [];

generate_virtual('fun'=Table, Items, MaxSize) when is_list(Items) ->
    generate_limit_check(Table, length(Items), MaxSize),
    Pred = fun(X) -> is_function(X) end,
    [{I,imem_datatype:fun_to_io(I)} || I <- lists:filter(Pred,Items)];
generate_virtual('fun', I, _) when is_function(I)-> [{I,imem_datatype:fun_to_io(I)}];
generate_virtual('fun', _, _) -> [];

generate_virtual(integer=Table, Items, MaxSize) when is_list(Items) ->
    generate_limit_check(Table, length(Items), MaxSize),
    Pred = fun(X) -> is_integer(X) end,
    [{I,imem_datatype:integer_to_io(I)} || I <- lists:filter(Pred,Items)];
generate_virtual(integer=Table, Items, MaxSize) when is_binary(Items) ->
    generate_limit_check(Table, byte_size(Items), MaxSize),
    [{I,list_to_binary([I])} || I <- binary_to_list(Items)];
generate_virtual(integer, I, _) when is_integer(I)-> [{I,imem_datatype:integer_to_io(I)}];
generate_virtual(integer, _, _) -> [];

generate_virtual(ipaddr=Table, Items, MaxSize) when is_list(Items) ->
    generate_limit_check(Table, length(Items), MaxSize),
    Pred = fun
        ({A,B,C,D}) when is_integer(A), is_integer(B), is_integer(C), is_integer(D) -> true;
        (_) -> false
    end,                                %% ToDo: IpV6
    [{I,imem_datatype:ipaddr_to_io(I)} || I <- lists:filter(Pred,Items)];
generate_virtual(ipaddr, {A,B,C,D}=I, _) when is_integer(A), is_integer(B), is_integer(C), is_integer(D) -> [{I,imem_datatype:ipaddr_to_io(I)}];
generate_virtual(ipaddr, _, _) -> [];      %% ToDo: IpV6

generate_virtual(list=Table, Items, MaxSize) when is_list(Items) ->
    generate_limit_check(Table, length(Items), MaxSize),
    [{I,imem_datatype:term_to_io(I)} || I <- Items];

generate_virtual(json=Table, Items, MaxSize) when is_binary(Items) ->
    generate_virtual(json=Table, imem_json:to_proplist(Items), MaxSize);
generate_virtual(json=Table, Items, MaxSize) ->
    generate_limit_check(Table, length(Items), MaxSize),
    case Items of
        [] ->                       [];
        [{_,_}|_] = PL ->           [{V,K} || {K,V} <- PL];
        Arr when is_list(Arr) ->    [{I,imem_datatype:term_to_io(I)} || I <- Arr];
        _ ->                        []
    end;

generate_virtual(timestamp=Table, Items, MaxSize) when is_list(Items) ->
    generate_limit_check(Table, length(Items), MaxSize),
    Pred = fun
        ({Meg,Sec,Mic}) when is_number(Meg), is_integer(Sec), is_integer(Mic) -> true;
        (_) -> false
    end,
    [{I,imem_datatype:timestamp_to_io(I)} || I <- lists:filter(Pred,Items)];
generate_virtual(timestamp, {Meg,Sec,Mic}=I, _) when is_number(Meg), is_integer(Sec), is_integer(Mic) -> 
    [{I,imem_datatype:integer_to_io(I)}];
generate_virtual(timestamp, _, _) -> [];

generate_virtual(tuple=Table, Items, MaxSize) when is_list(Items) ->
    generate_limit_check(Table, length(Items), MaxSize),
    Pred = fun({const,X}) -> is_tuple(X); (_) -> false end,
    [{I,imem_datatype:term_to_io(I)} || {const,I} <- lists:filter(Pred,Items)];
generate_virtual(tuple, I, _) when is_tuple(I)-> [{I,imem_datatype:term_to_io(I)}];
generate_virtual(tuple, _, _) -> [];

generate_virtual(pid=Table, Items, MaxSize) when is_list(Items) ->
    generate_limit_check(Table, length(Items), MaxSize),
    Pred = fun(X) -> is_pid(X) end,
    [{I,imem_datatype:pid_to_io(I)} || I <- lists:filter(Pred,Items)];
generate_virtual(pid, I, _) when is_pid(I)-> [{I,imem_datatype:pid_to_io(I)}];
generate_virtual(pid, _, _) -> [];

generate_virtual(ref=Table, Items, MaxSize) when is_list(Items) ->
    generate_limit_check(Table, length(Items), MaxSize),
    Pred = fun(X) -> is_reference(X) end,
    [{I,imem_datatype:ref_to_io(I)} || I <- lists:filter(Pred,Items)];
generate_virtual(ref, I, _) when is_reference(I)-> [{I,imem_datatype:ref_to_io(I)}];
generate_virtual(ref, _, _) -> [];

generate_virtual(string, [], _) -> [];
generate_virtual(string=Table, Items, MaxSize) when is_list(Items) ->
    generate_limit_check(Table, length(Items), MaxSize),
    case io_lib:printable_unicode_list(Items) of
        true ->     [{I,imem_datatype:integer_to_io(I)} || I <- Items];
        false ->    [{I,imem_datatype:string_to_io(I)} || I <- Items]
    end;

generate_virtual(term=Table, Items, MaxSize) when is_list(Items) ->
    generate_limit_check(Table, length(Items), MaxSize),
    [{I,imem_datatype:term_to_io(I)} || I <- Items];
generate_virtual(term, I, _) -> [{I,imem_datatype:term_to_io(I)}];

generate_virtual(userid=Table, Items, MaxSize) when is_list(Items) ->
    generate_limit_check(Table, length(Items), MaxSize),
    Pred = fun(X) -> is_integer(X) end,
    [{I,imem_datatype:term_to_io(I)} || I <- lists:filter(Pred,Items)];  %% ToDo: filter in imem_account
generate_virtual(userid, I, _) when is_integer(I)-> 
    [{I,imem_datatype:integer_to_io(I)}];                                %% ToDo: filter in imem_account
generate_virtual(userid, system, _) -> 
    [{system,imem_datatype:atom_to_io(system)}];
generate_virtual(userid, _, _) -> [];

generate_virtual(Table, Items, _MaxSize) -> 
    ?UnimplementedException({"Unsupported virtual table generation",{Table,Items}}).

generate_limit_check(Table, CurSize, MaxSize) when CurSize > MaxSize -> 
    ?ClientError({"Too much data for virtual table generation",{Table,CurSize,MaxSize}});
generate_limit_check(_,_,_) -> ok. 

send_reply_to_client(SockOrPid, Result) ->
    NewResult = {self(), Result},
    imem_server:send_resp(NewResult, SockOrPid).

update_prepare(IsSec, SKey, [{_Node,Schema,Table}|_], ColMap, ChangeList) ->
    PTN = imem_meta:physical_table_name(Table),
    {TableType, DefRec, Trigger} =  imem_meta:trigger_infos({Schema,PTN}),
    User = if_call_mfa(IsSec, meta_field_value, [SKey, user]),
    TableInfo = {Schema,PTN,TableType,list_to_tuple(DefRec),Trigger,User,[]},  % No trigger options supported for now
    %% transform a ChangeList   
        % [1,nop,{?EmptyMR,{def,"2","'2'"}},"2"],               %% no operation on this line
        % [5,ins,{},"99"],                                      %% insert {def,"99", undefined}
        % [3,del,{?EmptyMR,{def,"5","'5'"}},"5"],               %% delete {def,"5","'5'"}
        % [4,upd,{?EmptyMR,{def,"12","'12'"}},"112"]            %% update {def,"12","'12'"} to {def,"112","'12'"}
    %% into an UpdatePlan                                       {table} = {Schema,PTN,Type}
        % [1,{table},{def,"2","'2'"},{def,"2","'2'"}],          %% no operation on this line
        % [5,{table},{},{def,"99", undefined}],                 %% insert {def,"99", undefined}
        % [3,{table},{def,"5","'5'"},{}],                       %% delete {def,"5","'5'"}
        % [4,{table},{def,"12","'12'"},{def,"112","'12'"}]      %% failing update {def,"12","'12'"} to {def,"112","'12'"}
    UpdPlan = update_prepare(IsSec, SKey, TableInfo, ColMap, ChangeList, []),
    UpdPlan.

-define(replace(__X,__Cx,__New), setelement(?MainIdx, __X, setelement(__Cx,element(?MainIdx,__X), __New))). 
-define(ins_repl(__X,__Cx,__New), setelement(__Cx,__X, __New)). 

update_prepare(_IsSec, _SKey, _TableInfo, _ColMap, [], Acc) -> Acc;
update_prepare(IsSec, SKey, {S,Tab,Typ,_,Trigger,User,TrOpts}=TableInfo, ColMap, [[Item,nop,Recs|_]|CList], Acc) ->
    Action = [{S,Tab,Typ}, Item, element(?MainIdx,Recs), element(?MainIdx,Recs), Trigger, User, TrOpts],     
    update_prepare(IsSec, SKey, TableInfo, ColMap, CList, [Action|Acc]);
update_prepare(IsSec, SKey, {S,Tab,Typ,_,Trigger,User,TrOpts}=TableInfo, ColMap, [[Item,del,Recs|_]|CList], Acc) ->
    Action = [{S,Tab,Typ}, Item, element(?MainIdx,Recs), {}, Trigger, User, TrOpts],     
    update_prepare(IsSec, SKey, TableInfo, ColMap, CList, [Action|Acc]);
update_prepare(IsSec, SKey, {S,Tab,Typ,DefRec,Trigger,User,TrOpts}=TableInfo, ColMap, [[Item,upd,Recs|Values]|CList], Acc) ->
    % ?Info("update_prepare ColMap~n~p", [ColMap]),
    % ?Info("update_prepare Values~n~p", [Values]),
    if  
        length(Values) > length(ColMap) ->      ?ClientError({"Too many values",{Item,Values}});        
        length(Values) < length(ColMap) ->      ?ClientError({"Too few values",{Item,Values}});        
        true ->                                 ok    
    end,            
    UpdateMap = lists:sort(
        [ case CMap of
            #bind{tind=0,cind=0,func=Proj,btree=BTree} when is_function(Proj) ->
                case BTree of
                    {hd,#bind{tind=?MainIdx,cind=Cx,type=Type}=B} ->
                        Fx = fun(X) -> 
                            OldVal = Proj(X),
                            case {Value, OldVal} of 
                                {?navio,?nav} ->
                                    X;                                                  % attribute still not present
                                {?navio,_} ->       
                                    ?replace(X,Cx,tl(?BoundVal(B,X)));                  % remove head
                                _ ->
                                    case imem_datatype:io_to_db(Item,OldVal,list_type(Type),undefined,undefined,<<>>,false,Value) of
                                        OldVal ->   X;
                                        NewVal ->   ?replace(X,Cx,[NewVal|tl(?BoundVal(B,X))])
                                    end
                            end
                        end,     
                        {Cx,1,Fx};
                    {last,#bind{tind=?MainIdx,cind=Cx,type=Type}=B} ->
                        Pos = length(?BoundVal(B,Recs)),
                        Fx = fun(X) -> 
                            OldVal = Proj(X),
                            case {Value, OldVal} of 
                                {?navio,?nav} ->
                                    X;                                                  % attribute still not present
                                {?navio,_} ->       
                                    ?replace(X,Cx,lists:sublist(?BoundVal(B,X),Pos-1)); % remove last element
                                _ ->
                                    case imem_datatype:io_to_db(Item,OldVal,list_type(Type),undefined,undefined,<<>>,false,Value) of
                                        OldVal ->   X;
                                        NewVal ->   ?replace(X,Cx,lists:sublist(?BoundVal(B,X),Pos-1) ++ NewVal)
                                    end
                            end
                        end,     
                        {Cx,Pos,Fx};
                    {element,PosBind,#bind{tind=?MainIdx,cind=Cx,type=Type}=B} ->    
                        Pos=imem_sql_expr:bind_tree(PosBind,Recs), 
                        Fx = fun(X) -> 
                            OldVal = Proj(X),
                            case imem_datatype:io_to_db(Item,OldVal,tuple_type(Type,Pos),undefined,undefined,<<>>,false,Value) of
                                OldVal ->   X;
                                NewVal ->   ?replace(X,Cx,setelement(Pos,?BoundVal(B,X),NewVal))
                            end
                        end,     
                        {Cx,Pos,Fx};
                     {bytes,#bind{tind=?MainIdx,cind=Cx,type=T}=B,StartBind,LenBind} when T==bitstring;T==binary;T==term;T==binterm ->    
                        Start=imem_sql_expr:bind_tree(StartBind,Recs), 
                        Len=imem_sql_expr:bind_tree(LenBind,Recs), 
                        Fx = fun(X) -> 
                            OldVal = Proj(X),
                            OldBin = ?BoundVal(B,X),
                            case imem_datatype:io_to_db(Item,OldVal,binary,undefined,undefined,<<>>,false,Value) of
                                OldVal ->   X;
                                NewVal when size(NewVal) /= size(OldVal) ->
                                    ?ClientError({"Wrong byte size of replacement binary",{Item,size(NewVal)}});
                                NewVal when Start >= 0 -> 
                                    Prefix = binary:part(OldBin,0,Start),
                                    Suffix = binary:part(OldBin,Start+Len, size(OldBin)-Len-Start),
                                    ?replace(X,Cx,<<Prefix/binary,NewVal/binary,Suffix/binary>>);
                                NewVal when Start < 0 -> 
                                    Prefix = binary:part(OldBin,0,size(OldBin)+Start),
                                    Suffix = binary:part(OldBin,size(OldBin)+Start+Len, size(OldBin)+Start-Len),
                                    ?replace(X,Cx,<<Prefix/binary,NewVal/binary,Suffix/binary>>)
                            end
                        end,     
                        {Cx,abs(Start)+1,Fx};
                    {bytes,#bind{tind=?MainIdx,cind=Cx,type=T}=B,StartBind} when T==bitstring;T==binary;T==term;T==binterm ->    
                        Start=imem_sql_expr:bind_tree(StartBind,Recs), 
                        Fx = fun(X) -> 
                            OldVal = Proj(X),
                            OldBin = ?BoundVal(B,X),
                            case imem_datatype:io_to_db(Item,OldVal,binary,undefined,undefined,<<>>,false,Value) of
                                OldVal ->   X;
                                NewVal when Start >= 0 -> 
                                    Prefix = binary:part(OldBin,0,Start),
                                    ?replace(X,Cx,<<Prefix/binary,NewVal/binary>>);
                                NewVal when size(OldBin)+Start >= 0 -> 
                                    Prefix = binary:part(OldBin,0,size(OldBin)+Start),
                                    ?replace(X,Cx,<<Prefix/binary,NewVal/binary>>);
                                NewVal when size(OldBin)+Start < 0 -> 
                                    ?replace(X,Cx,NewVal)
                            end
                        end,     
                        {Cx,abs(Start)+1,Fx};
                    {bits,#bind{tind=?MainIdx,cind=Cx,type=T}=B,StartBind,LenBind} when T==bitstring;T==binary;T==term;T==binterm ->    
                        Start=imem_sql_expr:bind_tree(StartBind,Recs), 
                        Len=imem_sql_expr:bind_tree(LenBind,Recs), 
                        Fx = fun(X) -> 
                            OldVal = Proj(X),
                            OldBin = ?BoundVal(B,X),
                            case imem_datatype:io_to_db(Item,OldVal,integer,undefined,undefined,<<>>,false,Value) of
                                OldVal ->   X;
                                NewInt when Start >= 0 -> 
                                    <<Prefix:Start,_:Len,Suffix/bitstring>> = OldBin,
                                    ?replace(X,Cx,<<Prefix:Start,NewInt:Len,Suffix/bitstring>>);
                                NewInt when Start < 0 -> 
                                    PrefixLen = bit_size(OldBin)+Start,
                                    <<Prefix:PrefixLen,_:Len,Suffix/bitstring>> = OldBin,
                                    ?replace(X,Cx,<<Prefix:PrefixLen,NewInt:Len,Suffix/bitstring>>)
                            end
                        end,     
                        {Cx,abs(Start)+1,Fx};
                    {bits,#bind{tind=?MainIdx,cind=Cx,type=T}=B,StartBind} when T==bitstring;T==binary;T==term;T==binterm ->    
                        Start=imem_sql_expr:bind_tree(StartBind,Recs), 
                        Fx = fun(X) -> 
                            OldVal = Proj(X),
                            OldBin = ?BoundVal(B,X),
                            case imem_datatype:io_to_db(Item,OldVal,integer,undefined,undefined,<<>>,false,Value) of
                                OldVal ->   X;
                                NewInt when Start >= 0 -> 
                                    Len =  bit_size(OldBin)-Start,
                                    <<Prefix:Start,_:Len>> = OldBin,
                                    ?replace(X,Cx,<<Prefix:Start,NewInt:Len>>);
                                NewInt when Start < 0 -> 
                                    Len = -Start,
                                    PrefixLen = bit_size(OldBin)+Start,
                                    <<Prefix:PrefixLen,_:Len>> = OldBin,
                                    ?replace(X,Cx,<<Prefix:PrefixLen,NewInt:Len>>)
                            end
                        end,     
                        {Cx,abs(Start)+1,Fx};
                   {nth,PosBind,#bind{tind=?MainIdx,cind=Cx,type=Type}=B} ->
                        Pos = imem_sql_expr:bind_tree(PosBind,Recs),
                        Fx = fun(X) -> 
                            OldVal = Proj(X),
                            case {Value, OldVal} of 
                                {?navio,?nav} ->
                                    X;                                                  % attribute still not present
                                {?navio,_} ->
                                    Old = ?BoundVal(B,X),       
                                    case Pos of 
                                        1 ->    ?replace(X,Cx,tl(Old));                 % remove head
                                        _ ->    ?replace(X,Cx,lists:sublist(Old,Pos-1) ++ lists:nthtail(Pos, Old))
                                    end;
                                _ ->
                                    case imem_datatype:io_to_db(Item,OldVal,list_type(Type),undefined,undefined,<<>>,false,Value) of
                                        OldVal ->               
                                            X;
                                        NewVal1 when Pos==1 ->  
                                            ?replace(X,Cx,[NewVal1|tl(?BoundVal(B,X))]);
                                        NewVal2 ->              
                                            Old = ?BoundVal(B,X),
                                            ?replace(X,Cx,lists:sublist(Old,Pos-1) ++ NewVal2 ++ lists:nthtail(Pos,Old))
                                    end 
                            end
                        end,     
                        {Cx,Pos,Fx};
                    {from_binterm,#bind{tind=?MainIdx,cind=Cx,type=Type}} ->
                        Fx = fun(X) -> 
                            OldVal = Proj(X),
                            case imem_datatype:io_to_db(Item,OldVal,Type,undefined,undefined,<<>>,false,Value) of
                                OldVal ->   X;
                                NewVal ->   ?replace(X,Cx,NewVal)
                            end
                        end,     
                        {Cx,1,Fx};
                    {json_value,AttName,#bind{tind=?MainIdx,cind=Cx}=B} ->
                        Fx = fun(X) -> 
                            OldVal = Proj(X),
                            case {Value, OldVal} of 
                                {<<>>,?nav} ->  
                                    X;                         % attribute still not present
                                {<<>>,_} ->     
                                    ?replace(X,Cx,imem_json:remove(AttName,?BoundVal(B,X))); 
                                _ ->
                                    case Value of
                                        OldVal ->   X;
                                        _ ->
                                            try   
                                                case imem_json:decode(Value) of
                                                    OldVal ->   X;
                                                    NewVal1 ->  ?replace(X,Cx,imem_json:put(AttName,NewVal1,?BoundVal(B,X)))
                                                end
                                            catch _:_ ->
                                                case imem_json:decode(imem_datatype:add_dquotes(Value)) of
                                                    OldVal ->   X;
                                                    NewVal2 ->  ?replace(X,Cx,imem_json:put(AttName,NewVal2,?BoundVal(B,X)))
                                                end
                                            end
                                    end
                            end
                        end,     
                        {Cx,1,Fx};
                    {map_get,KeyBind,#bind{tind=?MainIdx,cind=Cx}=B} ->    
                        Key=imem_sql_expr:bind_tree(KeyBind,Recs), 
                        Fx = fun(X) -> 
                            OldVal = Proj(X),
                            case {Value, OldVal} of 
                                {?navio,?nav} ->
                                    X;                % attribute still not present
                                {?navio,_} ->
                                    ?replace(X,Cx,maps:remove(Key,?BoundVal(B,X)));
                                _ ->
                                    case imem_datatype:io_to_db(Item,OldVal,term,undefined,undefined,<<>>,false,Value) of
                                        OldVal ->   X;
                                        NewVal ->   ?replace(X,Cx,maps:put(Key,NewVal,?BoundVal(B,X)))
                                    end
                            end
                        end,     
                        {Cx,1,Fx};
                    {map_get,KeyBind,{map_get,ParentKeyBind,#bind{tind=?MainIdx,cind=Cx}=B}} ->    
                        Key=imem_sql_expr:bind_tree(KeyBind,Recs), 
                        ParentKey=imem_sql_expr:bind_tree(ParentKeyBind,Recs), 
                        Fx = fun(X) -> 
                            OldVal = Proj(X),
                            case {Value, OldVal} of 
                                {?navio,?nav} ->
                                    X;                % attribute still not present
                                {?navio,_} ->
                                    NewParent = maps:remove(Key,maps:get(ParentKey,?BoundVal(B,X))),
                                    ?replace(X,Cx,maps:put(ParentKey,NewParent,?BoundVal(B,X))); 
                                _ ->
                                    case imem_datatype:io_to_db(Item,OldVal,term,undefined,undefined,<<>>,false,Value) of
                                        OldVal ->   
                                            X;
                                        NewVal ->   
                                            NewParent = maps:put(Key,NewVal,maps:get(ParentKey,?BoundVal(B,X))),
                                            ?replace(X,Cx,maps:put(ParentKey,NewParent,?BoundVal(B,X)))
                                    end
                            end
                        end,     
                        {Cx,2,Fx};
                    {json_value,AttName,{json_value,ParentName,#bind{tind=?MainIdx,cind=Cx}=B}} -> 
                        Fx = fun(X) -> 
                            OldVal = Proj(X),
                            case {Value, OldVal} of 
                                {<<>>,?nav} ->  
                                    X;                         % attribute still not present
                                {<<>>,_} ->
                                    NewParent = imem_json:remove(AttName,imem_json:get(ParentName,?BoundVal(B,X))),
                                    ?replace(X,Cx,imem_json:put(ParentName,NewParent,?BoundVal(B,X))); 
                                _ ->
                                    case Value of
                                        OldVal ->   X;
                                        _ ->
                                            try   
                                                case imem_json:decode(Value) of
                                                    OldVal ->   
                                                        X;
                                                    NewVal1 -> 
                                                        NewParent1 = imem_json:put(AttName,NewVal1,imem_json:get(ParentName,?BoundVal(B,X))),
                                                        ?replace(X,Cx,imem_json:put(ParentName,NewParent1,?BoundVal(B,X)))
                                                end
                                            catch _:_ ->
                                                case imem_json:decode(imem_datatype:add_dquotes(Value)) of
                                                    OldVal ->   
                                                        X;
                                                    NewVal2 ->   
                                                         NewParent2 = imem_json:put(AttName,NewVal2,imem_json:get(ParentName,?BoundVal(B,X))),
                                                        ?replace(X,Cx,imem_json:put(ParentName,NewParent2,?BoundVal(B,X)))
                                               end
                                            end
                                    end
                            end
                        end,     
                        {Cx,1,Fx};
                    Other ->    ?SystemException({"Internal error, bad projection binding",{Item,Other}})
                end;
            #bind{tind=0,cind=0} ->  
                ?SystemException({"Internal update error, constant projection binding",{Item,CMap}});
            #bind{tind=?MainIdx,cind=0,type=tuple} ->
                Fx = fun(X) -> 
                    OldVal = element(?MainIdx,X),
                    case imem_datatype:io_to_db(Item,OldVal,tuple,undefined,undefined,<<>>,false,Value) of
                        OldVal ->   X;
                        NewVal ->   setelement(?MainIdx,X,NewVal)
                    end
                end,     
                {0,0,Fx};
            #bind{tind=?MainIdx,cind=Cx,type=T,len=L,prec=P,default=D} ->
                Fx = fun(X) -> 
                    ?replace(X,Cx,imem_datatype:io_to_db(Item,?BoundVal(CMap,X),T,L,P,D,false,Value))
                end,     
                {Cx,0,Fx}
          end
          || 
          {#bind{readonly=R}=CMap,Value} 
          <- lists:zip(ColMap,Values), R==false % , Value /= ?navio
        ]),    
    UpdatedRecs = update_recs(Recs, UpdateMap),
    NewRec = if_call_mfa(IsSec, apply_validators, [SKey, DefRec, element(?MainIdx, UpdatedRecs), Tab]),
    Action = [{S,Tab,Typ}, Item, element(?MainIdx,Recs), NewRec, Trigger, User, TrOpts],     
    update_prepare(IsSec, SKey, TableInfo, ColMap, CList, [Action|Acc]);
update_prepare(IsSec, SKey, {S,Tab,Typ,DefRec,Trigger,User,TrOpts}=TableInfo, ColMap, [[Item,ins,Recs|Values]|CList], Acc) ->
    % ?Info("ColMap~n~p~n", [ColMap]),
    % ?Info("Values~n~p~n", [Values]),
    if  
        length(Values) > length(ColMap) ->      ?ClientError({"Too many values",{Item,Values}});        
        length(Values) < length(ColMap) ->      ?ClientError({"Not enough values",{Item,Values}});        
        true ->                                 ok    
    end,            
    InsertMap = lists:sort(
        [ case CMap of
            #bind{tind=0,cind=0,func=Proj,btree=BTree} when is_function(Proj) ->
                case BTree of
                    {hd,#bind{tind=?MainIdx,cind=Cx,type=Type}} ->
                        Fx = fun(X) -> 
                            NewVal = imem_datatype:io_to_db(Item,<<>>,list_type(Type),undefined,undefined,<<>>,false,Value),
                            ?ins_repl(X,Cx,[NewVal])
                        end,     
                        {Cx,1,Fx};
                    {element,Pos,#bind{tind=?MainIdx,cind=Cx,name=Name,type=Type}} when is_number(Pos) ->    
                        Fx = fun(X) -> 
                            NewVal = imem_datatype:io_to_db(Item,<<>>,tuple_type(Type,Pos),undefined,undefined,<<>>,false,Value),
                            OldList = case element(Cx,X) of
                                T when is_tuple(T) ->   tuple_to_list(T);
                                _ ->                    []
                            end,
                            NewPos = length(OldList) + 1,
                            case Pos of
                                1 ->        ?ins_repl(X,Cx,{NewVal}); 
                                NewPos ->   ?ins_repl(X,Cx,list_to_tuple(OldList ++ [NewVal]));
                                _ ->        ?ClientError({"Missing tuple element",{Item,Name,NewPos}})
                            end
                        end,     
                        {Cx,Pos,Fx};
                    {map_get,KeyBind,#bind{tind=?MainIdx,cind=Cx}} ->
                        Key=imem_sql_expr:bind_tree(KeyBind,Recs), 
                        Fx = fun(X) -> 
                            NewVal = imem_datatype:io_to_db(Item,<<>>,term,undefined,undefined,<<>>,false,Value),
                            ?ins_repl(X,Cx,maps:put(Key,NewVal,#{}))
                        end,
                        {Cx,0,Fx};
                    {nth,Pos,#bind{tind=?MainIdx,cind=Cx,name=Name,type=Type}} when is_number(Pos) ->
                        Fx = fun(X) -> 
                            NewVal = imem_datatype:io_to_db(Item,<<>>,list_type(Type),undefined,undefined,<<>>,false,Value),
                            OldList = case element(Cx,X) of
                                L when is_list(L) ->    L;
                                _ ->                    []
                            end,
                            NewPos = length(OldList) + 1,
                            case Pos of
                                1 ->        ?ins_repl(X,Cx,[NewVal]); 
                                NewPos ->   ?ins_repl(X,Cx,OldList ++ [NewVal]);
                                _ ->        ?ClientError({"Missing list element",{Item,Name,NewPos}})
                            end
                        end,     
                        {Cx,Pos,Fx};
                    {from_binterm,#bind{tind=?MainIdx,cind=Cx,type=Type}} ->
                        Fx = fun(X) -> 
                            ?ins_repl(X,Cx,imem_datatype:io_to_db(Item,<<>>,Type,undefined,undefined,<<>>,false,Value))
                        end,     
                        {Cx,0,Fx};
                    {json_value,AttName,#bind{tind=?MainIdx,cind=Cx}=B} ->
                        Fx = fun(X) ->
                            case  Value of
                                <<>> ->     
                                    X;
                                ?navio ->   
                                    X;
                                _ ->        
                                    try
                                        ?replace(X,Cx,imem_json:put(AttName,imem_json:decode(Value),?BoundVal(B,X)))
                                    catch _:_ ->
                                        ?replace(X,Cx,imem_json:put(AttName,imem_json:decode(imem_datatype:add_dquotes(Value)),?BoundVal(B,X)))
                                    end
                            end
                        end,
                        {Cx,0,Fx};
                    Other ->    ?SystemException({"Internal error, bad projection binding",{Item,Other}})
                end;
            #bind{tind=0,cind=0} ->  
                ?SystemException({"Internal update error, constant projection binding",{Item,CMap}});
            #bind{tind=?MainIdx,cind=0,type=tuple} ->
                Fx = fun(_X) ->
                    NewVal = imem_datatype:io_to_db(Item,{},tuple,undefined,undefined,<<>>,false,Value),
                    % setelement(?MainIdx,X,NewVal)
                    NewVal 
                end,     
                {0,0,Fx};                
            #bind{tind=?MainIdx,cind=Cx,type=T,len=L,prec=P,default=D} ->
                Fx = fun(X) -> 
                    ?ins_repl(X,Cx,imem_datatype:io_to_db(Item,?nav,T,L,P,D,false,Value))
                end,     
                {Cx,0,Fx}
          end
          || 
          {#bind{readonly=R}=CMap,Value} 
          <- lists:zip(ColMap,Values), R==false, Value /= ?navio
        ]),
    NewRec = if_call_mfa(IsSec, apply_validators, [SKey, DefRec, update_recs(DefRec, InsertMap), Tab]),
    Action = [{S,Tab,Typ}, Item, {},  NewRec, Trigger, User, TrOpts],     
    update_prepare(IsSec, SKey, TableInfo, ColMap, CList, [Action|Acc]);
update_prepare(_IsSec, _SKey, _TableInfo, _ColMap, [CLItem|_], _Acc) ->
    ?ClientError({"Invalid format of change list", CLItem}).

update_recs(Recs, []) -> Recs;
update_recs(Recs, [{_Cx,_Pos,Fx}|UpdateMap]) ->
    % ?LogDebug("Update rec ~p:~p ~n~p~n", [_Cx,_Pos,Recs]),
    NewRecs = Fx(Recs),
    % ?LogDebug("Updated rec ~n~p~n", [NewRecs]),
    update_recs(NewRecs, UpdateMap).

list_type(list) -> term;
list_type([Type]) when is_atom(Type) -> Type;
list_type(Other) -> ?ClientError({"Invalid list type",Other}).

tuple_type(tuple,_) -> term;
tuple_type({Type},_) when is_atom(Type) -> Type;
tuple_type(T,Pos) when is_tuple(T),(size(T)>=Pos),is_atom(element(Pos,T)) -> element(Pos,T);
tuple_type(Other,_) -> ?ClientError({"Invalid tuple type",Other}).

% update_bag(IsSec, SKey, Table, ColMap, [C|CList]) ->
%     ?UnimplementedException({"Cursor update not supported for bag tables",Table}).

receive_raw() ->
    receive_raw(50, []).

receive_raw(Timeout) ->
    receive_raw(Timeout, []).

receive_raw(Timeout,Acc) ->    
    case receive 
            R ->    ?Debug("receive_raw got:~n~p~n", [R]),
                    R
        after Timeout ->
            stop
        end of
        stop ->         lists:reverse(Acc);
        {_,Result} ->   receive_raw(Timeout,[Result|Acc])
    end.

receive_recs(StmtResult, Complete) ->
    receive_recs(StmtResult,Complete,50,[]).

receive_recs(StmtResult, Complete, Timeout) ->
    receive_recs(StmtResult, Complete, Timeout,[]).

receive_recs(#stmtResults{stmtRefs=[StmtRef]}=StmtResult,Complete,Timeout,Acc) ->    
    case receive
            R ->    % ?Debug("~p got:~n~p~n", [?TIMESTAMP,R]),
                    R
        after Timeout ->
            stop
        end of
        stop ->     
            Unchecked = case Acc of
                [] ->                       
%                   [{StmtRef,[],Complete}];
                    throw({no_response,{expecting,Complete}});
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
                    if 
                        length(List) =< 10 ->
                            ?Debug("Received:~n~p~n", [List]);
                        true ->
                            ?Debug("Received: ~p items ~p~n~p~n~p~n", [length(List),hd(List), '...', lists:last(List)])
                    end,            
                    List;
                StmtRefs ->
                    throw({bad_stmtref,lists:delete(StmtRef, StmtRefs)})
            end;
        {_, Result} ->   
            receive_recs(StmtResult,Complete,Timeout,[Result|Acc])
    end.

result_tuples(List,RowFun) when is_list(List), is_function(RowFun) ->  
    [list_to_tuple(R) || R <- lists:map(RowFun,List)].

%% --Interface functions  (calling imem_if for now, not exported) ---------

if_call_mfa(IsSec,Fun,Args) ->
    case IsSec of
        true -> apply(imem_sec,Fun,Args);
        _ ->    apply(imem_meta, Fun, lists:nthtail(1, Args))
    end.
