-module(imem_statement).

-include("imem_seco.hrl").

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
        , fetch_recs_async/4        %% ToDo: implement proper return of RowFun(), match conditions and joins
        , fetch_close/3
        , close/2
        ]).

-export([ create_stmt/3
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

fetch_recs_async(SKey, Pid, Sock, IsSec) when is_pid(Pid) ->
    gen_server:cast(Pid, {fetch_recs_async, IsSec, SKey, Sock}).

fetch_close(SKey, Pid, IsSec) when is_pid(Pid) ->
    gen_server:call(Pid, {fetch_close, IsSec, SKey}).

update_cursor_prepare(SKey, Pid, IsSec, ChangeList) when is_pid(Pid) ->
    gen_server:call(Pid, {update_cursor_prepare, IsSec, SKey, ChangeList}).

update_cursor_execute(SKey, Pid, IsSec, none) when is_pid(Pid) ->
    gen_server:call(Pid, {update_cursor_excute, IsSec, SKey, none});
update_cursor_execute(SKey, Pid, IsSec, optimistic) when is_pid(Pid) ->
    gen_server:call(Pid, {update_cursor_execute, IsSec, SKey, optimistic}).

close(SKey, Pid) when is_pid(Pid) ->
    gen_server:cast(Pid, {close, SKey}).

init([Statement]) ->
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
            MonitorRef ->   catch erlang:demonitor(MonitorRef, [flush]),
                            catch FetchCtx0#fetchCtx.pid ! abort
        end,
        if_call_mfa(IsSec,update_tables,[SKey, UpdatePlan, Lock]) 
    catch
        _:Reason ->  Reason
    end,
    % io:format(user, "~p - update_cursor_execute result ~p~n", [?MODULE, Reply]),
    FetchCtx1 = FetchCtx0#fetchCtx{monref=undefined, status=aborted, metarec=undefined},
    {reply, Reply, State#state{fetchCtx=FetchCtx1}};
handle_call({fetch_close, _IsSec, _SKey}, _From, #state{fetchCtx=#fetchCtx{pid=undefined, monref=undefined}}=State) ->
    {reply, ok, State#state{fetchCtx=#fetchCtx{}}};
handle_call({fetch_close, _IsSec, _SKey}, _From, #state{fetchCtx=#fetchCtx{pid=Pid, monref=MonitorRef}}=State) ->
    catch erlang:demonitor(MonitorRef, [flush]),
    catch Pid ! abort, 
    {reply, ok, State#state{fetchCtx=#fetchCtx{}}}.

handle_cast({fetch_recs_async, _IsSec, _SKey, Sock}, #state{fetchCtx=#fetchCtx{status=aborted}}=State) ->
    Result = term_to_binary({error,"Fetch aborted, execute fetch_close before refetch"}),
    send_reply_to_client(Sock, Result),
    {noreply, State}; 
handle_cast({fetch_recs_async, IsSec, _SKey, Sock}, #state{statement=Stmt, seco=SKey, fetchCtx=#fetchCtx{pid=Pid}}=State) ->
    #statement{tables=[{_Schema,Table,_Alias}|_], block_size=BlockSize, matchspec=MatchSpec, meta=MetaMap, limit=Limit} = Stmt,
    MetaRec = list_to_tuple([if_call_mfa(IsSec, meta_field_value, [SKey, N]) || N <- MetaMap]),
    NewFetchCtx = case Pid of
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
    {noreply, State#state{reply=Sock,fetchCtx=NewFetchCtx}};  
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
    % io:format(user, "~p - terminating monitor not found~n", [?MODULE]),
    catch Pid ! abort, 
    ok;
terminate(_Reason, #state{fetchCtx=#fetchCtx{pid=Pid, monref=MonitorRef}}) ->
    % io:format(user, "~p - terminating after demonitor~n", [?MODULE]),
    erlang:demonitor(MonitorRef, [flush]),
    catch Pid ! abort, 
    ok.

code_change(_OldVsn, State, _Extra) -> {ok, State}.

send_reply_to_client(SockOrPid, Result) ->
    case SockOrPid of
        Pid when is_pid(Pid)    -> Pid ! Result;
        Sock                    -> gen_tcp:send(Sock, Result)
    end.

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
        [{Ci,imem_datatype:value_to_db(Item,imem_nil,T,L,P,D,false,Value)} || 
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
    {
        setup,
        fun setup/0,
        fun teardown/1,
        {with, [
              fun test_without_sec/1
            , fun test_with_sec/1
        ]}
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

        SKey=case IsSec of
            true ->     ?imem_test_admin_login();
            false ->    none
        end,

        ?assertEqual(ok, imem_sql:exec(SKey, "create table def (col1 varchar2(10), col2 integer);", 0, "Imem", IsSec)),
        ?assertEqual(ok, insert_range(SKey, 15, def, "Imem", IsSec)),
        TableRows1 = lists:sort(if_call_mfa(IsSec,read,[SKey, def])),
        [Meta] = if_call_mfa(IsSec, read, [SKey, ddTable, {'Imem',def}]),
        io:format(user, "Meta table~n~p~n", [Meta]),
        io:format(user, "original table~n~p~n", [TableRows1]),

        {ok, _Clm2, _RowFun2, StmtRef2} = imem_sql:exec(SKey, "select col1, col2 from def;", 4, "Imem", IsSec),
        ?assertEqual(ok, imem_statement:fetch_recs_async(SKey, StmtRef2, self(), IsSec)),
        Result2a = receive 
            R2a ->    binary_to_term(R2a)
        end,
        {List2a, false} = Result2a,
        ?assertEqual(4, length(List2a)),           
        %% ChangeList2 = [[OP,ID] ++ L || {OP,ID,L} <- lists:zip3([nop, ins, del, upd], [1,2,3,4], lists:map(RowFun2,List2a))],
        %% io:format(user, "change list~n~p~n", [ChangeList2]),
        ChangeList2 = [
        [1,nop,{{def,"2",2},{}},"2",2],         %% no operation on this line
        [5,ins,{},"99","undefined"],            %% insert {def,"99", undefined}
        [3,del,{{def,"5",5},{}},"5",5],         %% delete {def,"5","'5'"}
        [4,upd,{{def,"12",12},{}},"112",12]     %% update {def,"12","'12'"} to {def,"112","'12'"}
        ],
        ?assertEqual({ClEr,{"Cannot update readonly field",{4,{"12","112"}}}}, update_cursor_prepare(SKey, StmtRef2, IsSec, ChangeList2)),
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

        ?assertEqual(ok, close(SKey, StmtRef2)),
        ?assertEqual(ok, imem_sql:exec(SKey, "drop table def;", 0, "Imem", IsSec)),

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
    if_call_mfa(IsSec,write,[SKey,Table,{Table,integer_to_list(N),N}]),
    insert_range(SKey, N-1, Table, Schema, IsSec).
