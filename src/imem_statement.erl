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
        , fetch_recs/4          %% ToDo: implement proper return of RowFun(), match conditions and joins
        , fetch_recs_async/4    %% ToDo: implement proper return of RowFun(), match conditions and joins
        , close/2
        , update_cursor/4       %% take change list and apply in one transaction
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

update_cursor(SKey, Pid, IsSec, ChangeList) when is_pid(Pid) ->
    gen_server:call(Pid, {update_cursor, IsSec, SKey, ChangeList}).

close(SKey, Pid) when is_pid(Pid) ->
    gen_server:cast(Pid, {close, SKey}).

init([Statement]) ->
    {ok, #state{statement=Statement}}.

handle_call({set_seco, SKey}, _From, State) ->    
    {reply,ok,State#state{seco=SKey}};
handle_call({update_cursor, IsSec, _SKey, ChangeList}, _From, #state{statement=Stmt, seco=SKey, fetchCtx=FetchCtx}=State) ->
    #statement{tables=[{_Schema,Table,_Alias}|_], cols=ColMap} = Stmt,
    case FetchCtx#fetchCtx.monref of
        undefined ->    ok;
        MonitorRef ->   catch erlang:demonitor(MonitorRef, [flush]),
                        catch FetchCtx#fetchCtx.pid ! abort
    end, 
    io:format(user, "~p - received change list~n~p~n", [?MODULE, ChangeList]),
    % [nop,1,{{def,"2","'2'"},{}},"2"],       %% no operation on this line
    % [ins,5,{},"99"],                        %% insert {def,"99", undefined}
    % [del,3,{{def,"5","'5'"},{}},"5"],       %% delete {def,"5","'5'"}
    % [upd,4,{{def,"12","'12'"},{}},"112"]    %% update {def,"12","'12'"} to {def,"112","'12'"}
    Update = fun() ->
        update_set(IsSec, SKey, Table, ColMap, ChangeList) 
    end,
    Result = case if_call_mfa(IsSec,transaction,[SKey, Update]) of
        {atomic, ok}                                -> ok;
        {aborted,{throw,{Exception,Reason}}} ->     {Exception,Reason};
        {aborted,{exit,{Exception,Reason}}} ->      {Exception,Reason};
        Error ->                                    Error
    end,
    io:format(user, "~p - update_cursor result ~p~n", [?MODULE, Result]),
    NewFetchCtx = FetchCtx#fetchCtx{monref=undefined, status=aborted, metarec=undefined},
    {reply, Result, State#state{fetchCtx=NewFetchCtx}}.  

update_set(_IsSec, _SKey, _Table, _ColMap, []) -> ok;
update_set(IsSec, SKey, Table, ColMap, [[nop,Item,Recs|_]|CList]) ->
    if_call_mfa(IsSec,update_xt,[SKey, Table, Item, element(1,Recs), element(1,Recs)]), 
    update_set(IsSec, SKey, Table, ColMap, CList);
update_set(IsSec, SKey, Table, ColMap, [[del,Item,Recs|_]|CList]) ->
    if_call_mfa(IsSec,update_xt,[SKey, Table, Item, element(1,Recs), {}]), 
    update_set(IsSec, SKey, Table, ColMap, CList);
update_set(IsSec, SKey, Table, ColMap, [[upd,Item,Recs|Values]|CList]) ->
    ValMap = lists:usort([{Ci,Value} || {#ddColMap{tind=Ti, cind=Ci},Value} <- lists:zip(ColMap,Values), Ti==1]),
    IndMap = lists:usort([Ci || {Ci,_} <- ValMap]),
    if 
        length(ValMap) /= length(IndMap) ->     ?ClientError({"Contradicting column update",{Item,ValMap}});
        true ->                                 ok
    end,        
    NewRec = lists:foldl(fun({Ci,Value},Rec) -> setelement(Ci,Rec,Value) end, element(1,Recs), ValMap),
    if_call_mfa(IsSec,update_xt,[SKey, Table, Item, element(1,Recs), NewRec]), 
    update_set(IsSec, SKey, Table, ColMap, CList);
update_set(IsSec, SKey, Table, ColMap, ChangeList) ->
    ColInfo = if_call_mfa(IsSec,column_infos, [SKey, Table]),
    DefRec = list_to_tuple([Table|if_call_mfa(IsSec,column_info_items, [SKey, ColInfo, default])]),
    io:format(user, "~p - default record ~p~n", [?MODULE, DefRec]), 
    update_set(IsSec, SKey, Table, ColMap, DefRec, ChangeList).


update_set(IsSec, SKey, Table, ColMap, DefRec, [[ins,Item,_|Values]|CList]) ->
    ValMap = lists:usort([{Ci,Value} || {#ddColMap{tind=Ti, cind=Ci},Value} <- lists:zip(ColMap,Values), Ti==1]),
    IndMap = lists:usort([Ci || {Ci,_} <- ValMap]),
    HasKey = lists:member(2,IndMap),
    if 
        length(ValMap) /= length(IndMap) ->     ?ClientError({"Contradicting column insert",{Item,ValMap}});
        HasKey /= true  ->                      ?ClientError({"Missing key column",{Item,ValMap}});
        true ->                                 ok
    end,
    Rec = lists:foldl(fun({Ci,Value},Rec) -> setelement(Ci,Rec,Value) end, DefRec, ValMap),
    if_call_mfa(IsSec,update_xt,[SKey, Table, Item, {}, Rec]), 
    update_set(IsSec, SKey, Table, ColMap, CList).

% update_bag(IsSec, SKey, Table, ColMap, [C|CList]) ->
%     ?UnimplementedException({"Cursor update not supported for bag tables",Table}).

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
        SKey=case IsSec of
            true ->     ?imem_test_admin_login();
            false ->    none
        end,

        ?assertEqual(ok, imem_sql:exec(SKey, "create table def (col1 varchar2(10), col2 char);", 0, "Imem", IsSec)),
        ?assertEqual(ok, insert_range(SKey, 15, "def", "Imem", IsSec)),
        TableRows1 = lists:sort(if_call_mfa(IsSec,read,[SKey, def])),
        io:format(user, "original table~n~p~n", [TableRows1]),

        {ok, _Clm2, _RowFun2, StmtRef2} = imem_sql:exec(SKey, "select col1 from def;", 4, "Imem", IsSec),
        ?assertEqual(ok, imem_statement:fetch_recs_async(SKey, StmtRef2, self(), IsSec)),
        Result2a = receive 
            R2a ->    binary_to_term(R2a)
        end,
        {List2a, false} = Result2a,
        ?assertEqual(4, length(List2a)),           
        %% ChangeList2 = [[OP,ID] ++ L || {OP,ID,L} <- lists:zip3([nop, ins, del, upd], [1,2,3,4], lists:map(RowFun2,List2a))],
        %% io:format(user, "change list~n~p~n", [ChangeList2]),
        ChangeList2 = [
        [nop,1,{{def,"2","'2'"},{}},"2"],       %% no operation on this line
        [ins,5,{},"99"],                        %% insert {def,"99", undefined}
        [del,3,{{def,"5","'5'"},{}},"5"],       %% delete {def,"5","'5'"}
        [upd,4,{{def,"12","'12'"},{}},"112"]    %% update {def,"12","'12'"} to {def,"112","'12'"}
        ],
        ?assertEqual({ClEr,{"Key update not allowed",{4,{"12","112"}}}}, update_cursor(SKey, StmtRef2, IsSec, ChangeList2)),
        TableRows2 = lists:sort(if_call_mfa(IsSec,read,[SKey, def])),
        io:format(user, "unchanged table~n~p~n", [TableRows2]),
        ?assertEqual(TableRows1, TableRows2),

        ChangeList3 = [
        [nop,1,{{def,"2","'2'"},{}},"2"],       %% no operation on this line
        [ins,5,{},"99"],                        %% insert {def,"99", undefined}
        [del,3,{{def,"5","'5'"},{}},"5"],       %% delete {def,"5","'5'"}
        [upd,4,{{def,"12","'12'"},{}},"12"]     %% nop update {def,"12","'12'"}
        ],
        ExpectedRows3 = [
        {def,"2","'2'"},                        %% no operation on this line
        {def,"99",undefined},                   %% insert {def,"99", undefined}
        {def,"12","'12'"}                       %% nop update {def,"12","'12'"}
        ],
        RemovedRows3 = [
        {def,"5","'5'"}                         %% delete {def,"5","'5'"}
        ],

        ?assertEqual(ok, update_cursor(SKey, StmtRef2, IsSec, ChangeList3)),
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


insert_range(_SKey, 0, _TableName, _Schema, _IsSec) -> ok;
insert_range(SKey, N, TableName, Schema, IsSec) when is_integer(N), N > 0 ->
    imem_sql:exec(SKey, "insert into " ++ TableName ++ " values (" ++ integer_to_list(N) ++ ", '" ++ integer_to_list(N) ++ "');", 0, Schema, IsSec),
    insert_range(SKey, N-1, TableName, Schema, IsSec).
