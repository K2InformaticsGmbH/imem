-module(erlimem_fsm). 

-behaviour(gen_fsm).

-include("imem_seco.hrl").
-include("imem_sql.hrl").

-define(KeyMax,[]).     %% value bigger than any possible sort key {SortFun(Recs),Id}
-define(KeyMin,{}).     %% value smaller than any possible sort key {SortFun(Recs),Id}

-record(ctx,    { %% session context
                  skey
                , id
                , bl                  %% block length -> State
                , stmtRef             %% statement reference
                , isSec               %% secure mode or not
                , rowFun              %% RowFun -> State
                , sortFun             %% SortFun -> State
                , sortSpec            %% SortSpec (based on statement full map)
                , replyTo             %% reply pid
                }).

-record(state,  { %% fsm combined state
                  ctx                 %% statement & fetch context
                , tableId             %% ets raw buffer table id 
                , indexId             %% ets index table id 
                , bl                  %% block_length (passed to init)
                , gl                  %% gui max length (row count) = gui_max(#state.bl)
                , stmtRef             %% statement reference
                , rowFun              %% RowFun
                , sortSpec            %% from imem statement, changed by gui events
                , sortFun             %% from imem statement, follows sortSpec (calculated by imem statement)
                , filterSpec = {}     %% {FType,[ColF|ColFs]}  changed by gui events FType= and|or  ColF = [{Col,["value1".."valuen"]}]
                , filterFun           %% follows filterSpec

                , rawCnt = 0          %% buffer row count
                , rawTop = 99999999   %% id of top buffer row 
                , rawBot = 0          %% id of bottom buffer row
                , dirtyCnt = 0        %% count of dirty rows in buffer
                , dirtyTop = 99999999 %% record id of first dirty row in buffer
                , dirtyBot = 0        %% record id of last dirty row in buffer

                , indCnt = 0          %% count of indexed buffer entries (after filtering) 
                , indTop = []         %% smallest index after filtering, initialized to big value
                , indBot = {}         %% biggest index after filtering, initialized to small value

                , bufCnt = 0          %% buffer row count           (either rawCnt or indCnt, depending on nav)
                , bufTop              %% id of top buffer row       (either rawTop or indTop, depending on nav)
                , bufBot              %% id of bottom buffer row    (either rawBot or indBot, depending on nav)

                , guiCnt = 0          %% count of scrollable entries in gui 
                , guiTop              %% top gui pointer (Id for raw / SortKey for ind)
                , guiBot              %% bottom gui pointer (Id for raw / SortKey for ind)
                , guiCol = false      %% index collision (stale view in gui)

                , nav = raw           %% navigation   raw | ind
                , pfc=0               %% pending fetch count (in flight to DB or back)
                , tailMode = false    %% tailMode scheduled
                , stack = undefined   %% command stack {button,Button,ReplyTo}
                , replyTo             %% reply pid
                }).

-record(gres,   { %% response sent back to gui
                  operation           %% rep (replace) | app (append) | prp (prepend) | nop | close
                , cnt = 0             %% current buffer size (raw table or index table size)
                , toolTip = ""        %% current buffer sizes RawCnt/IndCnt plus status information
                , message = ""        %% error message
                , beep = false        %% alert with a beep if true
                , state = empty       %% determines color of buffer size indicator
                , loop = undefined    %% gui should come back with this command
                , rows = []           %% rows to show (append / prepend / merge)
                , keep = 0            %% row count to be kept
                }).

-define(block_size,10).
-define(MustCommit,"Please commit or rollback changes before clearing data").


%% --------------------------------------------------------------------
%% Include files
%% --------------------------------------------------------------------

%% --------------------------------------------------------------------
%% External exports

-export([ start/6
        , start_link/6
        , stop/1
        ]).

-export([ rows/2        %% incoming rows from database
        , gui_req/4     %% button '>' '>>' '>|' '>|...' '<' '<<' '...'
                        %% 
                        %% update List of change records [Id,Op,Col1,Col2....Coln] Op=ins|upd|del|nop
        ]).

%% gen_fsm callbacks

-export([ empty/2
        , filling/2
        , autofilling/2
        , completed/2
        , tailing/2
        ]).

-export([ init/1
        , handle_event/3
        , handle_sync_event/4
        , handle_info/3
        , terminate/3
        , code_change/4
        ]).

-export([ filter_fun/1
        , filter_and/2
        , filter_or/2
        ]).


%% ====================================================================
%% External functions
%% ====================================================================

start(SKey,Id,BL,IsSec,Sql,ReplyTo) ->
    StmtResult = exec(SKey, Id, BL, IsSec, Sql),
    #stmtResult{stmtRef=StmtRef,rowFun=RowFun,sortFun=SortFun,sortSpec=SortSpec}=StmtResult,
    Ctx = #ctx{skey=SKey,id=Id,bl=BL,stmtRef=StmtRef,rowFun=RowFun,sortFun=SortFun,sortSpec=SortSpec,isSec=IsSec,replyTo=ReplyTo}, 
	{ok,Pid} = gen_fsm:start(?MODULE,Ctx,[]),
    {?MODULE,Pid}.

start_link(SKey,Id,BL,IsSec,Sql,ReplyTo) ->
    StmtResult = exec(SKey, Id, BL, IsSec, Sql),
    #stmtResult{stmtRef=StmtRef,rowFun=RowFun,sortFun=SortFun,sortSpec=SortSpec}=StmtResult,
    Ctx = #ctx{skey=SKey,id=Id,bl=BL,stmtRef=StmtRef,rowFun=RowFun,sortFun=SortFun,sortSpec=SortSpec,isSec=IsSec,replyTo=ReplyTo}, 
	{ok, Pid} = gen_fsm:start_link(?MODULE,Ctx,[]),
    {?MODULE,Pid}.

stop(Pid) -> 
	gen_fsm:send_all_state_event(Pid,stop).


gui_req(button, '>|', ReplyTo, {?MODULE,Pid}) -> 
    ?Log("button --evt-- ~p~n", ['>|']),
    gen_fsm:send_event(Pid,{button, '>|', ReplyTo});
gui_req(button, '>|...', ReplyTo, {?MODULE,Pid}) -> 
    ?Log("button --evt-- ~p~n", ['>|...']),
    gen_fsm:send_event(Pid,{button, '>|...', ReplyTo});
gui_req(button, '...', ReplyTo, {?MODULE,Pid}) -> 
    ?Log("button --evt-- ~p~n", ['...']),
    gen_fsm:send_event(Pid,{button, '...', ReplyTo});

gui_req(update, ChangeList, ReplyTo, {?MODULE,Pid}) -> 
    ?Log("update -allevt- ~p~n", [ChangeList]),
    gen_fsm:send_all_state_event(Pid,{update, ChangeList, ReplyTo});
gui_req(button, Button, ReplyTo, {?MODULE,Pid}) -> 
    ?Log("button -allevt- ~p~n", [Button]),
    gen_fsm:send_all_state_event(Pid,{button, Button, ReplyTo}).


rows({Rows,Completed},{?MODULE,Pid}) -> 
    % ?Log("rows ~p ~p~n", [length(Rows),Completed]),
    gen_fsm:send_event(Pid,{rows, {Rows,Completed}}).


exec(SKey, Id, BL, IsSec, Sql) ->
    ?Log("exec -- ~p: ~s~n", [Id, Sql]),
    {ok, StmtResult} = imem_sql:exec(SKey, Sql, BL, 'Imem', IsSec),
    % #stmtResult{stmtCols=StmtCols} = StmtResult,
    % ?Log("Statement Cols : ~p~n", [StmtCols]),
    StmtResult.

fetch(FetchMode,TailMode, #state{ctx=Ctx,stmtRef=StmtRef}=State) ->
    #ctx{skey=SKey,isSec=IsSec} = Ctx,
    Opts = case {FetchMode,TailMode} of
        {none,none} ->    [];
        {FM,none} ->      [{fetch_mode,FM}];
        {FM,TM} ->        [{fetch_mode,FM},{tailMode,TM}]
    end,
    Result = imem_statement:fetch_recs_async(SKey, StmtRef, self(), Opts, IsSec),
    % ?Log("fetch (~p, ~p) ~p~n", [FetchMode, TailMode, Result]),
    ok = Result,
    NewPfc=State#state.pfc +1,
    State#state{pfc=NewPfc}.

prefetch(filling,#state{pfc=0}=State) ->  fetch(none,none,State);
prefetch(filling,State) ->                State;
prefetch(_,State) ->                      State.

fetch_close(#state{stmtRef=StmtRef}=State) ->
    #ctx{skey=SKey,isSec=IsSec} = State#state.ctx, 
    Result = imem_statement:fetch_close(SKey, StmtRef, IsSec),
    ?Log("fetch_close -- ~p~n", [Result]),
    State#state{pfc=0}.



%% ====================================================================
%% Server functions
%% ====================================================================
%% --------------------------------------------------------------------
%% Func: init/1
%% Returns: {ok, SN, StateData}          |
%%          {ok, SN, StateData, Timeout} |
%%          ignore                              |
%%          {stop, StopReason}
%% --------------------------------------------------------------------

init(#ctx{bl=BL,replyTo=ReplyTo,stmtRef=StmtRef,rowFun=RowFun,sortFun=SortFun,sortSpec=SortSpec}=Ctx) ->
    TableId=ets:new(raw, [ordered_set]),        %% {Id,Op,Keys,Col1,Col2,...Coln}
    IndexId=ets:new(ind, [ordered_set]),        %% {{SortFun(Keys),Id},Id}
    FilterSpec = (#state{})#state.filterSpec, 
    Nav = navigation_type(SortFun,FilterSpec),
    FilterFun = filter_fun(FilterSpec),
    State0=#state{   bl=BL
                        , gl=gui_max(BL)
                        , ctx=Ctx   
                        , tableId=TableId
                        , indexId=IndexId
                        , stmtRef=StmtRef
                        , rowFun=RowFun
                        , sortFun=SortFun
                        , sortSpec=SortSpec
                        , filterFun=FilterFun
                        , filterSpec=FilterSpec
                        , nav=Nav
                        , replyTo=ReplyTo
                 },
    {ok, empty, set_buf_counters(State0)}.

navigation_type(SortFun,FilterSpec) -> 
    case catch (SortFun(1)) of
        {} ->
            case FilterSpec of 
                {} ->   raw;
                _ ->    ind
            end;
        _ ->    
            ind
    end. 

buf_cnt(#state{nav=raw,rawCnt=RawCnt}) -> RawCnt;
buf_cnt(#state{nav=ind,indCnt=IndCnt}) -> IndCnt.

buf_top(#state{nav=raw,rawTop=RawTop}) -> RawTop;
buf_top(#state{nav=ind,indTop=IndTop}) -> IndTop.

buf_bot(#state{nav=raw,rawBot=RawBot}) -> RawBot;
buf_bot(#state{nav=ind,indBot=IndBot}) -> IndBot.

set_buf_counters(#state{nav=raw,rawCnt=RawCnt,rawTop=RawTop,rawBot=RawBot}=State0) -> 
    State0#state{bufCnt=RawCnt,bufTop=RawTop,bufBot=RawBot};
set_buf_counters(#state{nav=ind,indCnt=IndCnt,indTop=IndTop,indBot=IndBot}=State0) -> 
    State0#state{bufCnt=IndCnt,bufTop=IndTop,bufBot=IndBot}.

filter_fun({}) ->
    fun(_) -> true end;
filter_fun({'and',Conditions}) ->
    fun(R) -> 
        filter_and(R,Conditions)
    end;
filter_fun({'or',Conditions}) ->
    fun(R) -> 
        filter_or(R,Conditions)
    end.

filter_and(_,[]) -> true;
filter_and(R,[{Col,Values}|Conditions]) ->
    case lists:is_member(element(Col+3,R), Values) of
        true ->     filter_and(R,Conditions);
        false ->    false
    end.

filter_or(_,[]) -> false;
filter_or(R,[{Col,Values}|Conditions]) ->
    case lists:is_member(element(Col+3,R), Values) of
        false ->    filter_or(R,Conditions);
        true ->     true
    end.

reply_stack(_SN,ReplyTo, #state{stack=undefined}=State0) ->
    % stack is empty, nothing to do    
    State0#state{replyTo=ReplyTo};
reply_stack(SN,ReplyTo, #state{stack={button,_Button,RT}}=State0) ->
    % stack is obsolete, overriden by new command, reply delayed request with nop    
    State1 = gui_nop(#gres{state=SN},State0#state{stack=undefined,replyTo=RT}),
    State1#state{replyTo=ReplyTo}.

%% --------------------------------------------------------------------
%% Func: SN/2	 non-synchronized event handling
%% Returns: {next_state, NextSN, NextStateData}          |
%%          {next_state, NextSN, NextStateData, Timeout} |
%%          {stop, Reason, NewStateData}
%% --------------------------------------------------------------------

%% Only data input from DB and button events for '>|', '>|...' and '...' handled here
%% Other buttons and commands are handled through all_state_event in handle_event/3

empty({button, '>|', ReplyTo}, State0) ->
    % start fetch
    State1 = fetch(push,none, State0#state{tailMode=false}),
    {next_state, autofilling, State1#state{stack={button,'>|',ReplyTo}}};
empty({button, '>|...', ReplyTo}, State0) ->
    % start fetch, schedule tail
    State1 = fetch(push,true, State0#state{tailMode=true}),
    {next_state, autofilling, State1#state{stack={button,'>|...',ReplyTo}}};
empty({button, '...', ReplyTo}, State0) ->
    % skip fetch, schedule tail
    State1 = fetch(skip,true, State0#state{tailMode=true}),
    {next_state, tailing, State1#state{stack={button,'...',ReplyTo}}};
empty(Other, State) ->
    ?Log("empty -- unexpected erlimem_fsm event ~p in empty state~n", [Other]),
    {next_state, empty, State}.

filling({button, _, ReplyTo}=Cmd, #state{bufCnt=0}=State0) ->
    % too quick, defer request to when we have the first block of data 
    State1 = reply_stack(filling, ReplyTo, State0),
    {next_state, filling, State1#state{stack=Cmd}};
filling({button, _, ReplyTo}=Cmd, #state{bufBot=B,guiBot=B}=State0) ->
    % fetch in flight, defer request to when we have the next block of data 
    State1 = reply_stack(filling, ReplyTo, State0),
    {next_state, filling, State1#state{stack=Cmd}};
filling({button, '...', ReplyTo}, #state{dirtyCnt=DC}=State0) when DC==0 ->
    % close fetch and clear buffers, schedule tail mode
    State1 = reply_stack(filling, ReplyTo, State0),
    State2 = fetch_close(State1),
    State3 = data_clear(State2),
    State4 = fetch(skip,true,State3),
    State5 = gui_clear(#gres{state=tailing,loop='...'}, State4#state{tailMode=true}),
    {next_state, tailing, State5};
filling({button, '...', ReplyTo}, State0) ->
    % reject command because of uncommitted changes
    State1 = gui_nop(#gres{state=filling,beep=true,message=?MustCommit},State0#state{replyTo=ReplyTo}),
    {next_state, filling, State1};
filling({button, '>|...', ReplyTo}=Cmd, State0) ->
    % switch fetch to push mode and schedule tail mode, defer answer to bulk fetch complete 
    State1 = reply_stack(filling, ReplyTo, State0),
    State2 = fetch(push,true,State1),
    {next_state, autofilling, State2#state{tailMode=true,stack=Cmd}};
filling({button, '>|', ReplyTo}=Cmd, State0) ->
    % switch fetch to push mode, defer answer to bulk fetch complete 
    State1 = reply_stack(filling, ReplyTo, State0),
    State2 = fetch(push,none,State1),
    {next_state, autofilling, State2#state{stack=Cmd}};
filling({rows, {Recs,false}}, #state{bl=BL,stack={_,button,Target,_}}=State0) when is_integer(Target) ->
    % receive and store data, prefetch if a 'target sprint' is ongoing
    State1 = data_append(filling, {Recs,false},State0),
    NewBufCnt = State1#state.bufCnt,
    State2 = if  
        (Target+BL > NewBufCnt) ->  prefetch(filling,State1);
        true ->                     State1
    end,    
    {next_state, filling, State2};
filling({rows, {Recs,false}}, State0) ->
    % receive and store data, no prefetch needed here
    State1 = data_append(filling, {Recs,false},State0),
    {next_state, filling, State1};
filling({rows, {Recs,true}}, State0) ->
    % receive and store data, close the fetch and switch state, no prefetch needed here
    State1 = fetch_close(State0),
    State2 = data_append(completed, {Recs,true},State1),
    {next_state, completed, State2};
filling(Other, State) ->
    ?Log("filling -- unexpected event ~p~n", [Other]),
    {next_state, filling, State}.


autofilling({button, '...', ReplyTo}, #state{dirtyCnt=DC}=State0) when DC==0->
    % stop fetch, clear buffer and start tailing
    State1 = reply_stack(tailing, ReplyTo, State0),
    State2 = fetch_close(State1),
    State3 = data_clear(State2),
    State4 = fetch(skip,true, State3#state{tailMode=true}),
    State5 = gui_clear(#gres{state=tailing, loop='>|...'},State4),
    {next_state, tailing, State5};
autofilling({button, '...', ReplyTo}, State0) ->
    % reject because of uncommitted changes
    State1 = gui_nop(#gres{state=autofilling,beep=true,message=?MustCommit},State0#state{replyTo=ReplyTo}),
    {next_state, autofilling, State1};
autofilling({button, '>|...', ReplyTo}=Cmd, #state{tailMode=TailMode}=State0) ->
    if 
        (TailMode == false) ->
            % too late to change to seamless tail mode now
            State1 = gui_nop(#gres{state=autofilling,beep=true},State0#state{replyTo=ReplyTo}),
            {next_state, autofilling, State1};
        true ->
            % tailing will happen anyways at the end of the bulk fetch, keep command on stack
            State1 = reply_stack(autofilling, ReplyTo, State0),
            State2 = gui_clear(State1),
            {next_state, autofilling, State2#state{stack=Cmd}}
    end;
autofilling({button, '>|', ReplyTo}=Cmd, #state{tailMode=TailMode}=State0) ->
    if 
        (TailMode == true) ->
            % too late to revoke tail mode now
            State1 = gui_nop(#gres{state=autofilling,beep=true},State0#state{replyTo=ReplyTo}),
            {next_state, autofilling, State1};
        true ->
            % already waiting for end of fetch, keep command on stack
            State1 = reply_stack(autofilling, ReplyTo, State0),
            {next_state, autofilling, State1#state{stack=Cmd}}
    end;
autofilling({rows, {Recs,false}}, State0) ->
    % revceive and store input from DB
    State1 = data_append(autofilling,{Recs,false},State0),
    {next_state, autofilling, State1#state{pfc=0}};
autofilling({rows, {Recs,true}}, #state{tailMode=false}=State0) ->
    % revceive and store last input from DB, close fetch, switch state
    State1 = fetch_close(State0),
    State2 = data_append(completed,{Recs,true},State1),
    {next_state, completed, State2#state{pfc=0}};
autofilling({rows, {Recs,true}}, State0) ->
    % revceive and store last input from DB, switch state to tail mode
    State1= data_append(tailing,{Recs,true},State0),
    {next_state, tailing, State1#state{pfc=0}};
autofilling(Other, State) ->
    ?Log("autofilling -- unexpected event ~p~n", [Other]),
    {next_state, autofilling, State}.

tailing({button, '...', ReplyTo}, #state{dirtyCnt=DC}=State0) when DC==0->
    % clear buffer and resume tailing
    State1 = reply_stack(tailing, ReplyTo, State0),
    State2 = data_clear(State1),
    State3 = gui_clear(#gres{state=tailing, loop='>|...'},State2),
    {next_state, tailing, State3};
tailing({button, '...', ReplyTo}, State0) ->
    % reject because of uncommitted changes
    State1 = gui_nop(#gres{state=tailing,beep=true,message=?MustCommit},State0#state{replyTo=ReplyTo}),
    {next_state, tailing, State1};
tailing({button, '>|...', ReplyTo}, State0) ->
    % resume tailing
    State1 = reply_stack(tailing, ReplyTo, State0),
    State2 = serve_bot(tailing, '>|...', State1),
    {next_state, tailing, State2};
tailing({button, '>|', ReplyTo}=Cmd, #state{bufCnt=0}=State0) ->
    % wait for data and then show bottom
    State1 = reply_stack(tailing, ReplyTo, State0),
    {next_state, tailing, State1#state{stack=Cmd}};
tailing({button, '>|', ReplyTo}, State0) ->
    % show bottom
    State1 = reply_stack(tailing, ReplyTo, State0),
    State2 = serve_bot(tailing, undefined, State1),
    {next_state, tailing, State2};
tailing({rows, {Recs,tail}}, State0) ->
    State1 = data_append(tailing,{Recs,tail},State0),
    {next_state, tailing, State1#state{pfc=0}};
tailing(Other, State) ->
    ?Log("tailing -- unexpected event ~p in state~n~p~n", [Other,State]),
    {next_state, tailing, State}.

completed({button, '...', ReplyTo}, #state{dirtyCnt=DC}=State0) when DC==0 ->
    % clear buffers, close and reopen fetch with skip and tail options
    State1 = reply_stack(completed, ReplyTo, State0),
    State2 = fetch_close(State1),
    State3 = fetch(skip,true,State2),
    State4 = data_clear(State3),
    State5 = gui_clear(#gres{state=tailing,loop='>|...'},State4#state{tailMode=true}),
    {next_state, tailing, State5};
completed({button, '...', ReplyTo}, State0) ->
    % reject because of uncommitted changes
    State1 = gui_nop(#gres{state=completed,beep=true,message=?MustCommit},State0#state{replyTo=ReplyTo}),
    {next_state, completed, State1};
completed({button, '>|...', ReplyTo}, State0) ->
    % keep data (if any) and switch to tail mode
    State1 = reply_stack(completed, ReplyTo, State0),
    State2 = fetch(skip,true,State1),
    State3 = gui_clear(State2),
    State4 = gui_nop(#gres{state=tailing,loop='>|...'},State3#state{tailMode=true}),
    {next_state, tailing, State4};
completed({button, '>|', ReplyTo}, #state{bufCnt=0}=State0) ->
    % reject command because we have no data
    State1 = reply_stack(completed, ReplyTo, State0),
    State1 = gui_nop(#gres{state=completed,beep=true},State1),
    {next_state, completed, State1};
completed({button, '>|', ReplyTo}, #state{bl=BL,bufBot=BufBot}=State0) ->
    % jump to buffer bottom
    State1 = reply_stack(completed, ReplyTo, State0),
    State2 = gui_replace_until(BufBot,BL,#gres{state=completed},State1),
    {next_state, completed, State2};
completed({rows, _}, State) ->
    % ignore unsolicited rows
    {next_state, completed, State};
completed(Other, State) ->
    ?Log("completed -- unexpected event ~p~n", [Other]),
    {next_state, completed, State}.


%% --------------------------------------------------------------------
%% Func: SN/3	 synchronized event handling
%% Returns: {next_state, NextSN, NextStateData}            |
%%          {next_state, NextSN, NextStateData, Timeout}   |
%%          {reply, ReplyTo, NextSN, NextStateData}          |
%%          {reply, ReplyTo, NextSN, NextStateData, Timeout} |
%%          {stop, Reason, NewStateData}                          |
%%          {stop, Reason, ReplyTo, NewStateData}
%% --------------------------------------------------------------------


%% --------------------------------------------------------------------
%% Func: handle_event/3  handling async "send_all_state_event""
%% Returns: {next_state, NextSN, NextStateData}          |
%%          {next_state, NextSN, NextStateData, Timeout} |
%%          {stop, Reason, NewStateData}
%% --------------------------------------------------------------------

handle_event({button, '>', ReplyTo}, empty, State0) ->
    State1 = fetch(none,none, State0#state{tailMode=false}),
    {next_state, filling, State1#state{stack={button,'>',ReplyTo}}};
handle_event({button, '>', ReplyTo}, SN, State0) ->
    State1 = reply_stack(SN, ReplyTo, State0),
    {next_state, SN, serve_fwd(SN, State1)};
handle_event({button, '>>', ReplyTo}, SN, State0) ->
    State1 = reply_stack(SN, ReplyTo, State0),
    {next_state, SN, serve_ffwd(SN, State1)};
handle_event({button, '|<', ReplyTo}, SN, State0) ->
    State1 = reply_stack(SN, ReplyTo, State0),
    {next_state, SN, serve_top(SN, State1)};
handle_event({button, '<', ReplyTo}, SN, State0) ->
    State1 = reply_stack(SN, ReplyTo, State0),
    {next_state, SN, serve_bwd(SN, State1)};
handle_event({button, '<<', ReplyTo}, SN, State0) ->
    State1 = reply_stack(SN, ReplyTo, State0),
    {next_state, SN, serve_fbwd(SN, State1)};
handle_event({button, Target, ReplyTo}, SN, State0) when is_integer(Target) ->
    State1 = reply_stack(SN, ReplyTo, State0),
    {next_state, SN, serve_target(SN, Target, State1)};
handle_event({update, ChangeList, ReplyTo}, SN, State0) ->
    State1 = reply_stack(SN, ReplyTo, State0),
    State2 = data_update(SN, ChangeList, State1),
    {next_state, SN, State2};
handle_event({button, commit, ReplyTo}, SN, State0) ->
    State1 = reply_stack(SN, ReplyTo, State0),
    State2 = data_commit(SN, State1),
    {next_state, SN, State2};
handle_event({button, rollback, ReplyTo}, SN, State0) ->
    State1 = reply_stack(SN, ReplyTo, State0),
    State2 = data_rollback(SN, State1),
    {next_state, SN, State2};
handle_event({button, close, ReplyTo}, SN, #state{dirtyCnt=DC}=State0) when DC==0 ->
    State1 = reply_stack(SN, ReplyTo, State0),
    State2 = fetch_close(State1),
    State3 = gui_close(#gres{state=SN},State2),
    {stop, normal, State3};
handle_event({button, close, ReplyTo}, SN, State0) ->
    State1 = reply_stack(SN, ReplyTo, State0),
    State2 = gui_nop(#gres{state=SN,beep=true,message=?MustCommit},State1),
    {next_state, SN, State2}.


%% --------------------------------------------------------------------
%% Func: handle_sync_event/4 handling sync "send_all_state_event""
%% Returns: {next_state, NextSN, NextStateData}            |
%%          {next_state, NextSN, NextStateData, Timeout}   |
%%          {reply, ReplyTo, NextSN, NextStateData}          |
%%          {reply, ReplyTo, NextSN, NextStateData, Timeout} |
%%          {stop, Reason, NewStateData}                          |
%%          {stop, Reason, ReplyTo, NewStateData}
%% --------------------------------------------------------------------
handle_sync_event(_Event, _From, empty, StateData) ->
    {no_reply, empty, StateData,infinity}.

%% --------------------------------------------------------------------
%% Func: handle_info/3
%% Returns: {next_state, NextSN, NextStateData}          |
%%          {next_state, NextSN, NextStateData, Timeout} |
%%          {stop, Reason, NewStateData}
%% --------------------------------------------------------------------
handle_info({_Pid,{Rows,Completed}}, SN, State) ->
    Fsm = {?MODULE,self()},
    Fsm:rows({Rows,Completed}),
    {next_state, SN, State, infinity}.

%% --------------------------------------------------------------------
%% Func: terminate/3
%% Purpose: Shutdown the fsm
%% Returns: any
%% --------------------------------------------------------------------
terminate(_Reason, _SN, _StatData) -> ok.

%% --------------------------------------------------------------------
%% Func: code_change/4
%% Purpose: Convert process state when code is changed
%% Returns: {ok, NewState, NewStateData}
%% --------------------------------------------------------------------
code_change(_OldVsn, SN, StateData, _Extra) ->
    {ok, SN, StateData}.

%% --------------------------------------------------------------------
%%% Internal functions
%% --------------------------------------------------------------------

gui_max(BL) when BL < 10 -> 30;
gui_max(BL) -> 3 * BL.

gui_response(Gres0, #state{nav=raw,rawCnt=RawCnt,replyTo=ReplyTo}=State) ->
    Gres1 = Gres0#gres{cnt=RawCnt,toolTip=integer_to_list(RawCnt)},
    ReplyTo ! Gres1,
    % ?Log("ReplyTo  ~p~n", [ReplyTo]),
    ?Log("Gres  ~p~n", [Gres1]),
    State;
gui_response(Gres0, #state{nav=ind,rawCnt=RawCnt,indCnt=IndCnt,guiCol=true,replyTo=ReplyTo}=State) ->
    ToolTip = integer_to_list(RawCnt) ++ [$/] ++ integer_to_list(IndCnt) ++ " page needs refresh",
    Gres1 = Gres0#gres{cnt=IndCnt,toolTip=ToolTip},
    ReplyTo ! Gres1,
    % ?Log("ReplyTo  ~p~n", [ReplyTo]),
    ?Log("Gres  ~p~n", [Gres1]),
    State;
gui_response(Gres, #state{nav=ind,rawCnt=RawCnt,indCnt=IndCnt,replyTo=ReplyTo}=State) ->
    ToolTip = integer_to_list(RawCnt) ++ [$/] ++ integer_to_list(IndCnt),
    Gres1 = Gres#gres{cnt=IndCnt,toolTip=ToolTip},
    ReplyTo ! Gres1,
    % ?Log("ReplyTo  ~p~n", [ReplyTo]),
    ?Log("Gres  ~p~n", [Gres1]),
    State.

gui_close(GuiResult,State) -> 
    ?Log("gui_close () ~p~n", [GuiResult#gres.state]),
    gui_response(GuiResult#gres{operation=close},State).

gui_nop(GuiResult,State) -> 
    ?Log("gui_nop () ~p ~p~n", [GuiResult#gres.state, GuiResult#gres.loop]),
    gui_response(GuiResult#gres{operation=nop},State).

gui_clear(GuiResult,State0) ->
    ?Log("gui_clear () ~p ~p~n", [GuiResult#gres.state, GuiResult#gres.loop]),
    Default = #state{}, 
    State1 = State0#state{ guiCnt=Default#state.guiCnt, guiTop=Default#state.guiTop         
                         , guiBot=Default#state.guiBot, guiCol=Default#state.guiCol},
    gui_response(GuiResult#gres{operation=clr,keep=0}, State1).

gui_replace(Top,Bot,GuiResult,State0) ->
    ?Log("gui_replace ~p to ~p ~p ~p~n", [Top, Bot, GuiResult#gres.state, GuiResult#gres.loop]),
    Rows=all_rows(Top,Bot,State0),
    Cnt=length(Rows),
    gui_response(GuiResult#gres{operation=rpl,rows=Rows,keep=Cnt}
        ,State0#state{guiCnt=Cnt,guiTop=Top,guiBot=Bot,guiCol=false}).

gui_replace_from(Top,Limit,GuiResult,#state{nav=raw,tableId=TableId,rowFun=RowFun}=State0) ->
    Ids= [Top | ids_after(Top, Limit-1, State0)],
    Cnt = length(Ids),
    Rows = rows_for_ids(Ids,TableId,RowFun),
    NewGuiTop = Top,
    NewGuiBot = lists:last(Ids),
    ?Log("gui_replace_from  ~p to ~p ~p ~p~n", [NewGuiTop, NewGuiBot, GuiResult#gres.state, GuiResult#gres.loop]),
    gui_response(GuiResult#gres{operation=rpl,rows=Rows,keep=Cnt}
        ,State0#state{guiCnt=Cnt,guiTop=NewGuiTop,guiBot=NewGuiBot});
gui_replace_from(Top,Limit,GuiResult,#state{nav=ind,tableId=TableId}=State0) ->
    Keys = [Top | keys_after(Top, Limit-1, State0)],
    Cnt = length(Keys),
    Rows = rows_for_keys(Keys,TableId),
    NewGuiTop = Top,
    NewGuiBot = lists:last(Keys),
    ?Log("gui_replace_from  ~p to ~p ~p ~p~n", [NewGuiTop, NewGuiBot, GuiResult#gres.state, GuiResult#gres.loop]),
    gui_response(GuiResult#gres{operation=rpl,rows=Rows,keep=Cnt}
        ,State0#state{guiCnt=Cnt,guiTop=NewGuiTop,guiBot=NewGuiBot}).

gui_replace_until(Bot,Limit,GuiResult,#state{nav=raw,tableId=TableId,rowFun=RowFun}=State0) ->
    Ids = ids_before(Bot, Limit-1, State0) ++ [Bot],
    Cnt = length(Ids),
    Rows = rows_for_ids(Ids,TableId,RowFun),
    NewGuiTop = hd(Ids),
    NewGuiBot = Bot,
    ?Log("gui_replace_until  ~p to ~p ~p ~p~n", [NewGuiTop, NewGuiBot, GuiResult#gres.state, GuiResult#gres.loop]),
    gui_response(GuiResult#gres{operation=rpl,rows=Rows,keep=Cnt}
        ,State0#state{guiCnt=Cnt,guiTop=NewGuiTop,guiBot=NewGuiBot});
gui_replace_until(Bot,Limit,GuiResult,#state{nav=ind,tableId=TableId}=State0) ->
    Keys = keys_before(Bot, Limit-1, State0) ++ [Bot],
    Cnt = length(Keys),
    Rows = rows_for_keys(Keys,TableId),
    NewGuiTop = hd(Keys),
    NewGuiBot = Bot,
    ?Log("gui_replace_until  ~p to ~p ~p ~p~n", [NewGuiTop, NewGuiBot, GuiResult#gres.state, GuiResult#gres.loop]),
    gui_response(GuiResult#gres{operation=rpl,rows=Rows,keep=Cnt}
        ,State0#state{guiCnt=Cnt,guiTop=NewGuiTop,guiBot=NewGuiBot}).

gui_prepend(GuiResult,#state{nav=raw,bl=BL,gl=GL,guiCnt=GuiCnt,guiTop=GuiTop}=State0) ->
    Rows = rows_before(GuiTop, BL, State0),
    Cnt = length(Rows),
    IdsKept = ids_after(GuiTop,min(GuiCnt,GL-Cnt-1),State0),
    NewGuiCnt = length(IdsKept)+1+Cnt,
    NewGuiTop = hd(hd(Rows)),
    NewGuiBot = lists:last(IdsKept),
    ?Log("gui_prepend (~p ~p) ~p ~p~n", [NewGuiTop, NewGuiBot, GuiResult#gres.state, GuiResult#gres.loop]),
    gui_response(GuiResult#gres{operation=prp,rows=Rows,keep=NewGuiCnt}
        ,State0#state{guiCnt=NewGuiCnt,guiTop=NewGuiTop,guiBot=NewGuiBot});
gui_prepend(GuiResult,#state{nav=ind,bl=BL,gl=GL,tableId=TableId,guiCnt=GuiCnt,guiTop=GuiTop}=State0) ->
    Keys=keys_before(GuiTop, BL, State0),
    Cnt = length(Keys),
    Rows = rows_for_keys(Keys,TableId),
    KeysKept = keys_after(GuiTop,min(GuiCnt,GL-Cnt-1),State0),
    NewGuiCnt = length(KeysKept)+1+Cnt,
    NewGuiTop = hd(Keys),
    NewGuiBot = lists:last(KeysKept),
    ?Log("gui_prepend (~p ~p) ~p ~p~n", [NewGuiTop, NewGuiBot, GuiResult#gres.state, GuiResult#gres.loop]),
    gui_response(GuiResult#gres{operation=prp,rows=Rows,keep=NewGuiCnt}
        ,State0#state{guiCnt=NewGuiCnt,guiTop=NewGuiTop,guiBot=NewGuiBot}).

gui_append(GuiResult,#state{nav=raw,bl=BL,gl=GL,guiCnt=GuiCnt,guiBot=GuiBot}=State0) ->
    Rows=rows_after(GuiBot, BL, State0),
    Cnt = length(Rows),
    IdsKept = ids_before(GuiBot,min(GuiCnt,GL-Cnt-1),State0),
    NewGuiCnt = length(IdsKept)+1+Cnt,
    NewGuiTop = hd(IdsKept),
    NewGuiBot = hd(lists:last(Rows)),
    ?Log("gui_append  (~p ~p) ~p ~p~n", [NewGuiTop, NewGuiBot, GuiResult#gres.state, GuiResult#gres.loop]),
    gui_response(GuiResult#gres{operation=app,rows=Rows,keep=NewGuiCnt}
        ,State0#state{guiCnt=NewGuiCnt,guiTop=NewGuiTop,guiBot=NewGuiBot});
gui_append(GuiResult,#state{nav=ind,bl=BL,gl=GL,tableId=TableId,guiCnt=GuiCnt,guiBot=GuiBot}=State0) ->
    Keys=keys_after(GuiBot, BL, State0),
    Cnt = length(Keys),
    Rows = rows_for_keys(Keys,TableId),
    KeysKept = keys_before(GuiBot,min(GuiCnt,GL-Cnt-1),State0),
    NewGuiCnt = length(KeysKept)+1+Cnt,
    NewGuiTop = hd(KeysKept),
    NewGuiBot = lists:last(Keys),
    ?Log("gui_append  (~p ~p) ~p ~p~n", [NewGuiTop, NewGuiBot, GuiResult#gres.state, GuiResult#gres.loop]),
    gui_response(GuiResult#gres{operation=app,rows=Rows,keep=NewGuiCnt}
        ,State0#state{guiCnt=NewGuiCnt,guiTop=NewGuiTop,guiBot=NewGuiBot}).


serve_empty(SN,true,State0) ->
    State1 = prefetch(SN,State0),          %% only when filling
    gui_nop(#gres{state=SN,beep=true},State1);
serve_empty(SN,false,State0) ->
    State1 = prefetch(SN,State0),          %% only when filling
    gui_nop(#gres{state=SN},State1).

serve_top(SN,#state{bl=BL,bufCnt=BufCnt,bufTop=BufTop}=State0) ->
    if
        (BufCnt == 0) ->
            %% no data, serve empty page
            serve_empty(SN,false,State0);
        (BufCnt >= BL+BL) ->
            %% enough data, serve it, no need for prefetch
            gui_replace_from(BufTop,BL,#gres{state=SN},State0);
        true ->
            %% we have data but may need to prefetch
            State1 = prefetch(SN,State0),          %% only when filling
            gui_replace_from(BufTop,BL,#gres{state=SN},State1)
    end.

serve_fwd(SN,#state{bl=BL,bufCnt=BufCnt,bufBot=BufBot,guiCnt=GuiCnt,guiBot=GuiBot,guiCol=GuiCol}=State0) ->
    if
        (BufCnt == 0) ->
            %% no data, serve empty gui
            serve_empty(SN,false,State0);
        (GuiCnt == 0) ->
            %% (re)initialize buffer
            serve_top(SN,State0);
        (GuiCol == true) andalso (GuiBot == BufBot) ->
            %% index view is stale at end of buffer, auto-refresh
            gui_replace_until(GuiBot,BL,#gres{state=SN},State0#state{guiCol=false});
        (GuiCol == true)  ->
            %% index view is stale replace next page
            gui_replace_from(GuiBot,BL,#gres{state=SN},State0#state{guiCol=false});
        (GuiBot == BufBot) ->
            %% we are at the end of the buffer, cannot go forward
            State1 = prefetch(SN,State0),          %% only when filling
            gui_nop(#gres{state=SN,beep=true},State1);
        true ->
            %% go forward
            State1 = prefetch(SN,State0),        %% ToDo: avoid prefetch if not close to the end
            gui_append(#gres{state=SN},State1)
    end.

serve_bwd(SN,#state{bl=BL,bufCnt=BufCnt,guiCnt=GuiCnt,guiTop=GuiTop}=State0) ->
    if
        (BufCnt == 0) ->
            %% no data, serve empty gui
            serve_empty(SN,true,State0);
        (GuiCnt == 0) ->
            %% (re)initialize buffer
            serve_top(SN,State0);
        true ->
            gui_prepend(#gres{state=SN},State0)
    end.

serve_ffwd(SN,#state{bl=BL,rawBot=RawBot,guiBot=GuiBot,replyTo=ReplyTo}=State0) ->
    if
      (RawBot == 0) ->
          serve_empty(SN,false,State0);
      (RawBot =< BL) ->
          serve_fwd(SN,State0);
      (GuiBot =< BL) ->
          serve_fwd(SN,State0);
      (GuiBot+GuiBot+BL =< RawBot) ->
          gui_replace(GuiBot+GuiBot-BL+1,GuiBot+GuiBot,#gres{state=SN},State0);
      (GuiBot+GuiBot =< RawBot) ->
          State1 = prefetch(SN,State0),          %% only when filling
          gui_replace(GuiBot+GuiBot-BL+1,GuiBot+GuiBot,#gres{state=SN},State1);
      true ->
          State1 = prefetch(SN,State0),          %% only when filling
          State1#state{stack={button,GuiBot+GuiBot,ReplyTo}}
    end.

serve_fbwd(SN,#state{bl=BL,rawBot=RawBot,guiTop=GuiTop}=State0) ->
    if
      (RawBot == 0) ->
          serve_empty(SN,true,State0);
      (GuiTop >= BL+BL) and (GuiTop div 2+BL == GuiTop-1)  ->
          gui_prepend(#gres{state=SN},State0);
      (GuiTop >= BL+BL) ->
          gui_replace(GuiTop div 2+1,GuiTop div 2+BL,#gres{state=SN},State0);
      true ->
          serve_top(SN,State0)
    end.

serve_target(SN,Target,#state{bl=BL,rawBot=RawBot,guiTop=GuiTop,guiBot=GuiBot,replyTo=ReplyTo}=State0) ->
    if
      (RawBot == 0) and (SN == complete) ->
          serve_empty(SN,true,State0);
      (RawBot == 0) ->         
          %% no_data now, com back later 
          State1 = prefetch(SN,State0),          %% only when filling
          % gui_nop(#gres{state=SN,loop=Target},State1);
          State1#state{stack={button,Target,ReplyTo}};
      (Target =< 0) and (RawBot+Target > 0) ->
          %% target given relative to RawBot, retry with absolute target position 
          serve_target(SN,Target,State0);
      (Target =< 0)  ->
          serve_top(SN,State0);
      (Target =< RawBot) and (RawBot < BL) ->
          serve_top(SN,State0);
      (Target =< RawBot) and (GuiTop > Target) ->
          %% serve block backward
          gui_replace(Target,Target+BL-1,#gres{state=SN},State0);
      (Target =< RawBot) and (GuiBot < Target) and (Target > RawBot-BL-BL) ->
          %% serve block forward
          State1 = prefetch(SN,State0),          %% only when filling
          gui_replace(Target-BL+1,Target,#gres{state=SN},State1);
      (Target =< RawBot) and (GuiBot < Target) ->
          %% serve block forward
          gui_replace(Target-BL+1,Target,#gres{state=SN},State0);
      (Target > RawBot) and (SN == completed) ->
          serve_bot(SN,undefined,State0);
      (Target > RawBot) ->
          State1 = prefetch(SN,State0),          %% only when filling
          gui_nop(#gres{state=SN,loop=Target},State1);
      true ->
          %% target should be in GUI already
          gui_nop(#gres{state=SN,message="target row already in gui"},State0)
    end.


    % State2 = if
    %     (RawBot == GuiBot) ->
    %         State1#state{stack=Cmd}; 
    %     (GuiBot >= RawBot-GL) ->
    %         gui_append(#gres{state=tailing,loop='>|...'},State1); 
    %     true ->
    %         gui_replace_until(RawBot,BL,#gres{state=tailing,loop='>|...'},State1)
    % end,
    % %% -------------------------

serve_bot(SN, Loop, #state{nav=Nav,bl=BL,gl=GL,bufCnt=BufCnt,bufBot=BufBot,guiCnt=GuiCnt,guiBot=GuiBot,guiCol=GuiCol}=State0) ->
    ?Log("serve_bot  (~p ~p) ~p ~p~n", [SN, Loop, GuiBot, BufBot]),
    if
        (BufCnt == 0) ->
            %% no data, serve empty
            serve_empty(#gres{state=SN,loop=Loop},false,State0);         
        (GuiCnt == 0) ->
            %% uninitialized view, must refresh    
            gui_replace_until(BufBot,BL,#gres{state=SN,loop=Loop},State0); 
        (GuiCol == true) ->
            %% dirty index view, must refresh anyways    
            gui_replace_until(BufBot,BL,#gres{state=SN,loop=Loop},State0); 
        (GuiBot == BufBot) ->
            %% gui is already there, noting to do       
            gui_nop(#gres{state=SN,loop=Loop},State0); 
        (Nav == raw) andalso (GuiBot < BufBot-GL) ->
            %% uninitialized view, must refresh    
            gui_replace_until(BufBot,BL,#gres{state=SN,loop=Loop},State0); 
        (Loop == '>|...') andalso (SN == tailing) ->
            %% tailing should append (don't call this far from bottom of big buffer)                 
            gui_append(#gres{state=SN,loop=Loop},State0); 
        true ->
            %% jump to end and discard other cases (avoid scrolling big buffer)                 
            gui_replace_until(BufBot,BL,#gres{state=SN,loop=Loop},State0)
    end.

serve_stack( _, #state{stack=undefined}=State) -> 
    % no stack, nothing to do
    State;
serve_stack( _, #state{nav=ind,bufBot=B,guiBot=B}=State) -> 
    % gui is current at the end of the boffer, no new interesting data, nothing to do
    State;
serve_stack(SN, #state{stack={button,'>',RT}}=State0) ->
    % deferred forward can be executed now
    State1 = serve_fwd(SN,State0#state{replyTo=RT}),
    State1#state{stack=undefined};
serve_stack(SN, #state{bufCnt=BufCnt,stack={button,Target,RT}}=State0) when is_integer(Target), (BufCnt>=Target) ->
    % deferred target can be executed now
    State1 = serve_target(SN,Target,State0#state{replyTo=RT}),
    State1#state{stack=undefined};
serve_stack(completed, #state{stack={button,_,RT}}=State0) ->
    % deferred button can be executed now (one of the forward buttons '>' '>>' '>|' '>|...')
    State1 = serve_bot(complete,undefined,State0#state{replyTo=RT}),
    State1#state{stack=undefined};
serve_stack(tailing, #state{stack={button,'>|',RT}}=State0) ->
    % deferred button can be executed now
    State1 = serve_bot(tailing,undefined,State0#state{replyTo=RT}),
    State1#state{stack=undefined};
serve_stack(tailing, #state{bufBot=BufBot,guiBot=GuiBot,stack={button,'>|...',RT}}=State0) ->
    if
        (BufBot == GuiBot) -> 
            % no new data, nothing to do, keep stack
            State0; 
        true ->
            % serve new data at the bottom of the buffer, ask client to come back
            State1 = serve_bot(tailing,'>|...',State0#state{replyTo=RT}),
            State1#state{stack=undefined}
    end;
serve_stack(_ , State) -> State.


all_rows(Top, Bot, _) when Top > Bot -> [];
all_rows(Top, Bot, #state{nav=raw,rowFun=RowFun,tableId=TableId}) ->
    Rows = ets:select(TableId,[{'$1',[{'>=',{element,1,'$1'},Top},{'=<',{element,1,'$1'},Bot}],['$_']}]),
    [gui_row_expand(R, TableId, RowFun) || R <- Rows];
all_rows(Top, Bot, #state{nav=ind,indexId=IndexId,tableId=TableId}) ->
    IndRows = ets:select(IndexId,[{'$1',[{'>=',{element,1,'$1'},Top},{'=<',{element,1,'$1'},Bot}],['$_']}]),
    [gui_row_as_list(ets:lookup(TableId, Id)) || {_,Id} <- IndRows].

rows_after(_, [], _) -> [];
rows_after(Key, Limit, #state{nav=raw,rowFun=RowFun,tableId=TableId}) ->
    case ets:select(TableId,[{'$1',[{'>',{element,1,'$1'},Key}],['$_']}],Limit) of
        {Rs, _Cont} ->      [gui_row_expand(R, TableId, RowFun) || R <- Rs];  
        '$end_of_table' ->  []
    end.

rows_for_keys([],_) -> [];
rows_for_keys(Keys,TableId) ->
    [gui_row_as_list(hd(ets:lookup(TableId, Id))) || {_,Id} <- Keys].

rows_for_ids([],_,_) -> [];
rows_for_ids(Ids,TableId,RowFun) ->
    [gui_row_expand(hd(ets:lookup(TableId, Id)), TableId, RowFun) || Id <- Ids].

keys_before(_, 0, _) -> [];
keys_before(Key, Limit, #state{nav=ind,indexId=IndexId}) ->
    case ets:select_reverse(IndexId,[{'$1',[{'<',{element,1,'$1'},{const,Key}}],[{element,1,'$1'}]}],Limit) of
        {Keys, _Cont} ->    lists:reverse(Keys);  
        '$end_of_table' ->  []
    end.

keys_after(_, 0, _) -> [];
keys_after(Key, Limit, #state{nav=ind,indexId=IndexId}) ->
    case ets:select(IndexId,[{'$1',[{'>',{element,1,'$1'},{const,Key}}],[{element,1,'$1'}]}],Limit) of
        {Keys, _Cont} ->    Keys;  
        '$end_of_table' ->  []
    end.

rows_before(_, 0, _) -> [];
rows_before(Key, Limit, #state{nav=raw,rowFun=RowFun,tableId=TableId}) ->
    case ets:select_reverse(TableId,[{'$1',[{'<',{element,1,'$1'},Key}],['$_']}],Limit) of
        {Rs, _Cont} ->      [gui_row_expand(R, TableId, RowFun) || R <- lists:reverse(Rs)];  
        '$end_of_table' ->  []
    end;
rows_before(Key, Limit, #state{tableId=TableId}=State) ->
    Keys = keys_before(Key, Limit, State),
    [gui_row_as_list(ets:lookup(TableId, Id)) || {_,Id} <- Keys].

ids_before(_, 0, _) -> [];
ids_before(Id, Limit, #state{nav=raw,tableId=TableId}) ->
    case ets:select_reverse(TableId,[{'$1',[{'<',{element,1,'$1'},Id}],[{element,1,'$1'}]}],Limit) of
        {Ids, _Cont} ->     lists:reverse(Ids);  
        '$end_of_table' ->  []
    end.

ids_after(_, 0, _) -> [];
ids_after(Id, Limit, #state{nav=raw,tableId=TableId}) ->
    case ets:select(TableId,[{'$1',[{'>',{element,1,'$1'},Id}],[{element,1,'$1'}]}],Limit) of
        {Ids, _Cont} ->     Ids;  
        '$end_of_table' ->  []
    end.


gui_row_as_list(FullRowTuple) ->
    List = tuple_to_list(FullRowTuple),
    [hd(List),lists:nth(2,List)|lists:nthtail(3,List)].

gui_row_expand({I,Op,RK}, TableId, RowFun) ->
    Row = RowFun(RK),
    ets:insert(TableId, list_to_tuple([I, Op, RK | Row])),
    [I,Op|Row];
gui_row_expand(FullRowTuple, _TableId, _RowFun) ->
    List = tuple_to_list(FullRowTuple),
    [hd(List),lists:nth(2,List)|lists:nthtail(3,List)].

raw_row_expand({I,Op,RK}, RowFun) ->
    list_to_tuple([I, Op, RK | RowFun(RK)]).

data_clear(State) -> 
    gui_clear(ind_clear(raw_clear(State))).

raw_clear(#state{tableId=TableId}=State) -> 
    ?Log("raw_clear~n", []),
    true = ets:delete_all_objects(TableId),    
    Default = #state{}, 
    set_buf_counters(State#state{ rawCnt = Default#state.rawCnt
                                , rawTop = Default#state.rawTop          
                                , rawBot = Default#state.rawBot          
                                , dirtyCnt = Default#state.dirtyCnt
                                , dirtyTop = Default#state.dirtyTop 
                                , dirtyBot = Default#state.dirtyBot        
                    }). 

ind_clear(#state{indexId=IndexId}=State) -> 
    ?Log("ind_clear~n", []),
    true = ets:delete_all_objects(IndexId),    
    Default = #state{}, 
    set_buf_counters(State#state{ indCnt = Default#state.indCnt
                                , indTop = Default#state.indTop                
                                , indBot = Default#state.indBot
                    }). 

gui_clear(State) -> 
    ?Log("gui_clear~n", []),
    Default = #state{}, 
    State#state{  guiCnt = Default#state.guiCnt
                , guiTop = Default#state.guiTop         
                , guiBot = Default#state.guiBot         
                , guiCol = Default#state.guiCol         
                }. 

data_append(SN, {[],_Complete},#state{nav=Nav,rawBot=RawBot}=State) -> 
    NewPfc=State#state.pfc-1,
    ?Log("data_append -~p- count ~p bufBottom ~p pfc ~p~n", [Nav,0,RawBot,NewPfc]),
    serve_stack(SN, State#state{pfc=NewPfc});
data_append(SN, {Recs,_Complete},#state{nav=raw,tableId=TableId,rawCnt=RawCnt,rawTop=RawTop,rawBot=RawBot}=State) ->
    NewPfc=State#state.pfc-1,
    Cnt = length(Recs),
    NewRawCnt = RawCnt+Cnt,
    NewRawTop = min(RawTop,RawBot+1),   % initialized to 1 and then changed only in delete or clear
    NewRawBot = RawBot+Cnt,
    ?Log("data_append -raw- count ~p bufBottom ~p pfc ~p~n", [Cnt,NewRawBot,NewPfc]),
    ets:insert(TableId, [list_to_tuple([I,nop|[R]])||{I,R}<-lists:zip(lists:seq(RawBot+1, NewRawBot), Recs)]),
    serve_stack(SN, set_buf_counters(State#state{pfc=NewPfc,rawCnt=NewRawCnt,rawTop=NewRawTop,rawBot=NewRawBot}));
data_append(SN, {Recs,_Complete},#state{nav=ind,tableId=TableId,indexId=IndexId
        ,rawCnt=RawCnt,rawTop=RawTop,rawBot=RawBot,indCnt=IndCnt
        ,guiTop=GuiTop,guiBot=GuiBot,guiCol=GuiCol
        ,rowFun=RowFun,filterFun=FilterFun,sortFun=SortFun}=State) ->
    NewPfc=State#state.pfc-1,
    Cnt = length(Recs),
    NewRawCnt = RawCnt+Cnt,
    NewRawTop = min(RawTop,RawBot+1),   % initialized to 1 and then changed only in delete or clear
    NewRawBot = RawBot+Cnt,
    RawRows = [raw_row_expand({I,nop,RK}, RowFun) || {I,RK} <- lists:zip(lists:seq(RawBot+1, NewRawBot), Recs)],
    ets:insert(TableId, RawRows),
    IndRows = [{{SortFun(element(3,R)),element(1,R)},element(1,R)} || R <- lists:filter(FilterFun,RawRows)],
    % ?Log("data_append -IndRows- ~p~n", [IndRows]),
    FunCol = fun({X,_},{IT,IB,C}) ->  {IT,IB,(C orelse ((X>IT) and (X<IB)))}  end, 
    {_,_,Collision} = lists:foldl(FunCol, {GuiTop, GuiBot, false}, IndRows),    %% detect data collisions with gui content
    ets:insert(IndexId, IndRows),
    NewIndCnt = IndCnt + length(IndRows),
    NewIndTop = ets:first(IndexId),
    NewIndBot = ets:last(IndexId),
    NewGuiCol = (GuiCol or Collision),    
    ?Log("data_append -ind- count ~p bufBottom ~p pfc=~p stale=~p~n", [Cnt,NewRawBot,NewPfc,NewGuiCol]),
    serve_stack(SN, set_buf_counters(State#state{ pfc=NewPfc
                                                , rawCnt=NewRawCnt,rawTop=NewRawTop,rawBot=NewRawBot
                                                , indCnt=NewIndCnt,indTop=NewIndTop,indBot=NewIndBot
                                                , guiCol=NewGuiCol}
                                    )
                ).

data_update(SN,ChangeList,State0) ->
    State1 = data_update_rows(ChangeList,State0),
    %% ToDo: return list of Ids chosen by fsm for inserts
    gui_nop(#gres{state=SN},State1).  

data_commit(SN, #state{guiTop=GuiTop,guiBot=GuiBot}=State) -> 
    %% ToDo: recalculate dirty rows using KeyUpdate
    %%       serve errors if present (no matter the size)
    gui_replace(GuiTop, GuiBot, #gres{state=SN},State). 

data_rollback(SN, #state{guiTop=GuiTop,guiBot=GuiBot}=State) -> 
    %% ToDo: recalculate dirty rows using Keys in buffer
    %%       serve errors if present (no matter the size)
    gui_replace(GuiTop, GuiBot, #gres{state=SN},State).  

data_update_rows([], State) -> State;
data_update_rows([Ch|ChangeList], State0) ->
    State1 = data_update_row(Ch, State0),
    data_update_rows(ChangeList, State1).

data_update_row([undefined,Fields], #state{tableId=TableId,rawBot=RawBot,dirtyTop=DT0,dirtyCnt=DC0}=State0) ->
    Id = RawBot+1,          %% ToDo: map Fields to complete rows ("" for undefined fields)
    ets:insert(TableId, list_to_tuple([Id,ins,{}|Fields])),    
    State0#state{dirtyTop=min(DT0,Id),dirtyBot=Id,dirtyCnt=DC0+1};
data_update_row([Id,Op,Fields], #state{tableId=TableId}=State0) when is_integer(Id) ->
    OldRow = ets:lookup(TableId, Id),
    {O,State1} = case {element(2,OldRow),Op} of
        {nop,nop} ->    {nop,State0};
        {nop,_} ->      DT = min(State0#state.dirtyTop,Id),
                        DB = max(State0#state.dirtyBot,Id),
                        DC = State0#state.dirtyCnt+1,
                        {Op, State0#state{dirtyTop=DT,dirtyBot=DB,dirtyCnt=DC}};
        {_,nop} ->      DC = State0#state.dirtyCnt-1,
                        {nop,State0#state{dirtyCnt=DC}};
        {ins,upd} ->    {ins,State0};
        {ins,ins} ->    {ins,State0};
        {ins,del} ->    DC = State0#state.dirtyCnt-1,
                        {nop,State0#state{dirtyCnt=DC}};
        {del,del} ->    {del,State0};
        {del,upd} ->    {upd,State0};
        {upd,upd} ->    {upd,State0};        
        {upd,del} ->    {del,State0}        
    end,
    ets:insert(TableId, list_to_tuple([Id,O,element(2,OldRow)|Fields])),
    State1.



%% TESTS ------------------------------------------------------------------

-include_lib("eunit/include/eunit.hrl").

-define(button(__Button), gui_req(button, __Button, self())).

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
                %% , fun test_with_sec/1
            ]}
        }
    }.
    
test_without_sec(_) -> 
    test_with_or_without_sec(false).

% test_with_sec(_) ->
%     test_with_or_without_sec(true).

test_with_or_without_sec(IsSec) ->
    try
        _ClEr = 'ClientError',
        % SeEx = 'SecurityException',
        ?Log("----TEST--- ~p ----Security ~p ~n", [?MODULE, IsSec]),

        ?Log("schema ~p~n", [imem_meta:schema()]),
        ?Log("data nodes ~p~n", [imem_meta:data_nodes()]),
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

        ?assertEqual(ok, insert_range(SKey, 111, def, 'Imem', IsSec)),


        % Fsm = start(SKey,"fsm test",10,false,"select * from def;",self()),
        Fsm = start(SKey,"fsm test",10,false,"select * from def order by col1;",self()),
        ?Log("test client pid ~p~n", [self()]),
        ?Log("fsm object id ~p~n", [Fsm]),
        Fsm:?button('>'),
        receive_respond(Fsm),
        Fsm:?button('>'),
        receive_respond(Fsm),
        Fsm:?button('>'),
        receive_respond(Fsm),
        Fsm:?button('>'),
        receive_respond(Fsm),
        Fsm:?button('>|...'),
        receive_respond(Fsm),
        Fsm:?button('<'),
        receive_respond(Fsm),
        Fsm:?button('>|'),
        receive_respond(Fsm),
        Fsm:?button('<'),
        receive_respond(Fsm),
        Fsm:?button('<'),
        receive_respond(Fsm),
        Fsm:?button('<'),
        receive_respond(Fsm),
        Fsm:?button('<'),
        receive_respond(Fsm),
        Fsm:?button('<'),
        receive_respond(Fsm),
        Fsm:?button(close),

        ?assertEqual(ok, imem_sql:exec(SKey, "drop table def;", 0, 'Imem', IsSec)),

        case IsSec of
            true ->     ?imem_logout(SKey);
            false ->    ok
        end

    catch
        Class:Reason ->  ?Log("Exception ~p:~p~n~p~n", [Class, Reason, erlang:get_stacktrace()]),
        ?assert( true == "all tests completed")
    end,
    ok. 

%% --Interface functions  (calling imem_if for now, not exported) ---------

if_call_mfa(IsSec,Fun,Args) ->
    case IsSec of
        true -> apply(imem_sec,Fun,Args);
        _ ->    apply(imem_meta, Fun, lists:nthtail(1, Args))
    end.

insert_range(_SKey, 0, _Table, _Schema, _IsSec) -> ok;
insert_range(SKey, N, Table, Schema, IsSec) when is_integer(N), N > 0 ->
    if_call_mfa(IsSec, write,[SKey,Table,{Table,integer_to_list(N),N}]),
    insert_range(SKey, N-1, Table, Schema, IsSec).

receive_raw() ->
    receive_raw(50, []).

% receive_raw(Timeout) ->
%     receive_raw(Timeout, []).

receive_raw(Timeout,Acc) ->    
    case receive 
            R ->    % ?Log("got:~n~p~n", [R]),
                    R
        after Timeout ->
            stop
        end of
        stop ->     lists:reverse(Acc);
        Result ->   receive_raw(Timeout,[Result|Acc])
    end.

receive_respond(Fsm) ->
    process_responses(Fsm,receive_raw()).

process_responses(_Fsm,[]) -> ok;
process_responses(Fsm,[R|Responses]) ->
    case R of 
      #gres{loop=undefined,rows=_Rows} ->  
         ok; %?Log("~p~n", [_Rows]);
      #gres{loop='>|...'} ->    ok;
      #gres{loop=Loop} ->
          timer:sleep(30),       
          Fsm:?button(Loop),
          receive_respond(Fsm)
    end,
    process_responses(Fsm,Responses).
