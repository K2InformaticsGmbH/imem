-module(erlimem_fsm). 

-behaviour(gen_fsm).

-include("imem_seco.hrl").
-include("imem_sql.hrl").


-record(ctx,    { %% session context
                  skey
                , id
                , bl                  %% block length
                , stmtRef             %% statement reference
                , isSec               %% secure mode or not
                , rowFun              %% RowFun
                , replyTo             %% reply pid
                }).

-record(state,  { %% fsm combined state
                  ctx                 %% statement & fetch context
                , tableId             %% ets table id 
                , stmtRef             %% statement reference
                , rowFun              %% RowFun
                , bl                  %% block_length (passed to init)
                , gl                  %% gui max length (row count) = gui_max(#state.bl)
                , bufTop = 0          %% sequence number of top buffer row (or 0)
                , bufBot = 0          %% sequence number of bottom buffer row (or 0)
                , dirtyTop = 99999999 %% record id of first dirty row in buffer
                , dirtyBot = 0        %% record id of last dirty row in buffer
                , dirtyCount = 0      %% count of dirty rows in buffer
                , rawTop = 0          %% sequence number of top raw gui row (or 0)
                , rawBot = 0          %% sequence number of bot raw gui row (or 0)
                , tailMode = false    %% tailMode scheduled
                , pfc=0               %% pending fetch count (in flight to DB or back)
                , replyTo             %% reply pid
                , stack=undefined     %% command stack {Tab,button,Button,ReplyTo}
                }).

-record(gres,   { %% response sent back to gui
                  operation           %% rep (replace) | app (append) | prp (prepend) | nop | cre (create filter/sort table)
                , bufBot=0            %% current buffer bottom (last sequence number)
                , rawTop=0            %% first row id in gui (smaller ones must be clipped away)
                , rawBot=0            %% last row id in gui (larger ones must be clipped away)
                , message = []        %% help message
                , beep = false        %% alert with a beep if true
                , state = empty       %% determines color of buffer size indicator
                , loop = undefined    %% gui should come back with this command
                , rows = []           %% rows to show (append / prepend / merge)
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
        , gui_req/5     %% button '>' '>>' '>|' '>|...' '<' '<<' '...'
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

%% ====================================================================
%% External functions
%% ====================================================================

start(SKey,Id,BL,IsSec,Sql,ReplyTo) ->
    StmtResult = exec(SKey, Id, BL, IsSec, Sql),
    #stmtResult{stmtRef=StmtRef,rowFun=RowFun}=StmtResult,
    Ctx = #ctx{skey=SKey,id=Id,bl=BL,stmtRef=StmtRef,rowFun=RowFun,isSec=IsSec,replyTo=ReplyTo}, 
	{ok,Pid} = gen_fsm:start(?MODULE,Ctx,[]),
    {?MODULE,Pid}.

start_link(SKey,Id,BL,IsSec,Sql,ReplyTo) ->
    StmtResult = exec(SKey, Id, BL, IsSec, Sql),
    #stmtResult{stmtRef=StmtRef,rowFun=RowFun}=StmtResult,
    Ctx = #ctx{skey=SKey,id=Id,bl=BL,stmtRef=StmtRef,rowFun=RowFun,isSec=IsSec,replyTo=ReplyTo}, 
	{ok, Pid} = gen_fsm:start_link(?MODULE,Ctx,[]),
    {?MODULE,Pid}.

stop(Pid) -> 
	gen_fsm:send_event(Pid,stop).

gui_req(Tab, button, Button, ReplyTo, {?MODULE,Pid}) -> 
    ?Log("button -- ~p~n", [Button]),
	gen_fsm:send_event(Pid,{Tab, button, Button, ReplyTo});
gui_req(Tab, update, ChangeList, ReplyTo, {?MODULE,Pid}) -> 
    ?Log("ChangeList -- ~p~n", [ChangeList]),
    gen_fsm:send_event(Pid,{Tab, update, ChangeList, ReplyTo}).

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

init(#ctx{bl=BL,replyTo=ReplyTo,stmtRef=StmtRef,rowFun=RowFun}=Ctx) ->
    TableId=ets:new(results, [ordered_set, public]),
    {ok, empty, #state{bl=BL, replyTo=ReplyTo, gl=gui_max(BL), ctx=Ctx, stmtRef=StmtRef, rowFun=RowFun, tableId=TableId}}.

%% --------------------------------------------------------------------
%% Func: SN/2	 non-synchronized event handling
%% Returns: {next_state, NextSN, NextStateData}          |
%%          {next_state, NextSN, NextStateData, Timeout} |
%%          {stop, Reason, NewStateData}
%% --------------------------------------------------------------------


empty({Tab, button, '>|', ReplyTo}, State0) ->
    State1 = fetch(push,none,reset(State0,false)),
    {next_state, autofilling, State1#state{stack={Tab,button,'>|',ReplyTo}}};
empty({Tab, button, '>|...', ReplyTo}, State0) ->
    State1 = fetch(push,true,reset(State0,true)),
    {next_state, autofilling, State1#state{stack={Tab,button,'>|...',ReplyTo}}};
empty({Tab, button, '...', ReplyTo}, State0) ->
    State1 = fetch(skip,true,reset(State0,true)),
    {next_state, tailing, State1#state{stack={Tab,button,'...',ReplyTo}}};
empty({Tab, button, Button, ReplyTo}, State0) ->
    State1 = fetch(none,none,reset(State0,false)),
    {next_state, filling, State1#state{stack={Tab,button,Button,ReplyTo}}};
empty(Other, State) ->
    ?Log("empty -- unexpected erlimem_fsm event ~p in empty state~n", [Other]),
    {next_state, empty, State}.

reset(State,TailMode) -> 
    State#state{  bufTop = 0          
                , bufBot = 0          
                , dirtyTop = 99999999 
                , dirtyBot = 0        
                , dirtyCount = 0     
                , rawTop = 0         
                , rawBot = 0         
                , pfc=0              
                , stack=undefined    
                , tailMode=TailMode
                }. 

reply_stack(_SN,ReplyTo, #state{stack=undefined}=State0) ->
    State0#state{replyTo=ReplyTo};
reply_stack(SN,ReplyTo, #state{stack={Tab,button,_Button,RT}}=State0) ->
    State1 = gui_nop(Tab, #gres{state=SN},State0#state{stack=undefined,replyTo=RT}),
    State1#state{replyTo=ReplyTo}.

filling({Tab, button, '...', ReplyTo}, #state{dirtyCount=DC}=State0) when DC==0 ->
    State1 = reply_stack(filling, ReplyTo, State0),
    State2 = fetch_close(State1),
    State3 = clear_data(State2),
    State4 = fetch(skip,true,State3),
    State5 = gui_replace(Tab, 0, 0, #gres{state=tailing,loop='...'}, State4#state{tailMode=true}),
    {next_state, tailing, State5};
filling({Tab, button, '...', ReplyTo}, State0) ->
    State1 = gui_nop(Tab, #gres{state=filling,beep=true,message=?MustCommit},State0#state{replyTo=ReplyTo}),
    {next_state, filling, State1};
filling({Tab, button, '<', ReplyTo}, State0) ->
    State1 = reply_stack(filling, ReplyTo, State0),
    {next_state, filling, serve_bwd(Tab, filling, State1)};
filling({Tab, button, '<<', ReplyTo}, State0)  ->
    State1 = reply_stack(filling, ReplyTo, State0),
    {next_state, filling, serve_fbwd(Tab, filling, State1)};
filling({_Tab, button, _Button, ReplyTo}=Cmd, #state{bufBot=0}=State0) ->
    State1 = reply_stack(filling, ReplyTo, State0),
    {next_state, filling, State1#state{stack=Cmd}};
filling({_Tab, button, _Button, ReplyTo}=Cmd, #state{bufBot=B,rawBot=B}=State0) ->
    State1 = reply_stack(filling, ReplyTo, State0),
    {next_state, filling, State1#state{stack=Cmd}};
filling({_Tab, button, '>|...', ReplyTo}=Cmd, State0) ->
    State1 = reply_stack(filling, ReplyTo, State0),
    State2 = fetch(push,true,State1),
    {next_state, autofilling, State2#state{tailMode=true,stack=Cmd}};
filling({Tab, button, Target, ReplyTo}, State0) when is_integer(Target) ->
    State1 = reply_stack(filling, ReplyTo, State0),
    {next_state, filling, serve_target(Tab, filling, Target, State1)};
filling({Tab, button, '>', ReplyTo}, State0) ->
    State1 = reply_stack(filling, ReplyTo, State0),
    {next_state, filling, serve_fwd(Tab, filling, State1)};
filling({Tab, button, '>>', ReplyTo}, State0) ->
    State1 = reply_stack(filling, ReplyTo, State0),
    {next_state, filling, serve_ffwd(Tab, filling, State1)};
filling({_Tab, button, '>|', ReplyTo}=Cmd, State0) ->
    State1 = reply_stack(filling, ReplyTo, State0),
    State2 = fetch(push,none,State1),
    {next_state, autofilling, State2#state{stack=Cmd}};
filling({rows, {Recs,false}}, #state{bl=BL,stack={_,button,Target,_}}=State0) when is_integer(Target) ->
    State1 = append_data(filling, {Recs,false},State0),
    NewBufBot = State1#state.bufBot,
    State2 = if  
        (Target+BL >= NewBufBot) ->
            prefetch(filling,State1);
        true ->
            State1
    end,    
    {next_state, filling, State2};
filling({rows, {Recs,false}}, State0) ->
    State1 = append_data(filling, {Recs,false},State0),
    {next_state, filling, State1};
filling({rows, {Recs,true}}, State0) ->
    State1 = fetch_close(State0),
    State2 = append_data(completed, {Recs,true},State1),
    {next_state, completed, State2};
filling({Tab, update, ChangeList, ReplyTo}, State0) ->
    State1 = reply_stack(filling, ReplyTo, State0),
    State2 = update_data(Tab, filling, ChangeList, State1),
    {next_state, filling, State2};
filling({_Tab, button, 'x', ReplyTo}, State0) ->
    State1 = reply_stack(filling, ReplyTo, State0),
    State2 = fetch_close(State1),
    {stop, normal, State2};
filling(Other, State) ->
    ?Log("filling -- unexpected event ~p~n", [Other]),
    {next_state, filling, State}.


autofilling({Tab, button, '...', ReplyTo}, State0) ->
    State1 = gui_nop(Tab, #gres{state=autofilling,beep=true},State0#state{replyTo=ReplyTo}),
    {next_state, autofilling, State1};
autofilling({Tab, button, Target, ReplyTo}, State0) when is_integer(Target) ->
    State1 = reply_stack(autofilling, ReplyTo, State0),
    {next_state, autofilling, serve_target(Tab, autofilling, Target, State1)};
autofilling({Tab, button, '>|', ReplyTo}=Cmd, #state{tailMode=TailMode}=State0) ->
    if 
        (TailMode == true) ->
            State1 = gui_nop(Tab, #gres{state=autofilling,beep=true},State0#state{replyTo=ReplyTo}),
            {next_state, autofilling, State1};
        true ->
            State1 = reply_stack(autofilling, ReplyTo, State0),
            {next_state, autofilling, State1#state{stack=Cmd}}
    end;
autofilling({Tab, button, '>|...', ReplyTo}=Cmd, #state{tailMode=TailMode}=State0) ->
    if 
        (TailMode == false) ->
            State1 = gui_nop(Tab, #gres{state=autofilling,beep=true},State0#state{replyTo=ReplyTo}),
            {next_state, autofilling, State1};
        true ->
            State1 = reply_stack(autofilling, ReplyTo, State0),
            {next_state, autofilling, State1#state{stack=Cmd}}
    end;
autofilling({Tab, button, '>', ReplyTo}, State0) ->
    State1 = reply_stack(autofilling, ReplyTo, State0),
    {next_state, autofilling, serve_fwd(Tab, autofilling, State1)};
autofilling({Tab, button, '>>', ReplyTo}, State0) ->
    State1 = reply_stack(autofilling, ReplyTo, State0),
    {next_state, autofilling, serve_ffwd(Tab, autofilling, State1)};
autofilling({Tab, button, '<', ReplyTo}, State0) ->
    State1 = reply_stack(autofilling, ReplyTo, State0),
    {next_state, autofilling, serve_bwd(Tab, autofilling, State1)};
autofilling({Tab, button, '<<', ReplyTo}, State0) ->
    State1 = reply_stack(autofilling, ReplyTo, State0),
    {next_state, autofilling, serve_fbwd(Tab, autofilling, State1)};
autofilling({rows, {Recs,false}}, State0) ->
    State1 = append_data(autofilling,{Recs,false},State0),
    {next_state, autofilling, State1#state{pfc=0}};
autofilling({rows, {Recs,true}}, #state{tailMode=false}=State0) ->
    State1 = fetch_close(State0),
    State2 = append_data(completed,{Recs,true},State1),
    {next_state, completed, State2#state{pfc=0}};
autofilling({rows, {Recs,true}}, State0) ->
    State1= append_data(tailing,{Recs,true},State0),
    {next_state, tailing, State1#state{pfc=0}};
autofilling({Tab, update, ChangeList, ReplyTo}, State0) ->
    State1 = reply_stack(autofilling, ReplyTo, State0),
    State2 = update_data(Tab, autofilling, ChangeList, State1),
    {next_state, autofilling, State2};
autofilling({_Tab, button, 'x', ReplyTo}, State0) ->
    State1 = reply_stack(filling, ReplyTo, State0),
    State2 = fetch_close(State1),
    {stop, normal, State2};
autofilling(Other, State) ->
    ?Log("autofilling -- unexpected event ~p~n", [Other]),
    {next_state, autofilling, State}.

tailing({Tab, button, '...', ReplyTo}, #state{dirtyCount=DC}=State0) when DC==0->
    State1 = reply_stack(tailing, ReplyTo, State0),
    State2 = clear_data(State1),
    State3 = gui_replace(Tab, 0, 0, #gres{state=tailing, loop='>|...'},State2),
    {next_state, tailing, State3};
tailing({Tab, button, '...', ReplyTo}, State0) ->
    State1 = gui_nop(Tab, #gres{state=tailing,beep=true,message=?MustCommit},State0#state{replyTo=ReplyTo}),
    {next_state, tailing, State1};
tailing({Tab, button, '>|', ReplyTo}, State0) ->
    State1 = reply_stack(tailing, ReplyTo, State0),
    State2 = serve_last(Tab, tailing, State1),
    {next_state, tailing, State2};
tailing({Tab, button, '>|...', ReplyTo}=Cmd, #state{bl=BL,bufBot=BufBot,rawBot=RawBot}=State0) ->
    State1 = reply_stack(tailing, ReplyTo, State0),
    State2 = if
        (BufBot == RawBot) ->
            State1#state{stack=Cmd}; 
        (RawBot >= BufBot-BL) ->
            gui_append(Tab, RawBot+1, BufBot, #gres{state=tailing,loop='>|...'},State1); 
        (BufBot >= BL) ->
            gui_replace(Tab, BufBot-BL+1, BufBot, #gres{state=tailing,loop='>|...'},State1);
        true ->
            gui_replace(Tab, 1, BufBot, #gres{state=tailing,loop='>|...'},State1)
    end,
    {next_state, tailing, State2};
tailing({_Tab, button, _, ReplyTo}=Cmd, #state{bufBot=0}=State0) ->
    State1 = reply_stack(tailing, ReplyTo, State0),
    {next_state, tailing, State1#state{stack=Cmd}};
tailing({Tab, button, Target, ReplyTo}, State0) when is_integer(Target) ->
    State1 = reply_stack(tailing, ReplyTo, State0),
    {next_state, tailing, serve_target(Tab, tailing, Target,State1)};
tailing({Tab, button, '>', ReplyTo}, State0) ->
    State1 = reply_stack(tailing, ReplyTo, State0),
    {next_state, tailing, serve_fwd(Tab, tailing, State1)};
tailing({Tab, button, '>>', ReplyTo}, State0) ->
    State1 = reply_stack(tailing, ReplyTo, State0),
    {next_state, tailing, serve_ffwd(Tab, tailing, State1)};
tailing({Tab, button, '<', ReplyTo}, State0) ->
    State1 = reply_stack(tailing, ReplyTo, State0),
    {next_state, tailing, serve_bwd(Tab, tailing, State1)};
tailing({Tab, button, '<<', ReplyTo}, State0) ->
    State1 = reply_stack(tailing, ReplyTo, State0),
    {next_state, tailing, serve_fbwd(Tab, tailing, State1)};
tailing({rows, {Recs,tail}}, State0) ->
    State1 = append_data(tailing,{Recs,tail},State0),
    {next_state, tailing, State1#state{pfc=0}};
tailing({Tab, update, ChangeList, ReplyTo}, State0) ->
    State1 = reply_stack(tailing, ReplyTo, State0),
    State2 = update_data(Tab, tailing, ChangeList, State1),
    {next_state, tailing, State2};
tailing({_Tab, button, 'x', ReplyTo}, State0) ->
    State1 = reply_stack(tailing, ReplyTo, State0),    
    State2 = fetch_close(State1),
    {stop, normal, State2};
tailing(Other, State) ->
    ?Log("tailing -- unexpected event ~p in state~n~p~n", [Other,State]),
    {next_state, tailing, State}.

completed({Tab, button, '...', ReplyTo}, #state{dirtyCount=DC}=State0) when DC==0 ->
    State1 = reply_stack(completed, ReplyTo, State0),
    State2 = fetch(skip,true,State1),
    State3 = clear_data(State2),
    State4 = gui_replace(Tab, 0, 0, #gres{state=tailing,loop='>|...'},State3#state{tailMode=true}),
    {next_state, tailing, State4};
completed({Tab, button, '...', ReplyTo}, State0) ->
    State1 = gui_nop(Tab, #gres{state=completed,beep=true,message=?MustCommit},State0#state{replyTo=ReplyTo}),
    {next_state, completed, State1};
completed({Tab, button, '>|...', ReplyTo}, State0) ->
    State1 = reply_stack(completed, ReplyTo, State0),
    State2 = fetch(skip,true,State1),
    State3 = gui_nop(Tab, #gres{state=tailing,loop='>|...'},State2#state{tailMode=true}),
    {next_state, tailing, State3};
completed({Tab, button, _, ReplyTo}, #state{bufBot=0}=State0) ->
    State1 = reply_stack(completed, ReplyTo, State0),
    State1 = gui_nop(Tab, #gres{state=completed,beep=true},State1),
    {next_state, completed, State1};
completed({Tab, button, '>|', ReplyTo}, State0) ->
    State1 = reply_stack(completed, ReplyTo, State0),
    {next_state, completed, serve_last(Tab, completed, State1)};
completed({Tab, button, Target, ReplyTo}, State0) when is_integer(Target) ->
    State1 = reply_stack(completed, ReplyTo, State0),
    {next_state, completed, serve_target(Tab, completed, Target, State1)};
completed({Tab, button, '>', ReplyTo}, State0) ->
    State1 = reply_stack(completed, ReplyTo, State0),
    {next_state, completed, serve_fwd(Tab, completed, State1)};
completed({Tab, button, '>>', ReplyTo}, State0) ->
    State1 = reply_stack(completed, ReplyTo, State0),
    {next_state, completed, serve_ffwd(Tab, completed, State1)};
completed({Tab, button, '<', ReplyTo}, State0) ->
    State1 = reply_stack(completed, ReplyTo, State0),
    {next_state, completed, serve_bwd(Tab, completed, State1)};
completed({Tab, button, '<<', ReplyTo}, State0) ->
    State1 = reply_stack(completed, ReplyTo, State0),
    {next_state, completed, serve_fbwd(Tab, completed, State1)};
completed({rows, _}, State) ->
    {next_state, completed, State};
completed({Tab, update, ChangeList, ReplyTo}, State0) ->
    State1 = reply_stack(completed, ReplyTo, State0),
    State2 = update_data(Tab, completed, ChangeList, State1),
    {next_state, completed, State2};
completed({_Tab, button, 'x', ReplyTo}, State0) ->
    State1 = reply_stack(completed, ReplyTo, State0),
    {stop, normal, State1};
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

handle_event(_Event, empty, StateData) ->
    {no_reply, empty, StateData, infinity}.

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

gui_response(_Tab,ReplyTo,Gres) ->
    ReplyTo ! Gres.

gui_nop(Tab,GuiResult,#state{bufBot=BufBot,rawTop=RawTop,rawBot=RawBot,replyTo=ReplyTo}=State) -> 
    ?Log("gui_nop () ~p ~p~n", [GuiResult#gres.state, GuiResult#gres.loop]),
    gui_response(Tab,ReplyTo,GuiResult#gres{bufBot=BufBot,rawTop=RawTop,rawBot=RawBot}),
    State.

gui_replace(Tab,GuiTop,GuiBot,GuiResult,#state{bufBot=BufBot,replyTo=ReplyTo}=State) ->
    ?Log("gui_replace (~p ~p) ~p ~p~n", [GuiTop, GuiBot, GuiResult#gres.state, GuiResult#gres.loop]),
    Rows=gui_rows(Tab,GuiTop,GuiBot,State),
    gui_response(Tab,ReplyTo,GuiResult#gres{operation=rpl,rows=Rows,bufBot=BufBot,rawTop=GuiTop,rawBot=GuiBot}),
    State#state{rawTop=GuiTop,rawBot=GuiBot}.

gui_prepend(Tab,GuiTop,GuiBot,GuiResult,#state{gl=GL,bufBot=BufBot,rawTop=RawTop,rawBot=RawBot,replyTo=ReplyTo}=State) ->
    case RawTop-1 of
        GuiBot ->       ?Log("gui_prepend (~p ~p) ~p ~p~n", [GuiTop, GuiBot, GuiResult#gres.state, GuiResult#gres.loop]);
        GB ->           ?Log("gui_prepend (~p ~p) ~p ~p EXPECTED RawBot ~p~n", [GuiTop, GuiBot, GuiResult#gres.state, GuiResult#gres.loop, GB])
    end,
    NewRawBot = if 
        (RawBot >= GuiTop+GL) ->        GuiTop+GL-1;
        true ->                         RawBot
    end,
    Rows=gui_rows(Tab,GuiTop,GuiBot,State),
    gui_response(Tab,ReplyTo,GuiResult#gres{operation=prp,rows=Rows,bufBot=BufBot,rawTop=GuiTop,rawBot=NewRawBot}),
    State#state{rawTop=GuiTop,rawBot=NewRawBot}.

gui_append(Tab,GuiTop,GuiBot,GuiResult,#state{gl=GL,bufBot=BufBot,rawTop=RawTop,rawBot=RawBot,replyTo=ReplyTo}=State) ->
    case RawBot+1 of
        GuiTop ->   ?Log("gui_append (~p ~p) ~p ~p~n", [GuiTop, GuiBot, GuiResult#gres.state, GuiResult#gres.loop]);
        GT ->       ?Log("gui_append (~p ~p) ~p ~p EXPECTED RawTop ~p~n", [GuiTop, GuiBot, GuiResult#gres.state, GuiResult#gres.loop, GT])
    end,
    NewRawTop = if 
        (RawTop =< GuiBot-GL) ->        GuiBot-GL+1;
        true ->                         RawTop
    end,
    Rows=gui_rows(Tab,GuiTop,GuiBot,State),
    gui_response(Tab,ReplyTo,GuiResult#gres{operation=app,rows=Rows,bufBot=BufBot,rawTop=NewRawTop,rawBot=GuiBot}),
    State#state{rawTop=NewRawTop,rawBot=GuiBot}.

gui_rows(_Tab,Top,Bottom,#state{rowFun=RowFun,tableId=TableId}) ->
    % ?Log("RowFun ~p~n", [RowFun]),
    Rows = ets:select(TableId,[{'$1',[{'>=',{element,1,'$1'},Top},{'=<',{element,1,'$1'},Bottom}],['$_']}]),
    [gui_row_expand(R, TableId, RowFun) || R <- Rows].

gui_row_expand({I,Op,RK}, TableId, RowFun) ->
    Row = RowFun(RK),
    ets:insert(TableId, list_to_tuple([I, Op, RK | Row])),
    [integer_to_list(I),Op|Row];
gui_row_expand(FullRowTuple, _TableId, _RowFun) ->
    List = tuple_to_list(FullRowTuple),
    [integer_to_list(hd(List)),lists:nth(2,List)|lists:nthtail(3,List)].


    % lists:foldl(
    %     fun ([I,Op,RK],Rws) when is_integer(I) ->
    %             Row = F(RK),
    %             ets:insert(TableId, list_to_tuple([I, Op, RK | Row])),
    %             Rws ++ [[integer_to_list(I),Op|Row]];
    %         ([I,_Op,_RK|Rest],Rws) when is_integer(I) ->
    %             [[[integer_to_list(I),Op|Rest]]|Rws] 
    %     end
    %     , []
    %     , Rows
    % ).

clear_data(#state{tableId=TableId}=State) -> 
    ?Log("clear_data~n", []),
    true = ets:delete_all_objects(TableId),    
    State#state{  bufTop = 0          
                , bufBot = 0          
                , dirtyTop = 99999999 
                , dirtyBot = 0        
                , dirtyCount = 0     
                , rawTop = 0         
                , rawBot = 0         
                }. 

append_data(SN, {[],_Complete},State) -> 
    NewPfc=State#state.pfc-1,
    case NewPfc of
        0 ->    ?Log("append_data -- count ~p~n", [0]);
        _ ->    ?Log("append_data -- count ~p PendingFetchCount ~p~n", [0, NewPfc])
    end,
    serve_stack(SN, State#state{pfc=NewPfc});
append_data(SN, {Recs,_Complete},#state{tableId=TableId,bufBot=BufBot}=State) ->
    NewPfc=State#state.pfc-1,
    Count = length(Recs),
    NewBufBot = BufBot+Count,
    case NewPfc of
        0 ->    ?Log("append_data -- count ~p bufBottom ~p~n", [Count,NewBufBot]);
        _ ->    ?Log("append_data -- count ~p bufBottom ~p pfc ~p~n", [Count,NewBufBot,NewPfc])
    end,
    ets:insert(TableId, [list_to_tuple([I,nop|[R]])||{I,R}<-lists:zip(lists:seq(BufBot+1, NewBufBot), Recs)]),
    %% ToDo: insert into filter/sort table
    serve_stack(SN, State#state{pfc=NewPfc,bufBot=NewBufBot}).

update_data(Tab,SN,ChangeList,State0) ->
    State1 = update_data_rows(ChangeList,State0),
    gui_nop(Tab,#gres{state=SN},State1).  %% ToDo: return list of Ids chosen by fsm for inserts

update_data_rows([], State) -> State;
update_data_rows([Ch|ChangeList], State0) ->
    State1 = update_data_row(Ch, State0),
    update_data_rows(ChangeList, State1).

update_data_row([undefined,Fields], #state{tableId=TableId,bufBot=BufBot,dirtyTop=DT0,dirtyCount=DC0}=State0) ->
    Id = BufBot+1,          %% ToDo: map Fields to complete rows ("" for undefined fields)
    ets:insert(TableId, list_to_tuple([Id,ins,{}|Fields])),    
    State0#state{dirtyTop=int_min(DT0,Id),dirtyBot=Id,dirtyCount=DC0+1};
update_data_row([Id,Op,Fields], #state{tableId=TableId}=State0) when is_integer(Id) ->
    OldRow = ets:lookup(TableId, Id),
    {O,State1} = case {element(2,OldRow),Op} of
        {nop,nop} ->    {nop,State0};
        {nop,_} ->      DT = int_min(State0#state.dirtyTop,Id),
                        DB = int_max(State0#state.dirtyBot,Id),
                        DC = State0#state.dirtyCount+1,
                        {Op, State0#state{dirtyTop=DT,dirtyBot=DB,dirtyCount=DC}};
        {_,nop} ->      DC = State0#state.dirtyCount-1,
                        {nop,State0#state{dirtyCount=DC}};
        {ins,upd} ->    {ins,State0};
        {ins,ins} ->    {ins,State0};
        {ins,del} ->    DC = State0#state.dirtyCount-1,
                        {nop,State0#state{dirtyCount=DC}};
        {del,del} ->    {del,State0};
        {del,upd} ->    {upd,State0};
        {upd,upd} ->    {upd,State0};        
        {upd,del} ->    {del,State0}        
    end,
    ets:insert(TableId, list_to_tuple([Id,O,element(2,OldRow)|Fields])),
    State1.

int_min(A,B) when A=<B -> A;
int_min(_,B) -> B.

int_max(A,B) when A>=B -> A;
int_max(_,B) -> B.

serve_stack(_SN, #state{stack=undefined}=State) -> State;
serve_stack(SN, #state{stack={Tab,button,'>',RT}}=State0) ->
    State1 = serve_fwd(Tab,SN,State0#state{replyTo=RT}),
    State1#state{stack=undefined};
serve_stack(SN, #state{bufBot=BufBot,stack={Tab,button,Target,RT}}=State0) when is_integer(Target), (BufBot>=Target) ->
    State1 = serve_target(Tab,SN,Target,State0#state{replyTo=RT}),
    State1#state{stack=undefined};
serve_stack(completed, #state{stack={Tab,button,_,RT}}=State0) ->
    State1 = serve_last(Tab,complete,State0#state{replyTo=RT}),
    State1#state{stack=undefined};
serve_stack(tailing, #state{stack={Tab,button,'>|',RT}}=State0) ->
    State1 = serve_last(Tab,tailing,State0#state{replyTo=RT}),
    State1#state{stack=undefined};
serve_stack(tailing, #state{bl=BL,bufBot=BufBot,rawBot=RawBot,stack={Tab,button,'>|...',RT}}=State0) ->
    if
        (BufBot == RawBot) ->
            State0#state{stack={Tab,button,'>|...',RT}}; 
        (RawBot >= BufBot-BL) ->
            gui_append(Tab, RawBot+1, BufBot, #gres{state=tailing,loop='>|...'},State0#state{stack=undefined}); 
        (BufBot >= BL) ->
            gui_replace(Tab, BufBot-BL+1, BufBot, #gres{state=tailing,loop='>|...'},State0#state{stack=undefined});
        true ->
            gui_replace(Tab, 1, BufBot, #gres{state=tailing,loop='>|...'},State0#state{stack=undefined})
    end;
serve_stack(_ , State) -> State.

serve_empty(Tab,SN,true,State0) ->
    State1 = prefetch(SN,State0),          %% only when filling
    gui_nop(Tab,#gres{state=SN,beep=true},State1);
serve_empty(Tab,SN,false,State0) ->
    State1 = prefetch(SN,State0),          %% only when filling
    gui_nop(Tab,#gres{state=SN},State1).

serve_fwd(Tab,SN,#state{bl=BL,bufBot=BufBot,rawBot=RawBot}=State0) ->
    if
      (BufBot == 0) ->
          serve_empty(Tab,SN,false,State0);
      (BufBot =< BL) ->
          serve_first(Tab,SN,State0);
      (BufBot >= RawBot+BL+BL) ->
          gui_append(Tab,RawBot+1,RawBot+BL,#gres{state=SN},State0);
      (BufBot >= RawBot+BL) ->
          State1 = prefetch(SN,State0),          %% only when filling
          gui_append(Tab,RawBot+1,RawBot+BL,#gres{state=SN},State1);
      (BufBot == RawBot) ->
          State1 = prefetch(SN,State0),          %% only when filling
          gui_nop(Tab,#gres{state=SN},State1);
      true ->
          State1 = prefetch(SN,State0),          %% only when filling
          gui_append(Tab,RawBot+1,BufBot,#gres{state=SN},State1)
    end.

serve_ffwd(Tab,SN,#state{bl=BL,bufBot=BufBot,rawBot=RawBot,replyTo=ReplyTo}=State0) ->
    if
      (BufBot == 0) ->
          serve_empty(Tab,SN,false,State0);
      (BufBot =< BL) ->
          serve_fwd(Tab,SN,State0);
      (RawBot =< BL) ->
          serve_fwd(Tab,SN,State0);
      (RawBot+RawBot+BL =< BufBot) ->
          gui_replace(Tab,RawBot+RawBot-BL+1,RawBot+RawBot,#gres{state=SN},State0);
      (RawBot+RawBot =< BufBot) ->
          State1 = prefetch(SN,State0),          %% only when filling
          gui_replace(Tab,RawBot+RawBot-BL+1,RawBot+RawBot,#gres{state=SN},State1);
      true ->
          State1 = prefetch(SN,State0),          %% only when filling
          State1#state{stack={Tab,button,RawBot+RawBot,ReplyTo}}
    end.

serve_first(Tab,SN,#state{bl=BL,bufBot=BufBot,rawTop=RawTop,rawBot=RawBot}=State0) ->
    if
      (BufBot == 0) ->
          serve_empty(Tab,SN,false,State0);
      (BufBot >= BL+BL) and (RawTop == 1) and (RawBot >= BL) ->
          gui_nop(Tab,#gres{state=SN},State0);
      (BufBot >= BL+BL) and (RawTop > 1) and (RawTop =< BL+1) ->
          gui_prepend(Tab,1,RawTop-1,#gres{state=SN},State0);
      (BufBot >= BL+BL) ->
          gui_replace(Tab,1,BL,#gres{state=SN},State0);
      (BufBot >= BL) and (RawTop == 1) and (RawBot >= BL) ->
          State1 = prefetch(SN,State0),          %% only when filling
          gui_nop(Tab,#gres{state=SN},State1);
      (BufBot >= BL) and (RawTop =< BL) ->
          State1 = prefetch(SN,State0),          %% only when filling
          gui_replace(Tab,1,BL,#gres{state=SN},State1);
      (BufBot >= BL) ->
          State1 = prefetch(SN,State0),          %% only when filling
          gui_replace(Tab,1,BL,#gres{state=SN},State1);
      (BufBot < BL) and (RawTop == 1) and (RawBot == BufBot) ->
          State1 = prefetch(SN,State0),          %% only when filling
          gui_nop(Tab,#gres{state=SN},State1);
      (BufBot < BL) ->
          State1 = prefetch(SN,State0),          %% only when filling
          gui_replace(Tab,1,BufBot,#gres{state=SN},State1)
    end.

serve_last(Tab,SN,#state{bl=BL,bufBot=BufBot,rawTop=RawTop,rawBot=RawBot}=State0) ->
    State1 = prefetch(SN,State0),          %% only when filling
    if
      (BufBot == 0) ->
          serve_empty(Tab,SN,false,State1);
      (RawBot == BufBot) ->
          gui_nop(Tab,#gres{state=SN},State1);
      (BufBot =< BL) ->
          gui_replace(Tab,1,BufBot,#gres{state=SN},State1);
      (RawBot >= BufBot-BL+1) ->
          gui_append(Tab,RawBot+1,BufBot,#gres{state=SN},State1);
      true ->
          gui_replace(Tab,BufBot-BL+1,BufBot,#gres{state=SN},State1)
    end.

serve_bwd(Tab,SN,#state{bl=BL,bufBot=BufBot,rawTop=RawTop}=State0) ->
    if
      (BufBot == 0) ->
          serve_empty(Tab,SN,true,State0);
      (BufBot =< BL) ->
          serve_first(Tab,SN,State0);
      (RawTop > BL) ->
          gui_prepend(Tab,RawTop-BL,RawTop-1,#gres{state=SN},State0);
      true ->
          serve_first(Tab,SN,State0)
    end.

serve_fbwd(Tab,SN,#state{bl=BL,bufBot=BufBot,rawTop=RawTop}=State0) ->
    if
      (BufBot == 0) ->
          serve_empty(Tab,SN,true,State0);
      (RawTop >= BL+BL) and (RawTop div 2+BL == RawTop-1)  ->
          gui_prepend(Tab,RawTop div 2+1,RawTop div 2+BL,#gres{state=SN},State0);
      (RawTop >= BL+BL) ->
          gui_replace(Tab,RawTop div 2+1,RawTop div 2+BL,#gres{state=SN},State0);
      true ->
          serve_first(Tab,SN,State0)
    end.

serve_target(Tab,SN,Target,#state{bl=BL,bufBot=BufBot,rawTop=RawTop,rawBot=RawBot,replyTo=ReplyTo}=State0) ->
    if
      (BufBot == 0) and (SN == complete) ->
          serve_empty(Tab,SN,true,State0);
      (BufBot == 0) ->         
          %% no_data now, com back later 
          State1 = prefetch(SN,State0),          %% only when filling
          % gui_nop(Tab,#gres{state=SN,loop=Target},State1);
          State1#state{stack={Tab,button,Target,ReplyTo}};
      (Target =< 0) and (BufBot+Target > 0) ->
          %% target given relative to BufBot, retry with absolute target position 
          serve_target(Tab,SN,Target,State0);
      (Target =< 0)  ->
          serve_first(Tab,SN,State0);
      (Target =< BufBot) and (BufBot < BL) ->
          serve_first(Tab,SN,State0);
      (Target =< BufBot) and (RawTop > Target) ->
          %% serve block backward
          gui_replace(Tab,Target,Target+BL-1,#gres{state=SN},State0);
      (Target =< BufBot) and (RawBot < Target) and (Target > BufBot-BL-BL) ->
          %% serve block forward
          State1 = prefetch(SN,State0),          %% only when filling
          gui_replace(Tab,Target-BL+1,Target,#gres{state=SN},State1);
      (Target =< BufBot) and (RawBot < Target) ->
          %% serve block forward
          gui_replace(Tab,Target-BL+1,Target,#gres{state=SN},State0);
      (Target > BufBot) and (SN == completed) ->
          serve_last(Tab,SN,State0);
      (Target > BufBot) ->
          State1 = prefetch(SN,State0),          %% only when filling
          gui_nop(Tab,#gres{state=SN,loop=Target},State1);
      true ->
          %% target should be in GUI already
          gui_nop(Tab,#gres{state=SN,message="target row already in gui"},State0)
    end.

%% TESTS ------------------------------------------------------------------

-include_lib("eunit/include/eunit.hrl").

-define(button(__Button), gui_req(raw, button, __Button, self())).

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

        ?assertEqual(ok, insert_range(SKey, 11, def, 'Imem', IsSec)),

        Fsm = start(SKey,"fsm test",10,false,"select * from def;",self()),
        Fsm:?button('>'),
        receive_respond(Fsm),
        Fsm:?button('>'),
        receive_respond(Fsm),
        Fsm:?button('>'),
        receive_respond(Fsm),
        Fsm:?button('>>'),
        receive_respond(Fsm),
        Fsm:?button('>|...'),
        receive_respond(Fsm),
        Fsm:?button('<<'),
        receive_respond(Fsm),
        Fsm:?button('>|'),
        receive_respond(Fsm),
        Fsm:?button('<<'),
        receive_respond(Fsm),
        Fsm:?button('<<'),
        receive_respond(Fsm),
        Fsm:?button('<<'),
        receive_respond(Fsm),
        Fsm:?button('<<'),
        receive_respond(Fsm),
        Fsm:?button('<<'),
        receive_respond(Fsm),
        Fsm:?button('x'),

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
      #gres{loop=undefined} ->  ok;
      #gres{loop='>|...'} ->    ok;
      #gres{loop=Loop} ->
          timer:sleep(30),       
          Fsm:?button(Loop),
          receive_respond(Fsm)
    end,
    process_responses(Fsm,Responses).
