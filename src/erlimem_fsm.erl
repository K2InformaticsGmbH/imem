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
                , reply               %% reply pid
                }).

-record(bs,     { %% buffer state
                  top = 0             %% sequence number of top buffer row (or 0)
                , bottom = 0          %% sequence number of bottom buffer row (or 0)
                , dirtyTop = 99999999 %% record id of first dirty row in buffer
                , dirtyBottom = 0     %% record id of last dirty row in buffer
                , dirtyCount = 0      %% count of dirty rows in buffer
                }).

-record(gs,     { %% gui state
                  top = 0             %% sequence number of top gui row (or 0)
                , bottom = 0          %% sequence number of bottom gui row (or 0)
                }).

-record(state,  { %% fsm combined state
                  ctx                 %% statement & fetch context
                , tableId             %% ets table id 
                , stmtRef             %% statement reference
                , rowFun              %% RowFun
                , bl                  %% block_length (passed to init)
                , gl                  %% gui max length (row count) = gui_max(#state.bl)
                , bs = #bs{}          %% buffer state
                , gs = #gs{}          %% gui state
                , tailMode = false    %% tailMode scheduled
                , pfc=0               %% pending fetch count (in flight to DB or back)
                , reply               %% reply pid
                }).

-record(gres,   { %% response sent back to gui
                  operation           %% rep (replace) | app (append) | prp (prepend) | nop 
                , bufBottom=0         %% current buffer bottom (last sequence number)
                , guiTop=0            %% first row id in gui (smaller ones must be clipped away)
                , guiBottom=0         %% last row id in gui (larger ones must be clipped away)
                , guiFocus=undefined  %% make this row visible
                , message = []        %% help message
                , beep = false        %% alert with a beep if true
                , state = empty       %% color of buffer size indicator
                , loop = undefined    %% gui should come back with this command
                , rows = []           %% rows to show (append / prepend / merge)
                }).

-define(block_size,10).


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

start(SKey,Id,BL,IsSec,Sql,Reply) ->
    StmtResult = exec(SKey, Id, BL, IsSec, Sql),
    #stmtResult{stmtRef=StmtRef,rowFun=RowFun}=StmtResult,
    Ctx = #ctx{skey=SKey,id=Id,bl=BL,stmtRef=StmtRef,rowFun=RowFun,isSec=IsSec,reply=Reply}, 
	{ok,Pid} = gen_fsm:start(?MODULE,Ctx,[]),
    {?MODULE,Pid}.

start_link(SKey,Id,BL,IsSec,Sql,Reply) ->
    StmtResult = exec(SKey, Id, BL, IsSec, Sql),
    #stmtResult{stmtRef=StmtRef,rowFun=RowFun}=StmtResult,
    Ctx = #ctx{skey=SKey,id=Id,bl=BL,stmtRef=StmtRef,rowFun=RowFun,isSec=IsSec,reply=Reply}, 
	{ok, Pid} = gen_fsm:start_link(?MODULE,Ctx,[]),
    {?MODULE,Pid}.

stop(Pid) -> 
	gen_fsm:send_event(Pid,stop).

gui_req(Tab, button, Button, Reply, {?MODULE,Pid}) -> 
    ?Log("~p button -- ~p~n", [erlang:now(),Button]),
	gen_fsm:send_event(Pid,{Tab, button, Button, Reply});
gui_req(Tab, update, ChangeList, Reply, {?MODULE,Pid}) -> 
    ?Log("~p ChangeList -- ~p~n", [erlang:now(),ChangeList]),
    gen_fsm:send_event(Pid,{Tab, update, ChangeList, Reply}).

rows({Rows,Completed},{?MODULE,Pid}) -> 
    % ?Log("~p rows ~p ~p~n", [erlang:now(), length(Rows),Completed]),
    gen_fsm:send_event(Pid,{rows, {Rows,Completed}}).


exec(SKey, Id, BL, IsSec, Sql) ->
    ?Log("~p exec -- ~p: ~s~n", [erlang:now(), Id, Sql]),
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
    % ?Log("~p fetch (~p, ~p) ~p~n", [erlang:now(), FetchMode, TailMode, Result]),
    ok = Result,
    NewPfc=State#state.pfc +1,
    State#state{pfc=NewPfc}.

prefetch(filling,#state{pfc=0}=State) ->  fetch(none,none,State);
prefetch(filling,State) ->                State;
prefetch(_,State) ->                      State.

fetch_close(#state{stmtRef=StmtRef}=State) ->
    #ctx{skey=SKey,isSec=IsSec} = State#state.ctx, 
    Result = imem_statement:fetch_close(SKey, StmtRef, IsSec),
    ?Log("~p fetch_close -- ~p~n", [erlang:now(), Result]),
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

init(#ctx{bl=BL,reply=Reply,stmtRef=StmtRef,rowFun=RowFun}=Ctx) ->
    TableId=ets:new(results, [ordered_set, public]),
    {ok, empty, #state{bl=BL, reply=Reply, gl=gui_max(BL), ctx=Ctx, stmtRef=StmtRef, rowFun=RowFun, tableId=TableId}}.

%% --------------------------------------------------------------------
%% Func: SN/2	 non-synchronized event handling
%% Returns: {next_state, NextSN, NextStateData}          |
%%          {next_state, NextSN, NextStateData, Timeout} |
%%          {stop, Reason, NewStateData}
%% --------------------------------------------------------------------


empty({Tab, button, '>|', Reply}, State0) ->
    State1 = fetch(push,none,reset(State0,false)),
    State2 = gui_nop(Tab, #gres{state=autofilling,loop='>|'},State1#state{reply=Reply}),
    {next_state, autofilling, State2};
empty({Tab, button, '>|...', Reply}, State0) ->
    State1 = fetch(push,true,reset(State0,true)),
    State2 = gui_nop(Tab, #gres{state=autofilling,loop='>|...'},State1#state{reply=Reply}),
    {next_state, autofilling, State2};
empty({Tab, button, '...', Reply}, State0) ->
    State1 = fetch(skip,true,reset(State0,true)),
    State2 = gui_nop(Tab, #gres{state=tailing,loop='...'},State1#state{reply=Reply}),
    {next_state, tailing, State2};
empty({_Tab, button, 'x', Reply}, State0) ->
    State1 = fetch_close(State0),
    {stop, normal, State1#state{reply=Reply}};
empty({Tab, button, Button, Reply}, State0) ->
    State1 = fetch(none,none,reset(State0,false)),
    State2 = gui_nop(Tab, #gres{state=filling,loop=Button},State1#state{reply=Reply}),
    {next_state, filling, State2};
empty(Other, State) ->
    ?Log("~p empty -- unexpected erlimem_fsm event ~p in empty state~n", [erlang:now(),Other]),
    {next_state, empty, State}.

reset(State,TailMode) -> State#state{bs=#bs{}, gs=#gs{}, pfc=0, tailMode=TailMode}. 


filling({Tab, button, '<', Reply}, State) ->
    {next_state, filling, serve_bwd(Tab, filling, State#state{reply=Reply})};
filling({Tab, button, '<<', Reply}, State)  ->
    {next_state, filling, serve_fbwd(Tab, filling,State#state{reply=Reply})};
filling({Tab, button, Button, Reply}, #state{bs=#bs{bottom=0}}=State0) ->
    State1 = gui_nop(Tab, #gres{state=filling,loop=Button},State0#state{reply=Reply}),
    {next_state, filling, State1};
filling({Tab, button, Button, Reply}, #state{bs=#bs{bottom=B},gs=#gs{bottom=B}}=State0) ->
    State1 = gui_nop(Tab, #gres{state=filling,loop=Button},State0#state{reply=Reply}),
    {next_state, filling, State1};
filling({Tab, button, Target, Reply}, State) when is_integer(Target) ->
    {next_state, filling, serve_target(Tab, filling,Target,State#state{reply=Reply})};
filling({Tab, button, '>', Reply}, State) ->
    {next_state, filling, serve_fwd(Tab, filling,State#state{reply=Reply})};
filling({Tab, button, '>>', Reply}, State) ->
    {next_state, filling, serve_ffwd(Tab, filling,State#state{reply=Reply})};
filling({Tab, button, '>|', Reply}, State0) ->
    State1 = fetch(push,none,State0),
    State2 = gui_nop(Tab, #gres{state=autofilling,loop='>|'},State1#state{reply=Reply}),
    {next_state, autofilling, State2};
filling({Tab, button, '>|...', Reply}, State0) ->
    State1 = fetch(push,true,State0),
    State2 = gui_nop(Tab, #gres{state=autofilling,loop='>|...'},State1#state{tailMode=true,reply=Reply}),
    {next_state, autofilling, State2};
filling({Tab, button, '...', Reply}, State0) ->
    State1 = fetch_close(State0),
    State2 = clear_data(State1),
    State3 = fetch(skip,true,State2),
    State4 = gui_replace(Tab, 0, 0, #gres{state=tailing,loop='...'}, State3#state{tailMode=true,reply=Reply}),
    {next_state, tailing, State4};
filling({rows, {Recs,false}}, State0) ->
    State1 = append_data({Recs,false},State0),
    {next_state, filling, State1};
filling({rows, {Recs,true}}, State0) ->
    State1 = fetch_close(State0),
    State2 = append_data({Recs,true},State1),
    {next_state, completed, State2};
filling({Tab, update, ChangeList, Reply}, State0) ->
    State1 = update_data(Tab, filling, ChangeList, State0#state{reply=Reply}),
    {next_state, filling, State1};
filling({_Tab, button, 'x', Reply}, State0) ->
    State1 = fetch_close(State0),
    {stop, normal, State1#state{reply=Reply}};
filling(Other, State) ->
    ?Log("~p filling -- unexpected event ~p~n", [erlang:now(),Other]),
    {next_state, filling, State}.


autofilling({Tab, button, '...', Reply}, State0) ->
    State1 = gui_nop(Tab, #gres{state=autofilling,beep=true},State0#state{reply=Reply}),
    {next_state, autofilling, State1};
autofilling({Tab, button, Target, Reply}, State) when is_integer(Target) ->
    {next_state, autofilling, serve_target(Tab, autofilling,Target,State#state{reply=Reply})};
autofilling({Tab, button, '>|', Reply}, #state{bl=BL, bs=#bs{bottom=BufBottom},tailMode=TailMode}=State0) ->
    State1 = if 
      (TailMode == true) ->
          gui_nop(Tab, #gres{state=autofilling,beep=true},State0#state{reply=Reply});
      (BufBottom < BL) ->
          gui_nop(Tab, #gres{state=autofilling,loop='>|'},State0#state{reply=Reply});
      true ->
          gui_replace(Tab, BufBottom-BL+1,BufBottom,#gres{state=autofilling,loop='>|'},State0#state{reply=Reply})
    end,
    {next_state, autofilling, State1};
autofilling({Tab, button, '>|...', Reply}, #state{bl=BL, bs=#bs{bottom=BufBottom},tailMode=TailMode}=State0) ->
    State1 = if 
      (TailMode == false) ->
          gui_nop(Tab, #gres{state=autofilling,beep=true},State0#state{reply=Reply});
      (BufBottom < BL) ->
          gui_nop(Tab, #gres{state=autofilling,loop='>|...'},State0#state{reply=Reply});
      true ->
          gui_replace(Tab, BufBottom-BL+1,BufBottom,#gres{state=autofilling,loop='>|...'},State0#state{reply=Reply})
    end,
    {next_state, autofilling, State1};
autofilling({Tab, button, '>', Reply}, State) ->
    {next_state, autofilling, serve_fwd(Tab, autofilling,State#state{reply=Reply})};
autofilling({Tab, button, '>>', Reply}, State) ->
    {next_state, autofilling, serve_ffwd(Tab, autofilling,State#state{reply=Reply})};
autofilling({Tab, button, '<', Reply}, State) ->
    {next_state, autofilling, serve_bwd(Tab, autofilling,State#state{reply=Reply})};
autofilling({Tab, button, '<<', Reply}, State) ->
    {next_state, autofilling, serve_fbwd(Tab, autofilling,State#state{reply=Reply})};
autofilling({rows, {Recs,false}}, State0) ->
    State1 = append_data({Recs,false},State0),
    {next_state, autofilling, State1};
autofilling({rows, {Recs,true}}, #state{tailMode=true}=State0) ->
    State1 = append_data({Recs,true},State0),
    {next_state, tailing, State1};
autofilling({rows, {Recs,true}}, State0) ->
    State1 = append_data({Recs,true},State0),
    {next_state, completed, State1};
autofilling({Tab, update, ChangeList, Reply}, State0) ->
    State1 = update_data(Tab, autofilling, ChangeList, State0#state{reply=Reply}),
    {next_state, autofilling, State1};
autofilling({_Tab, button, 'x', Reply}, State0) ->
    State1 = fetch_close(State0),
    {stop, normal, State1#state{reply=Reply}};
autofilling(Other, State) ->
    ?Log("~p autofilling -- unexpected event ~p~n", [erlang:now(),Other]),
    {next_state, autofilling, State}.

tailing({Tab, button, '...', Reply}, State0) ->
    State1 = clear_data(State0),
    State2 = gui_replace(Tab, 0, 0, #gres{state=tailing, loop='>|...'},State1#state{reply=Reply}),
    {next_state, tailing, State2};
tailing({Tab, button, '>|', Reply}, State) ->
    {next_state, tailing, serve_last(Tab, tailing,State#state{reply=Reply})};
tailing({Tab, button, '>|...', Reply}, #state{bl=BL, bs=#bs{bottom=BufBottom}}=State0) ->
    State1 = gui_replace(Tab, BufBottom-BL+1, BufBottom, #gres{state=tailing,loop='>|...'},State0#state{reply=Reply}), 
    {next_state, tailing, State1};
tailing({Tab, button, _, Reply}, #state{bs=#bs{bottom=0}}=State0) ->
    State1 = gui_nop(Tab, #gres{state=completed,beep=true},State0#state{reply=Reply}),
    {next_state, tailing, State1};
tailing({Tab, button, Target, Reply}, State) when is_integer(Target) ->
    {next_state, tailing, serve_target(Tab, tailing,Target,State#state{reply=Reply})};
tailing({Tab, button, '>', Reply}, State) ->
    {next_state, tailing, serve_fwd(Tab, tailing,State#state{reply=Reply})};
tailing({Tab, button, '>>', Reply}, State) ->
    {next_state, tailing, serve_ffwd(Tab, tailing,State#state{reply=Reply})};
tailing({Tab, button, '<', Reply}, State) ->
    {next_state, tailing, serve_bwd(Tab, tailing,State#state{reply=Reply})};
tailing({Tab, button, '<<', Reply}, State) ->
    {next_state, tailing, serve_fbwd(Tab, tailing,State#state{reply=Reply})};
tailing({rows, {Recs,tail}}, State0) ->
    State1 = append_data({Recs,tail},State0),
    {next_state, tailing, State1};
tailing({Tab, update, ChangeList, Reply}, State0) ->
    State1 = update_data(Tab, tailing, ChangeList, State0#state{reply=Reply}),
    {next_state, tailing, State1};
tailing({_Tab, button, 'x', Reply}, State0) ->
    State1 = fetch_close(State0),
    {stop, normal, State1#state{reply=Reply}};
tailing(Other, State) ->
    ?Log("~p tailing -- unexpected event ~p in state~n~p~n", [erlang:now(),Other,State]),
    {next_state, tailing, State}.

completed({Tab, button, '...', Reply}, State0) ->
    State1 = fetch(skip,true,State0),
    State2 = clear_data(State1),
    State3 = gui_replace(Tab, 0, 0, #gres{state=tailing},State2#state{tailMode=true,reply=Reply}),
    {next_state, tailing, State3};
completed({Tab, button, '>|...', Reply}, State0) ->
    State1 = fetch(skip,true,State0),
    State2 = gui_nop(Tab, #gres{state=tailing,loop='>|...'},State1#state{tailMode=true,reply=Reply}),
    {next_state, tailing, State2};
completed({Tab, button, _, Reply}, #state{bs=#bs{bottom=0}}=State0) ->
    State1 = gui_nop(Tab, #gres{state=completed,beep=true},State0#state{reply=Reply}),
    {next_state, tailing, State1};
completed({Tab, button, '>|', Reply}, State) ->
    {next_state, completed, serve_last(Tab, completed,State#state{reply=Reply})};
completed({Tab, button, Target, Reply}, State) when is_integer(Target) ->
    {next_state, completed, serve_target(Tab, completed,Target,State#state{reply=Reply})};
completed({Tab, button, '>', Reply}, State) ->
    {next_state, completed, serve_fwd(Tab, completed,State#state{reply=Reply})};
completed({Tab, button, '>>', Reply}, State) ->
    {next_state, completed, serve_ffwd(Tab, completed,State#state{reply=Reply})};
completed({Tab, button, '<', Reply}, State) ->
    {next_state, completed, serve_bwd(Tab, completed,State#state{reply=Reply})};
completed({Tab, button, '<<', Reply}, State) ->
    {next_state, completed, serve_fbwd(Tab, completed,State#state{reply=Reply})};
completed({rows, _}, State) ->
    {next_state, completed, State};
completed({Tab, update, ChangeList, Reply}, State0) ->
    State1 = update_data(Tab, completed, ChangeList, State0#state{reply=Reply}),
    {next_state, completed, State1};
completed({_Tab, button, 'x', Reply}, State) ->
    {stop, normal, State#state{reply=Reply}};
completed(Other, State) ->
    ?Log("~p completed -- unexpected event ~p~n", [erlang:now(),Other]),
    {next_state, completed, State}.


%% --------------------------------------------------------------------
%% Func: SN/3	 synchronized event handling
%% Returns: {next_state, NextSN, NextStateData}            |
%%          {next_state, NextSN, NextStateData, Timeout}   |
%%          {reply, Reply, NextSN, NextStateData}          |
%%          {reply, Reply, NextSN, NextStateData, Timeout} |
%%          {stop, Reason, NewStateData}                          |
%%          {stop, Reason, Reply, NewStateData}
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
%%          {reply, Reply, NextSN, NextStateData}          |
%%          {reply, Reply, NextSN, NextStateData, Timeout} |
%%          {stop, Reason, NewStateData}                          |
%%          {stop, Reason, Reply, NewStateData}
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

gui_response(_Tab,Reply,Gres) ->
    Reply ! Gres.

gui_nop(Tab,GuiResult,#state{gs=GS,reply=Reply}=State) -> 
    ?Log("~p gui_nop () ~p ~p~n", [erlang:now(), GuiResult#gres.state, GuiResult#gres.loop]),
    #bs{bottom=BufBottom} = State#state.bs,
    #gs{top=GuiTop,bottom=GuiBottom}=GS,
    gui_response(Tab,Reply,GuiResult#gres{bufBottom=BufBottom,guiTop=GuiTop,guiBottom=GuiBottom}),
    State.

gui_replace(Tab,GuiTop,GuiBottom,GuiResult,#state{gs=GS,reply=Reply}=State) ->
    ?Log("~p gui_replace (~p ~p) ~p ~p~n", [erlang:now(), GuiTop, GuiBottom, GuiResult#gres.state, GuiResult#gres.loop]),
    #bs{bottom=BufBottom} = State#state.bs,
    Rows=gui_rows(Tab,GuiTop,GuiBottom,State),
    gui_response(Tab,Reply,GuiResult#gres{operation=rpl,rows=Rows,bufBottom=BufBottom,guiTop=GuiTop,guiBottom=GuiBottom}),
    State#state{gs=GS#gs{top=GuiTop,bottom=GuiBottom}}.

gui_prepend(Tab,GuiTop,GuiBottom,GuiResult,#state{gl=GL,gs=GS,reply=Reply}=State) ->
    case GS#gs.top-1 of
        GuiBottom ->    ?Log("~p gui_prepend (~p ~p) ~p ~p~n", [erlang:now(), GuiTop, GuiBottom, GuiResult#gres.state, GuiResult#gres.loop]);
        GB ->           ?Log("~p gui_prepend (~p ~p) ~p ~p EXPECTED GuiBottom ~p~n", [erlang:now(), GuiTop, GuiBottom, GuiResult#gres.state, GuiResult#gres.loop, GB])
    end,
    #bs{bottom=BufBottom} = State#state.bs,
    NewGuiBottom = if 
        (GS#gs.bottom >= GuiTop+GL) ->  GuiTop+GL-1;
        true ->                         GS#gs.bottom
    end,
    Rows=gui_rows(Tab,GuiTop,GuiBottom,State),
    gui_response(Tab,Reply,GuiResult#gres{operation=prp,rows=Rows,bufBottom=BufBottom,guiTop=GuiTop,guiBottom=NewGuiBottom}),
    State#state{gs=GS#gs{top=GuiTop,bottom=NewGuiBottom}}.

gui_append(Tab,GuiTop,GuiBottom,GuiResult,#state{gl=GL, gs=GS,reply=Reply}=State) ->
    case GS#gs.bottom+1 of
        GuiTop ->   ?Log("~p gui_append (~p ~p) ~p ~p~n", [erlang:now(), GuiTop, GuiBottom, GuiResult#gres.state, GuiResult#gres.loop]);
        GT ->       ?Log("~p gui_append (~p ~p) ~p ~p EXPECTED GuiTop ~p~n", [erlang:now(), GuiTop, GuiBottom, GuiResult#gres.state, GuiResult#gres.loop, GT])
    end,
    #bs{bottom=BufBottom} = State#state.bs,
    NewGuiTop = if 
        (GS#gs.top =< GuiBottom-GL) ->  GuiBottom-GL+1;
        true ->                         GS#gs.top
    end,
    Rows=gui_rows(Tab,GuiTop, GuiBottom, State),
    gui_response(Tab,Reply,GuiResult#gres{operation=app,rows=Rows,bufBottom=BufBottom,guiTop=NewGuiTop,guiBottom=GuiBottom}),
    State#state{gs=GS#gs{top=NewGuiTop,bottom=GuiBottom}}.

gui_rows(_Tab,Top,Bottom,#state{rowFun=RowFun,tableId=TableId}) ->
    % ?Log("~p RowFun ~p~n", [erlang:now(),RowFun]),
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
    ?Log("~p clear_data~n", [erlang:now()]),
    true = ets:delete_all_objects(TableId),    
    State#state{bs=#bs{}}.

append_data({[],_Complete},State) -> 
    NewPfc=State#state.pfc-1,
    case NewPfc of
        0 ->    ?Log("~p append_data -- count ~p~n", [erlang:now(), 0]);
        _ ->    ?Log("~p append_data -- count ~p PendingFetchCount ~p~n", [erlang:now(), 0, NewPfc])
    end,
    State#state{pfc=NewPfc};
append_data({Recs,_Complete},#state{tableId=TableId,bs=BS}=State) ->
    NewPfc=State#state.pfc-1,
    Count = length(Recs),
    Bottom = BS#bs.bottom,
    NewBottom = Bottom+Count,
    case NewPfc of
        0 ->    ?Log("~p append_data -- count ~p bottom ~p~n", [erlang:now(),Count,NewBottom]);
        _ ->    ?Log("~p append_data -- count ~p bottom ~p pfc ~p~n", [erlang:now(),Count,NewBottom, NewPfc])
    end,
    ets:insert(TableId, [list_to_tuple([I,nop|[R]])||{I,R}<-lists:zip(lists:seq(Bottom+1, NewBottom), Recs)]),
    State#state{pfc=NewPfc,bs=BS#bs{bottom=NewBottom}}.

update_data(Tab,SN,ChangeList,#state{bs=BS0,tableId=TableId}=State0) ->
    BS1 = update_data_rows(ChangeList, TableId, BS0),
    gui_nop(Tab,#gres{state=SN},State0#state{bs=BS1}).

update_data_rows([], _, BS) -> BS;
update_data_rows([Ch|ChangeList], TableId, BS0) ->
    BS1 = update_data_row(Ch, TableId, BS0),
    update_data_rows(ChangeList, TableId, BS1).

update_data_row([_,ins,Fields], TableId, #bs{bottom=BufBottom,dirtyTop=DT0,dirtyCount=DC0}=BS0) ->
    Id = BufBottom+1,
    ets:insert(TableId, list_to_tuple([Id,ins,{}|Fields])),
    BS0#bs{bottom=Id,dirtyTop=int_min(DT0,Id),dirtyBottom=Id,dirtyCount=DC0+1};
update_data_row([IdStr,Op,Fields], TableId, BS0) ->
    Id = list_to_integer(IdStr),
    OldRow = ets:lookup(TableId, Id),
    {O,BS1} = case {element(2,OldRow),Op} of
        {nop,nop} ->    {nop,BS0};
        {nop,_} ->      DT = int_min(BS0#bs.dirtyTop,Id),
                        DB = int_max(BS0#bs.dirtyBottom,Id),
                        DC = BS0#bs.dirtyCount + 1,
                        {Op, BS0#bs{dirtyTop=DT,dirtyBottom=DB,dirtyCount=DC}};
        {_,nop} ->      DC = BS0#bs.dirtyCount - 1,
                        {nop,BS0#bs{dirtyCount=DC}};
        {ins,upd} ->    {ins,BS0};
        {ins,ins} ->    {ins,BS0};
        {ins,del} ->    DC = BS0#bs.dirtyCount - 1,
                        {nop,BS0#bs{dirtyCount=DC}};
        {del,del} ->    {del,BS0};
        {del,upd} ->    {upd,BS0};
        {upd,upd} ->    {upd,BS0};        
        {upd,del} ->    {del,BS0}        
    end,
    ets:insert(TableId, list_to_tuple([Id,O,element(2,OldRow)|Fields])),
    BS1.

int_min(A,B) when A=<B -> A;
int_min(_,B) -> B.

int_max(A,B) when A>=B -> A;
int_max(_,B) -> B.


serve_empty(Tab,SN,true,State0) ->
    State1 = prefetch(SN,State0),          %% only when filling
    gui_nop(Tab,#gres{state=SN,beep=true},State1);
serve_empty(Tab,SN,false,State0) ->
    State1 = prefetch(SN,State0),          %% only when filling
    gui_nop(Tab,#gres{state=SN},State1).

serve_fwd(Tab,SN,#state{bl=BL,bs=#bs{bottom=BufBottom},gs=#gs{bottom=GuiBottom}}=State0) ->
    if
      (BufBottom == 0) ->
          serve_empty(Tab,SN,false,State0);
      (BufBottom =< BL) ->
          serve_first(Tab,SN,State0);
      (GuiBottom+BL+BL =< BufBottom) ->
          gui_append(Tab,GuiBottom+1,GuiBottom+BL,#gres{state=SN},State0);
      true ->
          State1 = prefetch(SN,State0),          %% only when filling
          gui_append(Tab,BufBottom-BL+1,BufBottom,#gres{state=SN},State1)
    end.

serve_ffwd(Tab,SN,#state{bl=BL,bs=#bs{bottom=BufBottom},gs=#gs{bottom=GuiBottom}}=State0) ->
    if
      (BufBottom == 0) ->
          serve_empty(Tab,SN,false,State0);
      (BufBottom =< BL) ->
          serve_fwd(Tab,SN,State0);
      (GuiBottom =< BL) ->
          serve_fwd(Tab,SN,State0);
      (GuiBottom+GuiBottom+BL =< BufBottom) ->
          gui_replace(Tab,GuiBottom+GuiBottom-BL+1,GuiBottom+GuiBottom,#gres{state=SN},State0);
      (GuiBottom+GuiBottom =< BufBottom) ->
          State1 = prefetch(SN,State0),          %% only when filling
          gui_replace(Tab,GuiBottom+GuiBottom-BL+1,GuiBottom+GuiBottom,#gres{state=SN},State1);
      true ->
          State1 = prefetch(SN,State0),          %% only when filling
          gui_nop(Tab,#gres{state=SN,loop=GuiBottom+GuiBottom},State1)
    end.

serve_first(Tab,SN,#state{bl=BL,bs=#bs{bottom=BufBottom},gs=#gs{top=GuiTop,bottom=GuiBottom}}=State0) ->
    if
      (BufBottom == 0) ->
          serve_empty(Tab,SN,false,State0);
      (BufBottom >= BL+BL) and (GuiTop == 1) and (GuiBottom >= BL) ->
          gui_nop(Tab,#gres{state=SN},State0);
      (BufBottom >= BL+BL) and (GuiTop > 1) and (GuiTop =< BL+1) ->
          gui_prepend(Tab,1,GuiTop-1,#gres{state=SN},State0);
      (BufBottom >= BL+BL) ->
          gui_replace(Tab,1,BL,#gres{state=SN},State0);
      (BufBottom >= BL) and (GuiTop == 1) and (GuiBottom >= BL) ->
          State1 = prefetch(SN,State0),          %% only when filling
          gui_nop(Tab,#gres{state=SN},State1);
      (BufBottom >= BL) and (GuiTop =< BL) ->
          State1 = prefetch(SN,State0),          %% only when filling
          gui_replace(Tab,1,BL,#gres{state=SN},State1);
      (BufBottom >= BL) ->
          State1 = prefetch(SN,State0),          %% only when filling
          gui_replace(Tab,1,BL,#gres{state=SN},State1);
      (BufBottom < BL) and (GuiTop == 1) and (GuiBottom == BufBottom) ->
          State1 = prefetch(SN,State0),          %% only when filling
          gui_nop(Tab,#gres{state=SN},State1);
      (BufBottom < BL) ->
          State1 = prefetch(SN,State0),          %% only when filling
          gui_replace(Tab,1,BufBottom,#gres{state=SN},State1)
    end.

serve_last(Tab,SN,#state{bl=BL,bs=#bs{bottom=BufBottom},gs=#gs{top=GuiTop,bottom=GuiBottom}}=State0) ->
    State1 = prefetch(SN,State0),          %% only when filling
    if
      (BufBottom == 0) ->
          serve_empty(Tab,SN,false,State0);
      (BufBottom =< BL) and (GuiTop == 1) and (GuiBottom == BufBottom) ->
          gui_nop(Tab,#gres{state=SN},State1);
      (BufBottom =< BL) ->
          gui_replace(Tab,1,BufBottom,#gres{state=SN},State1);
      (GuiBottom == BufBottom) and (GuiTop == BufBottom-BL+1) ->
          gui_nop(Tab,#gres{state=SN},State1);
      (GuiBottom >= BufBottom-BL+1) ->
          gui_append(Tab,GuiBottom+1,BufBottom,#gres{state=SN},State1);
      true ->
          gui_replace(Tab,BufBottom-BL+1,BufBottom,#gres{state=SN},State1)
    end.

serve_bwd(Tab,SN,#state{bl=BL,bs=#bs{bottom=BufBottom},gs=#gs{top=GuiTop}}=State0) ->
    if
      (BufBottom == 0) ->
          serve_empty(Tab,SN,true,State0);
      (BufBottom =< BL) ->
          serve_first(Tab,SN,State0);
      (GuiTop > BL) ->
          gui_prepend(Tab,GuiTop-BL,GuiTop-1,#gres{state=SN},State0);
      true ->
          serve_first(Tab,SN,State0)
    end.

serve_fbwd(Tab,SN,#state{bl=BL,bs=#bs{bottom=BufBottom},gs=#gs{top=GuiTop}}=State0) ->
    if
      (BufBottom == 0) ->
          serve_empty(Tab,SN,true,State0);
      (GuiTop >= BL+BL) and (GuiTop div 2+BL == GuiTop-1)  ->
          gui_prepend(Tab,GuiTop div 2+1,GuiTop div 2+BL,#gres{state=SN},State0);
      (GuiTop >= BL+BL) ->
          gui_replace(Tab,GuiTop div 2+1,GuiTop div 2+BL,#gres{state=SN},State0);
      true ->
          serve_first(Tab,SN,State0)
    end.

serve_target(Tab,SN,Target,#state{bl=BL, bs=#bs{bottom=BufBottom}, gs=#gs{top=GuiTop,bottom=GuiBottom}}=State0) ->
    if
      (BufBottom == 0) and (SN == complete) ->
          serve_empty(Tab,SN,true,State0);
      (BufBottom == 0) ->         
          %% no_data now, com back later 
          State1 = prefetch(SN,State0),          %% only when filling
          gui_nop(Tab,#gres{state=SN,loop=Target},State1);
      (Target =< 0) and (BufBottom+Target > 0) ->
          %% target given relative to BufBottom, retry with absolute target position 
          serve_target(Tab,SN,Target,State0);
      (Target =< 0)  ->
          serve_first(Tab,SN,State0);
      (Target =< BufBottom) and (BufBottom < BL) ->
          serve_first(Tab,SN,State0);
      (Target =< BufBottom) and (GuiTop > Target) ->
          %% serve block backward
          gui_replace(Tab,Target,Target+BL-1,#gres{state=SN},State0);
      (Target =< BufBottom) and (GuiBottom < Target) and (Target > BufBottom-BL-BL) ->
          %% serve block forward
          State1 = prefetch(SN,State0),          %% only when filling
          gui_replace(Tab,Target-BL+1,Target,#gres{state=SN},State1);
      (Target =< BufBottom) and (GuiBottom < Target) ->
          %% serve block forward
          gui_replace(Tab,Target-BL+1,Target,#gres{state=SN},State0);
      (Target > BufBottom) and (SN == completed) ->
          serve_last(Tab,SN,State0);
      (Target > BufBottom) ->
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

        ?assertEqual(ok, insert_range(SKey, 100, def, 'Imem', IsSec)),

        Fsm = start(SKey,"fsm test",10,false,"select * from def;",self()),
        Fsm:?button('>>'),
        receive_respond(Fsm),
        Fsm:?button('>>'),
        receive_respond(Fsm),
        Fsm:?button('>>'),
        receive_respond(Fsm),
        Fsm:?button('>>'),
        receive_respond(Fsm),
        Fsm:?button('>|...'),
        receive_respond(Fsm),
        Fsm:?button('>|'),
        receive_respond(Fsm),
        Fsm:?button('<'),
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
            R ->    % ?Log("~p got:~n~p~n", [erlang:now(),R]),
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
          timer:sleep(1),       
          Fsm:?button(Loop),
          receive_respond(Fsm)
    end,
    process_responses(Fsm,Responses).
