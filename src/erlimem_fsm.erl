-module(erlimem_fsm). 

-behaviour(gen_fsm).

-include("imem_seco.hrl").
-include("imem_sql.hrl").


-record(ctx,    { %% session context
                  skey
                , id
                , bl                  %% block length
                , isSec               %% secure mode or not
                , sql                 %% SQL used for statement
                , sr                  %% statement response
                , reply               %% reply pid
                }).

-record(bs,     { %% buffer state
                  top = 0             %% sequence number of top buffer row (or 0)
                , bottom = 0          %% sequence number of bottom buffer row (or 0)
                , buf = []            %% buffer content (test only)
                }).

-record(gs,     { %% gui state
                  top = 0             %% sequence number of top gui row (or 0)
                , bottom = 0          %% sequence number of bottom gui row (or 0)
                }).

-record(state,  { %% fsm combined state
                  ctx                 %% statement & fetch context
                , fsm                 %% own pid (for tests)
                , bl=10               %% block_length
                , tailMode = false   %% tailMode scheduled
                , stack = []          %% pending command stack    
                , bs = #bs{}          %% buffer state
                , gs = #gs{}          %% gui state
                }).

-record(gres,   { %% response sent back to gui
                  rows = []           %% rows to show (append / prepend / merge)
                , bufBottom           %% current buffer bottom (last sequence number)
                , message = []        %% help message
                , beep = false        %% alert with a beep if true
                , signal = empty      %% color of buffer size indicator
                , loop = undefined    %% gui should come back with this command
                }).

-define(block_size,10).


%% --------------------------------------------------------------------
%% Include files
%% --------------------------------------------------------------------

%% --------------------------------------------------------------------
%% External exports

-export([ start/1
        , start_link/1
        , stop/1
        ]).

-export([ rows/2        %% incoming rows from database
        , button/2      %% browse_data '>' '>>' '>|' '>|...' '<' '<<' '...'
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

start(Ctx) -> 
	gen_fsm:start(?MODULE,Ctx,[]).

start_link(Ctx) -> 
	gen_fsm:start_link(?MODULE,Ctx,[]).

stop(Pid) -> 
	gen_fsm:send_event(Pid,stop).

button(Pid, Button) -> 
  ?Log("~p button -- ~p~n", [erlang:now(),Button]),
	gen_fsm:send_event(Pid,{button, Button}).

rows(Pid, {Rows,Completed}) -> 
  ?Log("~p rows ~p ~p~n", [erlang:now(), length(Rows),Completed]),
  gen_fsm:send_event(Pid,{rows, {Rows,Completed}}).


%% ====================================================================
%% Server functions
%% ====================================================================
%% --------------------------------------------------------------------
%% Func: init/1
%% Returns: {ok, StateName, StateData}          |
%%          {ok, StateName, StateData, Timeout} |
%%          ignore                              |
%%          {stop, StopReason}
%% --------------------------------------------------------------------

init(#ctx{skey=SKey,id=Id,bl=BL,isSec=IsSec,sql=Sql}=Ctx) ->
    {ok, empty, #state{bl=BL, ctx=Ctx#ctx{sr=exec(SKey, Id, BL, IsSec, Sql)}, fsm=self()}}.

%% --------------------------------------------------------------------
%% Func: StateName/2	 non-synchronized event handling
%% Returns: {next_state, NextStateName, NextStateData}          |
%%          {next_state, NextStateName, NextStateData, Timeout} |
%%          {stop, Reason, NewStateData}
%% --------------------------------------------------------------------

empty({button, '>|'}, #state{bl=BL,ctx=Ctx,fsm=Fsm}=State0) ->
    fetch(push,none,State0),
    gui_nop(#gres{signal=autofilling,loop=0},State0),
    {next_state, autofilling, #state{bl=BL,ctx=Ctx,fsm=Fsm, tailMode=false}};
empty({button, '>|...'}, #state{bl=BL,ctx=Ctx,fsm=Fsm}=State0) ->
    fetch(push,true,State0),
    gui_nop(#gres{signal=autofilling,loop=0},State0),
    {next_state, autofilling, #state{bl=BL,ctx=Ctx,fsm=Fsm, tailMode=true}};
empty({button, '...'}, #state{bl=BL,ctx=Ctx,fsm=Fsm}=State0) ->
    fetch(skip,true,State0),
    gui_nop(#gres{signal=tailing,loop=0},State0),
    {next_state, tailing, #state{bl=BL,ctx=Ctx,fsm=Fsm, tailMode=true}};
empty({button, Target}, #state{bl=BL,ctx=Ctx,fsm=Fsm}=State0) when is_integer(Target) ->
    fetch(none,none,State0),
    {next_state, filling, #state{bl=BL,ctx=Ctx,fsm=Fsm, stack=[Target]}};
empty({button, _}, #state{bl=BL,ctx=Ctx,fsm=Fsm}=State0) ->
    fetch(none,none,State0),
    {next_state, filling, #state{bl=BL,ctx=Ctx,fsm=Fsm, stack=['>']}};
empty({button,close}, State) ->
    fetch_close(State),
    {stop, normal, State};
empty({rows, _}, State) ->
    {next_state, empty, State};
empty(Other, State) ->
    ?Log("~p empty -- unexpected event ~p~n", [erlang:now(),Other]),
    {next_state, empty, State}.


filling({button, '<'}, #state{gs=#gs{top=0}}=State) ->
    gui_nop(#gres{signal=filling,beep=true},State),
    {next_state, filling, State};
filling({button, '<<'}, #state{gs=#gs{top=0}}=State) ->
    gui_nop(#gres{signal=filling,beep=true},State),
    {next_state, filling, State};
filling({button, '<'}, #state{bl=BL, gs=#gs{top=GuiTop}}=State0) ->
    State1 = if
      GuiTop > BL ->
        gui_replace(GuiTop-BL,GuiTop-1,#gres{signal=filling},State0);
      true ->
        gui_replace(1,BL,#gres{signal=filling},State0)
    end,
    {next_state, filling, State1};
filling({button, '<<'}, #state{bl=BL, gs=#gs{top=GuiTop}}=State0)  ->
    State1 = if 
      (GuiTop > BL+BL) ->
          gui_replace(GuiTop div 2-BL,GuiTop div 2-1,#gres{signal=filling},State0);
      true ->
          gui_replace(1,BL,#gres{signal=filling},State0)
    end,
    {next_state, filling, State1};
filling({button, Target}, #state{bl=BL, bs=#bs{bottom=BufBottom}, gs=#gs{top=GuiTop,bottom=GuiBottom}}=State0) when is_integer(Target) ->
    State1 = if
      (BufBottom == 0) ->
          State0#state{stack=[Target]};
      (Target =< 0-BufBottom) ->
          gui_replace(1,BL,#gres{signal=filling},State0);
      (Target >= 0-BL) and (Target < 0)  ->
          fetch(none,none,State0),
          gui_replace(BufBottom+Target-BL+1,BufBottom+Target,#gres{signal=filling},State0);
      (Target < 0) ->
          gui_replace(BufBottom+Target-BL+1,BufBottom+Target,#gres{signal=filling},State0);
      (Target == 0) ->
          fetch(none,none,State0),
          gui_replace(BufBottom+Target-BL+1,BufBottom+Target,#gres{signal=filling},State0);
      (Target < BL) and (BufBottom < BL+BL) ->
          fetch(none,none,State0),
          gui_replace(1,BL,#gres{signal=filling},State0);
      (Target < BL) ->
          gui_replace(1,BL,#gres{signal=filling},State0);
      (Target >= BufBottom-BL) and (Target =< BufBottom) ->
          fetch(none,none,State0),
          gui_replace(Target-BL+1,Target,#gres{signal=filling},State0);
      (Target > BufBottom) ->
          fetch(none,none,State0),
          gui_replace(BufBottom-BL+1,BufBottom,#gres{signal=filling,loop=Target},State0);
      (Target =< BufBottom) and (GuiTop > Target) ->
          gui_replace(Target,Target+BL-1,#gres{signal=filling},State0);
      (Target =< BufBottom) and (GuiBottom < Target) ->
          gui_replace(Target-BL+1,Target,#gres{signal=filling},State0);
      true ->
          gui_nop(#gres{signal=filling,message="target row already in gui"},State0),
          State0
    end,
    {next_state, filling, State1};
filling({button, Button}, #state{bs=#bs{bottom=0}}=State0) ->
    {next_state, filling, State0#state{stack=[Button]}};
filling({button, Button}, #state{bs=#bs{bottom=B},gs=#gs{bottom=B}}=State0) ->
    {next_state, filling, State0#state{stack=[Button]}};
filling({button, '>'}, #state{bl=BL,bs=#bs{bottom=BufBottom},gs=#gs{bottom=GuiBottom}}=State0) ->
    State1 = if
      (GuiBottom+BL+BL =< BufBottom) ->
          gui_replace(GuiBottom+1,GuiBottom+BL,#gres{signal=filling},State0);
      (GuiBottom+BL =< BufBottom) ->
          fetch(none,none,State0),
          gui_replace(GuiBottom+1,GuiBottom+BL,#gres{signal=filling},State0);
      (GuiBottom < BufBottom) ->
          fetch(none,none,State0),
          gui_replace(BufBottom-BL+1,BufBottom,#gres{signal=filling},State0);
      true ->
          gui_nop(#gres{signal=filling,message="unexpected state for '>'"},State0),
          State0        
    end,
    {next_state, filling, State1};
filling({button, '>>'}, #state{bl=BL,bs=#bs{bottom=BufBottom},gs=#gs{bottom=GuiBottom}}=State0) ->
    State1 = if
      (GuiBottom+GuiBottom+BL =< BufBottom) ->
          gui_replace(GuiBottom+GuiBottom-BL+1, GuiBottom+GuiBottom,#gres{signal=filling},State0);
      (GuiBottom+GuiBottom =< BufBottom) ->
          fetch(none,none,State0),
          gui_replace(GuiBottom+GuiBottom-BL+1, GuiBottom+GuiBottom,#gres{signal=filling},State0);
      true ->
          fetch(none,none,State0),
          gui_nop(#gres{signal=filling,loop=GuiBottom+GuiBottom},State0),
          State0
    end,
    {next_state, filling, State1};
filling({button, '>|'}, State) ->
    fetch(push,none,State),
    gui_nop(#gres{signal=autofilling,loop=0},State),
    {next_state, autofilling, State};
filling({button, '>|...'}, State0) ->
    fetch(push,true,State0),
    gui_nop(#gres{signal=autofilling,loop=0},State0),
    {next_state, autofilling, State0#state{tailMode=true}};
filling({button, '...'}, #state{}=State0) ->
    fetch_close(State0),
    fetch(skip,true,State0),
    gui_nop(#gres{signal=tailing,loop=0},State0),
    {next_state, tailing, State0#state{tailMode=true}};

filling({rows, {Recs,false}}, #state{bl=BL, bs=#bs{bottom=BufBottom}, stack=[Target]}=State0) when is_integer(Target) ->
    State1 = if 
        (Target =< BufBottom-BL) ->
            append_data({Recs,false},State0#state{stack=[]});
        true ->
            fetch(none,none,State0),
            append_data({Recs,false},State0)
    end,
    {next_state, filling, State1};
filling({rows, {Recs,false}}, #state{stack=[Button]}=State0) ->
    State1 = append_data({Recs,false},State0#state{stack=[]}),
    filling({button, Button}, State1);
filling({rows, {Recs,false}}, State0) ->
    State1 = append_data({Recs,false},State0),
    {next_state, filling, State1};
filling({rows, {Recs,true}}, #state{stack=[Button]}=State0) ->
    fetch_close(State0),
    State1 = append_data({Recs,true},State0#state{stack=[]}),
    completed({button, Button}, State1);
filling({rows, {Recs,true}}, State0) ->
    fetch_close(State0),
    State1 = append_data({Recs,true},State0),
    {next_state, completed, State1};
filling({button,close}, State) ->
    fetch_close(State),
    {stop, normal, State};
filling(Other, State) ->
    ?Log("~p filling -- unexpected event ~p~n", [erlang:now(),Other]),
    {next_state, filling, State}.


autofilling({button, '...'}, State0) ->
    State1 = gui_nop(#gres{signal=autofilling,beep=true,loop=0},State0),
    {next_state, autofilling, State1};
autofilling({button, Target}, #state{bl=BL, bs=#bs{bottom=BufBottom}, gs=#gs{top=GuiTop, bottom=GuiBottom}}=State0) when is_integer(Target) ->
    State1 = if
      (BufBottom == 0) ->
          State0#state{stack=[Target]};
      (Target =< 0-BufBottom) ->
          gui_replace(1,BL,#gres{signal=autofilling},State0);
      (Target < 0) ->
          gui_replace(BufBottom+Target-BL+1,BufBottom+Target,#gres{signal=autofilling},State0);
      (Target == 0) ->
          gui_replace(BufBottom-BL+1,BufBottom,#gres{signal=autofilling,loop=0},State0);
      (Target < BL) ->
          gui_replace(1,BL,#gres{signal=autofilling},State0);
      (Target =< BufBottom) and (GuiTop > Target) ->
          gui_replace(Target,Target+BL-1,#gres{signal=autofilling},State0);
      (Target =< BufBottom) and (GuiBottom < Target) ->
          gui_replace(Target-BL+1,Target,#gres{signal=autofilling},State0);
      (Target > BufBottom) ->
          gui_replace(BufBottom-BL+1,BufBottom,#gres{signal=autofilling,loop=Target},State0);
      true ->
          gui_nop(#gres{signal=autofilling,message="target row already in gui"},State0),
          State0
    end,
    {next_state, autofilling, State1};
autofilling({button, '>|'}, #state{tailMode=TailMode}=State0) ->
    State1 = case TailMode of
      true ->
          gui_nop(#gres{signal=autofilling,beep=true,loop=0},State0);
      false ->
          gui_nop(#gres{signal=autofilling,loop=0},State0)
    end,
    {next_state, autofilling, State1};
autofilling({button, '>|...'}, #state{tailMode=TailMode}=State0) ->
    State1 = case TailMode of
      true ->
          gui_nop(#gres{signal=autofilling,loop=0},State0);
      false ->
          gui_nop(#gres{signal=autofilling,beep=true,loop=0},State0)
    end,
    {next_state, autofilling, State1};
autofilling({button, '>'}, #state{bl=BL,bs=#bs{bottom=BufBottom},gs=#gs{bottom=GuiBottom}}=State0) ->
    State1 = if
      (GuiBottom+BL =< BufBottom) ->
          gui_replace(GuiBottom+1,GuiBottom+BL,#gres{signal=autofilling},State0);
      true ->
          gui_replace(BufBottom-BL+1,BufBottom,#gres{signal=autofilling},State0)
    end,
    {next_state, autofilling, State1};
autofilling({button, '>>'}, #state{bl=BL,bs=#bs{bottom=BufBottom},gs=#gs{bottom=GuiBottom}}=State0) ->
    State1 = if
      (GuiBottom+GuiBottom =< BufBottom) ->
          gui_replace(GuiBottom+GuiBottom-BL+1,GuiBottom+GuiBottom,#gres{signal=autofilling},State0);
      true ->
          gui_replace(BufBottom-BL+1,BufBottom,#gres{signal=autofilling},State0)
    end,
    {next_state, autofilling, State1};
autofilling({button, '<'}, #state{bl=BL,bs=#bs{bottom=BufBottom},gs=#gs{top=GuiTop}}=State0) ->
    State1 = if
      (GuiTop > BL) ->
          gui_replace(GuiTop+BL,GuiTop-1,#gres{signal=autofilling},State0);
      true ->
          gui_replace(1,BufBottom,#gres{signal=autofilling},State0)
    end,
    {next_state, autofilling, State1};
autofilling({button, '<<'}, #state{bl=BL,gs=#gs{top=GuiTop}}=State0) ->
    State1 = if
      (GuiTop > BL+BL) ->
          gui_replace(GuiTop div 2-BL,GuiTop div 2-1,#gres{signal=autofilling},State0);
      true ->
          gui_replace(1,BL,#gres{signal=autofilling},State0)
    end,
    {next_state, autofilling, State1};
autofilling({rows, {Recs,false}}, #state{bl=BL, bs=#bs{bottom=BufBottom}, stack=[Target]}=State0) when is_integer(Target) ->
    State1 = append_data({Recs,false},State0#state{stack=[]}),
    State2 = gui_replace(BufBottom-BL+1,BufBottom,#gres{signal=autofilling,loop=0},State1), 
    {next_state, autofilling, State2};
autofilling({rows, {Recs,true}}, #state{bl=BL, tailMode=false, bs=#bs{bottom=BufBottom}, stack=[Target]}=State0) when is_integer(Target) ->
    fetch_close(State0),
    State1 = append_data({Recs,true},State0#state{stack=[]}),
    State2 = gui_replace(BufBottom-BL+1,BufBottom,#gres{signal=completed},State1), 
    {next_state, completed, State2};
autofilling({rows, {Recs,true}}, #state{bl=BL, tailMode=true, bs=#bs{bottom=BufBottom}, stack=[Target]}=State0) when is_integer(Target) ->
    State1 = append_data({Recs,true},State0#state{stack=[]}),
    State2 = gui_replace(BufBottom-BL+1,BufBottom,#gres{signal=tailing,loop=0},State1), 
    {next_state, tailing, State2};
autofilling({rows, {Recs,false}}, #state{bl=BL,bs=#bs{bottom=BufBottom},stack=['>|']}=State0) ->
    State1 = append_data({Recs,false},State0#state{stack=[]}),
    State2 = gui_replace(BufBottom-BL+1,BufBottom,#gres{signal=autofilling,loop=0},State1),
    {next_state, autofilling, State2};
autofilling({rows, {Recs,true}}, #state{bl=BL,bs=#bs{bottom=BufBottom},stack=['>|']}=State0) ->
    fetch_close(State0),
    State1 = append_data({Recs,true},State0#state{stack=[]}),
    State2 = gui_replace(BufBottom-BL+1,BufBottom,#gres{signal=completed},State1),
    {next_state, completed, State2};
autofilling({rows, {Recs,false}}, #state{bl=BL,bs=#bs{bottom=BufBottom},stack=['>|...']}=State0) ->
    State1 = append_data({Recs,false},State0#state{tailMode=true,stack=[]}),
    State2 = gui_replace(BufBottom-BL+1,BufBottom,#gres{signal=autofilling,loop=0},State1),
    {next_state, autofilling, State2};
autofilling({rows, {Recs,true}}, #state{bl=BL,bs=#bs{bottom=BufBottom},stack=['>|...']}=State0) ->
    State1 = append_data({Recs,true},State0#state{tailMode=true,stack=[]}),
    State2 = gui_replace(BufBottom-BL+1,BufBottom,#gres{signal=tailing,loop=0},State1),
    {next_state, tailing, State2};
autofilling({rows, {Recs,false}}, State0) ->
    State1 = append_data({Recs,false},State0),
    {next_state, autofilling, State1};
autofilling({rows, {Recs,true}}, #state{tailMode=true}=State0) ->
    State1 = append_data({Recs,true},State0),
    {next_state, tailing, State1};
autofilling({rows, {Recs,true}}, State0) ->
    State1 = append_data({Recs,true},State0),
    {next_state, completed, State1};
autofilling({button,close}, State) ->
    fetch_close(State),
    {stop, normal, State};
autofilling(Other, State) ->
    ?Log("~p autofilling -- unexpected event ~p~n", [erlang:now(),Other]),
    {next_state, autofilling, State}.

tailing({button, '...'}, State0) ->
    State1 = gui_clear(#gres{signal=tailing,loop=0},State0),
    {next_state, tailing, State1};
tailing({button, '>|'}, #state{bl=BL, bs=#bs{bottom=BufBottom}}=State0) ->
    State1 = gui_replace(BufBottom-BL+1, BufBottom, #gres{signal=tailing},State0), 
    {next_state, tailing, State1};
tailing({button, '>|...'}, #state{bl=BL, bs=#bs{bottom=BufBottom}}=State0) ->
    State1 = gui_replace(BufBottom-BL+1, BufBottom, #gres{signal=tailing,loop=0},State0), 
    {next_state, tailing, State1};
tailing({button, Target}, #state{bl=BL, bs=#bs{bottom=BufBottom}, gs=#gs{top=GuiTop,bottom=GuiBottom}}=State0) when is_integer(Target) ->
    State1 = if
      (BufBottom == 0) ->
          State0#state{stack=[Target]};
      (Target =< 0-BufBottom) ->
          gui_replace(1,BL,#gres{signal=tailing},State0);
      (Target >= 0-BL) and (Target < 0)  ->
          gui_replace(BufBottom+Target-BL+1,BufBottom+Target,#gres{signal=tailing},State0);
      (Target == 0) and (GuiBottom < BufBottom-BL)  ->
          gui_append(BufBottom-BL+1,BufBottom,#gres{signal=tailing,loop=0},State0);
      (Target == 0)  ->
          gui_append(GuiBottom+1,BufBottom,#gres{signal=tailing,loop=0},State0);
      (Target < BL) ->
          gui_replace(1,BL,#gres{signal=tailing},State0);
      (Target =< BufBottom) and (GuiTop > Target) ->
          gui_replace(Target,Target+BL-1,#gres{signal=tailing},State0);
      (Target =< BufBottom) and (GuiBottom < Target) ->
          gui_replace(Target-BL+1,Target,#gres{signal=tailing},State0);
      (Target > BufBottom) ->
          gui_replace(BufBottom-BL+1,BufBottom,#gres{signal=tailing,loop=0},State0);
      true ->
          gui_nop(#gres{signal=tailing,message="target row already in gui"},State0),
          State0
    end,
    {next_state, tailing, State1};
tailing({button, '>'}, #state{bl=BL,bs=#bs{bottom=BufBottom},gs=#gs{bottom=GuiBottom}}=State0) ->
    State1 = if
      (GuiBottom+BL =< BufBottom) ->
          gui_replace(GuiBottom+1,GuiBottom+BL,#gres{signal=tailing},State0);
      true ->
          gui_replace(BufBottom-BL+1,BufBottom,#gres{signal=tailing},State0)
    end,
    {next_state, tailing, State1};
tailing({button, '>>'}, #state{bl=BL,bs=#bs{bottom=BufBottom},gs=#gs{bottom=GuiBottom}}=State0) ->
    State1 = if
      (GuiBottom+GuiBottom =< BufBottom) ->
          gui_replace(GuiBottom+GuiBottom-BL+1,GuiBottom+GuiBottom,#gres{signal=tailing},State0);
      true ->
          gui_replace(BufBottom-BL+1,BufBottom,#gres{signal=tailing},State0)
    end,
    {next_state, tailing, State1};
tailing({button, '<'}, #state{bl=BL,bs=#bs{bottom=BufBottom},gs=#gs{top=GuiTop}}=State0) ->
    State1 = if
      (GuiTop > BL) ->
          gui_replace(GuiTop+BL,GuiTop-1,#gres{signal=tailing},State0);
      true ->
          gui_replace(1,BufBottom,#gres{signal=tailing},State0)
    end,
    {next_state, tailing, State1};
tailing({button, '<<'}, #state{bl=BL,gs=#gs{top=GuiTop}}=State0) ->
    State1 = if
      (GuiTop > BL+BL) ->
          gui_replace(GuiTop div 2-BL,GuiTop div 2-1,#gres{signal=tailing},State0);
      true ->
          gui_replace(1,BL,#gres{signal=tailing},State0)
    end,
    {next_state, tailing, State1};
tailing({rows, {Recs,tail}}, State0) ->
    State1 = append_data({Recs,tail},State0),
    {next_state, tailing, State1};
tailing({button,close}, State) ->
    fetch_close(State),
    {stop, normal, State};
tailing(Other, State) ->
    ?Log("~p tailing -- unexpected event ~p~n", [erlang:now(),Other]),
    {next_state, tailing, State}.

completed({button, '...'}, State0) ->
    fetch(skip,true,State0),
    State1 = gui_clear(#gres{signal=tailing,loop=0},State0#state{tailMode=true}),
    {next_state, tailing, State1};
completed({button, '>|...'}, State0) ->
    fetch(skip,true,State0),
    State1 = gui_nop(#gres{signal=tailing,loop=0},State0#state{tailMode=true}),
    {next_state, tailing, State1};
completed({button, _}, #state{bs=#bs{bottom=0}}=State) ->
    gui_nop(#gres{signal=completed},State),
    {next_state, tailing, State};
completed({button, '>|'}, #state{bl=BL,bs=#bs{bottom=BufBottom}}=State0) ->
    State1 = gui_replace(BufBottom-BL+1,BufBottom,#gres{signal=completed},State0),
    {next_state, tailing, State1};
completed({button, Target}, #state{bl=BL, bs=#bs{bottom=BufBottom}, gs=#gs{top=GuiTop, bottom=GuiBottom}}=State0) when is_integer(Target) ->
    State1 = if
      (Target =< 0-BufBottom) ->
          gui_replace(1,BL,#gres{signal=completed},State0);
      (Target >= 0-BL) and (Target < 0)  ->
          gui_replace(BufBottom-BL+1,BufBottom,#gres{signal=completed},State0);
      (Target < 0)  ->
          gui_replace(BufBottom+Target-BL+1,BufBottom+Target,#gres{signal=completed},State0);
      (Target == 0)  ->
          gui_replace(BufBottom-BL+1,BufBottom,#gres{signal=completed},State0);
      (Target < BL) ->
          gui_replace(1,BL,#gres{signal=completed},State0);
      (Target =< BufBottom) and (GuiTop > Target) ->
          gui_replace(Target,Target+BL-1,#gres{signal=completed},State0);
      (Target =< BufBottom) and (GuiBottom < Target) ->
          gui_replace(Target-BL+1,Target,#gres{signal=completed},State0);
      (Target > BufBottom) ->
          gui_replace(BufBottom-BL+1,BufBottom,#gres{signal=completed},State0);
      true ->
          gui_nop(#gres{signal=completed,message="target row already in gui"},State0),
          State0
    end,
    {next_state, completed, State1};
completed({button, '>'}, #state{bl=BL,bs=#bs{bottom=BufBottom},gs=#gs{bottom=GuiBottom}}=State0) ->
    State1 = if
      (GuiBottom+BL =< BufBottom) ->
          gui_replace(GuiBottom+1,GuiBottom+BL,#gres{signal=completed},State0);
      true ->
          gui_replace(BufBottom-BL+1,BufBottom,#gres{signal=completed},State0)
    end,
    {next_state, completed, State1};
completed({button, '>>'}, #state{bl=BL,bs=#bs{bottom=BufBottom},gs=#gs{bottom=GuiBottom}}=State0) ->
    State1 = if
      (GuiBottom+GuiBottom =< BufBottom) ->
          gui_replace(GuiBottom+GuiBottom-BL+1,GuiBottom+GuiBottom,#gres{signal=completed},State0);
      true ->
          gui_replace(BufBottom-BL+1,BufBottom,#gres{signal=completed},State0)
    end,
    {next_state, completed, State1};
completed({button, '<'}, #state{bl=BL,bs=#bs{bottom=BufBottom},gs=#gs{top=GuiTop}}=State0) ->
    State1 = if
      (GuiTop > BL) ->
          gui_replace(GuiTop+BL,GuiTop-1,#gres{signal=completed},State0);
      true ->
          gui_replace(1,BufBottom,#gres{signal=completed},State0)
    end,
    {next_state, completed, State1};
completed({button, '<<'}, #state{bl=BL,gs=#gs{top=GuiTop}}=State0) ->
    State1 = if
      (GuiTop > BL+BL) ->
          gui_replace(GuiTop div 2-BL,GuiTop div 2-1,#gres{signal=completed},State0);
      true ->
          gui_replace(1,BL,#gres{signal=completed},State0)
    end,
    {next_state, completed, State1};
completed({rows, _}, State0) ->
    {next_state, completed, State0};
completed({button,close}, State) ->
    {stop, normal, State};
completed(Other, State) ->
    ?Log("~p completed -- unexpected event ~p~n", [erlang:now(),Other]),
    {next_state, completed, State}.


%% --------------------------------------------------------------------
%% Func: StateName/3	 synchronized event handling
%% Returns: {next_state, NextStateName, NextStateData}            |
%%          {next_state, NextStateName, NextStateData, Timeout}   |
%%          {reply, Reply, NextStateName, NextStateData}          |
%%          {reply, Reply, NextStateName, NextStateData, Timeout} |
%%          {stop, Reason, NewStateData}                          |
%%          {stop, Reason, Reply, NewStateData}
%% --------------------------------------------------------------------


%% --------------------------------------------------------------------
%% Func: handle_event/3  handling async "send_all_state_event""
%% Returns: {next_state, NextStateName, NextStateData}          |
%%          {next_state, NextStateName, NextStateData, Timeout} |
%%          {stop, Reason, NewStateData}
%% --------------------------------------------------------------------

handle_event(_Event, empty, StateData) ->
    {no_reply, empty, StateData, infinity}.

%% --------------------------------------------------------------------
%% Func: handle_sync_event/4 handling sync "send_all_state_event""
%% Returns: {next_state, NextStateName, NextStateData}            |
%%          {next_state, NextStateName, NextStateData, Timeout}   |
%%          {reply, Reply, NextStateName, NextStateData}          |
%%          {reply, Reply, NextStateName, NextStateData, Timeout} |
%%          {stop, Reason, NewStateData}                          |
%%          {stop, Reason, Reply, NewStateData}
%% --------------------------------------------------------------------
handle_sync_event(_Event, _From, empty, StateData) ->
    {no_reply, empty, StateData,infinity}.

%% --------------------------------------------------------------------
%% Func: handle_info/3
%% Returns: {next_state, NextStateName, NextStateData}          |
%%          {next_state, NextStateName, NextStateData, Timeout} |
%%          {stop, Reason, NewStateData}
%% --------------------------------------------------------------------
handle_info({_Pid,{Rows,Completed}}, StateName, State) ->
    rows(State#state.fsm, {Rows,Completed}),
    {next_state, StateName, State, infinity}.

%% --------------------------------------------------------------------
%% Func: terminate/3
%% Purpose: Shutdown the fsm
%% Returns: any
%% --------------------------------------------------------------------
terminate(_Reason, _StateName, _StatData) -> ok.

%% --------------------------------------------------------------------
%% Func: code_change/4
%% Purpose: Convert process state when code is changed
%% Returns: {ok, NewState, NewStateData}
%% --------------------------------------------------------------------
code_change(_OldVsn, StateName, StateData, _Extra) ->
    {ok, StateName, StateData}.

%% --------------------------------------------------------------------
%%% Internal functions
%% --------------------------------------------------------------------


exec(SKey, Id, BL, IsSec, Sql) ->
    ?Log("~p exec -- ~p: ~s~n", [erlang:now(), Id, Sql]),
    {ok, StmtResult} = imem_sql:exec(SKey, Sql, BL, 'Imem', IsSec),
    % #stmtResult{stmtCols=StmtCols} = StmtResult,
    % ?Log("Statement Cols : ~p~n", [StmtCols]),
    StmtResult.

gui_clear(GuiResult,State) -> 
    ?Log("~p gui_clear () ~p ~p~n", [erlang:now(), GuiResult#gres.signal, GuiResult#gres.loop]),
    #ctx{reply=Reply} = State#state.ctx,
    #bs{bottom=Bottom} = State#state.bs,
    Reply ! GuiResult#gres{bufBottom=Bottom},
    ok.

gui_nop(GuiResult,State) -> 
    ?Log("~p gui_nop () ~p ~p~n", [erlang:now(), GuiResult#gres.signal, GuiResult#gres.loop]),
    #ctx{reply=Reply} = State#state.ctx,
    #bs{bottom=Bottom} = State#state.bs,
    Reply ! GuiResult#gres{bufBottom=Bottom},
    ok.

fetch(FetchMode,TailMode, State) ->
    #ctx{skey=SKey,isSec=IsSec,sr=SR} = State#state.ctx,
    Opts = case {FetchMode,TailMode} of
        {none,none} ->    [];
        {FM,none} ->      [{fetch_mode,FM}];
        {FM,TM} ->        [{fetch_mode,FM},{tailMode,TM}]
    end,
    Result = imem_statement:fetch_recs_async(SKey, SR#stmtResult.stmtRef, self(), Opts, IsSec),
    ?Log("~p fetch -- ~p~n", [erlang:now(), Result]),
    Result.
 
fetch_close(State) ->
    #ctx{skey=SKey,isSec=IsSec,sr=SR} = State#state.ctx, 
    Result = imem_statement:fetch_close(SKey, SR, IsSec),
    ?Log("~p fetch_close -- ~p~n", [erlang:now(), Result]),
    Result.

gui_replace(GuiTop,GuiBottom,GuiResult,State) ->
    ?Log("~p gui_replace (~p ~p) ~p ~p~n", [erlang:now(), GuiTop,GuiBottom, GuiResult#gres.signal, GuiResult#gres.loop]),
    #ctx{reply=Reply} = State#state.ctx,
    #bs{bottom=Bottom} = State#state.bs,
    Reply ! GuiResult#gres{bufBottom=Bottom},
    State#state{gs=#gs{top=GuiTop,bottom=GuiBottom}}.

gui_append(GuiTop,GuiBottom,GuiResult,State) ->
    GS = State#state.gs,
    ExpectedGuiTop = GS#gs.bottom + 1, 
    case ExpectedGuiTop of
        GuiTop ->
            ?Log("~p gui_append (~p ~p) ~p ~p~n", [erlang:now(), GuiTop, GuiBottom, GuiResult#gres.signal, GuiResult#gres.loop]);
        GT ->
            ?Log("~p gui_append (~p ~p) ~p ~p EXPECTED GuiTop ~p~n", [erlang:now(), GuiTop, GuiBottom, GuiResult#gres.signal, GuiResult#gres.loop, GT])
    end,
    #ctx{reply=Reply} = State#state.ctx,
    #bs{bottom=Bottom} = State#state.bs,
    Reply ! GuiResult#gres{bufBottom=Bottom},
    State#state{gs=#gs{top=GuiTop,bottom=GuiBottom}}.

append_data({[],_Complete},State) -> 
    ?Log("~p append_data -- count ~p~n", [erlang:now(), 0]),
    State;
append_data({Recs,_Complete},State) ->
    Count = length(Recs),
    #bs{top=Top,bottom=Bottom,buf=Buf} = State#state.bs,
    NewBottom = Bottom+Count,
    ?Log("~p append_data -- count ~p bottom ~p~n", [erlang:now(),Count,NewBottom]),
    State#state{bs=#bs{top=Top,bottom=NewBottom,buf=Buf ++ Recs}}.



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

        {ok,Fsm} = start(#ctx{skey=SKey,id="fsm test",bl=10,isSec=false,sql="select * from def;",reply=self()}),
        button(Fsm,'>'),
        receive_respond(Fsm),
        button(Fsm,55),
        receive_respond(Fsm),
        % button(Fsm,'>>'),
        receive_respond(Fsm),
        receive_respond(Fsm),
        receive_respond(Fsm),
        receive_respond(Fsm),
        receive_respond(Fsm),
        button(Fsm,'<'),
        receive_respond(Fsm),
        button(Fsm,'<'),
        receive_respond(Fsm),
        button(Fsm,0),
        receive_respond(Fsm),
        timer:sleep(2000),
        button(Fsm,'>'),
        receive_respond(Fsm),
        button(Fsm,close),

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
      #gres{loop=Loop} ->       button(Fsm,Loop)
    end,
    process_responses(Fsm,Responses).
