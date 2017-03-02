-module(imem_test_writer).

-include("imem.hrl").           % import logging macros
-include("imem_meta.hrl").      % import exceptions and data types

-behavior(gen_server).          % implicitly defines the required callbacks

-record(ddTest,   % test table record definition with erlang types
                  { time    :: ddTimestamp() % {Megas,Secs,Micros}
                  , x       :: integer()        
                  , y       :: integer()
                  , z       :: float()
                  , label   :: list()
                  }
       ).
-define(ddTest, [timestamp,integer,integer,float,string]).  % test table DDerl types
-define(ddTestName,ddTest@).  %% node sharded test table (local content per node) 

-record(state0, % process state (stateless apart from delay)
                { wait      :: integer()}   % event delay
       ).
-record(state1, % extended process state with coordinates and label
                { wait      :: integer()    % event delay
                , x         :: integer()    % current x pos
                , y         :: integer()    % current y pos
                , z         :: float()      % generation 0..3
                , label     :: list()       % object name
                }
       ).
-record(state2, % extended process state with hierarchy info
                { wait = 500    :: integer()
                , x             :: integer()
                , y             :: integer()
                , z             :: float()
                , label         :: list()
                , childPids=[]  :: list()
                }
       ).

% gen_server API call exports

-export([ start/1
        , stop/0
        , wait/1
        , write_test_record/4
        ]).

% sample usage for demo:
% imem_test_writer:start(1000). % 1 second event loop
% imem_test_writer:stop().
% imem_test_writer:wait(100).
% l(imem_test_writer).          % module load -> apply the patch

% gen_server behavior callback exports

-export([ start_link/1
        , init/1
        , handle_call/3
        , handle_cast/2
        , handle_info/2
        , terminate/2
        , code_change/3
        ]).

start(Wait) ->
    ChildSpec = { imem_test_writer                      % ChildId
                , {imem_test_writer,start_link,[Wait]}  % {M,F,A}
                , permanent                             % Restart strategy
                , 1000                                  % Shutdown timeout
                , worker                                % Type
                , [imem_test_writer]                    % Modules
                },
    supervisor:start_child(imem_sup, ChildSpec).

stop() ->
    supervisor:terminate_child(imem_sup, imem_test_writer),
    supervisor:delete_child(imem_sup, imem_test_writer).

wait(Wait) when is_integer(Wait) ->
    gen_server:call(?MODULE, {wait, Wait}). 

start_link(Wait) when is_integer(Wait) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, Wait, []).

timestamp() -> erlang:now().

write_test_record(X,Y,Z,Label) ->
    Record = #ddTest{ time=timestamp()
                    , x=X
                    , y=Y
                    , z=Z
                    , label=Label
                    },
    imem_meta:dirty_write(?ddTestName, Record).

lead({Mega,Secs,_}) ->
    Phase = 0.051*math:pi()*(Mega*1000000 + Secs), 
    {round(1000*math:cos(Phase)), round(500*math:sin(Phase))}.

off() -> round(200*random:uniform())-100.

follow(Xp,Yp,X,Y,_,_) -> {(Xp+Xp+Xp+X+off()) div 4, (Yp+Yp+Yp+Y+off()) div 4}.

instruct_the_children(_, _, _, _, []) -> ok;
instruct_the_children(X, Y, T, W, Pids) ->
    Wait = W div (length(Pids) + 1),
    [erlang:send_after(I*Wait, Pid, {follow_me,X,Y,T,W}) 
      || {Pid,I} <- lists:zip(Pids, lists:seq(0,length(Pids)-1))
    ].

child_name(Np,I) -> Np ++ "_" ++ integer_to_list(I).

child_params(X,Y,Z,N,T,I) -> {X,Y,Z,child_name(N,I),T}.

start_link_children(X,Y,Z,N,T) ->
    StartResult = [gen_server:start_link( ?MODULE
                                        , child_params(X,Y,Z,N,T,I)
                                        , []
                                        ) || I <-lists:seq(1,5)
                  ],
    [Pid || {ok,Pid} <- StartResult].

% gen_server behavior callback implementation

init({Xp,Yp,Zp,N,T}) ->
    random:seed(timestamp()),
    Z = Zp + 1.0,
    {X,Y} = follow(Xp,Yp,Xp,Yp,Z,T),
    imem_test_writer:write_test_record(X,Y,Z,"Start " ++ N),
    {ok,#state2{wait=0,x=X,y=Y,z=Z,label=N}};
init(Wait) when is_integer(Wait) ->
    random:seed(timestamp()),
    imem_meta:create_check_table(?ddTestName, {record_info(fields, ddTest),?ddTest, #ddTest{}}, [{type,ordered_set},{record_name,ddTest}], system),
    imem_meta:truncate_table(?ddTestName),
    imem_test_writer:write_test_record(0,0,0.0,"starting--------------"),
    erlang:send_after(Wait, self(), next_event),    
    {ok,#state0{wait=Wait}}.

terminate(_Reason, _State) -> ok.
code_change(_OldVsn, State, _Extra) -> {ok, State}.
handle_cast(_Request, State) -> {noreply, State}.

handle_call({wait, Wait}, _From, #state2{}=State) ->
    {reply, ok, State#state2{wait=Wait}};
handle_call({wait, Wait}, _From, #state1{}=State) ->
    {reply, ok, State#state1{wait=Wait}};
handle_call({wait, Wait}, _From, #state0{}=State) ->
    {reply, ok, State#state0{wait=Wait}};
handle_call(stop, _From, State) ->
    {stop, normal, ok, State}.

handle_info({follow_me,Xp,Yp,T,_W}, #state2{x=Xo,y=Yo,z=Z,label=N,childPids=[]}=State) when Z<3.0 ->
    {X,Y} = follow(Xp,Yp,Xo,Yo,Z,T),
    imem_test_writer:write_test_record(X,Y,Z,N),
    ChildPids = start_link_children(0,0,Z,N,T),
    {noreply, State#state2{x=X,y=Y,childPids=ChildPids}};
handle_info({follow_me,Xp,Yp,T,W}, #state2{x=Xo,y=Yo,z=Z,label=N,childPids=ChildPids}=State) ->
    {X,Y} = follow(Xp,Yp,Xo,Yo,Z,T),
    imem_test_writer:write_test_record(X,Y,Z,N),
    instruct_the_children(X, Y, T, W, ChildPids),    
    {noreply, State#state2{x=X,y=Y}};
handle_info(next_event, #state2{wait=Wait,z=Z,label=N,childPids=ChildPids}=State) ->
    T = timestamp(),
    {X,Y} = lead(T),
    imem_test_writer:write_test_record(X,Y,Z,N),
    erlang:send_after(Wait, self(), next_event),
    instruct_the_children(X, Y, T, Wait, ChildPids),    
    {noreply, State#state2{x=X,y=Y}};
handle_info(next_event, #state1{wait=Wait,z=Z,label=N}) ->
    T = timestamp(),
    {X,Y} = lead(T),
    imem_test_writer:write_test_record(X,Y,Z,N),
    ChildPids = start_link_children(0,0,Z,N,T),
    erlang:send_after(Wait, self(), next_event),
    {noreply, #state2{x=X,y=Y,z=Z,wait=Wait,label=N,childPids=ChildPids}};
handle_info(next_event, #state0{wait=Wait}) ->
    N = "Node",
    Z = 0.0,
    {X,Y} = lead(timestamp()),
    imem_meta:truncate_table(?ddTestName),
    imem_test_writer:write_test_record(X,Y,Z,N),
    erlang:send_after(Wait, self(), next_event),
    {noreply, #state1{x=X,y=Y,z=Z,wait=Wait,label=N}}.
