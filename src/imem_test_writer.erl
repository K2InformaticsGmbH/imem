-module(imem_test_writer).

% -ifdef(TEST).                   % empty module in production

-include("imem.hrl").           % import logging macros
-include("imem_meta.hrl").      % import exceptions and data types

-behavior(gen_server).          % implicitly defines the required callbacks

-record(ddTest,   % test table record definition with erlang types
                  { time        :: ddTimestamp()
                  , x           :: integer()
                  , y           :: integer()
                  , z           :: float()
                  , label       :: list()
                  }
       ).
-define(ddTest, [timestamp,integer,integer,float,string]).  % test table DDerl types

-define(ddTestName,ddTest@).  %% node sharded test table (local content per node) 

-record(state,  % process state (stateless apart from delay)
                { wait = 500    :: integer()}
       ).
-record(state1, % extended process state with coordinates and label
                { wait = 500    :: integer()
                , x             :: integer()
                , y             :: integer()
                , z             :: float()
                , label         :: list()
                , childpids=[]  :: list()
                }
       ).

% gen_server API call exports

-export([ start/1
        , stop/0
        , wait/1
        , write_test_record/4
        , zip/2
        , fac/1
        , sort/1
        , p/1
        , off/1
        ]).

% sample usage for demo:

% imem_test_writer:fac(10).
% imem_test_writer:sort([3,1,5,7,1,23,9,9]).

% imem_test_writer:p([1,2,3,4]).
% imem_test_writer:p("AGRO").
% lists:usort(imem_test_writer:p("AGRO")).


% imem_test_writer:start(333).
% imem_test_writer:wait(100).
% imem_test_writer:wait(5).
% imem_test_writer:stop().
% l(imem_test_writer).
% imem_test_writer:start(1000).

% gen_server behavior callback exports

-export([ start_link/1
        , init/1
        , handle_call/3
        , handle_cast/2
        , handle_info/2
        , terminate/2
        , code_change/3
        % , format_status/2
        ]).

start(Wait) ->
    % gen_server:start({local, ?MODULE}, ?MODULE, Wait, []).
    ChildSpec = { imem_test_writer                      % ChildId
                , {imem_test_writer,start_link,[Wait]}  % {M,F,A}
                , permanent                             % Restart strategy
                , 1000                                  % Shutdown timeout
                , worker                                % Type
                , [imem_test_writer]                    % Modules
                },
    supervisor:start_child(imem_sup, ChildSpec).

stop() ->
    % gen_server:call(?MODULE, stop).
    supervisor:terminate_child(imem_sup, imem_test_writer),
    supervisor:delete_child(imem_sup, imem_test_writer).

wait(Wait) when is_integer(Wait) ->
    gen_server:call(?MODULE, {wait, Wait}). 

start_link({X,Y,Z,N,T}) ->
    gen_server:start_link({local, N}, ?MODULE, {X,Y,Z,N,T}, []);
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

fac(0) -> 1;
fac(N) when N > 0 -> N * fac(N-1). 

zip(L1,L2) -> zip(L1,L2,[]).

zip([],[],Acc) -> lists:reverse(Acc);
zip([E1|R1],[E2|R2],Acc) -> zip(R1,R2,[{E1,E2}|Acc]).

sort([Pivot|T]) -> sort([X || X <- T, X < Pivot]) ++ [Pivot] ++ sort([X || X <- T, X >= Pivot]);
sort([]) -> [].

p([]) -> [[]];
p(L) -> [ [H|T] || H <- L, T <- p(L--[H])].

off(Scale) -> round(Scale*random:uniform())-Scale div 2.

displace(3.0) -> off(100);
displace(2.0) -> off(200);
displace(1.0) -> off(50);
displace(_) ->   off(0).

% phase({_,Secs,_}) ->  0.05*math:pi()*(Secs rem 100).
phase({Mega,Secs,_}) ->  0.051*math:pi()*(Mega*1000000 + Secs).

move(_,_,_,_,3.0,T) -> {round(1000*math:cos(phase(T))), round(500*math:sin(phase(T)))};
move(Xp,Yp,X,Y,Z,_) -> {(Xp+X+displace(Z)) div 2, (Yp+Y+displace(Z)) div 2}.

% gen_server behavior callback implementation

init({Xp,Yp,Zp,N,T}) when Zp > 1.0 ->
    random:seed(timestamp()),
    Z = Zp - 1.0,
    {X,Y} = move(Xp,Yp,Xp,Yp,Z,T),
    imem_test_writer:write_test_record(X,Y,Z,"Start " ++ N),
    StartResult = [ gen_server:start_link(?MODULE, {X,Y,Z,N ++ "_" ++ integer_to_list(I),T}, []) || I <-lists:seq(1,5)],
    ChildPids = [ Pid || {ok,Pid} <- StartResult],
    {ok,#state1{wait=0,x=X,y=Y,z=Z,label=N,childpids=ChildPids}};
init({Xp,Yp,Zp,N,T}) ->
    random:seed(timestamp()),
    Z = Zp - 1.0,
    {X,Y} = move(Xp,Yp,Xp,Yp,Z,T),
    imem_test_writer:write_test_record(X,Y,Z,"Start " ++ N),
    {ok,#state1{wait=0,x=X,y=Y,z=Z,label=N}};
init(Wait) when is_integer(Wait) ->
    random:seed(timestamp()),
    imem_meta:create_check_table(?ddTestName, {record_info(fields, ddTest),?ddTest, #ddTest{}}, [{type,ordered_set},{record_name,ddTest}], system),
    imem_meta:truncate_table(?ddTestName),
    imem_test_writer:write_test_record(0,0,0.0,"starting------------------------------"),
    erlang:send_after(Wait, self(), next_event),    
    {ok,#state{wait=Wait}}.

terminate(_Reason, _State) -> ok.
code_change(_OldVsn, State, _Extra) -> {ok, State}.
% format_status(_Opt, [_PDict, _State]) -> ok.
handle_cast(_Request, State) -> {noreply, State}.

handle_call({wait, Wait}, _From, #state1{}=State) ->
    {reply, ok, State#state1{wait=Wait}};
handle_call({wait, Wait}, _From, #state{}=State) ->
    {reply, ok, State#state{wait=Wait}};
handle_call(stop, _From, State) ->
    {stop, normal, ok, State}.

instruct_the_children(_, _, _, _, []) -> ok;
instruct_the_children(X, Y, T, W, Pids) ->
    Wait = W div (length(Pids) + 1),
    [erlang:send_after(I*Wait, Pid, {follow_me,X,Y,T,W}) 
      || {Pid,I} <- lists:zip(Pids, lists:seq(0,length(Pids)-1))
    ].

handle_info(next_event, #state{wait=Wait}) ->
    N = "Node",
    Z = 3.0,
    T = timestamp(),
    {X,Y} = move(0,0,0,0,Z,T),
    imem_test_writer:write_test_record(X,Y,Z,N),
    StartResult = [ gen_server:start_link(?MODULE, {X,Y,Z,N ++ "_" ++ integer_to_list(I),T}, []) || I <-lists:seq(1,5)],
    ChildPids = [ Pid || {ok,Pid} <- StartResult],
    erlang:send_after(Wait, self(), next_event),
    {noreply, #state1{x=X,y=Y,z=Z,wait=Wait,label=N,childpids=ChildPids}};
handle_info(next_event, #state1{wait=Wait,x=Xold,y=Yold,z=Z,label=N,childpids=ChildPids}=State) ->
    T = timestamp(),
    {X,Y} = move(Xold,Yold,Xold,Yold,Z,T),
    imem_test_writer:write_test_record(X,Y,Z,N),
    instruct_the_children(X, Y, T, Wait, ChildPids),
    erlang:send_after(Wait, self(), next_event),
    {noreply, State#state1{x=X,y=Y}};
handle_info({follow_me,Xp,Yp,T,W}, #state1{x=Xold,y=Yold,z=Z,label=N,childpids=ChildPids}=State) ->
    {X,Y} = move(Xp,Yp,Xold,Yold,Z,T),
    imem_test_writer:write_test_record(X,Y,Z,N),
    instruct_the_children(X, Y, T, W, ChildPids),
    {noreply, State#state1{x=X,y=Y}};
handle_info(next_event, #state{wait=Wait}) ->
    Ran = random:uniform(),         % 0..1
    X = round(50*Ran)+1,            % bad code causing 1/0
    {_,Sec,_} = timestamp(),        % {MegaSec,Sec,MicroSec} since epoch
    Y = X * (Sec rem 100 + 1),      % rampy sample distribution
    Z = 5000.0/X,                   % spiky sample distribution
    imem_test_writer:write_test_record(X,Y,Z,integer_to_list(fac(X))),
    erlang:send_after(Wait, self(), next_event),
    {noreply, #state{wait=Wait}}.


% -record(state1, % fibonacci state
%                 { wait = 1000                ::integer()
%                 , fib_2 = 0                  ::integer()
%                 , fib_1 = 1                  ::integer()
%                 , n = 2                      ::integer()
%                 }).

% handle_info(next_event, #state{}) ->
%     imem_test_writer:write_test_record(0,0,0.0,"fib start"),
%     imem_test_writer:write_test_record(1,1,1.0,"fib next"),
%     handle_info(next_event, #state1{wait=1000,fib_2=0,fib_1=1,n=2});
% handle_info(next_event, #state1{wait=Wait,fib_2=L2,fib_1=L1,n=N}) ->
%     imem_test_writer:write_test_record(N,L2+L1,1.0/N,"fib next"),
%     erlang:send_after(Wait, self(), next_event),
%     {noreply, #state1{wait=Wait,fib_2=L1,fib_1=L2+L1,n=N+1}}.

% handle_call({wait, Wait}, _From, #state1{}=State) ->
%     {reply, ok, State#state1{wait=Wait}}.

% -endif.
