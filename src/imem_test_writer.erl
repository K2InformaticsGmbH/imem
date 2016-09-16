-module(imem_test_writer).

% -ifdef(TEST).                   % empty module in production

-include("imem.hrl").           % import logging macros
-include("imem_meta.hrl").      % import exceptions and data types

-behavior(gen_server).          % implicitly defines the required callbacks

-record(ddTest,   % test table record definition with erlang types
                  { time      ::ddTimestamp()
                  , x         ::integer()
                  , y         ::integer()
                  , oneOverX  ::float()
                  , label     ::list()
                  }
       ).
-define(ddTest, [timestamp,integer,integer,float,string]).  % test table DDerl types

-define(ddTestName,ddTest@).  %% node sharded test table (local content per node) 

-record(state,  % process state (stateless apart from delay)
                {wait = 500 :: integer()}
       ).

% gen_server API call exports

-export([ start_link/1
        , start/1
        , stop/0
        , wait/1
        , fac/1
        , write_test_record/4
        ]).

% sample usage for demo:
% imem_test_writer:start(333).
% imem_test_writer:wait(100).
% imem_test_writer:wait(5).
% imem_test_writer:stop().
% l(imem_test_writer).

% gen_server behavior callback exports

-export([ init/1
        , handle_call/3
        , handle_cast/2
        , handle_info/2
        , terminate/2
        , code_change/3
        , format_status/2
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

start_link(Wait) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, Wait, []).

wait(Wait) when is_integer(Wait) ->
    gen_server:call(?MODULE, {wait, Wait}). 

write_test_record(X,Y,OneOverX,Label) ->
    Record = #ddTest{ time=os:timestamp()
                    , x=X
                    , y=Y
                    , oneOverX=OneOverX
                    , label=Label
                    },
    imem_meta:write(?ddTestName, Record).

fac(0) -> 1;
fac(N) -> N * fac(N-1). 

% gen_server behavior callback implementation

init(Wait) ->
    ?Debug("~p starting...~n", [?MODULE]),
    imem_meta:create_check_table(?ddTestName, {record_info(fields, ddTest),?ddTest, #ddTest{}}, [{type,ordered_set},{record_name,ddTest}], system),
    imem_test_writer:write_test_record(0,0,0.0,"starting------------------------------"),
    random:seed(os:timestamp()),
    erlang:send_after(Wait, self(), next_event),    
    ?Debug("~p started!~n", [?MODULE]),
    {ok,#state{wait=Wait}}.

terminate(_Reason, _State) -> ok.
code_change(_OldVsn, State, _Extra) -> {ok, State}.
format_status(_Opt, [_PDict, _State]) -> ok.

handle_call({wait, Wait}, _From, #state{}=State) ->
    {reply, ok, State#state{wait=Wait}};
handle_call(stop, _From, State) ->
    {stop, normal, ok, State}.

handle_cast(_Request, State) ->
    {noreply, State}.

handle_info(next_event, #state{wait=Wait}) ->
    Ran = random:uniform(),         % 0..1
    X = round(50*Ran)+1,              % bad code causing 1/0
    {_,Sec,_} = os:timestamp(),     % {MegaSec,Sec,MicroSec} since epoch
    Y = X * (Sec rem 100 + 1),     % rampy sample distribution
    OneOverX = 5000.0/X,            % spiky sample distribution
    imem_test_writer:write_test_record(X,Y,OneOverX,integer_to_list(fac(X))),
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
