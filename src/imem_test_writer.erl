-module(imem_test_writer).

-include("imem_meta.hrl").

-behavior(gen_server).

-record(ddTest,   %% test table    
                  { time      ::ddTimestamp()
                  , x         ::integer()
                  , fX        ::integer()
                  , oneOverX  ::float()
                  , comment   ::list()
                  }
       ).
-define(ddTest, [timestamp,integer,integer,float,string]).

-define(ddTestName,ddTest@).  %% node sharded 

-record(state,  % factorial state (stateless apart from delay)
                {wait = 1000}).

% gen_server API calls

-export([ start_link/1
        , start/1
        , stop/0
        , start_supervised/1
        , stop_supervised/0
        , wait/1
        , write_test_record/4
        , fac/1
        ]).

% gen_server behavior callbacks

-export([ init/1
        , handle_call/3
        , handle_cast/2
        , handle_info/2
        , terminate/2
        , code_change/3
        , format_status/2
        ]).

% gen_server API calls

start(Wait) ->
    gen_server:start({local, ?MODULE}, ?MODULE, Wait, []).

stop() ->
    gen_server:call({local, ?MODULE}, stop).

start_supervised(Wait) ->
    ChildSpec = { imem_test_writer                      % ChildId
                , {imem_test_writer,start_link,[Wait]}  % {M,F,A}
                , permanent                             % Restart
                , 1000                                  % Shutdown
                , worker                                % Type
                , [imem_test_writer]                    % Modules
                },
    supervisor:start_child(imem_sup, ChildSpec).

stop_supervised() ->
    supervisor:terminate_child(imem_sup, imem_test_writer),
    supervisor:delete_child(imem_sup, imem_test_writer).

start_link(Wait) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, Wait, []).

wait(Wait) when is_integer(Wait) ->
    gen_server:call(?MODULE, {wait, Wait}). 

write_test_record(X,FX,OneOverX,Comment) ->
    Record = #ddTest{ time=erlang:now()
                    , x=X
                    , fX=FX
                    , oneOverX=OneOverX
                    , comment=Comment
                    },
    imem_meta:write(?ddTestName, Record).

fac(0) -> 1;
fac(N) -> N * fac(N-1). 

% gen_server behavior callbacks

init(Wait) ->
    ?Log("~p starting...~n", [?MODULE]),
    imem_meta:create_check_table(?ddTestName, {record_info(fields, ddTest),?ddTest, #ddTest{}}, [{type,ordered_set},{record_name,ddTest}], system),
    imem_test_writer:write_test_record(0,0,0.0,"fac start"),
    random:seed(now()),
    erlang:send_after(Wait, self(), write_record),    
    ?Log("~p started!~n", [?MODULE]),
    {ok,#state{wait=Wait}}.

handle_info(write_record, #state{wait=Wait}) ->
    Ran = random:uniform(),       %% 0..1
    %% X = round(30*Ran),         %% bad version causing 1/0
    X = round(29*Ran) +1 ,
    FX = fac(X),
    OneOverX = 1.0/X, 
    imem_test_writer:write_test_record(X,FX,OneOverX,"fac next"),
    erlang:send_after(Wait, self(), write_record),
    {noreply, #state{wait=Wait}}.

handle_call({wait, Wait}, _From, #state{}=State) ->
    {reply, ok, State#state{wait=Wait}};
handle_call(stop, _From, State) ->
    {stop, normal, ok, State}.

handle_cast(_Request, State) ->
    {noreply, State}.

terminate(_Reason, _State) -> ok.
code_change(_OldVsn, State, _Extra) -> {ok, State}.
format_status(_Opt, [_PDict, _State]) -> ok.





















%% -----------------------------------------------------------------------------
%% -define(ddTestName,ddTest_100@).   %% time partitioned 100 seconds / ,{purge_delay,500}

% -record(state1, % fibonacci state
%                 { wait = 1000                ::integer()
%                 , fib_2 = 0                  ::integer()
%                 , fib_1 = 1                  ::integer()
%                 , n = 2                       ::integer()
%                 }).

% handle_info(write_record, #state{}) ->
%     imem_test_writer:write_test_record(0,0,0.0,"fib start"),
%     imem_test_writer:write_test_record(1,1,1.0,"fib next"),
%     handle_info(write_record, #state1{wait=1000,fib_2=0,fib_1=1,n=2});
% handle_info(write_record, #state1{wait=Wait,fib_2=L2,fib_1=L1,n=N}) ->
%     imem_test_writer:write_test_record(N,L2+L1,1.0/N,"fib next"),
%     erlang:send_after(Wait, self(), write_record),
%     {noreply, #state1{wait=Wait,fib_2=L1,fib_1=L2+L1,n=N+1}}.

% handle_call({wait, Wait}, _From, #state1{}=State) ->
%     {reply, ok, State#state1{wait=Wait}}.
