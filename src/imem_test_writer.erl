-module(imem_test_writer).

-include("imem_meta.hrl").

-behavior(gen_server).

-record(ddTest,                             %% test    
                  { time                    ::ddTimestamp()
                  , x = -1                  ::integer()
                  , fX = -1                 ::integer()
                  , oneOverX = 0.0          ::float()
                  , comment = "next"        ::list()
                  }
       ).
-define(ddTest, [timestamp,integer,integer,float,string]).

-record(state, {wait = 1000}).

% gen_server API calls

-export([ start_link/1
        , start/1
        , write_test_record/2
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

start_link(Params) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, Params, []).

start(Params) ->
    gen_server:start({local, ?MODULE}, ?MODULE, Params, []).

write_test_record(X,FX) ->
    Record = #ddTest{time=erlang:now()
      , x=X, fX=FX, oneOverX=1.0/X},
    imem_meta:write(ddTest, Record).

% gen_server behavior callbacks

init(Wait) ->
    ?Log("~p starting...~n", [?MODULE]),
    imem_meta:create_check_table(ddTest, {record_info(fields, ddTest),?ddTest, #ddTest{}}, [{type,ordered_set}], system),
    imem_meta:write(ddTest, #ddTest{time=erlang:now(), comment="start"}),
    random:seed(now()),
    erlang:send_after(Wait, self(), write_record),    
    ?Log("~p started!~n", [?MODULE]),
    {ok,#state{wait=Wait}}.

handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast(_Request, State) ->
    {noreply, State}.

fac(0) -> 1;
fac(N) -> N * fac(N-1). 

handle_info(write_record, #state{wait=Wait}=State) ->
    Ran = random:uniform(),       %% 0..1
    X = round(19*Ran)+1,
    FX = fac(X), 
    imem_test_writer:write_test_record(X,FX),
    erlang:send_after(Wait, self(), write_record),
    {noreply, State}.

terminate(_Reason, _State) -> ok.

code_change(_OldVsn, State, _Extra) -> {ok, State}.

format_status(_Opt, [_PDict, _State]) -> ok.


% -record(state1, % fibonacci state
%                 { fib_2 = 0                  ::integer()
%                 , fib_1 = 1                  ::integer()
%                 , n = 2                      ::integer()
%                 }).

% handle_info(write_record, #state{}) ->
%     imem_test_writer:write_test_record(1,1),
%     handle_info(write_record, #state1{});
% handle_info(write_record, #state1{fib_2=L2, fib_1=L1, n=N}) ->
%     X = N,
%     FX = L2+L1, 
%     imem_test_writer:write_test_record(X,FX),
%     erlang:send_after(500, self(), write_record),
%     {noreply, #state1{fib_2=L1, fib_1=FX, n=N+1}}.



