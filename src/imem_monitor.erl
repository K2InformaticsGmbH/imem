-module(imem_monitor).
-behavior(gen_server).

-record(state, {
        }).

-export([ start_link/0
        , monitor/1
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

%% - TODO - Stefan: call this interface fun from imem_seco to monitor a process
%                   returns a ref() of the monitor
monitor(Pid) when is_pid(Pid) -> gen_server:call(?MODULE, {monitor, Pid}).

start_link() ->
	gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

init(_Args) ->
    io:format(user, "~p started!~n", [?MODULE]),
    {ok,#state{}}.

handle_call({monitor, Pid}, _From, State) ->
    io:format(user, "~p - started monitoring pid ~p~n", [?MODULE, Pid]),
    {reply, erlang:monitor(process, Pid), State};
handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast({'DOWN', Ref, process, Pid, Reason}, State) ->
    io:format(user, "~p - died pid ~p ref ~p as ~p~n", [?MODULE, Pid, Ref, Reason]),
    dd_seco:cleanup(Pid),
    {noreply, State};
handle_cast(_Request, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reson, _State) -> ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

format_status(_Opt, [_PDict, _State]) -> ok.
