-module(imem_inet_tcp_dist).

-behavior(gen_server).

% Distribution interface export
-export([listen/1, accept/1, accept_connection/5, setup/5, close/1, select/1, is_node_name/1]).

% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3, reg_info/0]).

-safe([reg_info/0]).

listen(Name) ->
    start(),
    R = inet_tcp_dist:listen(Name),
    ?MODULE ! {listen, {Name, R}},
    R.

accept(Listen) ->
    R = inet_tcp_dist:accept(Listen),
    ?MODULE ! {accept, {Listen, R}},
    R.

accept_connection(AcceptPid, Socket, MyNode, Allowed, SetupTime) ->
    R = inet_tcp_dist:accept_connection(AcceptPid, Socket, MyNode, Allowed, SetupTime),
    ?MODULE ! {accept_connection, {{AcceptPid, Socket, MyNode, Allowed, SetupTime}, R}},
    R.

setup(Node, Type, MyNode, LongOrShortNames, SetupTime) ->
    R = inet_tcp_dist:setup(Node, Type, MyNode, LongOrShortNames, SetupTime),
    ?MODULE ! {setup, {{Node, Type, MyNode, LongOrShortNames, SetupTime}, R}},
    R.

close(Socket) ->
    R = inet_tcp_dist:close(Socket),
    ?MODULE ! {close, {Socket, R}},
    R.

select(Node) ->
    R = inet_tcp_dist:select(Node),
    ?MODULE ! {select, {Node, R}},
    R.

is_node_name(Node) ->
    R = inet_tcp_dist:is_node_name(Node),
    ?MODULE ! {is_node_name, {Node, R}},
    R.

% @doc gen_server callbacks for storing and retreiving data
-record(state, {calls = []}).

start() ->
    case catch erlang:is_process_alive(erlang:whereis(?MODULE)) of
        true -> ok;
        _ -> catch gen_server:start_link({local, ?MODULE}, ?MODULE, [], [])
    end.

init([]) -> {ok, #state{}}.

reg_info() ->
    {Name, {ok, {_, {net_address, {Ip, Port}, Host, tcp, inet}, _}}} = proplists:get_value(listen, getq()),
    {Name, {Ip, Port}, Host}.

getq() -> lists:reverse(gen_server:call(?MODULE, get)).

handle_call(get, _From, State) -> {reply, State#state.calls, State};
handle_call(_, _From, State) -> {reply, ok, State}.

handle_cast(_Request, State) -> {noreply, State}.

handle_info(Info, #state{calls = Calls} = State) -> {noreply, State#state{calls = [Info | Calls]}}.

terminate(_Reason, _State) -> ok.

code_change(_OldVsn, State, _Extra) -> {ok, State}.
