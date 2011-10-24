%%%-------------------------------------------------------------------
%%% @author André <>
%%% @copyright (C) 2011, André
%%% @doc
%%%
%%% @end
%%% Created : 11 Jul 2011 by André <>
%%%-------------------------------------------------------------------
-module(myoci).

-behaviour(gen_server).

%% API
-export([start_link/0, get_rows/2, get_session/0, execute_sql/4, release_session/1]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-define(SERVER, ?MODULE). 

%%%===================================================================
%%% API
%%%===================================================================
get_rows(ConnectionId,_) ->
    gen_server:call(?SERVER, {get_rows, ConnectionId}). %% ConnectionId == SessionId

get_session() ->
    gen_server:call(?SERVER, get_session).

release_session(SessionId) ->
    gen_server:call(?SERVER, {release_session, SessionId}).

execute_sql(SessionId,_,[_,{_,_,Batchsize}],MaxRowCount) ->
    gen_server:call(?SERVER, {execute_sql, SessionId, Batchsize, MaxRowCount}).
    
%%--------------------------------------------------------------------
%% @doc
%% Starts the server
%%
%% @spec start_link() -> {ok, Pid} | ignore | {error, Error}
%% @end
%%--------------------------------------------------------------------
start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Initializes the server
%%
%% @spec init(Args) -> {ok, State} |
%%                     {ok, State, Timeout} |
%%                     ignore |
%%                     {stop, Reason}
%% @end
%%--------------------------------------------------------------------
init([]) ->
    random:seed(now()),
    ets:new(ocimock, [named_table]),
    {ok, 0}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling call messages
%%
%% @spec handle_call(Request, From, State) ->
%%                                   {reply, Reply, State} |
%%                                   {reply, Reply, State, Timeout} |
%%                                   {noreply, State} |
%%                                   {noreply, State, Timeout} |
%%                                   {stop, Reason, Reply, State} |
%%                                   {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_call({execute_sql, SessionId, BatchSize, MaxRowCount}, _From, _State) ->
    Records = generate_random_records([], BatchSize),
    [{SessionId, SessionData}] = ets:lookup(ocimock, SessionId),
    ets:insert(ocimock, {SessionId, SessionData ++ Records}),
    {reply, {ok, SessionId, statement_handle}, MaxRowCount};
handle_call({get_rows, SessionId}, _From, MaxRowCount) ->
    [{SessionId, Records}] = ets:lookup(ocimock, SessionId),    
    {Reply, Rest} = create_answer(Records, MaxRowCount),
    ets:insert(ocimock, {SessionId,Rest}),
    {reply, Reply, MaxRowCount};
handle_call(get_session, _From, State) ->
    SessionId = erlang:phash2(erlang:now()),
    ets:insert(ocimock, {SessionId, []}),
    {reply, {ok, SessionId}, State};
handle_call({release_session, SessionId}, _From, State) ->
    ets:delete(ocimock, SessionId),
    {reply, ok, State}.



%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling cast messages
%%
%% @spec handle_cast(Msg, State) -> {noreply, State} |
%%                                  {noreply, State, Timeout} |
%%                                  {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_cast(_Msg, State) ->
    {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling all non call/cast messages
%%
%% @spec handle_info(Info, State) -> {noreply, State} |
%%                                   {noreply, State, Timeout} |
%%                                   {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_info(_Info, State) ->
    {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any
%% necessary cleaning up. When it returns, the gen_server terminates
%% with Reason. The return value is ignored.
%%
%% @spec terminate(Reason, State) -> void()
%% @end
%%--------------------------------------------------------------------
terminate(_Reason, _State) ->
    shutdown.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Convert process state when code is changed
%%
%% @spec code_change(OldVsn, State, Extra) -> {ok, NewState}
%% @end
%%--------------------------------------------------------------------
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

generate_random_records(Acc, NrToGenerate) when NrToGenerate > 0 ->
    generate_random_records([["1","1","1","1", random_msisdn()]] ++ Acc, NrToGenerate -1);
generate_random_records(Acc, 0) -> Acc.

random_msisdn() ->
    R = random_between(1000000, 9999999),
    "4179" ++ integer_to_list(R).

random_between(N,M) ->
    N + random:uniform(M-N).

create_answer(Records, NrOfRows) when NrOfRows > length(Records)->
    {{done, Records}, []};
create_answer(Records, NrOfRows) ->
    {A,B} = lists:split(NrOfRows, Records),
    {{more, A}, B}.

