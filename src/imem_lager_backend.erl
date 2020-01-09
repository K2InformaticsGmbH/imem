-module(imem_lager_backend).

-behaviour(gen_event).

-include("imem_meta.hrl").

%% gen_event callbacks

-export([init/1, handle_event/2, handle_call/2, handle_info/2, terminate/2, code_change/3]).
-export([trace/1, trace/2, config_to_id/1]).

-record(state, {tn_event, level = info, application, is_initialized = false, table}).

%%%===================================================================
%%% trace
%%%===================================================================

trace(Filter) -> trace(Filter, debug).

trace(Filter, Level) ->
    Trace0 = {Filter, Level, ?MODULE},
    case lager_util:validate_trace(Trace0) of
        {ok, Trace} ->
            {MinLevel, Traces} = lager_config:get(loglevel),
            case lists:member(Trace, Traces) of
                false -> lager_config:set(loglevel, {MinLevel, [Trace | Traces]});
                _ -> ok
            end,
            {ok, Trace};
        Error -> Error
    end.

%%%===================================================================
%%% gen_event callbacks
%%%===================================================================

init(Params) ->
    State = state_from_params(#state{}, Params),
    {ok, State}.

%% lager is started before imem, and this event handler is initialized
%% before imem is stared. So we have to wait till imem is started to
%% create the table or reading the config.

handle_event({log, LagerMsg}, #state{is_initialized = false, application = App} = State) ->
    case proplists:get_value(application, lager_msg:metadata(LagerMsg)) of
        App ->
            try
                Table = (State#state.table) (),
                ok = create_check_ddLog(Table),
                imem_meta:subscribe({table, ddConfig, simple}),
                handle_event({log, LagerMsg}, State#state{table = Table, is_initialized = true})
            catch
                _ : {badmatch, {error, {already_exists, {table, ddConfig, simple}}}} ->
                    io:format(user, "[warning] ~p:~p:~p re-subscription attempt~n", [?MODULE, ?FUNCTION_NAME, ?LINE]),
                    handle_event({log, LagerMsg}, State#state{table = (State#state.table) (), is_initialized = true});
                _:Exception ->
                    io:format(user, "[error] ~p:~p:~p ~p~n", [?MODULE, ?FUNCTION_NAME, ?LINE, Exception]),
                    {ok, State}
            end;
        _ ->
            %% application message is not obtained so waiting to initialize
            {ok, State}
    end;
handle_event({log, LagerMsg}, #state{application = App, table = Table, level = LogLevel} = State) ->
    case lager_util:is_loggable(LagerMsg, LogLevel, ?MODULE) of
        true ->
            Level = lager_msg:severity_as_int(LagerMsg),
            Message = lager_msg:message(LagerMsg),
            Metadata = lager_msg:metadata(LagerMsg),
            Mod = proplists:get_value(module, Metadata),
            StackTrace = proplists:get_value(stacktrace, Metadata, []),
            case proplists:get_value(application, Metadata) of
                App ->
                    Fun = proplists:get_value(function, Metadata),
                    Line = proplists:get_value(line, Metadata),
                    Pid = proplists:get_value(pid, Metadata),
                    Fields =
                        lists:filtermap(
                            fun
                                ({node, _}) -> false;
                                ({application, _}) -> false;
                                ({module, _}) -> false;
                                ({function, _}) -> false;
                                ({line, _}) -> false;
                                ({pid, _}) -> false;
                                ({imem_table, _}) -> false;
                                ({stacktrace, _}) -> false;
                                ({enum, V}) -> {true, V};
                                (_) -> true
                            end,
                            Metadata
                        ),
                    NPid =
                        if
                            is_list(Pid) -> list_to_pid(Pid);
                            true -> Pid
                        end,
                    EntryTuple =
                        list_to_tuple(
                            [
                                ddLog,
                                ?TIME_UID,
                                lager_util:num_to_level(Level),
                                NPid,
                                Mod,
                                Fun,
                                Line,
                                node(),
                                Fields,
                                list_to_binary(Message),
                                StackTrace
                            ]
                        ),
                    try
                        imem_meta:dirty_write(Table, EntryTuple)
                    catch
                        _:Error ->
                            io:format(
                                user,
                                "[error] ~p:~p:~p failed to write to ~p, ~p~n",
                                [?MODULE, ?FUNCTION_NAME, ?LINE, Table, Error]
                            )
                    end;
                _ ->
                    %% not a log event from the associated application
                    no_op
            end;
        false -> no_op
    end,
    {ok, State};
handle_event({lager_imem_options, Params}, State) -> {ok, state_from_params(State, Params)};
handle_event(_Event, State) -> {ok, State}.

handle_call({set_loglevel, Level}, State) -> {ok, ok, State#state{level = lager_util:level_to_num(Level)}};
handle_call(get_loglevel, State = #state{level = Level}) -> {ok, Level, State}.

handle_info(
    {mnesia_table_event, {write, {ddConfig, Match, Table, _, _}, _}},
    #state{tn_event = Match, table = OldTable} = State
) ->
    io:format(
        user,
        "[info] ~p:~p:~p changing default table from ~p to ~p~n",
        [?MODULE, ?FUNCTION_NAME, ?LINE, OldTable, Table]
    ),
    create_check_ddLog(Table),
    {ok, State#state{table = Table}};
handle_info(_Info, State) ->
    %% we'll get (unused) log rotate messages
    {ok, State}.

terminate(_Reason, _State) -> ok.

code_change(_OldVsn, State, _Extra) -> {ok, State}.

config_to_id(Config) -> {?MODULE, proplists:get_value(application, Config)}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

state_from_params(OrigState = #state{level = OldLevel, application = OldApplication, tn_event = OldTableEvent}, Params) ->
    TableNameFunc =
        case proplists:get_value(table_name_fun, Params) of
            {Mod, Fun} -> fun Mod:Fun/0;
            _ -> exit({badarg, missing_table_name_fun})
        end,
    Level = proplists:get_value(level, Params, OldLevel),
    TableEvent = proplists:get_value(tn_event, Params, OldTableEvent),
    Application = proplists:get_value(application, Params, OldApplication),
    OrigState#state{
        level = lager_util:level_to_num(Level),
        table = TableNameFunc,
        tn_event = TableEvent,
        application = Application
    }.

create_check_ddLog(Name) ->
    case
    catch
    imem_meta:create_check_table(
        Name,
        {record_info(fields, ddLog), ?ddLog, #ddLog{}},
        [{record_name, element(1, #ddLog{})}, {type, ordered_set}, {purge_delay, 430000}],
        lager_imem
    ) of
        {ok, _} -> ok;
        {'ClientError', {"Table already exists", _}} -> ok;
        Result -> Result
    end.
