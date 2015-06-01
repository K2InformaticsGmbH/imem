-module(imem_client).

-include("imem_client.hrl").


-behavior(gen_server).

-record(state, {}).

-export([ start_link/1
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

% Library APIs
-export([ get_profile/3
        ]).

start_link(Params) ->
    ?Info("~p starting...~n", [?MODULE]),
    case gen_server:start_link({local, ?MODULE}, ?MODULE, Params, [{spawn_opt, [{fullsweep_after, 0}]}]) of
        {ok, _} = Success ->
            ?Info("~p started!~n", [?MODULE]),
            Success;
        Error ->
            ?Error("~p failed to start ~p~n", [?MODULE, Error]),
            Error
    end.

init(_Args) ->
    {ok,#state{}}.

handle_call(_Request, _From, State) -> {reply, ok, State}.
handle_cast(_Request, State)        -> {noreply, State}.
handle_info(_Info, State)           -> {noreply, State}.

terminate(Reason, _State) ->
    case Reason of
        normal -> ?Info("~p normal stop~n", [?MODULE]);
        shutdown -> ?Info("~p shutdown~n", [?MODULE]);
        {shutdown, _Term} -> ?Info("~p shutdown : ~p~n", [?MODULE, _Term]);
        _ -> ?Error("~p stopping unexpectedly : ~p~n", [?MODULE, Reason])
    end.

code_change(_OldVsn, State, _Extra) -> {ok, State}.
format_status(_Opt, [_PDict, _State]) -> ok.


get_profile(Mod, Profile, Options) ->
    case Mod:get_options(all, Profile) of
        {error, inets_not_started} ->
            {ok, _Pid} = inets:start(Mod, [{profile, Profile}]),
            ok = Mod:set_options(Options, Profile),
            ok = imem_cache:write({?MODULE, Mod, Profile}, Options);
        _ ->
            case imem_cache:read({?MODULE,Mod,Profile}) of
                [Options] -> Profile;
                _Cache ->
                    ok = inets:stop(Mod, Profile),
                    {ok, _Pid} = inets:start(Mod, [{profile, Profile}]),
                    ok = Mod:set_options(Options, Profile),
                    ok = imem_cache:write({?MODULE, Mod, Profile}, Options)
            end
    end,
    Profile.
