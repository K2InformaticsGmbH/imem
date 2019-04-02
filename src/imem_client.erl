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
-export([get_profile/3, http_get/2, fix_git_raw_url/1, http/5]).

start_link(Params) ->
    ?Info("~p starting...~n", [?MODULE]),
    case gen_server:start_link(
           {local, ?MODULE}, ?MODULE, Params,
           [{spawn_opt, [{fullsweep_after, 0}]}]
    ) of
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

fix_git_raw_url(Url) ->
    case re:run(
        Url, "github.com/([^/]+)/([^/]+)/raw/([^/]+)/(.*)",
        [{capture, [1, 2, 3, 4], list}]
    ) of
        {match, [Owner, Repo, Commit, Path]} ->
            "https://" ++ filename:join([
                "raw.githubusercontent.com/",
                Owner, Repo, Commit, Path
            ]);
        nomatch -> error(bad_url)
    end.

http_get(Url, Token) ->
    case httpc:request(
        get, {Url, [{"Authorization", "token " ++ Token}]},
        [], [{body_format, binary}]
    ) of
        {ok, {{HttpVsn, StatusCode, ReasonPhrase}, RespHeaders, RespBody}} ->
            #{httpVsn => HttpVsn, statusCode => StatusCode,
              reasonPhrase => ReasonPhrase, headers => RespHeaders,
              body => RespBody};
        {error, Error} -> error(Error)
    end.


http(Op, Url, ReqHeaders, Auth, Body) when is_map(Body) ->
    http(
        Op, {Url, ReqHeaders, "application/json"}, Auth, imem_json:encode(Body)
    ).

http(Op, {Url, ReqHeaders, ContentType}, {basic, User, Password}, Body)
    when is_binary(Body)
->
    Encoded = base64:encode_to_string(lists:append([User,":",Password])),
    ReqHeaders1 = [{"Authorization","Basic " ++ Encoded} | ReqHeaders],
    http(Op, {Url, ReqHeaders1, ContentType, Body}).

http(get, {Url, ReqHeaders, _, _}) ->
    http_req(get, {Url, ReqHeaders});
http(post, {Url, ReqHeaders, ContentType, Body}) ->
    http_req(post, {Url, ReqHeaders, ContentType, Body}).

http_req(Method, Request) ->
    case httpc:request(Method, Request, [], [{body_format, binary}]) of
        {ok, {{HttpVsn, StatusCode, ReasonPhrase}, RespHeaders, RespBody}} ->
            #{httpVsn => HttpVsn, statusCode => StatusCode,
              reasonPhrase => ReasonPhrase, headers => RespHeaders,
              body => parse_http_resp(RespHeaders, RespBody)};
        {error, Error} -> error(Error)
    end.

parse_http_resp([{_,_}|_] = RespHeaders, RespBody) ->
    parse_http_resp(maps:from_list(RespHeaders), RespBody);
parse_http_resp(#{"content-type" := ContentType}, Body) ->
    case re:run(ContentType, "application/json") of
        {match, _} ->
            try imem_json:decode(Body, [return_maps]) catch _:_ -> Body end;
        _ -> Body
    end;
parse_http_resp(_, Body) -> Body.
