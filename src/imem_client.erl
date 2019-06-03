-module(imem_client).

-include("imem_client.hrl").

-behavior(gen_server).

-record(state, {}).

-export([ start_link/1
        ]).

-define( APP_AUTH_PROFILE(__Dom, __Acc)
       , ?GET_CONFIG( appAuthProfile
                    , [__Dom, __Acc]
                    , no_auth
                    , "app authentication profile for domain and account id in context. "
                      "for http: {basic,\"username\",\"password\"} or "
                      "{token,\"token\"} or no_auth"
                    )
       ).

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
-export([get_profile/3, get_auth_profile/2, fix_url/1, http/5, http/7]).

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

get_auth_profile(Domain, AccountId) when is_binary(Domain) -> 
    get_auth_profile(binary_to_list(Domain), AccountId);
get_auth_profile(Domain, AccountId) when is_list(Domain) -> 
    ?APP_AUTH_PROFILE(Domain, AccountId).

fix_url(Url) ->
    case re:run(
        Url, "github.com/([^/]+)/([^/]+)/raw/([^/]+)/(.*)",
        [{capture, [1, 2, 3, 4], list}]
    ) of
        {match, [Owner, Repo, Commit, Path]} -> 
            fix_git_raw_url( Owner, Repo, Commit, Path);
        nomatch -> 
            Url
    end.

fix_git_raw_url(Owner, Repo, Commit, Path) ->
    "https://" ++ filename:join([
        "raw.githubusercontent.com/",
        Owner, Repo, Commit, Path
    ]).

http(Op, Url, ReqHeaders, Auth, Body) ->
   http(Op, Url, ReqHeaders, Auth, Body, [], []).

-spec(
    http(
        get | post | put | delete, httpc:url(), httpc:headers(),
        {token, string()} | {basic, string(), string()},
        map() | binary() | undefined,
        httpc:http_options(), httpc:options()
    ) ->
        #{httpVsn       := httpc:http_version(),
          statusCode    := httpc:status_code(),
          reasonPhrase  := httpc:reason_phrase(),
          headers       := httpc:headers(),
          body          := httpc:body() | map()}
        | {error, {invalid_json, httpc:body()}}
).
http(Op, Url, ReqHeaders, Auth, Body, HttpOptions, Options) when is_map(Body) ->
    http(
        Op, {Url, ReqHeaders, "application/json"}, Auth,
        imem_json:encode(Body), HttpOptions, Options
    );
http(Op, Url, ReqHeaders, Auth, Body, HttpOptions, Options) ->
    http(
        Op, {Url, ReqHeaders, "application/text"}, Auth, Body, HttpOptions,
        Options
     ).

http(
  Op, {Url, ReqHeaders, ContentType}, {token, Token}, Body, HttpOptions,
  Options
) ->
    ReqHeaders1 = [{"Authorization","token " ++ Token} | ReqHeaders],
    http(Op, {Url, ReqHeaders1, ContentType, Body}, HttpOptions, Options);
http(
  Op, {Url, ReqHeaders, ContentType}, {basic, User, Password}, Body,
  HttpOptions, Options
) ->
    Encoded = base64:encode_to_string(lists:append([User,":",Password])),
    ReqHeaders1 = [{"Authorization","Basic " ++ Encoded} | ReqHeaders],
    http(Op, {Url, ReqHeaders1, ContentType, Body}, HttpOptions, Options);
http(Op, {Url, ReqHeaders, ContentType}, no_auth, Body, HttpOptions, Options) ->
    http(Op, {Url, ReqHeaders, ContentType, Body}, HttpOptions, Options).

http(Method, {Url, ReqHeaders, _, _}, HttpOptions, Options)
  when Method == get; Method == delete ->
    http_req(Method, {Url, ReqHeaders}, HttpOptions, Options);
http(
  Method, {Url, ReqHeaders, ContentType, Body}, HttpOptions, Options
) when is_binary(Body) andalso (Method == put orelse Method == post) ->
    http_req(Method, {Url, ReqHeaders, ContentType, Body}, HttpOptions, Options).

http_req(Method, Request, HttpOptions, Options) ->
    case httpc:request(
           Method, Request, HttpOptions,
           lists:usort([{body_format, binary}|Options])
    ) of
        {ok, {{HttpVsn, StatusCode, ReasonPhrase}, RespHeaders, RespBody}} ->
            #{httpVsn => HttpVsn, statusCode => StatusCode,
              reasonPhrase => ReasonPhrase, headers => RespHeaders,
              body => parse_http_resp(RespHeaders, RespBody)};
        {error, _} = Error -> Error
    end.

parse_http_resp([{_,_}|_] = Headers, Body) ->
    MaybeContentType =
        lists:filtermap(
            fun({Field, Value}) ->
                case re:run(Field, "^content-type$", [caseless]) of
                    {match, _} ->
                        {true, list_to_binary(string:lowercase(Value))};
                    _ -> false
                end
            end,
            Headers
        ),
    case MaybeContentType of
        [<<"application/json", _/binary>>] ->
            try imem_json:decode(Body, [return_maps])
            catch _:_ -> {error, {invalid_json, Body}}
            end;
        _ -> Body
    end;
parse_http_resp(_, Body) -> Body.
