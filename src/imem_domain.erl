-module(imem_domain).
-behavior(gen_server).

-include("imem.hrl").
-include("imem_meta.hrl").

-record(ddDomSample,  %% imem helper table for domain samples, used in snapshot transforms to anonymize
                    { domKey                    ::term() 
                    , domPattern                ::term() 
                    , domMin                    ::term()
                    , domMax                    ::term()  
                    , opts                      ::list() 
                    }
       ).
-define(ddDomSample, [term,term,term,term,list]).
-define(DOM_SAMPLE_TABLE_OPTS,[{type,ordered_set}]).

-record(ddDomTrans,  %% imem helper table for domain translations, used in snapshot transforms to anonymize
                    { domKey                    ::term() 
                    , domTrans                  ::term()
                    , domPattern                ::term()  
                    , opts                      ::list() 
                    }
       ).
-define(ddDomTrans, [term,term,term,list]).
-define(DOM_TRANS_TABLE_OPTS,[]).

-record(state, {}).

% gen_server callbacks
-export([ init/1
        , handle_call/3
        , handle_cast/2
        , handle_info/2
        , terminate/2
        , code_change/3
        , format_status/2
        , start_link/1
        ]).

% domain translation functions to be used in snapshot transformations (ToDo: move to imem_domain)
-export([ string_pattern/1        %% binstr() -> {Pattern::list(),LastUnicode::integer()}  
        , string_random/3         %% StrDomain::atom(), ChrDomain::atom(), binstr() -> RandomizedString::string()
        % , integer_random/2     %% integer(), domain::atom() -> RandomizedInteger::integer()
        % , float_random/2       %% float(), domain::atom() -> RandomizedFloat::float()
        ]).


%% Domain stuff (ToDo: move to imem_domain)
string_pattern(B) when is_binary(B) ->
    string_pattern(unicode:characters_to_list(B, utf8 ), [], 0).

string_pattern([], Acc, Uni) ->
    {lists:reverse(Acc),Uni};
string_pattern([32|Rest], Acc, Uni) ->
  string_pattern(Rest, [32|Acc], Uni);                  % emit space 
string_pattern([$-|Rest], Acc, Uni) ->
  string_pattern(Rest, [$-|Acc], Uni);                  % emit dash
string_pattern([$.|Rest], Acc, Uni) ->
  string_pattern(Rest, [$.|Acc], Uni);                  % emit dot
string_pattern([$(|Rest], Acc, Uni) ->
  string_pattern(Rest, [$(|Acc], Uni);                  % emit (
string_pattern([$)|Rest], Acc, Uni) ->
  string_pattern(Rest, [$)|Acc], Uni);                  % emit )
string_pattern([$@|Rest], Acc, Uni) ->
  string_pattern(Rest, [$@|Acc], Uni);                  % emit aterate
string_pattern([$&|Rest], Acc, Uni) ->
  string_pattern(Rest, [$&|Acc], Uni);                  % emit ampersand
string_pattern([$,|Rest], Acc, Uni) ->
  string_pattern(Rest, [$,|Acc], Uni);                  % emit comma
string_pattern([$_|Rest], Acc, Uni) ->
  string_pattern(Rest, [$_|Acc], Uni);                  % emit unicode
string_pattern([Ch|Rest], Acc, _) when Ch>128 ->
  string_pattern(Rest, [$a|Acc], Ch);                   % emit unicode as lowe case
string_pattern([$0|Rest], Acc, Uni) ->
  string_pattern(Rest, [$0|Acc], Uni);                  % emit 0
string_pattern([Ch|Rest], Acc, Uni) when Ch>$0,Ch=<$9 ->
  string_pattern(Rest, [$9|Acc], Uni);                  % emit 9
string_pattern([Ch|Rest], [$a,$a,$a|_]=Acc, Uni) when Ch>96,Ch<123 ->
  string_pattern(Rest, Acc, Uni);                       % emit nothing, ignore lower case
string_pattern([Ch|Rest], Acc, Uni) when Ch>96,Ch<123 ->
  string_pattern(Rest, [$a|Acc], Uni);                  % emit lower case
string_pattern([Ch|Rest], [$A,$A,$A|_]=Acc, Uni) when Ch>64,Ch<91 ->
  string_pattern(Rest, Acc, Uni);                       % emit nothing, ignore upper case
string_pattern([Ch|Rest], Acc, Uni) when Ch>64,Ch<91 ->
  string_pattern(Rest, [$A|Acc], Uni);                  % emit upper case
string_pattern([Ch|Rest], Acc, _) ->
  string_pattern(Rest, Acc, Ch).                        % ignore special character wildcard

string_random(StrDomain, ChrDomain, BStr) when is_binary(BStr),is_atom(StrDomain),is_atom(ChrDomain) ->
  case imem_meta:read(ddDomTrans,{StrDomain,BStr}) of
    [] ->   string_new_random(StrDomain,ChrDomain,BStr);
    [#ddDomTrans{domTrans={StrDomain, Res}}] ->  Res
  end.


%% ----- SERVER INTERFACE ------------------------------------------------
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

init(_) ->

    DsDef = {record_info(fields, ddDomSample),?ddDomSample,#ddDomSample{}},
    catch imem_meta:create_check_table(ddDomSample, DsDef, ?DOM_SAMPLE_TABLE_OPTS, system),

    DtDef = {record_info(fields, ddDomTrans),?ddDomTrans,#ddDomTrans{}},
    catch imem_meta:create_check_table(ddDomTrans, DtDef, ?DOM_TRANS_TABLE_OPTS, system),
    imem_meta:create_or_replace_index(ddDomTrans, domTrans),

    process_flag(trap_exit, true),
    
    {ok,#state{}}.


handle_info(_Info, State) ->
    ?Info("Unknown info ~p!~n", [_Info]),
    {noreply, State}.

handle_call(_Request, _From, State) ->
    ?Info("Unknown request ~p from ~p!~n", [_Request, _From]),
    {reply, ok, State}.

handle_cast(_Request, State) ->
    ?Info("Unknown cast ~p!~n", [_Request]),
    {noreply, State}.

terminate(normal, _State) -> ?Info("~p normal stop~n", [?MODULE]);
terminate(shutdown, _State) -> ?Info("~p shutdown~n", [?MODULE]);
terminate({shutdown, _Term}, _State) -> ?Info("~p shutdown : ~p~n", [?MODULE, _Term]);
terminate(Reason, _State) -> ?Error("~p stopping unexpectedly : ~p~n", [?MODULE, Reason]).

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

format_status(_Opt, [_PDict, _State]) -> ok.

%% ----- PRIVATE APIS ------------------------------------------------

string_new_random(StrDomain, _ChrDomain, <<>>) ->
    ok = imem_meta:write(ddDomTrans, #ddDomTrans{domKey={StrDomain, <<>>}, domTrans={StrDomain, <<>>}, domPattern=[], opts=[]}),
    <<>>;
string_new_random(StrDomain, ChrDomain, BStr) ->
    {StrDomain, K} = imem_meta:dirty_next(ddDomSample,{StrDomain, random:uniform()}),
    [#ddDomSample{domPattern=DP,opts=Opts}] = imem_meta:read(ddDomSample,{StrDomain,K}),
    Res = string_random_chars(StrDomain, ChrDomain, Opts, DP),
    ok = imem_meta:write(ddDomTrans, #ddDomTrans{domKey={StrDomain, BStr}, domTrans={StrDomain, Res}, domPattern=DP, opts=Opts}),
    Res.

string_random_chars(StrDomain, ChrDomain, Opts, DP) ->
    string_random_chars(StrDomain, ChrDomain, Opts, DP, []).

string_random_chars(_StrDomain, _ChrDomain, _Opts, [], Acc) ->
    unicode:characters_to_binary(lists:reverse(Acc), unicode , utf8);
string_random_chars(StrDomain, ChrDomain, Opts, [Ch|Rest], Acc) when Ch==32;Ch==$.;Ch==$,;Ch==$-;Ch==$@ ->
    string_random_chars(StrDomain, ChrDomain, Opts, Rest, [Ch|Acc]);
string_random_chars(StrDomain, ChrDomain, Opts, [Ch|Rest], Acc) when Ch==$(;Ch==$);Ch==$_;Ch==$&;Ch==$0 ->
    string_random_chars(StrDomain, ChrDomain, Opts, Rest, [Ch|Acc]);
string_random_chars(StrDomain, ChrDomain, Opts, [$9|Rest], Acc) ->
    string_random_chars(StrDomain, ChrDomain, Opts, Rest, [$0 + random:uniform(9)|Acc]);
string_random_chars(StrDomain, ChrDomain, Opts, [Ch|Rest], Acc) when Ch==$A;Ch==$a ->
    {ChrDomain, K} = imem_meta:dirty_next(ddDomSample,{ChrDomain, random:uniform()}),
    [#ddDomSample{domPattern=DomPattern,domMin=DomMin}] = imem_meta:read(ddDomSample,{ChrDomain,K}),
    RanCh = case DomPattern of 
        alfaAsciiChar when Ch==$A ->  64 + random:uniform(25);
        alfaAsciiChar when Ch==$a ->  96 + random:uniform(25);
        unicodeChar ->                DomMin    % opts=Opts not used for now
    end,
    string_random_chars(StrDomain, ChrDomain, Opts, Rest, [RanCh|Acc]).

