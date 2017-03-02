-module(imem_config).

-include("imem.hrl").
-include("imem_config.hrl").
-include("imem_exception.hrl").
-include("imem_meta.hrl").

-behavior(gen_server).

-record(state, {}).

% gen_server behavior callbacks
-export([start_link/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
         code_change/3, format_status/2]).

-export([get_config_hlk/6, put_config_hlk/6, put_config_hlk/7]).

-export([encrypt/1, decrypt/1, reference_resolve/1, reference_resolve/2,
         val/2]).

-safe(val/2).

-define(CONFIG_TABLE_OPTS, [{record_name, ddConfig}, {type, ordered_set}]).

start_link(Params) ->
    ?Info("~p starting...~n", [?MODULE]),
    case gen_server:start_link({local, ?MODULE}, ?MODULE, Params,
                               [{spawn_opt, [{fullsweep_after, 0}]}]) of
        {ok, _} = Success ->
            ?Info("~p started!~n", [?MODULE]),
            Success;
        Error ->
            ?Error("~p failed to start ~p~n", [?MODULE, Error]),
            Error
    end.

init(_Args) ->
    try
        imem_meta:init_create_check_table(
          ?CONFIG_TABLE,
          {record_info(fields, ddConfig),?ddConfig, #ddConfig{}},
          ?CONFIG_TABLE_OPTS, system),
        process_flag(trap_exit, true),
        {ok, #state{}}
    catch
        _Class:Reason -> {stop, {Reason, erlang:get_stacktrace()}}
    end.

handle_call(_Request, _From, State) -> {reply, ok, State}.
handle_cast(_Request, State) -> {noreply, State}.
handle_info(_Info, State) -> {noreply, State}.

terminate(normal, _State) -> ?Info("~p normal stop~n", [?MODULE]);
terminate(shutdown, _State) -> ?Info("~p shutdown~n", [?MODULE]);
terminate({shutdown, Term}, _State) -> ?Info("~p shutdown : ~p~n", [?MODULE, Term]);
terminate(Reason, _State) -> ?Error("~p stopping unexpectedly : ~p~n", [?MODULE, Reason]).

code_change(_OldVsn, State, _Extra) -> {ok, State}.
format_status(_Opt, [_PDict, _State]) -> ok.

val(Table, #ddConfig{hkl = K, val = V}) ->
    case imem_meta:read(Table, K) of
        [] -> V;
        [#ddConfig{hkl = K, val = OV}] ->
            case {type(OV), type(V)} of
                {T,T} ->
                    case V of
                        [FV|force] -> FV;
                        _ -> V
                    end;
                {OT,NT} ->
                    case V of
                        [_|{enc,_}] -> V;
                        [FV|force] -> FV;
                        _ ->
                            ?Error("Attempted type conversion from ~p to ~p", [OT, NT]),
                            ?Error("New value ~p", [V]),
                            ?ClientError({"Type conversion not allowed without 'force' flag", OT, NT})
                    end
            end
    end.

type(V) when is_atom        (V) -> atom;
type(V) when is_binary      (V) -> binary;
type(V) when is_bitstring   (V) -> bitstring;
type(V) when is_boolean     (V) -> boolean;
type(V) when is_float       (V) -> float;
type(V) when is_function    (V) -> function;
type(V) when is_function    (V) -> function;
type(V) when is_integer     (V) -> integer;
type(V) when is_list        (V) -> list;
type(V) when is_map         (V) -> map;
type(V) when is_pid         (V) -> pid;
type(V) when is_port        (V) -> port;
type(V) when is_reference   (V) -> reference;
type(V) when is_tuple       (V) -> tuple;
type(V) -> ?ClientError({"Value type unrecognized", V}).

get_config_hlk(Table, Key, Owner, Context, Default, _Documentation) ->
    get_config_hlk(Table, Key, Owner, Context, Default).
get_config_hlk({_Schema,Table}, Key, Owner, Context, Default) ->
    get_config_hlk(Table, Key, Owner, Context, Default);
get_config_hlk(Table, Key, Owner, Context, Default) when is_atom(Table), is_list(Context), is_atom(Owner) ->
    Remark = list_to_binary(["auto_provisioned from ",io_lib:format("~p",[Context])]),
    reference_resolve(
      Table,
      case (catch imem_meta:read_hlk(Table, [Key|Context])) of
          %% no value found, create global config with default value
          [] ->
              catch put_config_hlk(Table, Key, Owner, [], Default, Remark),
              Default;
          %% global config is relevant and matches default
          [#ddConfig{val=Default, hkl=[Key]}] ->
              Default;
          %% global config is relevant and differs from default
          [#ddConfig{val=OldVal, hkl=[Key], remark=R, owner=DefOwner}] ->
              case binary:longest_common_prefix([R,<<"auto_provisioned">>]) of
                  16 ->
                      %% comment starts with default comment may be overwrite
                      case {DefOwner, Owner} of
                          _ when ((?MODULE     =:= DefOwner)
                           orelse (Owner       =:= DefOwner)
                           orelse (undefined   =:= DefOwner)) ->
                              %% was created by imem_meta and/or same module
                              %% overwrite the default
                              catch put_config_hlk(Table, Key, Owner, [], Default, Remark),
                              Default;
                          _ ->
                              %% being accessed by non creator, protect creator's config value
                              OldVal
                      end;
                  _ ->
                      %% comment was changed by user, protect his config value
                      OldVal
              end;
          %% config value is overridden by user, return that value
          [#ddConfig{val=Val}] ->
              Val;
          %% fallback in case ddConf is deleted in a running system
          _ ->
              Default
      end).

put_config_hlk(Table, Key, Owner, Context, Value, Remark, _Documentation) ->
    put_config_hlk(Table, Key, Owner, Context, Value, Remark).
put_config_hlk({_Schema,Table}, Key, Owner, Context, Value, Remark) ->
    put_config_hlk(Table, Key, Owner, Context, Value, Remark);
put_config_hlk(Table, Key, Owner, Context, Value, Remark)
  when is_atom(Table), is_list(Context), is_binary(Remark) ->
    imem_meta:dirty_write(Table, #ddConfig{hkl=[Key|Context], val=Value,
                                           remark=Remark, owner=Owner}).

encrypt(Val) ->
    {_, EVal} = crypto:stream_encrypt(
                  crypto:stream_init(
                    rc4, atom_to_list(erlang:get_cookie())),
                  term_to_binary(Val)),
    [base64:encode(EVal)|{enc,0}].

decrypt([B64Val|{enc, 0}]) ->
    Val = base64:decode(B64Val),
    {_, ValBin} = crypto:stream_decrypt(
                      crypto:stream_init(
                        rc4, atom_to_list(erlang:get_cookie())),
                      Val),
    binary_to_term(ValBin);
decrypt(UnEncryptedVal) -> UnEncryptedVal.

reference_resolve(Term) -> reference_resolve(?CONFIG_TABLE, Term).

reference_resolve(Table, [ConfigKey|ref]) ->
    case (catch imem_meta:read_hlk(Table, ConfigKey)) of
        [#ddConfig{val=ResolvedRef}] ->
            reference_resolve(Table, ResolvedRef);
        _ -> ?ClientError({"Reference not found", ConfigKey})
    end;
reference_resolve(Table, [_|{enc,_}] = Val) ->
   reference_resolve(Table, decrypt(Val));
reference_resolve(Table, Val) when is_map(Val) ->
    maps:map(fun(_K, V) -> reference_resolve(Table, V) end, Val);
reference_resolve(Table, Val) when is_list(Val) ->
    lists:map(fun(V) -> reference_resolve(Table, V) end, Val);
reference_resolve(Table, Val) when is_tuple(Val) ->
    list_to_tuple(reference_resolve(Table, tuple_to_list(Val)));
reference_resolve(_Table, Val) -> Val.

%% ----- TESTS ------------------------------------------------
-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

encrypt_test_() ->
    {inparallel,
     [{P,?_assertEqual(D, decrypt(encrypt(D)))}
      || {P, D} <-
         [{"atom",      atom},
          {"int",       1},
          {"float",     1.9},
          {"ref",       make_ref()},
          {"pid",       self()},
          {"list",      [1,a,3.4]},
          {"binary",    <<"binary">>},
          {"fun",       fun() -> function end},
          {"map",       #{a => b}},
          {"tuple",     {1,a,3.4}}]
     ]}.

setup() ->
    ?imem_test_setup.

teardown(_) ->
    catch imem_meta:drop_table(test_config),
    ?imem_test_teardown.

db_1_test_() ->
    {
        setup,
        fun setup/0,
        fun teardown/1,
        {with, [fun config_operations/1]}
    }.    

config_operations(_) ->
    try 
        ?LogDebug("---TEST--- ~p()", [config_operations]),

        ?assertEqual(ok, imem_meta:create_table(test_config, {record_info(fields, ddConfig),?ddConfig, #ddConfig{}}, ?CONFIG_TABLE_OPTS, system)),
        ?assertEqual(test_value,get_config_hlk(test_config, {?MODULE,test_param}, test_owner, [test_context], test_value)),
        ?assertMatch([#ddConfig{hkl=[{?MODULE,test_param}],val=test_value}],imem_meta:read(test_config)), %% default created, owner set
        ?assertEqual(test_value,get_config_hlk(test_config, {?MODULE,test_param}, not_test_owner, [test_context], other_default)),
        ?assertMatch([#ddConfig{hkl=[{?MODULE,test_param}],val=test_value}],imem_meta:read(test_config)), %% default not overwritten, wrong owner
        ?assertEqual(test_value1,get_config_hlk(test_config, {?MODULE,test_param}, test_owner, [test_context], test_value1)),
        ?assertMatch([#ddConfig{hkl=[{?MODULE,test_param}],val=test_value1}],imem_meta:read(test_config)), %% new default overwritten by owner
        ?assertEqual(ok, put_config_hlk(test_config, {?MODULE,test_param}, test_owner, [],test_value2,<<"Test Remark">>)),
        ?assertEqual(test_value2,get_config_hlk(test_config, {?MODULE,test_param}, test_owner, [test_context], test_value3)),
        ?assertMatch([#ddConfig{hkl=[{?MODULE,test_param}],val=test_value2}],imem_meta:read(test_config)),
        ?assertEqual(ok, put_config_hlk(test_config, {?MODULE,test_param}, test_owner, [test_context],context_value,<<"Test Remark">>)),
        ?assertEqual(context_value,get_config_hlk(test_config, {?MODULE,test_param}, test_owner, [test_context], test_value)),
        ?assertEqual(context_value,get_config_hlk(test_config, {?MODULE,test_param}, test_owner, [test_context,details], test_value)),
        ?assertEqual(test_value2,get_config_hlk(test_config, {?MODULE,test_param}, test_owner, [another_context,details], another_value)),
        % ?LogDebug("success ~p~n", [get_config_hlk]),

        ?assertEqual(ok, imem_meta:drop_table(test_config)),
        ok
    catch
        Class:Reason ->     
            timer:sleep(100),
            ?LogDebug("Exception ~p:~p~n~p~n", [Class, Reason, erlang:get_stacktrace()]),
            throw({Class, Reason})
    end,
    ok.

-endif.
