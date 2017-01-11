-module(imem_config).

-include("imem.hrl").
-include("imem_config.hrl").

-behavior(gen_server).

-record(state, {}).

% gen_server behavior callbacks
-export([start_link/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
         code_change/3, format_status/2]).

-export([get_config_hlk/6, put_config_hlk/6, put_config_hlk/7]).

-define(CONFIG_TABLE_OPTS, [{record_name,ddConfig},{type,ordered_set}]).

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
        _Class:Reason -> {stop, {Reason,erlang:get_stacktrace()}}
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

get_config_hlk(Table, Key, Owner, Context, Default, _Documentation) ->
    get_config_hlk(Table, Key, Owner, Context, Default).
get_config_hlk({_Schema,Table}, Key, Owner, Context, Default) ->
    get_config_hlk(Table, Key, Owner, Context, Default);
get_config_hlk(Table, Key, Owner, Context, Default) when is_atom(Table), is_list(Context), is_atom(Owner) ->
    Remark = list_to_binary(["auto_provisioned from ",io_lib:format("~p",[Context])]),
    case (catch imem_meta:read_hlk(Table, [Key|Context])) of
        [] ->                                   
            %% no value found, create global config with default value
            catch put_config_hlk(Table, Key, Owner, [], Default, Remark),
            Default;
        [#ddConfig{val=Default, hkl=[Key]}] ->    
            %% global config is relevant and matches default
            Default;
        [#ddConfig{val=OldVal, hkl=[Key], remark=R, owner=DefOwner}] ->
            %% global config is relevant and differs from default
            case binary:longest_common_prefix([R,<<"auto_provisioned">>]) of
                16 ->
                    %% comment starts with default comment may be overwrite
                    case {DefOwner, Owner} of
                        _ when
                                  ((?MODULE     =:= DefOwner)
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
        [#ddConfig{val=Val}] ->
            %% config value is overridden by user, return that value
            Val;
        _ ->
            %% fallback in case ddConf is deleted in a running system
            Default
    end.

put_config_hlk(Table, Key, Owner, Context, Value, Remark, _Documentation) ->
    put_config_hlk(Table, Key, Owner, Context, Value, Remark).
put_config_hlk({_Schema,Table}, Key, Owner, Context, Value, Remark) ->
    put_config_hlk(Table, Key, Owner, Context, Value, Remark);
put_config_hlk(Table, Key, Owner, Context, Value, Remark)
  when is_atom(Table), is_list(Context), is_binary(Remark) ->
    imem_meta:dirty_write(Table, #ddConfig{hkl=[Key|Context], val=Value,
                                           remark=Remark, owner=Owner}).


%% ----- TESTS ------------------------------------------------
-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
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
