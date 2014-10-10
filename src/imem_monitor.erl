-module(imem_monitor).

-include("imem.hrl").
-include("imem_meta.hrl").

%% HARD CODED CONFIGURATIONS

-define(MONITOR_TABLE_OPTS,[{record_name,ddMonitor}
                           ,{type,ordered_set}
                           ,{purge_delay,430000}        %% 430000 = 5 Days - 2000 sec
                           ]).  

%% DEFAULT CONFIGURATIONS ( overridden in table ddConfig)

-define(GET_MONITOR_CYCLE_WAIT,?GET_IMEM_CONFIG(monitorCycleWait,[],10000)).
-define(GET_MONITOR_EXTRA,?GET_IMEM_CONFIG(monitorExtra,[],true)).
-define(GET_MONITOR_EXTRA_FUN,?GET_IMEM_CONFIG(monitorExtraFun,[],<<"fun(_) -> [{time,erlang:now()}] end.">>)).
-define(GET_MONITOR_DUMP,?GET_IMEM_CONFIG(monitorDump,[],true)).
-define(GET_MONITOR_DUMP_FUN,?GET_IMEM_CONFIG(monitorDumpFun,[],<<"">>)).


-behavior(gen_server).

-record(state, { extraFun = undefined       :: any()
               , extraHash = undefined      :: any()
               , dumpFun = undefined        :: any()
               , dumpHash = undefined       :: any()
               }
       ).

-export([ start_link/1
        ]).

% gen_server interface (monitoring calling processes)

% gen_server behavior callbacks
-export([ init/1
        , handle_call/3
        , handle_cast/2
        , handle_info/2
        , terminate/2
        , code_change/3
        , format_status/2
        ]).

-export([ write_monitor/0
        , write_monitor/2
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
    try
        catch imem_meta:create_check_table(?MONITOR_TABLE, {record_info(fields, ddMonitor),?ddMonitor, #ddMonitor{}}, ?MONITOR_TABLE_OPTS, system),    
        erlang:send_after(2000, self(), imem_monitor_loop),

        process_flag(trap_exit, true),
        {ok,#state{}}
    catch
        _Class:Reason -> {stop, {Reason,erlang:get_stacktrace()}} 
    end.

handle_call(_Request, _From, State) ->
    {reply, ok, State}.

%     {stop,{shutdown,Reason},State};
% handle_cast({stop, Reason}, State) ->
handle_cast(_Request, State) ->
    {noreply, State}.

handle_info(imem_monitor_loop, #state{extraFun=EF,extraHash=EH,dumpFun=DF,dumpHash=DH} = State) ->
    % save one imem_monitor record and trigger the next one
    % ?Debug("imem_monitor_loop start~n",[]),
    case ?GET_MONITOR_CYCLE_WAIT of
        MCW when (is_integer(MCW) andalso (MCW >= 100)) ->
            {EHash,EFun} = case {?GET_MONITOR_EXTRA, ?GET_MONITOR_EXTRA_FUN} of
                {false, _} ->       {undefined,undefined};
                {true, <<"">>} ->   {undefined,undefined};
                {true, EFStr} ->
                    case erlang:phash2(EFStr) of
                        EH ->   {EH,EF};
                        H1 ->   {H1,imem_meta:compile_fun(EFStr)}
                    end      
            end,
            {DHash,DFun} = case {?GET_MONITOR_DUMP, ?GET_MONITOR_DUMP_FUN} of
                {false, _} ->       {undefined,undefined};
                {true, <<"">>} ->   {undefined,undefined};
                {true, DFStr} ->
                    case erlang:phash2(DFStr) of
                        DH ->   {DH,DF};
                        H2 ->   {H2,imem_meta:compile_fun(DFStr)}
                    end      
            end,
            write_monitor(EFun,DFun),
            erlang:send_after(MCW, self(), imem_monitor_loop),
            {noreply, State#state{extraFun=EFun,extraHash=EHash,dumpFun=DFun,dumpHash=DHash}};
        _ ->
            ?Warn("running idle monitor with default timer ~p", [2000]),
            erlang:send_after(2000, self(), imem_monitor_loop),
            {noreply, State}
    end;
handle_info(_Info, State) ->
    {noreply, State}.

terminate(normal, _State) -> ?Info("~p normal stop~n", [?MODULE]);
terminate(shutdown, _State) -> ?Info("~p shutdown~n", [?MODULE]);
terminate({shutdown, Term}, _State) -> ?Info("~p shutdown : ~p~n", [?MODULE, Term]);
terminate(Reson, _State) -> ?Error("~p stopping unexpectedly : ~p~n", [?MODULE, Reson]).

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

format_status(_Opt, [_PDict, _State]) -> ok.


%% ------ MONITOR implementation -------------------------------------------------------

write_monitor() -> write_monitor(undefined,undefined).

write_monitor(ExtraFun,DumpFun) ->
    try  
        Now = erlang:now(),
        {{input,Input},{output,Output}} = erlang:statistics(io),
        Moni0 = #ddMonitor{ time=Now
                         , node = node()
                         , memory=erlang:memory(total)
                         , process_count=erlang:system_info(process_count)          
                         , port_count=erlang:system_info(port_count)
                         , run_queue=erlang:statistics(run_queue)
                         , wall_clock=element(1,erlang:statistics(wall_clock))
                         , reductions=element(1,erlang:statistics(reductions))
                         , input_io=Input
                         , output_io=Output
                         },
        Moni1 = case ExtraFun of
            undefined -> 
                Moni0;
            ExtraFun -> 
                try
                    Moni0#ddMonitor{extra=ExtraFun(Moni0)}
                catch 
                    _:ExtraError ->
                        ?Error("cannot monitor ~p", [ExtraError]),
                        Moni0
                end
        end,
        imem_meta:dirty_write(?MONITOR_TABLE, Moni1),
        case DumpFun of
            undefined -> 
                ok;
            DumpFun ->
                try 
                    DumpFun(Moni1)
                catch
                    _ : DumpError -> ?Error("cannot dump monitor ~p~n~p", DumpError)
                end
        end
    catch
        _:Err ->
            ?Error("cannot monitor ~p: ~p", [Err, erlang:get_stacktrace()]),
            {error,{"cannot monitor",Err}}
    end.


%% ----- TESTS ------------------------------------------------
-ifdef(TEST).

-include_lib("eunit/include/eunit.hrl").

setup() ->
    ?imem_test_setup.

teardown(_) ->
    ?imem_test_teardown.

db_test_() ->
    {
        setup,
        fun setup/0,
        fun teardown/1,
        {with, [
              fun monitor_operations/1
        ]}}.    

monitor_operations(_) ->
    try 

        ?LogDebug("---TEST---~p:test_monitor~n", [?MODULE]),

        ?assertEqual(ok, write_monitor()),
        MonRecs = imem_meta:read(?MONITOR_TABLE),
        ?LogDebug("MonRecs count ~p~n", [length(MonRecs)]),
        ?LogDebug("MonRecs last ~p~n", [lists:last(MonRecs)]),
        % ?LogDebug("MonRecs[1] ~p~n", [hd(MonRecs)]),
        % ?LogDebug("MonRecs ~p~n", [MonRecs]),
        ?assert(length(MonRecs) > 0),

        ?LogDebug("success ~p~n", [monitor])
    catch
        Class:Reason ->  ?LogDebug("Exception ~p:~p~n~p~n", [Class, Reason, erlang:get_stacktrace()]),
        throw ({Class, Reason})
    end,
    ok.
    
-endif.
