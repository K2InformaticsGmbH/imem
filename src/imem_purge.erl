-module(imem_purge).

-include("imem.hrl").
-include("imem_meta.hrl").

%% HARD CODED CONFIGURATIONS

-define(GET_PURGE_CYCLE_WAIT,?GET_CONFIG(purgeCycleWait,[],10000,"Wait time in msec between table purge cycles.")).     %% 10000 = 10 sec
-define(GET_PURGE_ITEM_WAIT,?GET_CONFIG(purgeItemWait,[],10,"Wait time in msec between individual table purges within a cycle.")).          %% 10 = 10 msec
-define(GET_PURGE_SCRIPT,?GET_CONFIG(purgeScript,[],false,"Do we want to use a special purge function to override the standard behaviour?")).
-define(GET_PURGE_SCRIPT_FUN,?GET_CONFIG(purgeScriptFun,[],
<<"fun (PartTables) ->
	MAX_TABLE_COUNT_PERCENT = 90,
	MIN_FREE_MEM_PERCENT = 40,
	TABLE_EXPIRY_MARGIN_SEC = -200,
	imem_purge:try_cleanup(MIN_FREE_MEM_PERCENT,
			       TABLE_EXPIRY_MARGIN_SEC, MAX_TABLE_COUNT_PERCENT,
			       PartTables)
end
">>,"Function used for tailoring the purge strategy to the system's needs.")).

-behavior(gen_server).

-record(state, {
                 purgeList=[]               :: list()
               , purgeFun = undefined       :: any()
               , purgeHash = undefined      :: any()
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

-export([try_cleanup/4]).

-safe(try_cleanup/4).

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
    erlang:send_after(10000, self(), purge_partitioned_tables),

    process_flag(trap_exit, true),
    {ok,#state{}}.

handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast(_Request, State) ->
    {noreply, State}.

handle_info(purge_partitioned_tables, State=#state{purgeFun=PF,purgeHash=PH,purgeList=[]}) ->
    % restart purge cycle by collecting list of candidates
    ?Debug("Purge collect start~n",[]), 
    case ?GET_PURGE_CYCLE_WAIT of
        PCW when (is_integer(PCW) andalso PCW > 1000) ->    
            Pred = fun imem_meta:is_local_or_schema_time_partitioned_table/1,
            case lists:sort(lists:filter(Pred,imem_meta:all_tables())) of
                [] ->   
                    erlang:send_after(PCW, self(), purge_partitioned_tables),
                    {noreply, State};
                PL ->   
                    {PHash,PFun} = case {?GET_PURGE_SCRIPT, ?GET_PURGE_SCRIPT_FUN} of
                        {false, _} ->       {undefined,undefined};
                        {true, <<"">>} ->   {undefined,undefined};
                        {true, PFStr} ->
                            case erlang:phash2(PFStr) of
                                PH ->   {PH,PF};        % existing compiled purge fun (may be undefined)
                                H1 ->   
                                    case (catch imem_compiler:compile(PFStr)) of
                                        CPF when is_function(CPF) ->  
                                            {H1,CPF};   % new compiled purge fun
                                        Err ->
                                            ?Error("Purge script fun compile failed with reason ~p",[Err]),
                                            {H1,undefined}
                                    end
                            end      
                    end,
                    try
                        case PFun of
                            undefined ->    ok;
                            P ->            P(PL)   % execute "emergency purge"
                        end
                    catch
                        _:Reason -> ?Error("Purge script Fun failed with reason ~p~n",[Reason])
                    end,
                    handle_info({purge_partitioned_tables, PCW, ?GET_PURGE_ITEM_WAIT}, State#state{purgeFun=PFun,purgeHash=PHash,purgeList=PL})   
            end;
        BadPCW ->
            ?Error("Invalid purge cycle wait time ~p",[BadPCW]),  
            erlang:send_after(10000, self(), purge_partitioned_tables),
            {noreply, State}
    end;
handle_info({purge_partitioned_tables,PurgeCycleWait,PurgeItemWait}, State=#state{purgeList=[Tab|Rest]}) ->
    % process one purge candidate
    ?Debug("Purge try table ~p~n",[Tab]), 
    case imem_if_mnesia:read(ddTable,{imem_meta:schema(), Tab}) of
        [] ->   
            ?Debug("Table deleted before it could be purged ~p~n",[Tab]); 
        [#ddTable{opts=Opts}] ->
            case lists:keyfind(purge_delay, 1, Opts) of
                false ->
                    ok;             %% no purge delay in table create options, do not purge this file
                {purge_delay,PD} ->
                    {Mega,Sec,_} = os:timestamp(),
                    PurgeEnd=1000000*Mega+Sec-PD,
                    [_,_,_,"_",PE,"@",_] = imem_meta:parse_table_name(Tab),
                    PartitionEnd=list_to_integer(PE),
                    if
                        (PartitionEnd >= PurgeEnd) ->
                            ok;     %% too young, do not purge this file  
                        true ->                     
                            ?Info("Purge time partition ~p~n",[Tab]), %% cannot log here
                            % FreedMemory = table_memory(Tab),
                            % Fields = [{table,Tab},{table_size,table_size(Tab)},{table_memory,FreedMemory}],   
                            % log_to_db(info,?MODULE,purge_time_partitioned_table,Fields,"purge table"), %% cannot log here
                            catch imem_meta:drop_table(Tab)
                    end
            end
    end,  
    case Rest of
        [] ->   erlang:send_after(PurgeCycleWait, self(), purge_partitioned_tables),
                {noreply, State#state{purgeList=[]}};
        Rest -> erlang:send_after(PurgeItemWait, self(), {purge_partitioned_tables,PurgeCycleWait,PurgeItemWait}),
                {noreply, State#state{purgeList=Rest}}
    end;
handle_info(_Info, State) ->
    {noreply, State}.

terminate(normal, _State) -> ?Info("~p normal stop~n", [?MODULE]);
terminate(shutdown, _State) -> ?Info("~p shutdown~n", [?MODULE]);
terminate({shutdown, _Term}, _State) -> ?Info("~p shutdown : ~p~n", [?MODULE, _Term]);
terminate(Reason, _State) -> ?Error("~p stopping unexpectedly : ~p~n", [?MODULE, Reason]).

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

format_status(_Opt, [_PDict, _State]) -> ok.

-spec try_cleanup(integer(),integer(),integer(),list()) -> any().
try_cleanup(MinMemFreePerCent, TableExpiryMarginSec,
            MaxTableCountPercent, PartTables) ->
	%[{time_to_part_expiry,table_size,partition_time,table}]
	SortedPartTables = lists:sort([{imem_meta:time_to_partition_expiry(T),
                                    imem_meta:table_size(T),
                                    lists:nth(3, imem_meta:parse_table_name(T)),
                                    T} || T <- PartTables]),
	{Os, FreeMemory, TotalMemory} = imem:get_os_memory(),
	MemFreePerCent = FreeMemory / TotalMemory * 100,
	%?Info("[~p] Free ~p%", [Os, MemFreePerCent]),
	if MemFreePerCent < MinMemFreePerCent ->
           ?Info("Free mem ~p% required min ~p%",
                 [MemFreePerCent, MinMemFreePerCent]),
	       %?Info("Possible purging canditate tables ~p",
           %      [SortedPartTables]),
	       DelCandidates =
           lists:foldl(
             fun ({TRemain, _RCnt, Class, _TName} = Itm, A) ->
                     if TRemain < TableExpiryMarginSec ->
                            ClassCnt =
                            length([Spt || Spt <- SortedPartTables,
                                           element(3, Spt) =:= Class]),
                            if ClassCnt > 1 -> [Itm | A];
                               true -> A
                            end;
                        true -> A
                     end
             end, [], SortedPartTables),
           case DelCandidates of
               [] ->
                   %[{time_to_part_expiry,table_size,partition_time,table}]
                   [{_, _, _, T} | _] =
                   lists:sort(fun ({_,R1,_,_}, {_,R2,_,_}) when R1>R2 -> true;
                                  (_, _) -> false
                              end, SortedPartTables),
                   imem_meta:log_to_db(
                     info, imem_meta, purgeScriptFun,
                     [{table, T}, {memFreePerCent, MemFreePerCent}],
                     "truncate table"),
                   % imem_meta:truncate_table(T),
                   % ?Info("[~p] Truncated table ~p", [Os, T]);
                   ?Info("[~p] table ~p need truncation", [Os, T]);
              [{_, _, _, T} | _] ->
                   imem_meta:log_to_db(
                     info, imem_meta, purgeScriptFun,
                     [{table, T}, {memFreePerCent, MemFreePerCent}],
                     "drop table"),
                   imem_meta:drop_table(T),
                   ?Info("[~p] Deleted table ~p", [Os, T])
           end;
       true ->
	       {MaxTablesConfig, CurrentTableCount} = imem_meta:get_tables_count(),
           MaxTablesCount = MaxTablesConfig * MaxTableCountPercent / 100,
           if CurrentTableCount =< MaxTablesCount ->
                  % Nothing to drop yet
                  ?Debug("No Purge: CurrentTableCount ~p (used ~p% of ~p)",
                        [CurrentTableCount,
                         round(CurrentTableCount / MaxTablesConfig * 100),
                         MaxTablesConfig]);
              true ->
                  SelectCount = round(CurrentTableCount - MaxTablesCount),
                  if SelectCount > 0 ->
                         ExpiredSortedPartTables =
                         lists:filter(
                           fun ({T,_,_,_}) ->
                                   if T < 0 -> true;
                                      true -> false
                                   end
                           end, SortedPartTables),
                         RSortedPartTables =
                         lists:sort(
                           fun ({_,R1,_,_}, {_,R2,_,_}) when R1 < R2 -> true;
                               (_, _) -> false
                           end, ExpiredSortedPartTables),
                         DropCandidates =
                         [T || {_,_,_,T} <-
                      %[{time_to_part_expiry,table_size,partition_time,table}]
                               lists:sublist(RSortedPartTables, SelectCount)],
                         ?Info("Tables deleted ~p", [DropCandidates]),
                         [imem_meta:drop_table(T) || T <- DropCandidates];
                     true -> ?Info("This should not print")
                  end
           end
    end.
