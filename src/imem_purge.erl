-module(imem_purge).

-include("imem.hrl").
-include("imem_meta.hrl").

%% HARD CODED CONFIGURATIONS

-define(GET_PURGE_CYCLE_WAIT,?GET_IMEM_CONFIG(purgeCycleWait,[],10000)).     %% 10000 = 10 sec
-define(GET_PURGE_ITEM_WAIT,?GET_IMEM_CONFIG(purgeItemWait,[],10)).          %% 10 = 10 msec
-define(GET_PURGE_SCRIPT,?GET_IMEM_CONFIG(purgeScript,[],false)).
-define(GET_PURGE_SCRIPT_FUN,?GET_IMEM_CONFIG(purgeScriptFun,[],
<<"fun (PartTables) ->
	MAX_TABLE_COUNT_PERCENT = 90,
	MIN_FREE_MEM_PERCENT = 40,
	TABLE_EXPIRY_MARGIN_SEC = -200,
    %[{time_to_part_expiry,table_size,partition_time,table}]
	SortedPartTables =
	    lists:sort([{imem_meta:time_to_partition_expiry(T),
			 imem_meta:table_size(T),
			 lists:nth(3, imem_meta:parse_table_name(T)),
             T}
			|| T <- PartTables]),
    {Os, FreeMemory, TotalMemory} = imem_if:get_os_memory(),
	MemFreePerCent = FreeMemory / TotalMemory * 100,
	%io:format(user, \"[~p] Free ~p%~n\", [Os, MemFreePerCent]),
	if MemFreePerCent < MIN_FREE_MEM_PERCENT ->
	       io:format(user, \"Free mem ~p% required min ~p%~n\", [MemFreePerCent, MIN_FREE_MEM_PERCENT]),
	       %io:format(user, \"Possible purging canditate tables ~p~n\", [SortedPartTables]),
	       MapFun = fun ({TRemain, RCnt, Class, TName} = Itm, A) ->
				if TRemain < TABLE_EXPIRY_MARGIN_SEC ->
				       ClassCnt = length([Spt
							  || Spt
								 <- SortedPartTables,
							     element(3, Spt) =:=
							       Class]),
				       if ClassCnt > 1 -> [Itm | A];
					  true -> A
				       end;
				   true -> A
				end
			end,
	       DelCandidates = lists:foldl(MapFun, [],
					   SortedPartTables),
	       if DelCandidates =:= [] ->
                %[{time_to_part_expiry,table_size,partition_time,table}]
		      TruncCandidates = lists:sort(fun ({_, R1, _, _},
							 {_, R2, _, _}) ->
							    if R1 > R2 -> true;
							       true -> false
							    end
						    end,
						    SortedPartTables),
		      [{_, _, _, T} | _] = TruncCandidates,
              imem_meta:log_to_db(info, imem_meta, purgeScriptFun, [{table, T}, {memFreePerCent, MemFreePerCent}], \"truncate table\"),
		      imem_meta:truncate_table(T),
		      io:format(user, \"[~p] Truncated table ~p~n\", [Os, T]);
		  true ->
		      [{_, _, _, T} | _] = DelCandidates,
              imem_meta:log_to_db(info, imem_meta, purgeScriptFun, [{table, T}, {memFreePerCent, MemFreePerCent}], \"drop table\"),
		      imem_meta:drop_table(T),
		      io:format(user, \"[~p] Deleted table ~p~n\", [Os, T])
	       end;
	   true ->
           {MaxTablesCount, CurrentTableCount} = imem_meta:get_tables_count(),
           CurrentTableCountParcent = round(CurrentTableCount / MaxTablesCount * 100),
           if CurrentTableCountParcent < MAX_TABLE_COUNT_PERCENT ->
                   % Nothing to drop yet
                   io:format(user, \"No Dropping (~p% of ~p)~n\", [CurrentTableCountParcent, MaxTablesCount]);
               true ->
                   DropCandidates = lists:sub_list(lists:sort(fun ({_, R1, _, _},
							 {_, R2, _, _}) ->
							    if R1 < R2 -> true;
							       true -> false
							    end
						    end,
						    SortedPartTables), 100 * (MAX_TABLE_COUNT_PERCENT - CurrentTableCountParcent)),
                   %[{time_to_part_expiry,table_size,partition_time,table}]
                   io:format(user, \"Drop table ~p~n\", [DropCandidates]),
		           [imem_meta:drop_table(T) || {_,_,_,T} <- DropCandidates]
           end
	end
end
">>)).

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
            Pred = fun imem_meta:is_local_time_partitioned_table/1,
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
                                PH ->   {PH,PF};
                                H1 ->   {H1,imem_meta:compile_fun(PFStr)}
                            end      
                    end,
                    try
                        case PFun of
                            undefined ->    ok;
                            P ->            P(PL)
                        end
                    catch
                        _:Reason -> ?Error("Purge script Fun failed with reason ~p~n",[Reason])
                    end,
                    handle_info({purge_partitioned_tables, PCW, ?GET_PURGE_ITEM_WAIT}, State#state{purgeFun=PFun,purgeHash=PHash,purgeList=PL})   
            end;
        _ ->  
            erlang:send_after(10000, self(), purge_partitioned_tables),
            {noreply, State}
    end;
handle_info({purge_partitioned_tables,PurgeCycleWait,PurgeItemWait}, State=#state{purgeList=[Tab|Rest]}) ->
    % process one purge candidate
    ?Debug("Purge try table ~p~n",[Tab]), 
    case imem_if:read(ddTable,{imem_meta:schema(), Tab}) of
        [] ->   
            ?Debug("Table deleted before it could be purged ~p~n",[Tab]); 
        [#ddTable{opts=Opts}] ->
            case lists:keyfind(purge_delay, 1, Opts) of
                false ->
                    ok;             %% no purge delay in table create options, do not purge this file
                {purge_delay,PD} ->
                    Name = atom_to_list(Tab),
                    {BaseName,PartitionName} = lists:split(length(Name)-length(imem_meta:node_shard())-11, Name),
                    case Rest of
                        [] ->   
                            ok;                     %% no follower, do not purge this file
                        [Next|_] ->
                            NextName = atom_to_list(Next),
                            case lists:prefix(BaseName,NextName) of
                                false -> 
                                    ok;             %% no follower, do not purge this file
                                true ->
                                    {Mega,Sec,_} = erlang:now(),
                                    PurgeEnd=1000000*Mega+Sec-PD,
                                    PartitionEnd=list_to_integer(lists:sublist(PartitionName,10)),
                                    if
                                        (PartitionEnd >= PurgeEnd) ->
                                            ok;     %% too young, do not purge this file  
                                        true ->                     
                                            ?Info("Purge time partition ~p~n",[Tab]), %% cannot log here
                                            % FreedMemory = table_memory(Tab),
                                            % Fields = [{table,Tab},{table_size,table_size(Tab)},{table_memory,FreedMemory}],   
                                            % log_to_db(info,?MODULE,purge_time_partitioned_table,Fields,"purge table"), %% cannot log here
                                            imem_meta:drop_table(Tab)
                                    end
                            end
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
