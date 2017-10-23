-module(imem_proll).    %% partition rolling (create new time partitions)

-include("imem.hrl").
-include("imem_meta.hrl").

%% HARD CODED CONFIGURATIONS
-ifdef(TEST).
-define(PROLL_FIRST_WAIT,1000).                                             %% 1000 = 1 sec
-define(PROLL_ERROR_WAIT,1000).                                             %% 1000 = 1 sec
-define(GET_PROLL_CYCLE_WAIT,?GET_CONFIG(prollCycleWait,[],1000,"Wait time in msec between partition rolls.")).     %% 1000 = 1 sec
-else.
-define(PROLL_FIRST_WAIT,30000).                                            %% 30000 = 30 sec
-define(PROLL_ERROR_WAIT,1000).                                             %% 1000 = 1 sec
-define(GET_PROLL_CYCLE_WAIT,?GET_CONFIG(prollCycleWait,[],100000,"Wait time in msec between partition rolls.")).   %% 100000 = 100 sec
-endif. %TEST

-define(GET_PROLL_ITEM_WAIT,?GET_CONFIG(prollItemWait,[],10,"Wait time in msec between individual partition rolls within a cycle.")).         %% 10 = 10 msec

-behavior(gen_server).

-record(state, {
                 prollList=[]               :: list({atom(),atom()})        %% list({TableAlias, TableName})
               }).

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
        , missing_partitions/3
        , missing_partitions/5
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
    erlang:send_after(?PROLL_FIRST_WAIT, self(), roll_partitioned_tables),

    process_flag(trap_exit, true),
    {ok,#state{}}.

handle_call(_Request, _From, State) ->
    {reply, ok, State}.

%     {stop,{shutdown,Reason},State};
% handle_cast({stop, Reason}, State) ->
handle_cast(_Request, State) ->
    {noreply, State}.

handle_info(roll_partitioned_tables, State=#state{prollList=[]}) ->
    % restart proll cycle by collecting list of partition name candidates
    case ?GET_PROLL_CYCLE_WAIT of
        PCW when (is_integer(PCW) andalso PCW >= 1000) ->
            ProllList = get_proll_list(PCW),
            handle_info({roll_partitioned_tables, PCW, ?GET_PROLL_ITEM_WAIT}, State#state{prollList=ProllList});
        Other ->
            ?Error("Partition rolling bad cycle period ~p",[Other]),
            erlang:send_after(10000, self(), roll_partitioned_tables),
            {noreply, State}
    end;
handle_info({roll_partitioned_tables,ProllCycleWait,ProllItemWait}, State=#state{prollList=[{TableAlias, TableName}|Rest]}) ->
    % process one proll candidate
    NextWait = case imem_if_mnesia:read(ddAlias,{imem_meta:schema(), TableAlias}) of
        [] ->   
            ?Info("TableAlias ~p deleted before partition ~p could be rolled",[TableAlias,TableName]),
            ProllItemWait; 
        [#ddAlias{}] ->
            try
                % ?Info("Trying to roll partition ~p",[TableName]), 
                imem_meta:create_partitioned_table(TableAlias, TableName),
                ?Info("Rolling time partition ~p suceeded",[TableName]),
                ProllItemWait
            catch
                 _:{'ClientError',{"Table already exists",TableName}} ->
                    ?Info("Time partition ~p already exists, rolling is skipped",[TableName]),
                    ProllItemWait;
                _:Reason -> 
                    ?Error("Rolling time partition ~p failed with reason ~p",[TableName, Reason]),
                    ?PROLL_ERROR_WAIT
            end
    end,  
    erlang:send_after(NextWait, self(), {roll_partitioned_tables, ProllCycleWait, ProllItemWait}),
    {noreply, State#state{prollList=Rest}};
handle_info({roll_partitioned_tables,ProllCycleWait,_}, State=#state{prollList=[]}) ->
    % ?Info("Partition rolling completed",[]), 
    erlang:send_after(ProllCycleWait, self(), roll_partitioned_tables),
    {noreply, State#state{prollList=[]}};
handle_info(_Info, State) ->
    {noreply, State}.

terminate(normal, _State) -> ?Info("~p normal stop~n", [?MODULE]);
terminate(shutdown, _State) -> ?Info("~p shutdown~n", [?MODULE]);
terminate({shutdown, _Term}, _State) -> ?Info("~p shutdown : ~p~n", [?MODULE, _Term]);
terminate(Reason, _State) -> ?Error("~p stopping unexpectedly : ~p~n", [?MODULE, Reason]).

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

format_status(_Opt, [_PDict, _State]) -> ok.

get_proll_list(PCW) ->
    Pred = fun imem_meta:is_time_partitioned_alias/1,
    case lists:usort(lists:filter(Pred,imem_meta:all_aliases())) of
        [] ->  [];
        AL ->
            % ?Info("Partition rolling collect result ~p",[AL]),
            try
                %% collect list of missing partitions
                {Sec,Micro} = ?TIMESTAMP,
                Intvl = PCW div 1000,
                CandidateTimes = [{Sec, Micro}, {Sec+Intvl, Micro}, {Sec+Intvl+Intvl, Micro}],
                IsHeadInCluster = node() == hd(lists:usort([node() | imem_meta:nodes()])),
                missing_partitions(AL,CandidateTimes,IsHeadInCluster)
            catch
                _:Reason ->
                    ?Error("Partition rolling collect failed with reason ~p~n",[Reason]),
                    []
            end
    end.

missing_partitions(AL, CandidateTimes, IsHeadInCluster) ->
    missing_partitions(AL, CandidateTimes, IsHeadInCluster, AL, []).

missing_partitions(_, [], _, _, Acc) -> lists:usort(Acc);
missing_partitions(AL, [_|Times], IsHeadInCluster, [], Acc) ->
    missing_partitions(AL, Times, IsHeadInCluster, AL, Acc);
missing_partitions(AL, [Next|Times], IsHeadInCluster, [TableAlias|Rest], Acc0) ->
    TableName = imem_meta:partitioned_table_name(TableAlias, Next),
    Acc1 = case catch(imem_meta:check_table(TableName)) of
        ok ->   
            Acc0;
        {'ClientError',{"Table does not exist",TableName}} -> 
            % checking if this is the first node in all of nodes as
            % partition rolling has to be done by one node at a time.
            case IsHeadInCluster of
                true -> [{TableAlias, TableName}|Acc0];
                false ->
                    % check if the table is local table then each node have
                    % to create their node partitioned table
                    case imem_meta:is_local_time_partitioned_table(TableName) of
                        true -> [{TableAlias, TableName}|Acc0];
                        false -> Acc0
                    end
            end;
        Error ->  
            ?Error("Rolling time partition collection ~p failed with reason ~p~n",[TableName, Error]),
            Acc0
    end,
    missing_partitions(AL, [Next|Times], IsHeadInCluster, Rest, Acc1).

