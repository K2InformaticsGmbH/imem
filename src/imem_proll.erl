-module(imem_proll).    %% partition rolling (create new time partitions)

-include("imem.hrl").
-include("imem_meta.hrl").

%% HARD CODED CONFIGURATIONS
-ifdef(TEST).
-define(PROLL_FIRST_WAIT,1000).                                             %% 1000 = 1 sec
-define(GET_PROLL_CYCLE_WAIT,?GET_IMEM_CONFIG(prollCycleWait,[],1000)).     %% 1000 = 1 sec
-else.
-define(PROLL_FIRST_WAIT,10000).                                            %% 10000 = 10 sec
-define(GET_PROLL_CYCLE_WAIT,?GET_IMEM_CONFIG(prollCycleWait,[],100000)).   %% 100000 = 100 sec
-endif. %TEST

-define(GET_PROLL_ITEM_WAIT,?GET_IMEM_CONFIG(prollItemWait,[],10)).         %% 10 = 10 msec

-behavior(gen_server).

-record(state, {
                 prollList=[]               :: list({atom(),atom()})        %% list({TableAlias,TableName})
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
        , missing_partitions/2
        , missing_partitions/4
        ]).

start_link(Params) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, Params, [{spawn_opt, [{fullsweep_after, 0}]}]).

init(_Args) ->
    ?Info("~p starting...~n", [?MODULE]),
    erlang:send_after(?PROLL_FIRST_WAIT, self(), roll_partitioned_tables),
    ?Info("~p started!~n", [?MODULE]),
    {ok,#state{}}.

handle_call(_Request, _From, State) ->
    {reply, ok, State}.

%     {stop,{shutdown,Reason},State};
% handle_cast({stop, Reason}, State) ->
handle_cast(_Request, State) ->
    {noreply, State}.

handle_info(roll_partitioned_tables, State=#state{prollList=[]}) ->
    % restart proll cycle by collecting list of partition name candidates
    % ?Debug("Partition rolling collect start~n",[]), 
    case ?GET_PROLL_CYCLE_WAIT of
        PCW when (is_integer(PCW) andalso PCW > 1000) ->    
            Pred = fun imem_meta:is_time_partitioned_alias/1,
            case lists:sort(lists:filter(Pred,imem_meta:all_aliases())) of
                [] ->   
                    erlang:send_after(PCW, self(), roll_partitioned_tables),
                    {noreply, State};
                AL ->   
                    PL = try
                        %% collect list of missing partitions
                        {Mega,Sec,Micro} = erlang:now(),
                        Intvl = PCW div 1000,
                        CandidateTimes = [{Mega,Sec,Micro},{Mega,Sec+Intvl,Micro},{Mega,Sec+Intvl+Intvl,Micro}],
                        missing_partitions(AL,CandidateTimes)
                    catch
                        _:Reason -> ?Error("Partition rolling collect failed with reason ~p~n",[Reason])
                    end,
                    handle_info({roll_partitioned_tables, PCW, ?GET_PROLL_ITEM_WAIT}, State#state{prollList=PL})   
            end;
        _ ->  
            erlang:send_after(10000, self(), roll_partitioned_tables),
            {noreply, State}
    end;
handle_info({roll_partitioned_tables,ProllCycleWait,ProllItemWait}, State=#state{prollList=[{TableAlias,TableName}|Rest]}) ->
    % process one proll candidate
    % ?Debug("Partition rolling try table ~p ~p~n",[TableAlias,TableName]), 
    case imem_if:read(ddAlias,{imem_meta:schema(), TableAlias}) of
        [] ->   
            ?Info("TableAlias ~p deleted before ~p could be rolled",[TableAlias,TableName]); 
        [#ddAlias{}] ->
            try
                imem_meta:create_partitioned_table(TableAlias, TableName),
                ?Info("Rolling time partition suceeded~p~n",[TableName])
            catch
                 _:{'ClientError',{"Table already exists",TableName}} ->
                    ?Info("Time partition ~p already exists, rolling is skipped~n",[TableName]);   
                _:Reason -> ?Error("Rolling time partition ~p failed with reason ~p ~n",[TableName, Reason])
            end
    end,  
    case Rest of
        [] ->   erlang:send_after(ProllCycleWait, self(), roll_partitioned_tables),
                {noreply, State#state{prollList=[]}};
        Rest -> erlang:send_after(ProllItemWait, self(), {roll_partitioned_tables,ProllCycleWait,ProllItemWait}),
                {noreply, State#state{prollList=Rest}}
    end;
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reson, _State) -> ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

format_status(_Opt, [_PDict, _State]) -> ok.


missing_partitions(AL, CandidateTimes) ->
    missing_partitions(AL, CandidateTimes, AL, []).

missing_partitions(_, [], _, Acc) -> lists:reverse(Acc);
missing_partitions(AL, [_|Times], [], Acc) ->
    missing_partitions(AL, Times, AL, Acc);
missing_partitions(AL, [Next|Times], [TableAlias|Rest], Acc0) ->
    TableName = imem_meta:partitioned_table_name(TableAlias,Next),
    Acc1 = case catch(imem_meta:check_table(TableName)) of
        ok ->   
            Acc0;
        {'ClientError',{"Table does not exist",TableName}} -> 
            [{TableAlias,TableName}|Acc0];
        Error ->  
            ?Error("Rolling time partition collection ~p failed with reason ~p~n",[TableName, Error]),
            Acc0
    end,
    missing_partitions(AL, [Next|Times], Rest, Acc1).

