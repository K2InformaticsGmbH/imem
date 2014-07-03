-module(imem_dal_skvh_load).
-include("imem_dal_skvh_load.hrl").

-behavior(gen_server).
-export([ init/1
        , handle_call/3
        , handle_cast/2
        , handle_info/2
        , code_change/3
        , terminate/2]).

-export([start/1]).

-record(state, {
            ctrl_table
          , output_table
          , channel
          , ltrc = #loadOutput{operation = channel}
          , ltra = #loadOutput{operation = audit}
          , reader_pid = undefined
          , audit_reader_pid = undefined
         }).

start(Channel) when is_list(Channel) -> start(list_to_binary(Channel));
start(Channel) when is_binary(Channel) ->
    CtrlTable = list_to_atom(lists:flatten(io_lib:format("~p_~s_control", [?MODULE, Channel]))),
    {ok, Pid} = gen_server:start_link({local, CtrlTable}, ?MODULE, [Channel], []),
    {?MODULE, Channel, Pid}.

init([Channel]) ->
    CtrlTable = list_to_atom(lists:flatten(io_lib:format("~p_~s_control", [?MODULE, Channel]))),
    catch imem_meta:drop_table(CtrlTable),
    ok = imem_meta:create_table(CtrlTable, {record_info(fields, loadControl),?loadControl,#loadControl{}}
                           , [{record_name,loadControl}], system),
    ok = imem_if:write(CtrlTable, #loadControl{}),
    ok = imem_if:write(CtrlTable, #loadControl{ operation = audit
                                              , keyregex = <<"01.01.1970 00:00:00">>}),
    ok = imem_if:subscribe({table, CtrlTable, detailed}),
    OutputTable = list_to_atom(lists:flatten(io_lib:format("~p_~s_output", [?MODULE, Channel]))),
    catch imem_meta:drop_table(OutputTable),
    ok = imem_meta:create_table(OutputTable, {record_info(fields, loadOutput),?loadOutput,#loadOutput{}}
                           , [{record_name,loadOutput}], system),
    ok = imem_if:write(OutputTable, #loadOutput{}),
    ok = imem_if:write(OutputTable, #loadOutput{operation = audit}),
    Self = self(),
    F = fun(F) ->
            ok = imem_if:subscribe({table, schema}),
            receive
                {mnesia_table_event,{delete,{schema,CtrlTable,_},_}} ->
                    Self ! {die,{table_dropped,CtrlTable}};
                {mnesia_table_event,{delete,{schema,OutputTable,_},_}} ->
                    Self ! {die,{table_dropped,OutputTable}};
                _ -> F(F)
            end
        end,
    spawn(fun() -> F(F) end),
    {ok, #state{ctrl_table=CtrlTable, output_table = OutputTable, channel = Channel}}.

handle_call(Req, _From, State) -> io:format(user, "Unknown handle_call ~p~n", [Req]), {ok, Req, State}.
handle_cast(Msg, State) -> io:format(user, "Unknown handle_cast ~p~n", [Msg]), {noreply, State}.

handle_info({mnesia_table_event, {write, CtrlTable
        , #loadControl{state = getkeys, operation = channel, keyregex = KRegx, limit = Limit}, _, _}}
        , #state{ctrl_table = CtrlTable} = State) ->
    {ok, _} = imem_if:unsubscribe({table, State#state.ctrl_table, detailed}),
    Parent = self(),
    spawn(fun() ->
        keys_read_process(Parent, State#state.channel, KRegx, <<"-1.0e100">>, Limit)
    end),
    {noreply, State#state{ltrc = (State#state.ltrc)#loadOutput{keys = [], keycounter = 0}}};
handle_info({mnesia_table_event, {write, CtrlTable, #loadControl{state = stop, operation = channel}, _, _}}
        , #state{ctrl_table = CtrlTable} = State) ->
    if State#state.reader_pid /= undefined ->
        case erlang:is_process_alive(State#state.reader_pid) of
            true ->
                io:format(user, "Killing reader ~p~n", [State#state.reader_pid]),
                State#state.reader_pid ! kill;
            _ -> ok
        end;
        true -> ok
    end,
    ok = imem_meta:write(State#state.output_table, #loadOutput{}),
    {noreply, State#state{ltrc = #loadOutput{operation = channel}, reader_pid = undefined}};
handle_info({mnesia_table_event, {write, CtrlTable,
    #loadControl{state = run, operation = channel, readdelay = ReadDelay}, _, _}},
    #state{ctrl_table = CtrlTable} =  State) ->
    ReaderPid =
        if State#state.reader_pid == undefined ->
               Parent = self(),
               spawn(fun() ->
                             Begning = erlang:now(),
                             random_read_process(Parent, State#state.channel
                                                 , ReadDelay, (State#state.ltrc)#loadOutput.keys
                                                 , {Begning, Begning, 0})
                     end);
           true -> State#state.reader_pid
        end,
    {noreply, State#state{reader_pid = ReaderPid}};

handle_info({mnesia_table_event, {write, CtrlTable, #loadControl{state = stop, operation = audit}, _, _}}
        , #state{ctrl_table = CtrlTable} = State) ->
    if State#state.audit_reader_pid /= undefined ->
        case erlang:is_process_alive(State#state.audit_reader_pid) of
            true ->
                io:format(user, "Killing audit reader ~p~n", [State#state.audit_reader_pid]),
                State#state.audit_reader_pid ! kill;
            _ -> ok
        end;
        true -> ok
    end,
    ok = imem_meta:write(State#state.output_table, #loadOutput{operation = audit}),
    {noreply, State#state{ltra = #loadOutput{operation = audit}, audit_reader_pid = undefined}};
handle_info({mnesia_table_event, {write, CtrlTable,
    #loadControl{state = run, operation = audit, keyregex = Key, readdelay = ReadDelay, limit = Limit}, _, _}},
    #state{ctrl_table = CtrlTable} =  State) ->
    ReaderPid =
        if State#state.audit_reader_pid == undefined ->
               Parent = self(),
               spawn(fun() ->
                             Begning = erlang:now(),
                             audit_read_process(Parent, State#state.channel
                                               , ReadDelay, Key, Limit
                                               , {Begning, Begning, 0})
                     end);
           true -> State#state.audit_reader_pid
        end,
    {noreply, State#state{audit_reader_pid = ReaderPid}};

handle_info({mnesia_table_event, {delete,Table,_,_,_}}, #state{ctrl_table = Table} = State) ->
    % Delete rows event from control table is ignored
    {noreply, State};

handle_info(resubscribe, State) ->
    ok = imem_if:subscribe({table, State#state.ctrl_table, detailed}),
    {noreply, State};
handle_info({keys, Keys}, #state{ltrc = LTRec} = State) ->
    NewLTRec = LTRec#loadOutput{keys = LTRec#loadOutput.keys ++ Keys
                              , keycounter = LTRec#loadOutput.keycounter + length(Keys)},
    ok = imem_if:write(State#state.output_table, NewLTRec),
    {noreply, State#state{ltrc = NewLTRec}};

handle_info({read, Count, TDiffSec, Key, Value}, State) ->
    NewLTRec = (State#state.ltrc)#loadOutput{ time = erlang:now()
                                   , totalread = Count
                                   , rate = Count / TDiffSec
                                   , lastItem = Key
                                   , lastValue = Value },
    ok = imem_if:write(State#state.output_table, NewLTRec),
    {noreply, State#state{ltrc = NewLTRec}};

handle_info({read_audit, Count, TDiffSec, Value}, State) ->
    NewLTRec = (State#state.ltra)#loadOutput{
                                   operation = audit
                                   , time = erlang:now()
                                   , totalread = Count
                                   , rate = Count / TDiffSec
                                   , lastValue = Value },
    ok = imem_if:write(State#state.output_table, NewLTRec),
    {noreply, State#state{ltra = NewLTRec}};

handle_info({die, Reason}, State) ->
    {stop, Reason, State};

handle_info(Msg, State) ->
    io:format(user, "Unknown handle_info ~p~n", [Msg]),
    {noreply, State}.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

terminate(_Reason, _State) -> ok.

keys_read_process(Parent, Channel, KeyRegex, FromKey, Limit) ->
    {ok, Keys} = imem_dal_skvh:readGT(system, Channel, <<"key">>, FromKey, Limit),
    FilteredKeys = [K || K <- Keys, re:run(K, KeyRegex) /= nomatch],
    if
        length(FilteredKeys) == Limit ->
            Parent ! {keys, FilteredKeys},
            Parent ! resubscribe;
        length(FilteredKeys) == 0 ->
            Parent ! resubscribe;
        length(FilteredKeys) < Limit ->
            Parent ! {keys, FilteredKeys},
            keys_read_process(Parent, Channel, KeyRegex, lists:last(Keys)
                              , Limit - length(FilteredKeys));
        true -> Parent ! resubscribe
    end.

random_read_process(Parent, Channel, ReadDelay, Keys, {Begning, LastUpdate, Count}) ->
    Key = lists:nth(random:uniform(length(Keys)), Keys),
    {ok, Value} = imem_dal_skvh:read(system, Channel, <<"value">>, Key),
    NowUs = erlang:now(),
    TDiffUs = timer:now_diff(NowUs, LastUpdate),
    {NewLastUpdate, NewCount} = if (TDiffUs > 1000000) ->
                                       Parent ! {read, Count, TDiffUs / 1000000, Key, Value},
                                       {NowUs, Count + 1};
                                 true ->
                                       {LastUpdate, Count + 1}
                              end,
    if ReadDelay > 0 -> timer:sleep(ReadDelay); true -> ok end,
    receive _ -> die
    after 1 -> random_read_process(Parent, Channel, ReadDelay, Keys, {Begning, NewLastUpdate, NewCount})
    end.

audit_read_process(Parent, Channel, ReadDelay, Key, Limit, {Begning, LastUpdate, Count}) ->
    {ok, Values} = imem_dal_skvh:audit_readGT(system, Channel, <<"tkvuquadruple">>, Key, Limit),
    NewCount = Count + length(Values),
    NowUs = erlang:now(),
    TDiffUs = timer:now_diff(NowUs, LastUpdate),
    {NewLastUpdate, NewCount} = if length(Values) > 0->
                                       Parent ! {read_audit, NewCount, TDiffUs / 1000000, lists:last(Values)},
                                       {NowUs, NewCount};
                                 true ->
                                       {LastUpdate, NewCount}
                              end,
    if ReadDelay > 0 -> timer:sleep(ReadDelay); true -> ok end,
    receive _ -> die
    after 1 ->              
              NextKey = if length(Values) > 0 ->
                               lists:nth(1, re:split(lists:last(Values), "\t"));
                           true -> Key
                        end,
              audit_read_process(Parent, Channel, ReadDelay, NextKey
                                 , Limit, {Begning, NewLastUpdate, NewCount})
    end.
