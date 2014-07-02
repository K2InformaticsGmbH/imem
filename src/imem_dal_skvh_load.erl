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
            table
          , channel
          , ltr = #loadTest{}
         }).

start(Channel) when is_list(Channel) ->
    {ok, Pid} = gen_server:start_link(?MODULE, [Channel], []),
    {?MODULE, Channel, Pid}.

init([Channel]) ->
    Table = list_to_atom(lists:flatten(io_lib:format("~p_~s_control", [?MODULE, Channel]))),
    catch imem_meta:drop_table(Table),
    ok = imem_meta:create_table(Table, {record_info(fields, loadTest),?loadTest,#loadTest{}}
                           , [{record_name,loadTest}], system),
    ok = imem_meta:write(Table, #loadTest{}),
    ok = imem_meta:subscribe({table, Table, detailed}),
    ok = imem_meta:subscribe({table, schema}),
    {ok, #state{table=Table, channel = Channel}}.

handle_call(Req, _From, State) ->
    {ok, Req, State}.

handle_cast(Msg, State) ->
    io:format(user, "handle_cast ~p~n", [Msg]),
    {noreply, State}.

handle_info({mnesia_table_event, {write, Table,
    #loadTest{state = getkey, keyregex = KRegx, limit = Limit, readdelay = RdDelay} = LTRec, [], _}},
    #state{channel = Channel, table = Table} =  State) ->
    {ok, _} = imem_meta:unsubscribe({table, Table, detailed}),
    {ok, _} = imem_meta:unsubscribe({table, schema}),
    Parent = self(),
    spawn(fun() ->
        keys_read_process(Parent, Channel, KRegx, <<"-1.0e100">>, Limit, RdDelay)
    end),
    {noreply, State#state{ltr = LTRec#loadTest{keys = [], keycounter = 0}}};
handle_info({mnesia_table_event, {write, Table, #loadTest{state = stopped} = LTRec, [], _}},
    #state{channel = Channel, table = Table} =  State) ->
    {ok, _} = imem_meta:unsubscribe({table, Table, detailed}),
    {ok, _} = imem_meta:unsubscribe({table, schema}),
    NewLTRec = LTRec#loadTest{keys = [], keycounter = 0},
    ok = imem_meta:write(Table, NewLTRec),
    ok = imem_meta:subscribe({table, Table, detailed}),
    ok = imem_meta:subscribe({table, schema}),
    {noreply, State#state{ltr = NewLTRec}};
handle_info({mnesia_table_event, {delete, {schema,Table, _}, _}}, #state{table = Table} =  State) ->
    % Dropped table kills the process
    {stop, {table_dropped, Table}, State};
handle_info({mnesia_table_event, {delete,Table,_,_,_}}, #state{table = Table} = State) ->
    % Delete rows event from control table is ignored
    {noreply, State};

handle_info(resubscribe, #state{table = Table} = State) ->
    ok = imem_meta:subscribe({table, Table, detailed}),
    ok = imem_meta:subscribe({table, schema}),
    {noreply, State};
handle_info({keys, Keys}, #state{ltr = LTRec, table = Table} = State) ->
    %io:format(user, "handle_info keys ~p, last ~p~n", [length(Keys), lists:last(Keys)]),
    NewLTRec = LTRec#loadTest{keys = LTRec#loadTest.keys ++ Keys
                              , keycounter = LTRec#loadTest.keycounter + length(Keys)},
    ok = imem_meta:write(Table, NewLTRec),
    {noreply, State#state{ltr = NewLTRec}};

handle_info(Msg, State) ->
    io:format(user, "handle_info ~p~n", [Msg]),
    {noreply, State}.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

terminate(_Reason, _State) -> ok.

keys_read_process(Parent, Channel, KeyRegex, FromKey, Limit, ReadDelay) ->
    {ok, Keys} = imem_dal_skvh:readGT(system, list_to_binary(Channel), <<"key">>, FromKey, Limit),
    FilteredKeys = [K || K <- Keys, re:run(K, KeyRegex) /= nomatch],
    if
        length(FilteredKeys) == Limit ->
            Parent ! {keys, FilteredKeys},
            Parent ! resubscribe;
        length(FilteredKeys) == 0 ->
            Parent ! resubscribe;
        length(FilteredKeys) < Limit ->
            Parent ! {keys, FilteredKeys},
            if ReadDelay > 0 -> timer:sleep(ReadDelay); true -> ok end,
            keys_read_process(Parent, Channel, KeyRegex, lists:last(Keys)
                              , Limit - length(FilteredKeys), ReadDelay);
        true -> Parent ! resubscribe
    end.
