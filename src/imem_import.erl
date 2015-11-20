-module(imem_import).

-include("imem_seco.hrl").

-export([ create_from_string/5
        ]).

-record(state,  { table      :: list()      %% table name
                , sep        :: list()      %% separator character(s)
                , names      :: list()      %% field names 
                , types      :: list()      %% field data types
                , defaults   :: list()      %% default values
                , data       :: list()      %% remaining to be imported 
        }).

create_from_string(_SKey, Str, _Opts, _Schema, _IsSec) when is_list(Str) ->
    HS = process_header(#state{data=re:split(Str,"[\n]",[{return,list}])}),       %% HeaderState
    %% ToDo: implement
    % Table = list_to_atom(HS#state.table),
    % Names = list_to_atom(HS#state.names),
    % Types = [list_to_atom(T) || T <- HS#state.types],
    % DefaultRecord = [ imem_datatype:string_to_term(T) || T <- HS#state.defaults],
    % if_call_mfa(IsSec, 'create_table', [SKey, Table, {Names,Types,DefaultRecord}, Opts]),
    import_data(HS).

process_header(_S) ->
    %% ToDo: implement
    ok.

import_data(_HS) ->
    %% ToDo: implement
    ok.


%% --Interface functions  (calling imem_if for now, not exported) ---------

% if_call_mfa(IsSec,Fun,Args) ->
%     case IsSec of
%         true -> apply(imem_sec, Fun, Args);
%         _ ->    apply(imem_meta, Fun, lists:nthtail(1, Args))
%     end.

%% TESTS ------------------------------------------------------------------
-ifdef(TEST).

-include_lib("eunit/include/eunit.hrl").

setup() -> 
    ?imem_test_setup.

teardown(_) ->
    % catch imem_meta:drop_table(import_test_3), 
    % catch imem_meta:drop_table(import_test_2), 
    % catch imem_meta:drop_table(import_test_1), 
    ?imem_test_teardown.

db_test_() ->
    {
        setup,
        fun setup/0,
        fun teardown/1,
        {with, [
                 fun test_without_sec/1
               , fun test_with_sec/1
        ]}
    }.
    
test_without_sec(_) -> 
    test_with_or_without_sec(false).

test_with_sec(_) ->
    test_with_or_without_sec(true).

test_with_or_without_sec(IsSec) ->
    try
        _ClEr = 'ClientError',
        %% SyEx = 'SystemException',    %% difficult to test
        % SeEx = 'SecurityException',
        ?LogDebug("---TEST--- ~p ----Security ~p ~n", [?MODULE, IsSec]),

        % ?LogDebug("schema ~p~n", [imem_meta:schema()]),
        % ?LogDebug("data nodes ~p~n", [imem_meta:data_nodes()]),
        ?assertEqual(true, is_atom(imem_meta:schema())),
        ?assertEqual(true, lists:member({imem_meta:schema(),node()}, imem_meta:data_nodes())),

        SKey = case IsSec of
            true -> ?imem_test_admin_login();
            _ ->    ok
        end,

        % ?LogDebug("~p:test_mnesia~n", [?MODULE]),

        ?assertEqual(true, is_atom(imem_meta:schema())),
        % ?LogDebug("success ~p~n", [schema]),
        ?assertEqual(true, lists:member({imem_meta:schema(),node()}, imem_meta:data_nodes())),
        % ?LogDebug("success ~p~n", [data_nodes]),

        % ?LogDebug("~p:import from string~n", [?MODULE]),

        Imp1 = "
        table_name\n
        import_test_1\n
        field_types\n
        integer,string,tuple\n
        field_defaults\n
        $not_a_value,[],{}\n
        field_names\n
        int1,   str2,       tup3\n
        1,      \"text1\",  {1}\n
        2,      [],         \n
        3,      \"äöü\",    {a}\n 
        4,      ,           \n
        ",

        ?assertEqual(ok, create_from_string(SKey, Imp1, [], "imem", IsSec)),
        % ?assertEqual(4,  if_call_mfa(IsSec, table_size, [SKey, import_test_1])),    

        % ?assertEqual(ok, imem_meta:drop_table(import_test_3)),
        % ?assertEqual(ok, imem_meta:drop_table(import_test_2)),
        % ?assertEqual(ok, imem_meta:drop_table(import_test_1)),
        % ?LogDebug("success ~p~n", [drop_tables]),

        case IsSec of
            true -> ?imem_logout(SKey);
            _ ->    ok
        end
    catch
        Class:Reason ->  ?LogDebug("Exception ~p:~p~n~p~n", [Class, Reason, erlang:get_stacktrace()]),
        ?assert( true == "all tests completed")
    end,
    ok. 

-endif.
