-module(imem_sql_account).

-include("imem_seco.hrl").

-export([ exec/5
        ]).

exec(SKey, {'create user', Name, {'identified by', Password}, Opts}, _Stmt, _Schema, IsSec) ->
    if_call_mfa(IsSec, admin_exec, [SKey, imem_account, create, [SKey, user, Name, Name, Password]]),
    case lists:member({account,lock}, Opts) of 
        true -> if_call_mfa(IsSec, admin_exec, [SKey, imem_account, lock, [SKey, Name]]);
        false -> ok
    end,
    case lists:member({password,expire}, Opts) of 
        true ->  if_call_mfa(IsSec, admin_exec, [SKey, imem_account, expire, [SKey, Name]]);
        false -> if_call_mfa(IsSec, admin_exec, [SKey, imem_account, renew, [SKey, Name]])
    end;

exec(SKey, {'alter user', Name, {spec, Specs}}, _Stmt, _Schema, IsSec) ->
    case lists:member({account,unlock}, Specs) of 
        true -> if_call_mfa(IsSec, admin_exec, [SKey, imem_account, unlock, [SKey, Name]]);
        false -> ok
    end,
    case lists:member({account,lock}, Specs) of 
        true -> if_call_mfa(IsSec, admin_exec, [SKey, imem_account, lock, [SKey, Name]]);
        false -> ok
    end,
    case lists:member({password,expire}, Specs) of 
        true ->  if_call_mfa(IsSec, admin_exec, [SKey, imem_account, expire, [SKey, Name]]);
        false -> ok
    end,
    case lists:keyfind(identified_by, 1, Specs) of 
        {identified_by, NewPassword} ->  
            if_call_mfa(IsSec, admin_exec, [SKey, imem_seco, change_credentials, [SKey, {pwdmd5,NewPassword}]]);
        false -> ok
    end;

exec(SKey, {'drop user', Name, Specs}, _Stmt, _Schema, IsSec) ->
    if_call_mfa(IsSec, admin_exec, [SKey, imem_account, delete, [SKey, Name, Specs]]).


%% --Interface functions  (calling imem_if for now, not exported) ---------

if_call_mfa(IsSec,Fun,Args) ->
    case IsSec of
        true -> apply(imem_sec,Fun,Args);
        _ ->    apply(imem_meta, Fun, lists:nthtail(1, Args))
    end.

%% TESTS ------------------------------------------------------------------

-include_lib("eunit/include/eunit.hrl").

setup() -> 
    ?imem_test_setup().

teardown(_) -> 
    SKey=?imem_test_admin_login(),
    catch imem_account:delete(SKey, <<"test_user_1">>, []), 
    ?imem_test_teardown().

db_test_() ->
    {
        setup,
        fun setup/0,
        fun teardown/1,
        {with, [
            fun test_with_sec/1
        ]}
    }.
    
test_with_sec(_) ->
    test_with_or_without_sec(true).

test_with_or_without_sec(IsSec) ->
    try
        ClEr = 'ClientError',
        UiEx = 'UnimplementedException',
        % SeEx = 'SecurityException',
        io:format(user, "----TEST--- ~p ----Security ~p ~n", [?MODULE, IsSec]),

        io:format(user, "schema ~p~n", [imem_meta:schema()]),
        io:format(user, "data nodes ~p~n", [imem_meta:data_nodes()]),
        ?assertEqual(true, is_atom(imem_meta:schema())),
        ?assertEqual(true, lists:member({imem_meta:schema(),node()}, imem_meta:data_nodes())),

        SKey=?imem_test_admin_login(),
        ?assertEqual(ok, imem_sql:exec(SKey, "CREATE USER test_user_1 IDENTIFIED BY a_password;", 0, "Imem", IsSec)),
        ?assertException(throw, {ClEr,{"Account already exists", <<"test_user_1">>}}, imem_sql:exec(SKey, "CREATE USER test_user_1 IDENTIFIED BY a_password;", 0, "Imem", IsSec)),
        ?assertException(throw, {UiEx,{"Unimplemented account delete option",[cascade]}}, imem_sql:exec(SKey, "DROP USER test_user_1 CASCADE;", 0, "Imem", IsSec)),
        ?assertEqual(ok, imem_sql:exec(SKey, "DROP USER test_user_1;", 0, "Imem", IsSec))
    catch
        Class:Reason ->  io:format(user, "Exception ~p:~p~n~p~n", [Class, Reason, erlang:get_stacktrace()]),
        ?assert( true == "all tests completed")
    end,
    ok. 

