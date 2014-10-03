-module(imem_sql_account).

-include("imem_seco.hrl").

-export([ exec/5
        ]).

exec(SKey, {'create user', Name, {'identified by', Password}, Opts}, _Stmt, _StmtOpts, IsSec) ->
    if_call_mfa(IsSec, admin_exec, [SKey, imem_account, create, [user, Name, Name, Password]]),
    case lists:member({account,lock}, Opts) of 
        true -> if_call_mfa(IsSec, admin_exec, [SKey, imem_account, lock, [Name]]);
        false -> ok
    end,
    case lists:member({password,expire}, Opts) of 
        true ->  if_call_mfa(IsSec, admin_exec, [SKey, imem_account, expire, [Name]]);
        false -> if_call_mfa(IsSec, admin_exec, [SKey, imem_account, renew, [Name]])
    end;

exec(SKey, {'alter user', Name, {spec, Specs}}, _Stmt, _StmtOpts, IsSec) ->
    case lists:member({account,unlock}, Specs) of 
        true -> if_call_mfa(IsSec, admin_exec, [SKey, imem_account, unlock, [Name]]);
        false -> ok
    end,
    case lists:member({account,lock}, Specs) of 
        true -> if_call_mfa(IsSec, admin_exec, [SKey, imem_account, lock, [Name]]);
        false -> ok
    end,
    case lists:member({password,expire}, Specs) of 
        true ->  if_call_mfa(IsSec, admin_exec, [SKey, imem_account, expire, [Name]]);
        false -> ok
    end,
    case lists:keyfind(identified_by, 1, Specs) of 
        {identified_by, NewPassword} ->  
            if_call_mfa(IsSec, admin_exec, [SKey, imem_seco, change_credentials, [{pwdmd5,NewPassword}]]);
        false -> ok
    end;

exec(SKey, {'drop user', Name, Specs}, _Stmt, _StmtOpts, IsSec) ->
    if_call_mfa(IsSec, admin_exec, [SKey, imem_account, delete, [Name, Specs]]);

exec(SKey, {'grant', Privileges, {'on', <<>>}, {'to', Grantees}, _Opts}, _Stmt, _StmtOpts, _IsSec) -> 
    % ?Info("grant privileges ~p~n", [Privileges]),
    % ?Info("grant grantees ~p~n", [Grantees]),
    % ?Info("grant opts ~p~n", [_Opts]),            % ToDo: implement grant options
    [grant_sys_priv(SKey,P,G) || P <- Privileges, G <- Grantees],
    ok;
exec(SKey, {'grant', Privileges, {'on', Object}, {'to', Grantees}, _Opts}, _Stmt, _StmtOpts, _IsSec) -> 
    % ?Info("grant privileges ~p~n", [Privileges]),
    % ?Info("grant object ~p~n", [Object]),
    % ?Info("grant grantees ~p~n", [Grantees]),
    % ?Info("grant opts ~p~n", [_Opts]),            % ToDo: implement grant options
    [grant_obj_priv(SKey,P,G,Object) || P <- Privileges, G <- Grantees],
    ok;
exec(SKey, {'revoke', Privileges, {'on', <<>>}, {'from', Grantees}, _Opts}, _Stmt, _StmtOpts, _IsSec) -> 
    % ?Info("revoke privileges ~p~n", [Privileges]),
    % ?Info("revoke grantees ~p~n", [Grantees]),
    % ?Info("revoke opts ~p~n", [_Opts]),
    [revoke_sys_priv(SKey,P,G) || P <- Privileges, G <- Grantees],
    ok;
exec(SKey, {'revoke', Privileges, {'on', Object}, {'from', Grantees}, _Opts}, _Stmt, _StmtOpts, _IsSec) -> 
    % ?Info("revoke privileges ~p~n", [Privileges]),
    % ?Info("revoke object ~p~n", [Object]),
    % ?Info("revoke grantees ~p~n", [Grantees]),
    % ?Info("revoke opts ~p~n", [_Opts]),
    [revoke_obj_priv(SKey,P,G,Object) || P <- Privileges, G <- Grantees],
    ok.


grant_sys_priv(SKey,PA,GBin) when is_atom(PA) ->
    P = case imem_role:exists(SKey,PA) of
        true ->     {role,PA};
        false ->    {perm,PA}
    end,
    G = case (catch list_to_existing_atom(binary_to_list(GBin))) of
        GA when is_atom(GA) -> 
            case imem_role:exists(SKey,GA) of
                true ->     {role,GA};
                false ->    {name,GBin}
            end;
        _ ->
            {name,GBin}
    end,
    case {P,G} of
        {{role,R1},{role,R0}} ->    imem_role:grant_role(SKey, R0, R1);
        {{role,R2},{name,AN}} ->    imem_role:grant_role(SKey, AN, R2);
        {{perm,P1},{role,R0}} ->    imem_role:grant_permission(SKey, R0, P1);
        {{perm,P2},{name,AN}} ->    imem_role:grant_permission(SKey, AN, P2)
    end.

revoke_sys_priv(SKey,PA,GBin) when is_atom(PA) ->
    P = case imem_role:exists(SKey,PA) of
        true ->     {role,PA};
        false ->    {perm,PA}
    end,
    G = case (catch list_to_existing_atom(binary_to_list(GBin))) of
        GA when is_atom(GA) -> 
            case imem_role:exists(SKey,GA) of
                true ->     {role,GA};
                false ->    {name,GBin}
            end;
        _ ->
            {name,GBin}
    end,
    case {P,G} of
        {{role,R1},{role,R0}} ->    imem_role:revoke_role(SKey, R0, R1);
        {{role,R2},{name,AN}} ->    imem_role:revoke_role(SKey, AN, R2);
        {{perm,P1},{role,R0}} ->    imem_role:revoke_permission(SKey, R0, P1);
        {{perm,P2},{name,AN}} ->    imem_role:revoke_permission(SKey, AN, P2)
    end.

grant_obj_priv(SKey,PA,GBin,OBin) when is_atom(PA) ->
    O = case (catch list_to_existing_atom(binary_to_list(OBin))) of
        OA when is_atom(OA) ->
            case (catch imem_meta:check_table(OA)) of
                ok ->   {table,OA};
                _ ->    {module,OA}
            end;
        _ ->
            ?SecurityException({"Object does not exist",{SKey,OBin}})
    end,
    G = case (catch list_to_existing_atom(binary_to_list(GBin))) of
        GA when is_atom(GA) -> 
            case imem_role:exists(SKey,GA) of
                true ->     {role,GA};
                false ->    {name,GBin}
            end;
        _ ->
            {name,GBin}
    end,
    case {G,O} of
        {{role,R0},{table,T1}} ->    imem_role:grant_permission(SKey,R0,{table,T1,PA});
        {{role,R0},{module,M1}} ->   imem_role:grant_permission(SKey,R0,{module,M1,PA});
        {{name,AN},{table,T2}} ->    imem_role:grant_permission(SKey,AN,{table,T2,PA});
        {{name,AN},{module,M2}} ->   imem_role:grant_permission(SKey,AN,{module,M2,PA})
    end.

revoke_obj_priv(SKey,PA,GBin,OBin) when is_atom(PA) ->
    O = case (catch list_to_existing_atom(binary_to_list(OBin))) of
        OA when is_atom(OA) ->
            case (catch imem_meta:check_table(OA)) of
                ok ->   {table,OA};
                _ ->    {module,OA}
            end;
        _ ->
            ?SecurityException({"Object does not exist",{SKey,OBin}})
    end,
    G = case (catch list_to_existing_atom(binary_to_list(GBin))) of
        GA when is_atom(GA) -> 
            case imem_role:exists(SKey,GA) of
                true ->     {role,GA};
                false ->    {name,GBin}
            end;
        _ ->
            {name,GBin}
    end,
    case {G,O} of
        {{role,R0},{table,T}} ->    imem_role:revoke_permission(SKey,R0,{table,T,PA});
        {{role,R0},{module,M}} ->   imem_role:revoke_permission(SKey,R0,{module,M,PA});
        {{name,AN},{table,T}} ->    imem_role:revoke_permission(SKey,AN,{table,T,PA});
        {{name,AN},{module,M}} ->   imem_role:revoke_permission(SKey,AN,{module,M,PA})
    end.

%% --Interface functions  (calling imem_if for now, not exported) ---------

if_call_mfa(IsSec,Fun,Args) ->
    case IsSec of
        true -> apply(imem_sec,Fun,Args);
        _ ->    apply(imem_meta, Fun, lists:nthtail(1, Args))
    end.

%% TESTS ------------------------------------------------------------------
-ifdef(TEST).

-include_lib("eunit/include/eunit.hrl").

setup() -> 
    ?imem_test_setup.

teardown(_) -> 
    SKey=?imem_test_admin_login(),
    catch imem_account:delete(SKey, <<"test_user_1">>, []), 
    ?imem_test_teardown.

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
        ?LogDebug("---TEST--- ~p ----Security ~p ~n", [?MODULE, IsSec]),

        ?LogDebug("schema ~p~n", [imem_meta:schema()]),
        ?LogDebug("data nodes ~p~n", [imem_meta:data_nodes()]),
        ?assertEqual(true, is_atom(imem_meta:schema())),
        ?assertEqual(true, lists:member({imem_meta:schema(),node()}, imem_meta:data_nodes())),

        SKey=?imem_test_admin_login(),
        ?assertEqual(ok, imem_sql:exec(SKey, "CREATE USER test_user_1 IDENTIFIED BY a_password;", 0, [{schema,imem}], IsSec)),
        UserId = imem_account:get_id_by_name(SKey,<<"test_user_1">>),
        ?LogDebug("UserId ~p~n", [UserId]),
        ?assertException(throw, {ClEr,{"Account already exists", <<"test_user_1">>}}, imem_sql:exec(SKey, "CREATE USER test_user_1 IDENTIFIED BY a_password;", 0, [{schema,imem}], IsSec)),
        ?assertException(throw, {UiEx,{"Unimplemented account delete option",[cascade]}}, imem_sql:exec(SKey, "DROP USER test_user_1 CASCADE;", 0, [{schema,imem}], IsSec)),
        ?assertEqual(false, imem_seco:has_permission(SKey, UserId, manage_system)),
        ?assertEqual(ok, imem_sql:exec(SKey, "GRANT manage_system TO test_user_1 with admin option;", 0, [{schema,imem}], IsSec)),
        ?assertEqual(true, imem_seco:has_permission(SKey, UserId, manage_system)),
        ?assertEqual(false, imem_seco:has_permission(SKey, UserId, {module,imem_test,execute})),
        ?assertEqual(ok, imem_sql:exec(SKey, "GRANT EXECUTE ON imem_test TO test_user_1;", 0, [{schema,imem}], IsSec)),
        % ?LogDebug("ddRole ~p~n", [imem_meta:read(ddRole)]),
        ?assertEqual(true, imem_seco:has_permission(SKey, UserId, {module,imem_test,execute})),
        ?assertEqual(false, imem_seco:has_permission(SKey, UserId, {table,ddTable,select})),
        ?assertEqual(ok, imem_sql:exec(SKey, "GRANT SELECT ON ddTable TO test_user_1;", 0, [{schema,imem}], IsSec)),
        ?assertEqual(true, imem_seco:has_permission(SKey, UserId, {table,ddTable,select})),
        ?assertEqual(false, imem_seco:has_permission(SKey, UserId, {table,ddTable,update})),
        ?assertEqual(false, imem_seco:has_permission(SKey, UserId, {table,ddTable,insert})),
        ?assertEqual(ok, imem_sql:exec(SKey, "grant update, insert on ddTable to test_user_1;", 0, [{schema,imem}], IsSec)),
        ?assertEqual(true, imem_seco:has_permission(SKey, UserId, {table,ddTable,update})),
        ?assertEqual(true, imem_seco:has_permission(SKey, UserId, {table,ddTable,insert})),
        ?assertEqual(ok, imem_sql:exec(SKey, "revoke update on ddTable from test_user_1;", 0, [{schema,imem}], IsSec)),
        ?assertEqual(false, imem_seco:has_permission(SKey, UserId, {table,ddTable,update})),
        ?assertEqual(true, imem_seco:has_permission(SKey, UserId, {table,ddTable,insert})),
        ?assertEqual(ok, imem_sql:exec(SKey, "revoke update,insert on ddTable from test_user_1;", 0, [{schema,imem}], IsSec)),
        ?assertEqual(false, imem_seco:has_permission(SKey, UserId, {table,ddTable,update})),
        ?assertEqual(false, imem_seco:has_permission(SKey, UserId, {table,ddTable,insert})),
        ?assertEqual(ok, imem_sql:exec(SKey, "revoke execute on imem_test from test_user_1;", 0, [{schema,imem}], IsSec)),
        ?assertEqual(false, imem_seco:has_permission(SKey, UserId, {module,imem_test,excecute})),
        ?assertEqual(ok, imem_sql:exec(SKey, "revoke manage_system from test_user_1;", 0, [{schema,imem}], IsSec)),
        ?assertEqual(false, imem_seco:has_permission(SKey, UserId, manage_system)),
        ?assertEqual(ok, imem_sql:exec(SKey, "DROP USER test_user_1;", 0, [{schema,imem}], IsSec))
    catch
        Class:Reason ->  ?LogDebug("Exception ~p:~p~n~p~n", [Class, Reason, erlang:get_stacktrace()]),
        ?assert( true == "all tests completed")
    end,
    ok. 

-endif.
