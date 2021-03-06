-module(imem_sql_account).

-include("imem_seco.hrl").

-export([exec/5]).

exec(SKey, {'create user', Name, {'identified by', Password}, Opts}, _Stmt, _StmtOpts, IsSec) ->
    {pwdmd5, PasswordMd5} = imem_seco:create_credentials(pwdmd5, Password),
    if_call_mfa(IsSec, admin_exec, [SKey, imem_account, create, [user, Name, Name, PasswordMd5]]),
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
    case lists:keyfind('identified by', 1, Specs) of
        {'identified by', NewPasswordMayBeDoubleQuoted} ->
            NewPassword = imem_datatype:strip_dquotes(NewPasswordMayBeDoubleQuoted),
            try
                begin
                    PasswordStrengthFun = imem_seco:password_strength_fun(),
                    PasswordStrengthFun(NewPassword)
                end of
                strong ->
                    NewCredentials = imem_seco:create_credentials(pwdmd5, NewPassword),
                    if_call_mfa(IsSec, admin_exec, [SKey, imem_seco, set_credentials, [Name,NewCredentials]]);
                Error when Error /= strong ->
                    ?ClientError(
                       lists:flatten(
                         io_lib:format(
                           "New password insufficiently complex ('~p')",
                           [Error])));
                Other ->
                    ?Error("Bad return from isPasswordComplex function '~p'"
                           " (should be strong|short|medium|weak)", [Other]),
                    ?ClientError("Bad password check function")
            catch
                _:Exception ->
                    ?Error("Cannot check password complexity ~p", [Exception]),
                    ?ClientError("Bad configuration or argument of isPasswordComplex function")
            end;
        false -> ok
    end;

exec(SKey, {'drop user', Name, Specs}, _Stmt, _StmtOpts, IsSec) ->
    if_call_mfa(IsSec, admin_exec, [SKey, imem_account, delete, [Name, Specs]]);

exec(SKey, {'create role', Name}, _Stmt, _StmtOpts, IsSec) ->
    if_call_mfa(IsSec, admin_exec, [SKey, imem_role, create, [Name]]);

exec(SKey, {'drop role', Name}, _Stmt, _StmtOpts, IsSec) ->
    if_call_mfa(IsSec, admin_exec, [SKey, imem_role, delete, [Name]]);

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
    Name = binary_to_list(OBin),
    Pred = fun({_,N}) ->
        case imem_meta:parse_table_name(N) of
            [_,_,Name,_,_,_,_] ->   true;
            _ ->                    false
        end
    end, 
    O = case (catch list_to_existing_atom(Name)) of
        OA when is_atom(OA) ->
            case (catch imem_meta:check_table(OA)) of
                ok ->   
                    {table,OA};
                _ ->    
                    case (catch OA:module_info()) of 
                        L when is_list(L) ->    
                            {module,OA};
                        _ ->
                            case lists:filter(Pred,[QN || #ddAlias{qname=QN} <- imem_meta:read(ddAlias)]) of
                                [] ->   ?SecurityException({"Object does not exist",{SKey,OBin}});
                                _ ->    {table,OA}
                            end
                    end
            end;
        _ ->
            case lists:filter(Pred,[QN || #ddAlias{qname=QN} <- imem_meta:read(ddAlias)]) of
                [] ->   ?SecurityException({"Object does not exist",{SKey,OBin}});
                _ ->    {table,list_to_atom(Name)}
            end
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
    Name = binary_to_list(OBin),
    Pred = fun({_,N}) ->
        case imem_meta:parse_table_name(N) of
            [_,_,Name,_,_,_,_] ->   true;
            _ ->                    false
        end
    end, 
    O = case (catch list_to_existing_atom(Name)) of
        OA when is_atom(OA) ->
            case (catch imem_meta:check_table(OA)) of
                ok ->   
                    {table,OA};
                _ ->
                    case (catch OA:module_info()) of 
                        L when is_list(L) ->    
                            {module,OA};
                        _ ->
                            case lists:filter(Pred,[QN || #ddAlias{qname=QN} <- imem_meta:read(ddAlias)]) of
                                [] ->   ?SecurityException({"Object does not exist",{SKey,OBin}});
                                _ ->    {table,OA}
                            end
                    end
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
