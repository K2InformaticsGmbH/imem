-module(imem_account).

-include_lib("eunit/include/eunit.hrl").

-include("imem_seco.hrl").

-export([ create/2
        , create/5
        , get/2
        , get_name/2
        , get_by_name/2
        , get_id_by_name/2
        , update/3
        , delete/2
        , delete/3
        , exists/2
        , lock/2
        , unlock/2
        , renew/2
        , expire/2
        , make_id/0
        ]).

%% --Interface functions  (calling imem_if for now, not exported) -------------------

if_write(_SeKey, Table, Row) -> 
    imem_meta:write(Table, Row).

if_read(_SeKey, Table, RowId) -> 
    imem_meta:read(Table, RowId).

if_delete(_SeKey, Table, RowId) ->
    imem_meta:delete(Table, RowId).

%% -- See similar Implementation in imem_account, imem_seco, imem_role -------------- 

if_dirty_index_read(_SeKey, Table, SecKey, Index) -> 
    imem_meta:dirty_index_read(Table, SecKey, Index).

if_select_account_by_name(_SeKey, <<"system">>) -> 
    {if_read(_SeKey, ddAccount, system),true};
if_select_account_by_name(_SeKey, Name) -> 
    {if_dirty_index_read(_SeKey,ddAccount,Name, #ddAccount.name),true}.


%% --Implementation ------------------------------------------------------------------

make_id() -> erlang:phash2(make_ref()).

create(SeKey, Type, Name, FullName, PasswordMd5) -> 
    AccountId = make_id(),
    Cred={pwdmd5, PasswordMd5},
    create(SeKey, #ddAccount{id=AccountId, name=Name, type=Type, fullName=FullName, credentials=[Cred]}).

create(SeKey, #ddAccount{id=AccountId, name=Name}=Account) when is_binary(Name) ->
    case imem_seco:have_permission(SeKey, manage_accounts) of
        true ->     case if_read(SeKey, ddAccount, AccountId) of
                        [#ddAccount{}] ->  
                            ?ClientError({"Account already exists", AccountId});
                        [] ->   
                            case if_select_account_by_name(SeKey, Name) of
                                {[],true} ->
                                    AccountDyn = #ddAccountDyn{id=AccountId},
                                    ok = if_write(SeKey, ddAccountDyn, AccountDyn),   
                                    ok = if_write(SeKey, ddAccount, Account),
                                    try
                                        ok=imem_role:create(SeKey, AccountId)
                                    catch
                                        _:Error ->  %% simple transaction rollback
                                                    delete(SeKey, Account),
                                                    delete(SeKey, AccountDyn),
                                                    ?SystemException(Error)
                                    end;
                                {[#ddAccount{}],true} ->    ?ClientError({"Account already exists", Name})
                            end
                    end;
        false ->    ?SecurityException({"Create account unauthorized",SeKey})
    end.

get(SeKey, AccountId) -> 
    case imem_seco:have_permission(SeKey, manage_accounts) of
        true ->     case if_read(SeKey, ddAccount, AccountId) of
                        [#ddAccount{}=Account] ->   Account;
                        [] ->                       ?ClientError({"Account does not exist", AccountId})
                    end;
        false ->    ?SecurityException({"Get account unauthorized",SeKey})
    end.

get_name(_SeKey, system) -> <<"system">>; 
get_name(SeKey, AccountId) -> 
    case imem_seco:have_permission(SeKey, manage_accounts) of
        true ->     case if_read(SeKey, ddAccount, AccountId) of
                        [#ddAccount{name=Name}] ->  Name;
                        [] ->                       ?ClientError({"Account does not exist", AccountId})
                    end;
        false ->    ?SecurityException({"Get account name unauthorized",SeKey})
    end.

get_by_name(SeKey, Name) -> 
    case imem_seco:have_permission(SeKey, manage_accounts) of
        true ->     case if_select_account_by_name(SeKey, Name) of
                        {[#ddAccount{}=Account],true} ->   Account;
                        {[],true} ->                       ?ClientError({"Account does not exist", Name})
                    end;
        false ->    ?SecurityException({"Get account unauthorized",SeKey})
    end.

get_id_by_name(SeKey, Name) -> 
    case imem_seco:have_permission(SeKey, manage_accounts) of
        true ->     case if_select_account_by_name(SeKey, Name) of
                        {[#ddAccount{}=Account],true} ->   Account#ddAccount.id;
                        {[],true} ->                       ?ClientError({"Account does not exist", Name})
                    end;
        false ->    ?SecurityException({"Get account unauthorized",SeKey})
    end.

update(SeKey, #ddAccount{id=AccountId}=Account, AccountNew) -> 
    case imem_seco:have_permission(SeKey, manage_accounts) of
        true ->     case if_read(SeKey, ddAccount, AccountId) of
                        [] ->           ?ClientError({"Account does not exist", AccountId});
                        [Account] ->    if_write(SeKey, ddAccount, AccountNew);
                        [_] ->          ?ConcurrencyException({"Account is modified by someone else", AccountId})
                    end;
        false ->    ?SecurityException({"Update account unauthorized",SeKey})
    end.    

update_dyn(SeKey, #ddAccountDyn{id=AccountId}=AccountDyn, AccountDynNew) -> 
    case imem_seco:have_permission(SeKey, manage_accounts) of
        true ->     case if_read(SeKey, ddAccountDyn, AccountId) of
                        [] ->           ?ClientError({"AccountDyn does not exist", AccountId});
                        [AccountDyn] -> if_write(SeKey, ddAccountDyn, AccountDynNew);
                        [_] ->          ?ConcurrencyException({"AccountDyn is modified by someone else", AccountId})
                    end;
        false ->    ?SecurityException({"Update account unauthorized",SeKey})
    end.    

delete(SeKey, Name, []) ->
    delete(SeKey, Name);
delete(_SeKey, _Name, Opts) ->
    ?UnimplementedException({"Unimplemented account delete option", Opts}).   %% ToDo: [cascade]

delete(SeKey, Name) when is_binary(Name)->
    delete(SeKey, get_by_name(SeKey, Name));
delete(SeKey, #ddAccount{id=AccountId}=Account) ->
    case imem_seco:have_permission(SeKey, manage_accounts) of
        true ->     case if_read(SeKey, ddAccount, AccountId) of
                        [] ->           ?ClientError({"Account does not exist", AccountId});
                        [Account] ->    delete(SeKey, AccountId);
                        [_] ->          ?ConcurrencyException({"Account is modified by someone else", AccountId})
                    end;
        false ->    ?SecurityException({"Delete account unauthorized",SeKey})
    end;        
delete(SeKey, AccountId) -> 
    case imem_seco:have_permission(SeKey, manage_accounts) of
        true ->     case if_delete(SeKey, ddAccount, AccountId) of
                        ok ->               imem_role:delete(SeKey, AccountId)
                    end;
        false ->    ?SecurityException({"Delete account unauthorized",SeKey})
    end.        

lock(SeKey, Name) when is_binary(Name)->
    lock(SeKey, get_by_name(SeKey, Name));
lock(SeKey, #ddAccount{}=Account) -> 
    update(SeKey, Account, Account#ddAccount{locked=true,lastFailureTime=calendar:local_time()});
lock(SeKey, AccountId) -> 
    Account = get(SeKey, AccountId),
    update(SeKey,  Account, Account#ddAccount{locked=true,lastFailureTime=calendar:local_time()}).

unlock(SeKey, Name) when is_binary(Name)->
    unlock(SeKey, get_by_name(SeKey, Name));
unlock(SeKey, #ddAccount{}=Account) -> 
    update(SeKey, Account, Account#ddAccount{locked=false,lastFailureTime=undefined});
unlock(SeKey, AccountId) -> 
    Account = get(SeKey, AccountId),
    update(SeKey, Account, Account#ddAccount{locked=false,lastFailureTime=undefined}).

renew(SeKey, Name) when is_binary(Name)->
    renew(SeKey, get_by_name(SeKey, Name));
renew(SeKey, #ddAccount{id=AccountId}) ->
    get(SeKey, AccountId),
    [AccountDyn] = if_read(SeKey,ddAccountDyn,AccountId), 
    update_dyn(SeKey, AccountDyn, AccountDyn#ddAccountDyn{lastLoginTime=calendar:local_time()});
renew(SeKey, AccountId) ->
    get(SeKey, AccountId),
    [AccountDyn] = if_read(SeKey,ddAccountDyn,AccountId), 
    update_dyn(SeKey, AccountDyn, AccountDyn#ddAccountDyn{lastLoginTime=calendar:local_time()}).

expire(SeKey, Name) when is_binary(Name)->
    expire(SeKey, get_by_name(SeKey, Name));
expire(SeKey, #ddAccount{id=AccountId}) -> 
    get(SeKey, AccountId),
    [AccountDyn] = if_read(SeKey,ddAccountDyn,AccountId), 
    update_dyn(SeKey, AccountDyn, AccountDyn#ddAccountDyn{lastLoginTime=undefined});
expire(SeKey, AccountId) ->
    get(SeKey, AccountId),
    [AccountDyn] = if_read(SeKey,ddAccountDyn,AccountId), 
    update_dyn(SeKey, AccountDyn, AccountDyn#ddAccountDyn{lastLoginTime=undefined}).

exists(SeKey, Name) when is_binary(Name)->
    exists(SeKey, get_by_name(SeKey, Name));
exists(SeKey, #ddAccount{id=AccountId}=Account) ->   %% exists unchanged
    case imem_seco:have_permission(SeKey, manage_accounts) of
        true ->     case if_read(SeKey, ddAccount, AccountId) of
                        [] ->           false;
                        [Account] ->    true;
                        [_] ->          false
                    end;
        false ->    ?SecurityException({"Exists account unauthorized",SeKey})
    end;                    
exists(SeKey, AccountId) ->                          %% exists, maybe in changed form
    case imem_seco:have_permission(SeKey, manage_accounts) of
        true ->     case if_read(SeKey, ddAccount, AccountId) of
                        [] ->           false;
                        [_] ->          true
                    end;
        false ->    ?SecurityException({"Exists account unauthorized",SeKey})
    end.            

