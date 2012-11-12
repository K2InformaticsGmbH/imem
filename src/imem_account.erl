-module(imem_account).

-include_lib("eunit/include/eunit.hrl").

-include("imem_seco.hrl").

-export([ create/2
        , get/2
        , get_by_name/2
        , update/3
        , delete/2
        , exists/2
        , lock/2
        , unlock/2
        ]).

%% --Interface functions  (calling imem_if for now, not exported) -------------------

if_write(_SeCo, Table, Row) -> 
    imem_if:write(Table, Row).

if_read(_SeCo, Table, RowId) -> 
    imem_if:read(Table, RowId).

if_delete(_SeCo, Table, RowId) ->
    imem_if:delete(Table, RowId).

if_select(_SeCo, Table, MatchSpec) ->
    imem_if:select(Table, MatchSpec). 

if_read_account_by_name(SeCo, Name) -> 
    MatchHead = #ddAccount{name='$1', _='_'},
    Guard = {'==', '$1', Name},
    Result = '$_',
    if_select(SeCo, ddAccount, [{MatchHead, [Guard], [Result]}]).

%% --Implementation ------------------------------------------------------------------


create(SeCo, #ddAccount{id=AccountId, name=Name}=Account) when is_binary(Name) ->
    case imem_seco:have_permission(SeCo, manage_accounts) of
        true ->     case if_read(SeCo, ddAccount, AccountId) of
                        [#ddAccount{}] ->  
                            ?ClientError({"Account already exists",AccountId});
                        [] ->   
                            case if_read_account_by_name(SeCo, Name) of
                                [] ->   
                                    ok = if_write(SeCo, ddAccount, Account),
                                    try
                                        ok=imem_role:create(SeCo,AccountId)
                                    catch
                                        _:Error ->  %% simple transaction rollback
                                                        delete(SeCo, Account),
                                                        ?SystemException(Error)
                                    end;
                                [#ddAccount{}] ->     ?ClientError({"Account name already exists for",Name})
                            end
                    end;
        false ->    ?SecurityException({"Create account unauthorized",SeCo})
    end.

get(SeCo, AccountId) -> 
    case imem_seco:have_permission(SeCo, manage_accounts) of
        true ->     case if_read(SeCo, ddAccount, AccountId) of
                        [#ddAccount{}=Account] ->   Account;
                        [] ->                       ?SystemException({"Account does not exist", AccountId})
                    end;
        false ->    ?SecurityException({"Get account unauthorized",SeCo})
    end.

get_by_name(SeCo, Name) -> 
    case imem_seco:have_permission(SeCo, manage_accounts) of
        true ->     case if_read_account_by_name(SeCo, Name) of
                        [#ddAccount{}=Account] ->   Account;
                        [] ->                       ?SystemException({"Account does not exist", Name})
                    end;
        false ->    ?SecurityException({"Get account unauthorized",SeCo})
    end.

update(SeCo, #ddAccount{id=AccountId}=Account, AccountNew) -> 
    case imem_seco:have_permission(SeCo, manage_accounts) of
        true ->     case if_read(SeCo, ddAccount, AccountId) of
                        [] ->           ?ClientError({"Account does not exist", AccountId});
                        [Account] ->    if_write(SeCo, ddAccount, AccountNew);
                        [_] ->          ?ConcurrencyException({"Account is modified by someone else", AccountId})
                    end;
        false ->    ?SecurityException({"Update account unauthorized",SeCo})
    end.    

delete(SeCo, #ddAccount{id=AccountId}=Account) ->
    case imem_seco:have_permission(SeCo, manage_accounts) of
        true ->     case if_read(SeCo, ddAccount, AccountId) of
                        [] ->           ?ClientError({"Account does not exist", AccountId});
                        [Account] ->    delete(SeCo, AccountId);
                        [_] ->          ?ConcurrencyException({"Account is modified by someone else", AccountId})
                    end;
        false ->    ?SecurityException({"Delete account unauthorized",SeCo})
    end;        
delete(SeCo, AccountId) -> 
    case imem_seco:have_permission(SeCo, manage_accounts) of
        true ->     case if_delete(SeCo, ddAccount, AccountId) of
                        ok ->               imem_role:delete(SeCo, AccountId)
                    end;
        false ->    ?SecurityException({"Delete account unauthorized",SeCo})
    end.        

lock(SeCo, #ddAccount{}=Account) -> 
    update(SeCo, Account, Account#ddAccount{locked=true});
lock(SeCo, AccountId) -> 
    Account = get(SeCo, AccountId),
    update(SeCo,  Account, Account#ddAccount{locked=true}).

unlock(SeCo, #ddAccount{}=Account) -> 
    update(SeCo, Account, Account#ddAccount{locked=false,lastFailureTime=undefined});
unlock(SeCo, AccountId) -> 
    Account = get(SeCo, AccountId),
    update(SeCo, Account, Account#ddAccount{locked=false,lastFailureTime=undefined}).

exists(SeCo, #ddAccount{id=AccountId}=Account) ->   %% exists unchanged
    case imem_seco:have_permission(SeCo, manage_accounts) of
        true ->     case if_read(SeCo, ddAccount, AccountId) of
                        [] ->           false;
                        [Account] ->    true;
                        [_] ->          false
                    end;
        false ->    ?SecurityException({"Exists account unauthorized",SeCo})
    end;                    
exists(SeCo, AccountId) ->                          %% exists, maybe in changed form
    case imem_seco:have_permission(SeCo, manage_accounts) of
        true ->     case if_read(SeCo, ddAccount, AccountId) of
                        [] ->           false;
                        [_] ->          true
                    end;
        false ->    ?SecurityException({"Exists account unauthorized",SeCo})
    end.            

