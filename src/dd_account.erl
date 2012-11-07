-module(dd_account).

-include_lib("eunit/include/eunit.hrl").

-include("dd.hrl").

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

if_get_account(SeCo, AccountId) -> 
    case if_read(SeCo, ddAccount, AccountId) of
        [] -> {dd_error, {"Account does not exist", AccountId}};
        [Row] -> Row
    end.

if_get_account_by_name(SeCo, Name) -> 
    MatchHead = #ddAccount{name='$1', _='_'},
    Guard = {'==', '$1', Name},
    Result = '$_',
    case if_select(SeCo, ddAccount, [{MatchHead, [Guard], [Result]}]) of
        [] ->           {dd_error, {"Account does not exist", Name}};
        [Account] ->    Account
    end.

%% --Implementation ------------------------------------------------------------------


create(SeCo, #ddAccount{id=AccountId, name=Name}=Account) when is_binary(Name) ->
    case dd_seco:have_permission(SeCo, manage_accounts) of
        true ->     case if_get_account(SeCo, AccountId) of
                        {dd_error, {"Account does not exist", AccountId}} ->   
                            case if_get_account_by_name(SeCo, Name) of
                                {dd_error, {"Account does not exist", Name}} ->   
                                    ok = if_write(SeCo, ddAccount, Account),
                                    case dd_role:create(SeCo,AccountId) of
                                        ok  ->      ok;
                                        Error ->    %% simple transaction rollback
                                                    delete(SeCo, Account),
                                                    ?SystemException(Error)
                                    end;
                                #ddAccount{} ->     ?ClientError({"Account name already exists for",Name});
                                Error ->            ?SystemException(Error)
                            end;
                        #ddAccount{} ->  
                            ?ClientError({"Account already exists",AccountId})
                    end;
        false ->    ?SecurityException({"Create account unauthorized",SeCo})
    end.

get(SeCo, AccountId) -> 
    case dd_seco:have_permission(SeCo, manage_accounts) of
        true ->     case if_get_account(SeCo, AccountId) of
                        #ddAccount{}=Account ->     Account;
                        {dd_error,Error} ->         ?ClientError(Error);
                        Error ->                    ?SystemException(Error)
                    end;
        false ->    ?SecurityException({"Get account unauthorized",SeCo})
    end.

get_by_name(SeCo, Name) -> 
    case dd_seco:have_permission(SeCo, manage_accounts) of
        true ->     case if_get_account_by_name(SeCo, Name) of
                        #ddAccount{}=Account ->     Account;
                        {dd_error,Error} ->         ?ClientError(Error);
                        Error ->                    ?SystemException(Error)
                    end;
        false ->    ?SecurityException({"Get account unauthorized",SeCo})
    end.

update(SeCo, #ddAccount{id=AccountId}=Account, AccountNew) -> 
    case dd_seco:have_permission(SeCo, manage_accounts) of
        true ->     case if_read(SeCo, ddAccount, AccountId) of
                        [] ->           ?ClientError({"Account does not exist", AccountId});
                        [Account] ->    if_write(SeCo, ddAccount, AccountNew);
                        [_] ->          ?ConcurrencyException({"Account is modified by someone else", AccountId});
                        Error ->        ?SystemException(Error)
                    end;
        false ->    ?SecurityException({"Update account unauthorized",SeCo})
    end.    

delete(SeCo, #ddAccount{id=AccountId}=Account) ->
    case dd_seco:have_permission(SeCo, manage_accounts) of
        true ->     case if_read(SeCo, ddAccount, AccountId) of
                        [] ->           ?ClientError({"Account does not exist", AccountId});
                        [Account] ->    delete(SeCo, AccountId);
                        [_] ->          ?ConcurrencyException({"Account is modified by someone else", AccountId});
                        Error ->        ?SystemException(Error)
                    end;
        false ->    ?SecurityException({"Delete account unauthorized",SeCo})
    end;        
delete(SeCo, AccountId) -> 
    case dd_seco:have_permission(SeCo, manage_accounts) of
        true ->     case if_delete(SeCo, ddAccount, AccountId) of
                        ok ->               dd_role:delete(SeCo, AccountId);
                        {dd_error,Error} -> ?ClientError(Error);
                        Error ->            ?SystemException(Error)
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
    case dd_seco:have_permission(SeCo, manage_accounts) of
        true ->     case if_read(SeCo, ddAccount, AccountId) of
                        [] ->           false;
                        [Account] ->    true;
                        [_] ->          false;
                        Error ->        ?SystemException(Error)
                    end;
        false ->    ?SecurityException({"Exists account unauthorized",SeCo})
    end;                    
exists(SeCo, AccountId) ->                          %% exists, maybe in changed form
    case dd_seco:have_permission(SeCo, manage_accounts) of
        true ->     case if_read(SeCo, ddAccount, AccountId) of
                        [] ->           false;
                        [_] ->          true;
                        Error ->        ?SystemException(Error)
                    end;
        false ->    ?SecurityException({"Exists account unauthorized",SeCo})
    end.            

