-ifndef(IMEM_DAL_SKVH_HRL).
-define(IMEM_DAL_SKVH_HRL, 0).

-define(ADDIF(__Flag,__Opts,__Code),
        (fun(_F,_O) ->
                case proplists:get_value(_F, _O, false) of
                    true -> __Code;
                    false -> ""
                end
        end)(__Flag,__Opts)).
-define(skvhTableTrigger(__Opts, __ExtraFun),
        list_to_binary(
          ["fun(OldRec,NewRec,Table,User) -> "
           "    {AuditTable,HistoryTable,TransTime,Channel}"
           "        = imem_dal_skvh:build_aux_table_info(Table),"
           "    AuditInfoList = imem_dal_skvh:audit_info(User,Channel,AuditTable,TransTime,OldRec,NewRec),",
           ?ADDIF(audit, __Opts,
           "ok = imem_dal_skvh:write_audit(AuditInfoList),"),
           ?ADDIF(history, __Opts,
           "ok = imem_dal_skvh:write_history(HistoryTable,AuditInfoList),"),
           (fun(_E) ->
                    if length(_E) == 0 -> "ok"; true ->
           _E end
            end)(__ExtraFun),
           " end."])
       ).

-endif. %IMEM_DAL_SKVH_HRL
