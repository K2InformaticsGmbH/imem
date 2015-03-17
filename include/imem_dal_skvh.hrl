-ifndef(IMEM_DAL_SKVH_HRL).
-define(IMEM_DAL_SKVH_HRL, true).

-define(ADDIF(__Flag,__Opts,__Code),
        (fun(_F,_O) ->
                case proplists:get_value(_F, _O, false) of
                    true -> __Code;
                    false -> ""
                end
        end)(__Flag,__Opts)).
-define(skvhTableTrigger(__Opts, __ExtraFun),
        list_to_binary(
          ["fun(OldRec,NewRec,Table,User) ->\n"
           "    {AuditTable,HistoryTable,TransTime,Channel}\n"
           "        = imem_dal_skvh:build_aux_table_info(Table),\n"
           "    AuditInfoList = imem_dal_skvh:audit_info(User,Channel,AuditTable,TransTime,OldRec,NewRec)",
           ?ADDIF(audit, __Opts,
           ",\n    ok = imem_dal_skvh:write_audit(AuditInfoList)"),
           ?ADDIF(history, __Opts,
           ",\n    ok = imem_dal_skvh:write_history(HistoryTable,AuditInfoList)"),
           (fun(_E) ->
                    if length(_E) == 0 -> "";
                       true -> ",\n    "++_E end
            end)(__ExtraFun),
           "\n    %extend_code_start"
           "\n    %extend_code_end"
           "\nend."])
       ).

-endif. %IMEM_DAL_SKVH_HRL
