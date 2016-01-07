-ifndef(IMEM_DAL_SKVH_HRL).
-define(IMEM_DAL_SKVH_HRL, true).

-define(ADDIF(__Flags,__Opts,__Code),
        (fun(_F,_O) ->
                 case lists:usort(
                        [true||_Fi<-_F,
                              proplists:get_value(_Fi,_O,false)==true]) of
                    [true] -> __Code;
                    [] -> ""
                end
        end)(__Flags,__Opts)).
-define(skvhTableTrigger(__Opts, __ExtraFun),
        list_to_binary(
          ["fun(OldRec,NewRec,Table,User,TrOpts) ->\n"
           "    start_trigger",
           ?ADDIF([audit], __Opts,
           ",\n    {[AuditTable|HistoryTable],TransTime,Channel}\n"
           "        = imem_dal_skvh:build_aux_table_info(Table),\n"
           "    AuditInfoList = imem_dal_skvh:audit_info(User,Channel,AuditTable,TransTime,OldRec,NewRec),\n"
           "    case lists:member(no_audit,TrOpts) of \n"
           "        true ->  ok;\n"
           "        false -> ok = imem_dal_skvh:write_audit(AuditInfoList)\n"
           "    end"),
           ?ADDIF([history], __Opts,
           ",\n    case lists:member(no_history,TrOpts) of \n"
           "        true ->  ok;\n"
           "        false -> ok = imem_dal_skvh:write_history(hd(HistoryTable),AuditInfoList)\n"
           "    end"),
           (fun(_E) ->
                    if length(_E) == 0 -> "";
                       true -> ",\n    "++_E end
            end)(__ExtraFun),
           "\n    %extend_code_start"
           "\n    %extend_code_end"
           "\nend."])
       ).

-endif. %IMEM_DAL_SKVH_HRL
