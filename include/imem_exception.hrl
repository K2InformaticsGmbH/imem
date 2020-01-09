-ifndef(IMEM_EXCEPTION_HRL).

-define(IMEM_EXCEPTION_HRL, true).
-define(ClientError(__Reason), ?THROW_EXCEPTION('ClientError', __Reason)).
-define(SystemException(__Reason), ?THROW_EXCEPTION('SystemException', __Reason)).
-define(ConcurrencyException(__Reason), ?THROW_EXCEPTION('ConcurrencyException', __Reason)).
-define(UnimplementedException(__Reason), ?THROW_EXCEPTION('UnimplementedException', __Reason)).
-define(
    THROW_EXCEPTION(__Ex, __Reason),
    (
        fun
            (_Reason) ->
                __Level =
                    case __Ex of
                        'UnimplementedException' -> warning;
                        'ConcurrencyException' -> warning;
                        'ClientError' -> warning;
                        _ -> error
                    end,
                _Rsn = _Reason,
                {__Head, __Fields} =
                    case _Rsn of
                        __Rsn when is_tuple(__Rsn) ->
                            [__H | __R] = tuple_to_list(__Rsn),
                            case __R of
                                [] -> {__H, []};
                                __R when is_tuple(__R) ->
                                    __RL = tuple_to_list(__R),
                                    {
                                        __H,
                                        lists:zip(
                                            [
                                                list_to_atom("ep" ++ integer_to_list(__N))
                                                || __N <- lists:seq(1, length(__RL))
                                            ],
                                            __RL
                                        )
                                    };
                                __R -> {__H, __R}
                            end;
                        __Else -> {__Level, [{ep1, __Else}]}
                    end,
                __Message =
                    if
                        is_atom(__Head) -> list_to_binary(atom_to_list(__Head));
                        is_list(__Head) -> list_to_binary(__Head);
                        true -> <<"invalid exception head">>
                    end,
                {_, {_, [_ | __ST]}} = (catch os:timestamp(1)),
                {__Module, __Function, __Line} = imem_meta:failing_function(__ST),
                __LogRec =
                    #ddLog{
                        logLevel = __Level,
                        pid = self(),
                        module = __Module,
                        function = __Function,
                        line = __Line,
                        node = node(),
                        fields = [{ex, __Ex} | __Fields],
                        message = __Message,
                        stacktrace = __ST
                    },
                __DbResult = {fun () -> __Level end (), (catch imem_meta:write_log(__LogRec))},
                case __DbResult of
                    {warning, ok} -> ok;
                    {warning, _} ->
                        lager:warning(
                            "[_IMEM_] {~p,~p,~p} ~s~n~p~n~p",
                            [__Module, __Function, __Line, __Message, __Fields, __ST]
                        );
                    {error, ok} -> ok;
                    {error, _} ->
                        lager:error(
                            "[_IMEM_] {~p,~p,~p} ~s~n~p~n~p",
                            [__Module, __Function, __Line, __Message, __Fields, __ST]
                        );
                    _ -> ok
                end,
                case __Ex of
                    'SecurityViolation' -> exit({__Ex, _Reason});
                    _ -> throw({__Ex, _Reason})
                end
        end
    ) (__Reason)
).
-endif.
