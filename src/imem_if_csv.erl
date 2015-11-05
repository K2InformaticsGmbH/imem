-module(imem_if_csv).
-behavior(gen_server).

-include("imem.hrl").
-include("imem_if.hrl").
-include("imem_if_csv.hrl").
-include_lib("kernel/include/file.hrl").

% gen_server
-record(state, {}).

-export([ init/1
        , handle_call/3
        , handle_cast/2
        , handle_info/2
        , terminate/2
        , code_change/3
        , format_status/2
        , read_blocks/4
        , start_link/1
        ]).

-export([select/4, column_names/1, fetch_start/5]).

start_link(Params) ->
    ?Info("~p starting...~n", [?MODULE]),
    case gen_server:start_link({local, ?MODULE}, ?MODULE, Params, [{spawn_opt, [{fullsweep_after, 0}]}]) of
        {ok, _} = Success ->
            ?Info("~p started!~n", [?MODULE]),
            Success;
        Error ->
            ?Error("~p failed to start ~p~n", [?MODULE, Error]),
            Error
    end.

init(_) ->
    % For application gracefull shutdown cleanup
    process_flag(trap_exit, true),
    {ok,#state{}}.

handle_call(_Request, _From, State) ->
    ?Info("Unknown request ~p from ~p!", [_Request, _From]),
    {reply, ok, State}.

handle_cast(_Request, State) ->
    ?Info("Unknown cast ~p!", [_Request]),
    {noreply, State}.

handle_info(Info, State) ->
    ?Info("Unknown info ~p!", [Info]),
    {noreply, State}.

terminate(normal, _State) -> ?Info("~p normal stop~n", [?MODULE]);
terminate(shutdown, _State) -> ?Info("~p shutdown~n", [?MODULE]);
terminate({shutdown, _Term}, _State) -> ?Info("~p shutdown : ~p~n", [?MODULE, _Term]);
terminate(Reason, _State) -> ?Error("~p stopping unexpectedly : ~p~n", [?MODULE, Reason]).

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

format_status(_Opt, [_PDict, _State]) -> ok.


column_names({CsvSchema,FileName}) ->
    UnquotedFN = imem_datatype:strip_dquotes(FileName),
    case select({CsvSchema,UnquotedFN}, [], 100, read) of
        '$end_of_table' ->  [<<"col1">>];
         {Rows, _} ->       first_longest_line(Rows,[])
    end.

first_longest_line([],Acc) ->           ?LogDebug("first_longest_line ~p",[Acc]), 
                                        Acc;     
first_longest_line([Row|Rows],Acc) ->
    [_|R] = tuple_to_list(Row), 
    if 
        (length(R) > length(Acc)) ->    first_longest_line(Rows,name_row(R));
        true ->                         first_longest_line(Rows,Acc)
    end.

name_row(Row) ->
    L = length(Row),
    case length(lists:usort(Row)) of
        L ->
            case lists:usort([is_name(R) || R <- Row]) of 
                [true] ->   Row;
                _ ->        name_default(L)
            end;
        _ ->
            name_default(L)
    end.

is_name(Bin) when is_list(Bin); is_binary(Bin) ->
    case re:run(Bin, "^[A-Za-z][A-Za-z0-9_]*$", [global]) of
        nomatch -> false;
        _ -> true
    end.

name_default(N) ->
    [list_to_binary(lists:flatten("col",integer_to_list(I))) || I <- lists:seq(1, N)].

fetch_start(Pid, {Schema,FileName}, MatchSpec, RowCount, Opts) ->
    UnquotedFN = imem_datatype:strip_dquotes(FileName),
    % ?LogDebug("UnquotedFN : ~p", [UnquotedFN]),
    F =
    fun(F,Contd0) ->
        receive
            abort ->
                % ?Info("[~p] got abort on ~p~n", [Pid, FileName]),
                ok;
            next ->
                case Contd0 of
                        undefined ->
                            % ?Info("[~p] got MatchSpec ~p for ~p limit ~p~n", [Pid,MatchSpec,FileName,BlockSize]),
                            case select({Schema,UnquotedFN}, MatchSpec, RowCount, read) of
                                '$end_of_table' ->
                                    % ?Info("[~p] got empty table~n", [Pid]),
                                    Pid ! {row, [?sot,?eot]};
                                {aborted, Reason} ->
                                    exit(Reason);
                                {Rows, Contd1} ->
                                    % ?Info("[~p] got rows~n~p~n",[Pid,Rows]),
                                    if  Contd1 == '$end_of_table' ->
                                            % ?Info("[~p] complete after ~p~n",[Pid,Contd1]),
                                            Pid ! {row, [?sot,?eot|Rows]};
                                        true ->
                                            % ?Info("[~p] continue with ~p~n",[Pid,Contd1]),
                                            Pid ! {row, [?sot|Rows]},
                                            F(F,Contd1)
                                    end
                            end;
                        Contd0 ->
                            % ?Info("[~p] got continuing fetch...~n", [Pid]),
                            case select(Contd0) of
                                '$end_of_table' ->
                                    % ?Info("[~p] complete after ~n",[Pid,Contd0]),
                                    Pid ! {row, ?eot};
                                {aborted, Reason} ->
                                    exit(Reason);
                                {Rows, Contd1} ->
                                        if  Contd1 == '$end_of_table' ->
                                            % ?Info("[~p] complete after ~p~n",[Pid,Contd1]),
                                            Pid ! {row, [?eot|Rows]};
                                        true ->
                                            % ?Info("[~p] continue with ~p~n",[Pid,Contd1]),
                                            Pid ! {row, Rows},
                                            F(F,Contd1)
                                    end
                            end
                end
        end
    end,
    spawn(fun() -> F(F,undefined) end).

select({_CsvSchema,UnquotedFN}, _MatchSpec, RowCount, _LockType) ->
    ?LogDebug("select UnquotedFN ~p",[UnquotedFN]),
    {ok, Io} = file:open(UnquotedFN, [raw, read, binary]),
    read_blocks(Io, 0, 100, RowCount).

select(#{io := Io, pos := Pos, blockSize := BlockSize, rowCount := RowCount}) ->
    read_blocks(Io, Pos, BlockSize, RowCount).

read_blocks(Io, Pos, BlockSize, RowCount) ->
    read_blocks(Io, Pos, BlockSize, RowCount, []).

read_blocks(Io, Pos, BlockSize, RowCount, Rows) -> 
    case file:pread(Io, Pos, BlockSize) of
        {ok, Bin} -> 
            AllLines = binary:split(Bin, [<<"\n">>],[global]),
            NewPos = case binary:last(Bin) of
                10 -> Pos + BlockSize;
                _ -> Pos +  BlockSize - size(lists:last(AllLines))
            end,
            Lines = lists:droplast(AllLines),
            NewRows = Rows ++ [begin  F = binary:replace(R, <<"\r">>, <<>>), 
                list_to_tuple([?CSV_RECORD_NAME|binary:split(F, [<<"\t">>],[global])])
                end || R <- Lines],
            if 
                length(NewRows) < RowCount -> read_blocks(Io, NewPos, BlockSize, RowCount, NewRows);
                length(NewRows) > RowCount -> 
                    RemovedRows = lists:sublist(NewRows, RowCount + 1, length(NewRows)),
                    RemovedDataSize = lists:foldl(fun(A, Acc) ->
                            Sum = lists:foldl(fun(B, Acc1) ->
                                Acc1 + size(B) end, 0, tl(tuple_to_list(A))),
                            Sum + Acc
                        end, 0, RemovedRows),
                    FinalPos = NewPos - (2 * length(RemovedRows)) - RemovedDataSize,
                    {lists:sublist(NewRows, RowCount), create_file_handler(Io, FinalPos, BlockSize, RowCount)};
                true -> {NewRows, create_file_handler(Io, NewPos, BlockSize, RowCount)}
            end;
        eof -> file:close(Io),{Rows, {'$end_of_table'}};
        {_, einval} -> file:close(Io), ?LogDebug("Error reading the file"), {aborted, einval}
    end.

create_file_handler(Io, Pos, BlockSize, RowCount) ->
    file:close(Io),
    #{io => Io, pos => Pos, blockSize => BlockSize, rowCount => RowCount}.
