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

-export([file_info/2]).

file_info(File) -> file_info(File, []).
file_info(File, _Opts) ->
    {ok, Io} = file:open(File, [raw, read, binary]),
    {ok, Data} = file:pread(Io, 0, ?INFO_BYTES),
    ok = file:close(Io),
    CRLFs = count_eol_seq(Data, "\r\n"),
    LFs = count_eol_seq(Data, "\n"),
    CRs = count_eol_seq(Data, "\r"),
    LineSeparator = if
        CRLFs == LFs andalso LFs == CRs -> <<"\r\n">>;
        LFs > CRs -> <<"\n">>;
        CRs > LFs -> <<"\r">>;
        true -> <<"\n">>
    end,
    {match, [[DataTillLastLineSep]]}
    = re:run(Data, <<".*", LineSeparator/binary>>,
             [global, dotall, {capture, all, binary}]),
    Rows = lists:reverse(
             case lists:reverse(
                    binary:split(DataTillLastLineSep, LineSeparator, [global])
                   ) of
                 [<<>>|RowsReversed] -> RowsReversed;
                 RowsReversed -> RowsReversed
             end),
    RowsSplitByComma = split_cols(Rows, <<",">>),
    RowsSplitBySemiColon = split_cols(Rows, <<";">>),
    RowsSplitByTab = split_cols(Rows, <<"\t">>),
    {ColumnSeparator,ColumnLength,SelectRowSplit} =
    case column_length(RowsSplitByTab) of
        Len when is_integer(Len) -> {<<"\t">>, Len, RowsSplitByTab};
        _ ->
            case column_length(RowsSplitBySemiColon) of
                Len when is_integer(Len) -> {<<";">>, Len, RowsSplitBySemiColon};
                _ ->
                    case column_length(RowsSplitByComma) of
                        Len when is_integer(Len) -> {<<",">>, Len, RowsSplitByComma};
                        _ ->
                            [Col] = default_columns(1),
                            {<<>>, 1, [<<Col/binary, (lists:nth(1, Rows))/binary>>]}
                    end
            end
    end,
    Columns = case lists:foldl(
                     fun(Rw, '$not_selected') ->
                             case length(Rw) == ColumnLength andalso is_name_row(Rw) of
                                 true -> Rw;
                                 false -> '$not_selected'
                             end;
                        (_, Columns) -> Columns
                     end, '$not_selected', SelectRowSplit) of
                  '$not_selected' -> default_columns(ColumnLength);
                  Clms -> Clms
              end,
    #{lineSeparator => LineSeparator, columnSeparator => ColumnSeparator,
      columnCount => ColumnLength, columns => Columns}.

count_eol_seq(D, Le) ->
    case re:run(D, Le, [global]) of
        nomatch -> 0;
        {match, Les} -> length(Les)
    end.

split_cols(Rows, SplitWith) ->
    SplitRows = lists:foldl(
                  fun(Row, Acc) ->
                          Acc ++ [binary:split(Row, SplitWith, [global])]
                  end, [], Rows),
    case lists:flatten(SplitRows) of
        Rows -> [];
        _ -> SplitRows
    end.

column_length(Rows) ->
    case lists:foldl(
           fun(R, Len) when is_integer(Len) ->
                   RL = length(R),
                   if RL >= Len -> RL;
                      true -> false
                   end;
              (_, Len) -> Len
           end, 0, Rows) of
        0 -> false;
        false -> false;
        L -> L
    end.


is_name_row(Row) ->
    L = length(Row),
    case length(lists:usort(Row)) of
        L ->
            case lists:usort([is_name(R) || R <- Row]) of 
                [true] ->   true;
                _ ->        false
            end;
        _ -> false
    end.

is_name(Bin) when is_list(Bin); is_binary(Bin) ->
    case re:run(Bin, "^[A-Za-z][A-Za-z0-9_]*$", [global]) of
        nomatch -> false;
        _ -> true
    end.

default_columns(N) ->
    [list_to_binary(lists:flatten("col",integer_to_list(I))) || I <- lists:seq(1, N)].

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

handle_info(_Info, State) ->
    ?Info("Unknown info ~p!", [_Info]),
    {noreply, State}.

terminate(normal, _State) -> ?Info("~p normal stop~n", [?MODULE]);
terminate(shutdown, _State) -> ?Info("~p shutdown~n", [?MODULE]);
terminate({shutdown, _Term}, _State) -> ?Info("~p shutdown : ~p~n", [?MODULE, _Term]);
terminate(Reason, _State) -> ?Error("~p stopping unexpectedly : ~p~n", [?MODULE, Reason]).

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

format_status(_Opt, [_PDict, _State]) -> ok.


column_names({_CsvSchema,FileName}) ->
    UnquotedFN = imem_datatype:strip_dquotes(FileName),
    #{columns := Columns} = file_info(UnquotedFN),
    Columns.

fetch_start(Pid, {Schema,FileName}, MatchSpec, RowCount, _Opts) ->
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
    % ?LogDebug("select UnquotedFN ~p",[UnquotedFN]),
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
            {Lines,NewPos} = case binary:last(Bin) of
                10 ->   case lists:last(AllLines) of
                            <<>> -> {lists:droplast(AllLines), Pos + BlockSize};
                            _ ->    {AllLines, Pos + BlockSize}
                        end;
                _ ->    {lists:droplast(AllLines),Pos + BlockSize - size(lists:last(AllLines))}
            end,
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
