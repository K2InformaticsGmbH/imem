-module(imem_if_csv).
-behavior(gen_server).

-include("imem.hrl").
-include("imem_if.hrl").
-include("imem_if_csv.hrl").
-include_lib("kernel/include/file.hrl").

%% -define(CSV_BLOCK_SIZE,?GET_CONFIG(csvBlockSize,[], 4096, "Minimum number of bytes to read for guessing csv paramaters")).
%% -define(CSV_MAX_LINE_SIZE,?GET_CONFIG(csvMaxLineSize,[], 10000000, "Maximum number of bytes to read for a single csv line")).

-define(CSV_BLOCK_SIZE, 4096).
-define(CSV_MAX_LINE_SIZE, 10000000).
-define(CSV_MAX_FILE_SIZE, 10000000).   % overridable in schema name: csv$eof$123 reads 123 bytes max per file
-define(CSV_DELIMITER, <<$">>).
-define(CSV_DELIMITER2, <<$",$">>).     % CSV escape of "
-define(CSV_DELIMITER3, <<$\\,$">>).    % Erlang escape of "

-define(ALL_CSV_OPTS, [lineSeparator, columnSeparator, columnCount, columns, header, encoding, skip]).
-define(ALL_CSV_SCHEMA_TOKENS, [ {<<"crlf">>,lineSeparator,<<"\r\n">>}
                               , {<<"lf">>,lineSeparator,<<"\n">>}
                               , {<<"eof">>,lineSeparator,<<"">>}
                               , {<<"tab">>,columnSeparator,<<"\t">>}
                               , {<<"comma">>,columnSeparator,<<",">>}
                               , {<<"colon">>,columnSeparator,<<":">>}
                               , {<<"pipe">>,columnSeparator,<<"|">>}
                               , {<<"semicolon">>,columnSeparator,<<";">>}
                               , {<<"space">>,columnSeparator,<<" ">>}
                               , {<<"header">>,header,true}
                               , {<<"utf8">>,encoding,utf8}
                               , {<<"ansi">>,encoding,ansi}
                               % , {<<"123">>,columnCount,123}  % specially treated
                               % , {<<"skip123">>,skip,123}
                               ]).

% gen_server
-record(state, {}).

-export([ init/1
        , handle_call/3
        , handle_cast/2
        , handle_info/2
        , terminate/2
        , code_change/3
        , format_status/2
        , start_link/1
        ]).

-export([ file_info/1
        , file_info/3
        , file_list/1
        , column_names/1
        , select/4
        , fetch_start/5
        ]).

% Functions applied with Common Test
-export([ column_count/1
    , count_char_seq/2
    , count_delimiter/2
    , delimited_split/2
    , delimited_split/3
    , file_info/2
    , first_binary_bytes/2
    , last_binary_bytes/2
    , split_cols/2
]).

file_info({Schema,FilePattern}) ->  file_info(FilePattern, schema_to_opts(Schema));
file_info(FilePattern) ->           file_info(FilePattern, #{}).

file_info(FilePattern, Opts) ->     file_info(FilePattern, Opts, <<>>).

file_info(_FilePattern, #{lineSeparator := <<>>} = Opts, _BinData) ->  
    Opts#{ columnSeparator => <<>>
         , columnCount => maps:get(columnCount, Opts, ?CSV_MAX_FILE_SIZE)
         , columns => default_columns(1)
         , encoding => utf8
         };
file_info(FilePattern, #{lineSeparator := _} = Opts, BinData) ->  
    file_info_col_sep(FilePattern, Opts, BinData);
file_info(FilePattern, Opts, <<>>) ->
    file_info(FilePattern, Opts, file_sample(FilePattern,Opts));
file_info(FilePattern, Opts, BinData) ->
    CRLFs = count_char_seq(BinData, <<"\r\n">>),
    LFs = count_char_seq(BinData, <<"\n">>),
    LineSeparator = if
        LFs == 0 ->     <<"\n">>;
        CRLFs == LFs -> <<"\r\n">>;
        true ->         <<"\n">>
    end,
    file_info_col_sep(FilePattern, Opts#{lineSeparator => LineSeparator}, BinData).

file_info_col_sep(FilePattern, #{columnSeparator := _} = Opts, BinData) ->
    file_info_col_count(FilePattern, Opts, BinData);
file_info_col_sep(FilePattern, Opts, <<>>) ->
    file_info_col_sep(FilePattern, Opts, file_sample(FilePattern,Opts));
file_info_col_sep(FilePattern, #{lineSeparator := LineSeparator} = Opts, BinData) ->
    Rows = delimited_split(BinData, LineSeparator),
    SepCounts = [{count_char_seq(BinData,Sep),Sep} || Sep <- [<<"\t">>,<<";">>,<<",">>,<<"|">>,<<":">>]],
    SepList = [S || {_,S} <- lists:reverse(lists:usort(SepCounts))],
    file_info_col_count(FilePattern, pick_col_separator(Rows, Opts, SepList), BinData).

pick_col_separator(_Rows, #{columns := Cols} = Opts, []) ->
    Opts#{ columnSeparator => <<>>
         , columnCount => length(Cols)
         };
pick_col_separator(_Rows, #{columnCount := ColumnCount} = Opts, []) ->
    Opts#{ columnSeparator => <<>>
         , columns => default_columns(ColumnCount)
         };
pick_col_separator(_Rows, Opts, []) ->
    Opts#{ columnSeparator => <<>>
         , columnCount => 1
         , columns => default_columns(1)
         };
pick_col_separator(Rows, Opts, [Sep|Seps]) ->
    ColumnCount = case maps:is_key(columnCount,Opts) of
        true ->     
            maps:get(columnCount,Opts);
        false ->    
            case maps:is_key(columns,Opts) of
                true ->     length(maps:get(columns,Opts));
                false ->    undefined
            end
    end,
    RowsSplitBySep = split_cols(Rows, Sep),
    case {column_count(RowsSplitBySep),ColumnCount} of
        {false,_} ->                    
            pick_col_separator(Rows,Opts,Seps);
        {ColumnCount,ColumnCount} ->
            Opts#{ columnSeparator => Sep};
        {CC,undefined} when is_integer(CC) ->
            Opts#{ columnSeparator => Sep
                 , columnCount => CC
                 };
        {_,_} ->                    
            pick_col_separator(Rows,Opts,Seps)
    end.

file_info_col_count(FilePattern, #{columnCount := _} = Opts, BinData) ->
    file_info_col_names(FilePattern, Opts, BinData);
file_info_col_count(FilePattern, Opts, <<>>) ->
    file_info_col_names(FilePattern, Opts, file_sample(FilePattern,Opts));
file_info_col_count(FilePattern, #{ lineSeparator := LineSeparator
                           , columnSeparator := ColumnSeparator} = Opts, BinData) ->
    Rows = delimited_split(BinData, LineSeparator),
    RowsSplitBySep = split_cols(Rows, ColumnSeparator),
    NewOpts = case column_count(RowsSplitBySep) of
        false ->                    Opts#{columnCount => 1};    
        CC when is_integer(CC)  ->  Opts#{columnCount => CC}
    end,
    file_info_col_names(FilePattern, NewOpts, BinData).

file_info_col_names(FilePattern, #{columns := _} = Opts, BinData) ->
    file_info_encoding(FilePattern, Opts, BinData);
file_info_col_names(FilePattern, Opts, <<>>) ->
    file_info_col_names(FilePattern, Opts, file_sample(FilePattern,Opts));
file_info_col_names(FilePattern, #{ header := true
                           , lineSeparator := LineSeparator
                           , columnSeparator := ColumnSeparator
                           , columnCount := ColumnCount} = Opts, BinData) ->
    Rows = delimited_split(BinData, LineSeparator),
    RowsSplitBySep = split_cols(Rows, ColumnSeparator),
    Columns = 
        case lists:foldl(
            fun (Rw, '$not_selected') ->
                if 
                    length(Rw) == ColumnCount ->
                        case is_name_row(Rw) of
                            true -> Rw;
                            false -> '$look_no_further'
                        end;
                    true -> '$not_selected'
                end;
                (_, Columns) -> Columns
            end, '$not_selected', RowsSplitBySep) of
            NotColumn when NotColumn == '$not_selected'; NotColumn == '$look_no_further' ->
                default_columns(ColumnCount);
            Clms ->     
                Clms
        end,
    file_info_encoding(FilePattern, Opts#{columns => Columns},BinData);
file_info_col_names(FilePattern, #{columnCount := ColumnCount} = Opts, BinData) ->
    file_info_encoding(FilePattern, Opts#{columns => default_columns(ColumnCount)}, BinData).

file_info_encoding(_FilePattern, #{encoding := _} = Opts, _BinData) ->
    Opts;
file_info_encoding(FilePattern, Opts, <<>>) ->
    file_info_encoding(FilePattern, Opts, file_sample(FilePattern,Opts));
file_info_encoding(_FilePattern, Opts, _BinData) ->
    Opts#{encoding => utf8}.        % ToDo: guess encoding

file_list(FilePattern) when is_binary(FilePattern) ->
    file_list(binary_to_list(FilePattern));
file_list(FilePattern) when is_list(FilePattern) ->
    IsFile = fun(_Name) -> not filelib:is_dir(_Name) end,
    [list_to_binary(FN) || FN <- lists:filter(IsFile,filelib:wildcard(FilePattern))].

file_sample(FilePattern, Opts) ->
    file_sample(FilePattern, Opts, file_list(imem_datatype:strip_dquotes(FilePattern))).

file_sample(FilePattern, _Opts, []) ->
    ?ClientErrorNoLogging({"No files matching name pattern", FilePattern});
file_sample(FilePattern, Opts, Files) ->
    file_sample(FilePattern, Opts, Files, file:open(hd(Files), [raw, read, binary])).

file_sample(_FilePattern, Opts, _Files, {ok, Io}) ->
    LineSeparator = case maps:is_key(lineSeparator,Opts) of
        false ->    <<"\n">>;
        true ->     maps:get(lineSeparator,Opts)
    end,
    {ok, D0} = read_line_or_more(Io, 0, ?CSV_BLOCK_SIZE, ?CSV_MAX_LINE_SIZE, LineSeparator),
    Data = case byte_size(D0) of
        ?CSV_BLOCK_SIZE ->  
            {ok, D1} = read_line_or_more(Io, ?CSV_BLOCK_SIZE, ?CSV_BLOCK_SIZE, ?CSV_MAX_LINE_SIZE, LineSeparator),
            case delimited_split(D1, LineSeparator) of
                [<<>>] ->   D0;
                [D2|_] ->   <<D0/binary,D2/binary,LineSeparator/binary>>  % ToDo: concat all except last for more statistics
            end;
        _ ->
            D0            
    end,
    ok = file:close(Io),
    Data;
file_sample(FilePattern,_Opts,_Files,Error) ->
    ?ClientErrorNoLogging({"Error opening CSV file", {FilePattern,Error}}).

schema_to_opts(Schema) when is_binary(Schema) ->
    schema_to_opts( binary:split(Schema, <<"$">>, [global]) -- [?CSV_SCHEMA,<<>>], #{}).

schema_to_opts([], #{skip := _, header := _} = Opts) -> Opts;
schema_to_opts([], #{skip := _} = Opts) -> Opts#{header => false};
schema_to_opts([], #{header := _} = Opts) -> Opts#{skip => 0};
schema_to_opts([], Opts) -> Opts#{skip => 0, header => false};
schema_to_opts([Token|Tokens], Opts) ->
    case (catch list_to_integer(binary_to_list(Token))) of
        N when is_integer(N) ->
            schema_to_opts(Tokens, Opts#{columnCount => N});
        _ ->
            case Token of
                <<"skip",Skip/binary>> ->
                    case (catch list_to_integer(binary_to_list(Skip))) of
                        S when is_integer(S) ->
                            schema_to_opts(Tokens, Opts#{skip => S});
                        _ ->
                            ?ClientErrorNoLogging({"Invalid CSV skip number",Token})
                    end;
                _ ->
                    case lists:keyfind(Token, 1, ?ALL_CSV_SCHEMA_TOKENS) of
                        {Token,Key,Value} ->  
                            schema_to_opts(Tokens, Opts#{Key => Value});
                        _ ->
                            ?ClientErrorNoLogging({"Invalid CSV schema token",Token})
                    end
            end
    end.

binary_ends_with(<<>>,_) -> false;
binary_ends_with(_,<<>>) -> false;
binary_ends_with(Bin,End) ->
    BinSize = byte_size(Bin),
    EndSize = byte_size(End),
    if 
        BinSize < EndSize ->    false;
        BinSize == EndSize ->   (Bin==End);
        true ->                 (binary:part(Bin,{BinSize,-EndSize}) == End)
    end.

count_char_seq(Subject, Pattern) ->
    % case re:run(Subject, Pattern, [global]) of
    %     nomatch -> 0;
    %     {match, Les} -> length(Les)
    % end.
    length(binary:matches(Subject, Pattern)).

split_cols(Rows, SplitWith) ->
    SplitRows = lists:foldl(
                  fun(Row, Acc) ->
                          Acc ++ [delimited_split(Row, SplitWith)]
                  end, [], Rows),
    case lists:flatten(SplitRows) of
        Rows  ->    [];
        _  ->       SplitRows
    end.

unquote(<<>>) -> <<>>;
unquote(?CSV_DELIMITER) -> ?CSV_DELIMITER;
unquote(?CSV_DELIMITER2) -> <<>>;
unquote(?CSV_DELIMITER3) -> ?CSV_DELIMITER3;
unquote(Bin) when is_binary(Bin) ->
    BinSize = byte_size(Bin),
    MinSize = byte_size(?CSV_DELIMITER2),
    if 
        BinSize =< MinSize ->   
            Bin;
        true ->                 
            DelSize = byte_size(?CSV_DELIMITER),
            case {binary:part(Bin,{0,DelSize}),binary:part(Bin,{BinSize,-DelSize})} of
                {?CSV_DELIMITER,?CSV_DELIMITER} ->
                    binary:replace(binary:replace(binary:part(Bin,{1,BinSize-2}),?CSV_DELIMITER2,?CSV_DELIMITER),?CSV_DELIMITER3,?CSV_DELIMITER);
                _ -> Bin
            end
    end.

count_delimiter(S, Delimiter) -> 
    Del = count_char_seq(S, Delimiter),
    DelDel = count_char_seq(S, ?CSV_DELIMITER2),
    SlashDel = count_char_seq(S, ?CSV_DELIMITER3),
    Del-DelDel-DelDel-SlashDel.  

delimited_split(Bin, Separator) -> delimited_split(Bin, Separator, ?CSV_DELIMITER).

delimited_split(Bin, Separator, Delimiter) ->
    delimited_join(binary:split(Bin, [Separator],[global]), Separator, Delimiter, 0, []).

delimited_join([], _Separator, _Delimiter, _Level, Acc) -> lists:reverse(Acc);
delimited_join([S|Splits], Separator, Delimiter, 0, Acc) ->
    NewAcc = [S|Acc],
    delimited_join(Splits, Separator, Delimiter, count_delimiter(S, Delimiter) rem 2, NewAcc);
delimited_join([S|Splits], Separator, Delimiter, 1, Acc) ->
    Existing = hd(Acc),
    NewAcc = [<<Existing/binary,Separator/binary,S/binary>>|tl(Acc)],
    delimited_join(Splits, Separator, Delimiter, (1 + count_delimiter(S, Delimiter)) rem 2, NewAcc).

column_count(Rows) ->
    case lists:foldl(
        fun (R, Len) when is_integer(Len) ->
            RL = length(R),
            if 
                RL >= Len ->    RL;
                R == [<<>>] ->  Len;
                true ->         false
            end;
            (_, Len) -> Len
        end, 0, Rows) of
        0 ->        false;
        false ->    false;
        L ->        L
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


column_names({Schema,FilePattern}) ->
    maps:get(columns,file_info(FilePattern, schema_to_opts(Schema))).

fetch_start(Pid, {Schema,FilePattern}, MatchSpec, RowLimit, _CursorOpts) ->
    UnquotedFN = imem_datatype:strip_dquotes(FilePattern),
    % ?LogDebug("CSV UnquotedFN : ~p", [UnquotedFN]),
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
                            case select({Schema,UnquotedFN}, MatchSpec, RowLimit, read) of
                                {aborted, Reason} ->
                                    exit(Reason);
                                {[],{'$end_of_table'}} ->
                                    Pid ! {row, [?sot,?eot]};
                                {Rows, {'$end_of_table'}} ->
                                    Pid ! {row, [?sot,?eot|Rows]};
                                {Rows, Contd1} ->
                                    Pid ! {row, [?sot|Rows]},
                                    F(F,Contd1)
                            end;
                        Contd0 ->
                            % ?Info("[~p] got continuing fetch...~n", [Pid]),
                            case select(Contd0) of
                                {aborted, Reason} ->
                                    exit(Reason);
                                {[],{'$end_of_table'}} ->
                                    Pid ! {row, ?eot};
                                {Rows, {'$end_of_table'}} ->
                                    Pid ! {row, [?eot|Rows]};
                                {Rows, Contd1} ->
                                    Pid ! {row, Rows},
                                    F(F,Contd1)
                            end
                end
        end
    end,
    spawn(fun() -> F(F,undefined) end).

select({Schema,FilePattern}, MatchSpec, RowLimit, LockType) ->
    select({Schema,FilePattern}, MatchSpec, RowLimit, LockType, file_list(imem_datatype:strip_dquotes(FilePattern))).

select({_Schema,FilePattern}, _MatchSpec, _RowLimit, _LockType, []) ->
    ?ClientErrorNoLogging({"No files matching name pattern", FilePattern});
select({Schema,_FilePattern}, MatchSpec, RowLimit, _LockType, Files) ->
    % ?LogDebug("CSV select FilePattern ~p",[FilePattern]),
    % ?LogDebug("CSV select Matchspec ~p",[MatchSpec]),
    CsvOpts = file_info(hd(Files), schema_to_opts(Schema)),
    % ?LogDebug("CSV Schema Opts : ~p", [CsvOpts]),
    {ok, Io} = file:open(hd(Files), [raw, read, binary]),
    CMS = ets:match_spec_compile(MatchSpec),
    read_blocks(Io, Files, CMS, 0, ?CSV_BLOCK_SIZE, ?CSV_MAX_LINE_SIZE, RowLimit, 0, CsvOpts).

select(#{io:=Io, files:=Files, cms:=CMS, pos:=Pos, blockSize:=BlockSize, maxLineSize:=MaxLineSize, rowLimit:=RowLimit, rowsSkipped:=RowsSkipped, opts:=CsvOpts}) ->
    read_blocks(Io, Files, CMS, Pos, BlockSize, MaxLineSize, RowLimit, RowsSkipped, CsvOpts).

read_blocks(Io, Files, CMS, Pos, BlockSize, MaxLineSize, RowLimit, RowsSkipped, CsvOpts) ->
    read_blocks(Io, Files, CMS, Pos, BlockSize, MaxLineSize, RowLimit, RowsSkipped, CsvOpts, []).

read_blocks(Io, [File|Files], CMS, 0, BlockSize, MaxLineSize, RowLimit, RowsSkipped, #{lineSeparator := <<>>} = CsvOpts, Rows) -> 
    LastFile = (Files == []),
    #{skip := Skip, columnCount := ColumnCount} = CsvOpts,
    case file:pread(Io, Skip, ColumnCount) of
        {ok, Bin} -> 
            file:close(Io),
            AllNewRows = case ets:match_spec_run([list_to_tuple([?CSV_RECORD_NAME, File, Skip, byte_size(Bin), Bin])],CMS) of
                [] ->       Rows;
                [Row] ->    [Row|Rows]
            end,
            AllNewRowsCount = length(AllNewRows),
            if 
                (AllNewRowsCount =< RowLimit) and LastFile ->               % last file read completely
                    {lists:reverse(AllNewRows), {'$end_of_table'}};
                (AllNewRowsCount < RowLimit) ->                             % next file may add one more row
                    {ok, Io1} = file:open(hd(Files), [raw, read, binary]),  % open next file
                    read_blocks(Io1, Files, CMS, 0, BlockSize, MaxLineSize, RowLimit, RowsSkipped, CsvOpts, AllNewRows);
                (AllNewRowsCount == RowLimit) ->                            % enough for now but keep next file open to read
                    {ok, Io2} = file:open(hd(Files), [raw, read, binary]),  % open next file
                    {lists:reverse(AllNewRows), continuation(Io2, Files, CMS, 0, BlockSize, MaxLineSize, RowLimit, RowsSkipped, CsvOpts)}
            end;
        eof -> 
            file:close(Io),
            case LastFile of 
                true ->       
                    {lists:reverse(Rows), {'$end_of_table'}};
                false ->   
                    {ok, IoN} = file:open(hd(Files), [raw, read, binary]),  % open next file
                    read_blocks(IoN, Files, CMS, 0, BlockSize, MaxLineSize, RowLimit, 0, CsvOpts, Rows)
            end;
        {_, einval} -> 
            file:close(Io), 
            ?LogDebug("Error reading the file ~p",[[File|Files]]), 
            {aborted, einval}
    end;
read_blocks(Io, [File|Files], CMS, Pos, BlockSize, MaxLineSize, RowLimit, RowsSkipped, CsvOpts, Rows) -> 
    #{ lineSeparator := LineSeparator
     , columnSeparator := ColumnSeparator
     , columnCount := ColumnCount
     , columns := _Columns
     , header := _Header
     , encoding := Encoding
     , skip := Skip
     } = CsvOpts,
    LSL = byte_size(LineSeparator),
    LastFile = (Files == []),
    case read_line_or_more(Io, Pos, BlockSize, MaxLineSize, LineSeparator) of
        {ok, Bin} -> 
            AllLines = delimited_split(Bin, LineSeparator),
            {Lines,FileRead} = case {binary_ends_with(Bin,LineSeparator), byte_size(Bin) rem BlockSize} of
                {true, 0} -> 
                    {lists:droplast(AllLines),false};   % drop trailing empty split
                {true, _} -> 
                    {lists:droplast(AllLines),true};    % drop trailing empty split
                {false, 0} ->
                    {lists:droplast(AllLines),false};   % drop possibly incomplete row
                {false, _} ->   
                    {AllLines,true}                     % eof expected
            end,
            RecFold = case ColumnSeparator of
                <<>> when ColumnCount == 1 ->
                    fun(Line,{P,Recs}) ->
                        LineSize = byte_size(Line)+LSL,
                        WithSizeFields = [P,LineSize,change_encoding(Line, Encoding, utf8)],
                        {P + LineSize, [list_to_tuple([?CSV_RECORD_NAME,File|WithSizeFields])|Recs]}
                    end;
                <<>> when ColumnCount > 1 ->
                    fun(Line,{P,Recs}) ->
                        LineSize = byte_size(Line)+LSL,
                        WithSizeFields = [P,LineSize,change_encoding(Line, Encoding, utf8)] ++ lists:duplicate(ColumnCount-1,<<>>),
                        {P + LineSize, [list_to_tuple([?CSV_RECORD_NAME,File|WithSizeFields])|Recs]}
                    end;
                CS when is_binary(CS) ->
                    fun(Line,{P,Recs}) ->
                        StrFields = [unquote(change_encoding(L, Encoding, utf8)) || L <- delimited_split(Line, CS)],
                        LineSize = byte_size(Line)+LSL,
                        WithSizeFields = case length(StrFields) of
                            ColumnCount ->
                                [P|[LineSize|StrFields]];
                            N when N < ColumnCount ->
                                [P,LineSize|StrFields] ++ lists:duplicate(ColumnCount-N,<<>>);
                            _ ->
                                [P,LineSize|lists:sublist(StrFields,ColumnCount)]
                        end,
                        {P + LineSize, [list_to_tuple([?CSV_RECORD_NAME,File|WithSizeFields])|Recs]}
                    end
            end,
            {NextPos,RevRecs} = lists:foldl(RecFold,{Pos,[]},Lines),
            RevRecsLength = length(RevRecs),
            {AllRecs, RowsSkipped1} = if
                (RowsSkipped == Skip) ->                    % skipping completed in previous block
                    {lists:reverse(RevRecs), RowsSkipped};
                (RowsSkipped + RevRecsLength) =< Skip  ->   % whole block result must be skipped
                    {[], RowsSkipped + RevRecsLength};
                true ->                                     % skipping complete in this block
                    {_, R} = lists:split(Skip-RowsSkipped,lists:reverse(RevRecs)),
                    {R, Skip}
            end,
            AllNewRows = Rows ++ ets:match_spec_run(AllRecs,CMS),
            AllNewRowsCount = length(AllNewRows),
            if 
                (AllNewRowsCount =< RowLimit) and FileRead and LastFile ->    % last file read completely
                    file:close(Io),
                    {AllNewRows, {'$end_of_table'}};
                (AllNewRowsCount < RowLimit) ->                                 % next block may add more rows
                    % ToDo: maybe increase BlockSize 
                    read_blocks(Io, [File|Files], CMS, NextPos, BlockSize, MaxLineSize, RowLimit, RowsSkipped1, CsvOpts, AllNewRows);
                (AllNewRowsCount > RowLimit) ->      % too many rows read, clip and re-read from next result row
                    % ToDo: maybe decrease BlockSize
                    ClippedRows = lists:sublist(AllNewRows, RowLimit),
                    ClippedPos = element(?CSV_IDX_OFFET,lists:nth(RowLimit+1,AllNewRows)),
                    {ClippedRows, continuation(Io, [File|Files], CMS, ClippedPos, BlockSize, MaxLineSize, RowLimit, RowsSkipped1, CsvOpts)};
                (AllNewRowsCount == RowLimit) ->                        % this file or next files may contain more data 
                    {AllNewRows, continuation(Io, [File|Files], CMS, NextPos, BlockSize, MaxLineSize, RowLimit, RowsSkipped1, CsvOpts)}
            end;
        eof -> 
            file:close(Io),
            case LastFile of 
                true ->       
                    {Rows, {'$end_of_table'}};
                false ->   
                    {ok, IoN} = file:open(hd(Files), [raw, read, binary]),  % open next file
                    read_blocks(IoN, Files, CMS, 0, BlockSize, MaxLineSize, RowLimit, 0, CsvOpts, Rows)
            end;
        {_, einval} -> 
            file:close(Io), 
            ?LogDebug("Error reading the file ~p at position ~p",[[File|Files],Pos]), 
            {aborted, einval}
    end.

read_line_or_more(Io, Pos, BlockSize, MaxLineSize, LineSeparator) ->
    read_line_or_more(Io, Pos, BlockSize, MaxLineSize, LineSeparator,0, []).

read_line_or_more(_Io, _Pos, _BlockSize, MaxLineSize, _LineSeparator, TotalBytes, _Acc) when TotalBytes > MaxLineSize ->
    ?Error("Maximum csv line size exceeded ~p", [MaxLineSize]),
    {aborted, einval};
read_line_or_more(Io, Pos, BlockSize, MaxLineSize, LineSeparator, TotalBytes, Acc) ->
    case file:pread(Io, Pos, BlockSize) of
        {ok, D} ->
            case {byte_size(D),byte_size(LineSeparator)} of
                {0,_} ->
                    list_to_binary(lists:reverse(Acc));
                {BlockSize,1} when Acc == [] ->  
                    case binary:match(D, LineSeparator) of
                        nomatch ->  read_line_or_more(Io, Pos+BlockSize, BlockSize, MaxLineSize, LineSeparator, byte_size(D), [D]);
                        _ ->        {ok, D}
                    end;
                {BlockSize,1} ->  
                    case binary:match(D, LineSeparator) of
                        nomatch ->  read_line_or_more(Io, Pos+BlockSize, BlockSize, MaxLineSize, LineSeparator, TotalBytes + byte_size(D), [D|Acc]);
                        _ ->        {ok, list_to_binary(lists:reverse([D|Acc]))}
                    end;
                {BlockSize,_} when Acc == [] ->  
                    case binary:match(D, LineSeparator) of
                        nomatch ->  read_line_or_more(Io, Pos+BlockSize, BlockSize, MaxLineSize, LineSeparator, byte_size(D), [D]);
                        _ ->        {ok, D}
                    end;
                {BlockSize,_} ->  
                    case binary:match(list_to_binary(lists:reverse([D|Acc])), LineSeparator) of
                        nomatch ->  read_line_or_more(Io, Pos+BlockSize, BlockSize, MaxLineSize, LineSeparator, TotalBytes + byte_size(D), [D|Acc]);
                        _ ->        {ok, list_to_binary(lists:reverse([D|Acc]))}
                    end;
                {_,_} when Acc == [] ->
                    {ok, D};
                {_,_} ->
                    {ok, list_to_binary(lists:reverse([D|Acc]))}
            end;
        Error ->
            Error
    end.

continuation(Io, Files, CMS, Pos, BlockSize, MaxLineSize, RowLimit, RowsSkipped, CsvOpts) ->
    #{io=>Io, files=>Files, cms=>CMS, pos=>Pos, blockSize=>BlockSize, maxLineSize=>MaxLineSize, rowLimit=>RowLimit, rowsSkipped=>RowsSkipped, opts=>CsvOpts}.

change_encoding(Data, From, From) ->
    Data;
change_encoding(Data, ansi, To) ->
    case unicode:characters_to_binary(Data, latin1, To) of
        {incomplete,Encoded, Rest} ->
            Encoded ++ change_encoding(Rest, latin1, To);
        {error, ConvertedData, RestData} ->
            After = last_binary_bytes(ConvertedData,10),
            Before = first_binary_bytes(RestData,10),
            ?Error("Error wile encoding from ~p to ~p after ~p before ~p", [ansi, To, After, Before]),
            error("Error while changing encoding. Please consult ddLog");
        Binary when is_binary(Binary) ->
            Binary
    end.    

last_binary_bytes(B,Len) when is_binary(B), size(B) >= Len -> binary:part(B, size(B)-Len, Len);
last_binary_bytes(B,_) when is_binary(B) -> B;
last_binary_bytes(_,_) -> <<"not_a_binary">>.

first_binary_bytes(B,Len) when is_binary(B), size(B) >= Len -> binary:part(B, 0, Len);
first_binary_bytes(B,_) when is_binary(B) -> B;
first_binary_bytes(_,_) -> <<"not_a_binary">>.
