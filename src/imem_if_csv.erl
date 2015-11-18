-module(imem_if_csv).
-behavior(gen_server).

-include("imem.hrl").
-include("imem_if.hrl").
-include("imem_if_csv.hrl").
-include_lib("kernel/include/file.hrl").

-define(CSV_INFO_BYTES, 2 * 1024).
-define(ALL_CSV_OPTS, [lineSeparator, columnSeparator, columnCount, columns, header, encoding])).
-define(ALL_CSV_SCHEMA_TOKENS, [ {<<"crlf">>,lineSeparator,<<"\r\n">>}
                               , {<<"lf">>,lineSeparator,<<"\n">>}
                               , {<<"tab">>,columnSeparator,<<"\t">>}
                               , {<<"comma">>,columnSeparator,<<",">>}
                               , {<<"colon">>,columnSeparator,<<":">>}
                               , {<<"pipe">>,columnSeparator,<<"|">>}
                               , {<<"semicolon">>,columnSeparator,<<";">>}
                               , {<<"header">>,header,true}
                               , {<<"utf8">>,encoding,utf8}
                               , {<<"ansi">>,encoding,ansi}
                               % , {<<"123">>,columnCount,123}  % specially treated
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
        , read_blocks/5
        , start_link/1
        ]).

-export([select/4, column_names/1, fetch_start/5]).

-export([file_info/2]).

file_info({Schema,File}) -> file_info(File, schema_to_opts(Schema));
file_info(File) ->          file_info(File, #{}).

file_info(File, Opts) -> file_info(File, Opts, <<>>).

file_info(File, #{lineSeparator := _} = Opts, BinData) ->  
    file_info_col_sep(File, Opts, BinData);
file_info(File, Opts, <<>>) ->
    file_info(File, Opts, file_sample(File,Opts));
file_info(File, Opts, BinData) ->
    CRLFs = count_char_seq(BinData, "\r\n"),
    LFs = count_char_seq(BinData, "\n"),
    LineSeparator = if
        LFs == 0 ->     <<"\n">>;
        CRLFs == LFs -> <<"\r\n">>;
        true ->         <<"\n">>
    end,
    file_info_col_sep(File, Opts#{lineSeparator => LineSeparator}, BinData).

file_info_col_sep(File, #{columnSeparator := _} = Opts, BinData) ->
    file_info_col_count(File, Opts, BinData);
file_info_col_sep(File, Opts, <<>>) ->
    file_info_col_sep(File, Opts, file_sample(File,Opts));
file_info_col_sep(File, #{lineSeparator := LineSeparator} = Opts, BinData) ->
    Rows = binary:split(BinData, LineSeparator, [global]),
    SepCounts = [{count_char_seq(BinData,Sep),Sep} || Sep <- [<<"\t">>,<<";">>,<<",">>]],
    SepList = [S || {_,S} <- lists:reverse(lists:usort(SepCounts))],
    file_info_col_count(File, pick_col_separator(Rows, Opts, SepList), BinData).

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
            maps:value(columnCount,Opts);
        false ->    
            case maps:is_key(columns,Opts) of
                true ->     length(maps:value(columns,Opts));
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

file_info_col_count(File, #{columnCount := _} = Opts, BinData) ->
    file_info_col_names(File, Opts, BinData);
file_info_col_count(File, Opts, <<>>) ->
    file_info_col_names(File, Opts, file_sample(File,Opts));
file_info_col_count(File, #{ lineSeparator := LineSeparator
                           , columnSeparator := ColumnSeparator} = Opts, BinData) ->
    Rows = binary:split(BinData, LineSeparator, [global]),
    RowsSplitBySep = split_cols(Rows, ColumnSeparator),
    NewOpts = case column_count(RowsSplitBySep) of
        false ->                    Opts#{columnCount => 1};    
        CC when is_integer(CC)  ->  Opts#{columnCount => CC}
    end,
    file_info_col_names(File, NewOpts, BinData).

file_info_col_names(File, #{columns := _} = Opts, BinData) ->
    file_info_encoding(File, Opts, BinData);
file_info_col_names(File, Opts, <<>>) ->
    file_info_col_names(File, Opts, file_sample(File,Opts));
file_info_col_names(File, #{ header := true
                           , lineSeparator := LineSeparator
                           , columnSeparator := ColumnSeparator
                           , columnCount := ColumnCount} = Opts, BinData) ->
    Rows = binary:split(BinData, LineSeparator, [global]),
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
    file_info_encoding(File, Opts#{columns => Columns},BinData);
file_info_col_names(File, #{columnCount := ColumnCount} = Opts, BinData) ->
    file_info_encoding(File, Opts#{columns => default_columns(ColumnCount)}, BinData).

file_info_encoding(_File, #{encoding := _} = Opts, _BinData) ->
    Opts;
file_info_encoding(File, Opts, <<>>) ->
    file_info_encoding(File, Opts, file_sample(File,Opts));
file_info_encoding(_File, Opts, _BinData) ->
    Opts#{encoding => utf8}.        % ToDo: guess encoding


file_sample(File,Opts) ->
    {ok, Io} = file:open(File, [raw, read, binary]),
    {ok, D0} = file:pread(Io, 0, ?CSV_INFO_BYTES),
    Data = case byte_size(D0) of
        ?CSV_INFO_BYTES ->  
            {ok, D1} = file:pread(Io, ?CSV_INFO_BYTES, ?CSV_INFO_BYTES),
            LS = case maps:is_key(lineSeparator,Opts) of
                false ->    <<"\n">>;
                true ->     maps:value(lineSeparator,Opts)
            end,
            case binary:split(D1, LS, [global]) of
                [<<>>] ->   D0;
                [D2|_] ->   <<D0/binary,D2/binary,LS/binary>>  % ToDo: concat all except last for more statistics
            end;
        _ ->
            D0            
    end,
    ok = file:close(Io),
    Data.

schema_to_opts(Schema) when is_binary(Schema) ->
    schema_to_opts( binary:split(Schema, <<"$">>, [global]) -- [?CSV_SCHEMA,<<>>], #{}).

schema_to_opts([], Opts) -> Opts;
schema_to_opts([Token|Tokens], Opts) ->
    case (catch list_to_integer(binary_to_list(Token))) of
        N when is_integer(N) ->
            schema_to_opts(Tokens, Opts#{columnCount => N});
        _ ->
            case lists:key_find(Token, 1, ?ALL_CSV_SCHEMA_TOKENS) of
                {Token,Key,Value} ->  
                    schema_to_opts(Tokens, Opts#{Key => Value});
                _ ->
                    ?ClientErrorNoLogging({"Invalid CSV schema token",Token})
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

count_char_seq(D, Le) ->
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


column_names({_CsvSchema,FileName}) ->
    UnquotedFN = imem_datatype:strip_dquotes(FileName),
    #{columns := Columns} = file_info(UnquotedFN),
    Columns.

fetch_start(Pid, {Schema,FileName}, MatchSpec, RowLimit, _CursorOpts) ->
    UnquotedFN = imem_datatype:strip_dquotes(FileName),
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

select({Schema,UnquotedFN}, _MatchSpec, RowLimit, _LockType) ->
    % ?LogDebug("select UnquotedFN ~p",[UnquotedFN]),
    {ok, Io} = file:open(UnquotedFN, [raw, read, binary]),
    CsvOpts = file_info(UnquotedFN, schema_to_opts(Schema)),
    ?LogDebug("CSV Schema Opts : ~p", [CsvOpts]),    
    read_blocks(Io, 0, ?CSV_INFO_BYTES, RowLimit, CsvOpts).

select(#{io := Io, pos := Pos, blockSize := BlockSize, rowLimit := RowLimit, opts := CsvOpts}) ->
    read_blocks(Io, Pos, BlockSize, RowLimit, CsvOpts).

read_blocks(Io, Pos, BlockSize, RowLimit, CsvOpts) ->
    read_blocks(Io, Pos, BlockSize, RowLimit, CsvOpts, []).

read_blocks(Io, Pos, BlockSize, RowLimit, CsvOpts, Rows) -> 
    #{ lineSeparator := LineSeparator
     , columnSeparator := ColumnSeparator
     , columnCount := ColumnCount
     , columns := _Columns
     , header := _Header
     , encoding := _Encoding
     } = CsvOpts,
    LSL = byte_size(LineSeparator),
    case file:pread(Io, Pos, BlockSize) of
        {ok, Bin} -> 
            AllLines = binary:split(Bin, [LineSeparator],[global]),
            Lines = case {binary_ends_with(Bin,LineSeparator), byte_size(Bin)} of
                {true,_} -> 
                    lists:droplast(AllLines);
                {false, BlockSize} ->
                    lists:droplast(AllLines);
                {false, _} ->   
                    AllLines  % eof after next read
            end,
            RecFold = fun(Line,{P,Recs}) ->
                StrFields = binary:split(Line, [ColumnSeparator],[global]),
                LineSize = byte_size(Line)+LSL,
                WithSizeFields = case length(StrFields) of
                    ColumnCount ->
                        [P|[LineSize|StrFields]];
                    N when N < ColumnCount ->
                        [P|[LineSize|StrFields]] ++ lists:duplicate(<<>>,ColumnCount-N);
                    _ ->
                        [P|[LineSize|lists:sublist(StrFields,ColumnCount)]]
                end,
                {P + LineSize, [list_to_tuple([?CSV_RECORD_NAME|WithSizeFields])|Recs]}
            end,
            {LastPos,RevRecs} = lists:foldl(RecFold,{Pos,[]},Lines),
            Filter = fun(_Record) -> true end,                  % ToDo: add predicate filtering with compiled matchspec
            AllNewRows = Rows ++ list:reverse(lists:filter(Filter, RevRecs)), % ToDo: adapt new BlockSize to situation
            if 
                length(AllNewRows) < RowLimit -> 
                    NewPos = LastPos + element(3,hd(RevRecs)),
                    read_blocks(Io, NewPos, BlockSize, RowLimit, CsvOpts, AllNewRows);
                length(AllNewRows) > RowLimit -> 
                    NewRows = Rows ++ lists:sublist(AllNewRows, RowLimit), 
                    NewPos = element(2,lists:nth(RowLimit+1,AllNewRows)),
                    {NewRows, continuation(Io, NewPos, BlockSize, RowLimit, CsvOpts)};
                true -> 
                    NewPos = LastPos + element(3,hd(RevRecs)),
                    {AllNewRows, continuation(Io, NewPos, BlockSize, RowLimit, CsvOpts)}
            end;
        eof -> 
            file:close(Io),
            {Rows, {'$end_of_table'}};
        {_, einval} -> 
            file:close(Io), 
            ?LogDebug("Error reading the file"), 
            {aborted, einval}
    end.

continuation(Io, Pos, BlockSize, RowLimit, CsvOpts) ->
    #{io => Io, pos => Pos, blockSize => BlockSize, rowLimit => RowLimit, opts => CsvOpts}.

%% TESTS ------------------------------------------------------------------
-ifdef(TEST).

-include_lib("eunit/include/eunit.hrl").


setup() -> 
    ?imem_test_setup.

teardown(_SKey) -> 
    ?imem_test_teardown.

csv1_test_() ->
    {
        setup,
        fun setup/0,
        fun teardown/1,
        {with,inorder,[fun test_csv_1/1]}
    }.
    
test_csv_1(_) ->
    try
        % ClEr = 'ClientError',
        % SeEx = 'SecurityException',

        ?LogDebug("----------------------------------"),
        ?LogDebug("---TEST--- ~p ~p", [?MODULE, test_csv_1]),
        ?LogDebug("----------------------------------"),

        ?LogDebug("schema ~p", [imem_meta:schema()]),
        ?assertEqual([],imem_statement:receive_raw()),

        Bin1 = <<"Col1\tCol2\r\nA1\t1\r\nA2\t2\r\n">>,
        ?assertEqual(3, count_char_seq(Bin1,<<"\r\n">>)),
        ?assertEqual(3, count_char_seq(Bin1,<<"\t">>)),
        ?assertEqual(3, count_char_seq(Bin1,<<"\t">>)),
        ?assertEqual(2, count_char_seq(Bin1,<<"A">>)),
        ?assertEqual(1, count_char_seq(Bin1,<<"A1">>)),

        ?assertEqual(2,column_count([[1,2]])),
        ?assertEqual(2,column_count([[a],[1,2]])),
        ?assertEqual(2,column_count([[a],[1,2],[<<>>]])),

        Rows1 = binary:split(Bin1, <<"\r\n">>, [global]),
        RowsSplitBySep1 = split_cols(Rows1, <<"\t">>),
        ?assertEqual([[<<"Col1">>,<<"Col2">>]
                     ,[<<"A1">>,<<"1">>]
                     ,[<<"A2">>,<<"2">>]
                     ,[<<>>]
                     ]
                     ,RowsSplitBySep1),
        ?assertEqual(2,column_count(RowsSplitBySep1)),
        SepCounts1 = [{count_char_seq(Bin1,Sep),Sep} || Sep <- [<<"\t">>,<<";">>,<<",">>]],
        ?assertEqual([{3,<<"\t">>},{0,<<";">>},{0,<<",">>}],SepCounts1),

        CsvFileName = "CsvTestFileName123abc.txt",
        file:write_file(CsvFileName,Bin1),
        ?assertMatch(  #{ columnCount := 2
                        , columnSeparator := <<"\t">>
                        , columns := [<<"col1">>,<<"col2">>]
                        , lineSeparator := <<"\r\n">>
                        , encoding := utf8
                        }
                        , file_info(CsvFileName)),

        ?assertMatch(  #{ columnCount := 2
                        , columnSeparator := <<"\t">>
                        , columns := [<<"Col1">>,<<"Col2">>]
                        , lineSeparator := <<"\r\n">>
                        }
                        , file_info(CsvFileName,#{header => true})),

        ?assertMatch(  #{ columnCount := 2
                        , columnSeparator := <<"\t">>
                        , columns := [<<"col1">>,<<"col2">>]
                        , lineSeparator := <<"\r\n">>
                        }
                        , file_info(CsvFileName,#{header => false})),

        Bin2 = <<"A\r\nCol1\tCol2\r\nA1\t1\r\nA2\t2">>,
        Rows2 = binary:split(Bin2, <<"\r\n">>, [global]),
        RowsSplitBySep2 = split_cols(Rows2, <<"\t">>),
        ?assertEqual([[<<"A">>]
                     ,[<<"Col1">>,<<"Col2">>]
                     ,[<<"A1">>,<<"1">>]
                     ,[<<"A2">>,<<"2">>]
                     ]
                     ,RowsSplitBySep2),
        ?assertEqual(2,column_count(RowsSplitBySep2)),
        SepCounts2 = [{count_char_seq(Bin2,Sep),Sep} || Sep <- [<<"\t">>,<<";">>,<<",">>]],
        ?assertEqual([{3,<<"\t">>},{0,<<";">>},{0,<<",">>}],SepCounts2),

        file:write_file(CsvFileName,Bin2),
        ?assertMatch(  #{ columnCount := 2
                        , columnSeparator := <<"\t">>
                        , columns := [<<"Col1">>,<<"Col2">>]
                        , lineSeparator := <<"\r\n">>
                        , encoding := utf8
                        }
                        , imem_if_csv:file_info(CsvFileName,#{header => true})),

        file:write_file(CsvFileName,<<"\r\nCol1\tCol2\r\nA1\t1\r\nA2\t2">>),
        ?assertMatch(  #{ columnCount := 2
                        , columnSeparator := <<"\t">>
                        , columns := [<<"Col1">>,<<"Col2">>]
                        , lineSeparator := <<"\r\n">>
                        }
                        , imem_if_csv:file_info(CsvFileName,#{header => true})),

        file:write_file(CsvFileName,<<"1\t2\r\nCol1\tCol2\r\nA1\t1\r\nA2\t2">>),
        ?assertMatch(  #{ columnCount := 2
                        , columnSeparator := <<"\t">>
                        , columns := [<<"col1">>,<<"col2">>]
                        , lineSeparator := <<"\r\n">>
                        }
                        , imem_if_csv:file_info(CsvFileName,#{header => true})),

        file:write_file(CsvFileName,<<"1\t2\nCol1\tCol2\nCol1\n">>),
        ?assertMatch(  #{ columnCount := 1
                        , columnSeparator := <<>>
                        , columns := [<<"col1">>]
                        , lineSeparator := <<"\n">>
                        }
                        , imem_if_csv:file_info(CsvFileName,#{header => true})),

        % ?assertEqual(
        %     {[{csv_rec,<<"Col1">>,<<"Col2">>}
        %      ,{csv_rec,<<"A1">>,<<"1">>}
        %      ,{csv_rec,<<"A2">>,<<"2">>}
        %      ]
        %     ,{'$end_of_table'}
        %     }
        % ,
        %     imem_if_csv:select({?CSV_SCHEMA,imem_datatype:strip_dquotes(CsvFileName)}, [], 100, read)
        % ),

        % exec_fetch_sort_equal(SKey, query00, 100, IsSec, "
        %     select * from csv$.\"" ++ CsvFileName ++ "\""   % \"C:\\Temp\\Test.txt\"
        %     ,
        %     [{<<"A1">>,<<"1">>}
        %     ,{<<"A2">>,<<"2">>}
        %     ,{<<"Col1">>,<<"Col2">>}
        %     ]
        % ),

        % exec_fetch_sort_equal(SKey, query00a, 100, IsSec, "
        %     select col2 from csv$.\"" ++ CsvFileName ++ "\""   % \"C:\\Temp\\Test.txt\"
        %     ,
        %     [{<<>>}
        %     ,{<<"1">>}
        %     ,{<<"2">>}
        %     ,{<<"Col2">>}
        %     ]
        % ),

        % exec_fetch_sort_equal(SKey, query00b, 100, IsSec, "
        %     select col2, col1 from csv$.\"C:\\Temp\\Test.txt\"
        %     "
        %     ,
        %     [{<<"1">>,<<"A1">>}
        %     ,{<<"2">>,<<"A2">>}
        %     ,{<<"Col2">>,<<"Col1">>}
        %     ]
        % ),

        % ?assert(false),
        ok
    catch
        Class:Reason ->
            timer:sleep(100),  
            ?LogDebug("Exception~n~p:~p~n~p~n", [Class, Reason, erlang:get_stacktrace()]),
            ?assert( true == "all tests completed")
    end,
    ok.     

-endif.
