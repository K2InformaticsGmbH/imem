-module(imem_if_sys_conf).
-behavior(gen_server).

-include("imem.hrl").
-include("imem_if_sys_conf.hrl").
-include_lib("kernel/include/file.hrl").

% gen_server
-record(state, {path = ""}).

-export([ init/1
        , handle_call/3
        , handle_cast/2
        , handle_info/2
        , terminate/2
        , code_change/3
        , format_status/2
        , start_link/1
        ]).

% API interface
-export([ create_sys_conf/1
        , table_columns/1
        , table_type/1
        , table_record_name/1
        , fetch_start/5
        , update_tables/2
        ]).

% public interface
-export([ path/0
        ]).

%% ----- gen_server -------------------------------------------

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
    process_flag(trap_exit, true),
    {ok, #state{}}.

handle_call(path, _From, State) -> {reply, {ok, State#state.path}, State};
handle_call({add, Path, Table}, _From, State) ->
    try
        imem_meta:create_check_table({ddSysConf, Table}, {record_info(fields, ddSysConf), ?ddSysConf, #ddSysConf{}}, [], system),
        ?Info("ddSysConf table ~p created!", [Table]),
        {reply, {ok, {ddSysConf, Table}}, State#state{path=Path}}
    catch
        Class:Reason ->
            ?Error("failed with ~p:~p(~p)~n", [Class, Reason, erlang:get_stacktrace()]),
            {reply, {error, Reason}, State}
    end;
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
terminate(_Reason, _State) -> ?Error("~p stopping unexpectedly : ~p~n", [?MODULE, _Reason]).

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

format_status(_Opt, [_PDict, _State]) -> ok.

%% ----- API interface ----------------------------------------

-spec create_sys_conf(ddString()) -> ok.
create_sys_conf(Path) ->
    case filelib:is_dir(Path) of
        true ->
            [case gen_server:call(?MODULE, {add, Path, list_to_atom("\"" ++ File ++ "\"")}) of
                {ok, {ddSysConf, _}} -> ok;
                {error, Reason} ->
                    ?SystemExceptionNoLogging({"System Configuration setup failed", {Path, File, Reason}})
            end
            || File <- filelib:wildcard("*.*", Path)],
            ok;
        false ->
            DirPath = filename:dirname(Path),
            File = list_to_atom("\"" ++ filename:basename(Path) ++ "\""),
            case gen_server:call(?MODULE, {add, DirPath, File}) of
                {ok, {ddSysConf, _}} -> ok;
                {error, Reason} ->
                     ?SystemExceptionNoLogging({"System Configuration setup failed", {Path, Reason}})
            end
    end.

-spec table_columns(atom()) -> [atom()].
table_columns(_Table) ->
    record_info(fields, ddSysConf).

-spec table_type(atom()) -> atom().
table_type(_Table) -> set.

-spec table_record_name(atom()) -> atom().
table_record_name(_Table) -> ddSysConf.

-spec fetch_start(pid(), atom(), [tuple()], integer(), ddOptions()) -> pid().
fetch_start(Pid, Table, MatchSpec, BlockSize, Opts) when is_atom(Table) ->
    fetch_start(Pid, imem_datatype:strip_dquotes(atom_to_list(Table)), MatchSpec, BlockSize, Opts);
fetch_start(Pid, Table, _MatchSpec, _BlockSize, _Opts) ->
    File = filename:join([path(), Table]),
    case filelib:is_file(File) of
        true ->
            spawn(
                fun() ->
                    {ok, FileInfo} = file:read_file_info(File, [{time, local}]),
                    Rows = [ file_content_rec(File)
                           , file_size_rec(FileInfo)
                           , file_time_rec(accessed, FileInfo)
                           , file_time_rec(modified, FileInfo)
                           , file_time_rec(created, FileInfo)],
                    receive
                        abort ->    ok;
                        next ->     Pid ! {row, [?sot,?eot|Rows]}
                    end
                end
            );
        false ->
            ?SystemExceptionNoLogging({"File not found",File})
    end.

%% ----- public interface -------------------------------------

path() ->
    {ok, Path} = gen_server:call(?MODULE, path),
    Path.


update_tables(UpdatePlans, Lock) -> update_tables(UpdatePlans, Lock, []).
update_tables([], _Lock, Acc) -> lists:reverse(Acc);
update_tables([[Table, Item, Old, New, Trigger, User, TrOpts]|UpdatePlan], Lock, Acc) ->
    if
        New#ddSysConf.item =/= Old#ddSysConf.item ->
            ?ClientErrorNoLogging({"Key modification not allowed", {Old#ddSysConf.item, New#ddSysConf.item}});
        New#ddSysConf.item =/= content ->
            ?ClientErrorNoLogging({"Readonly field", New#ddSysConf.item});
        true -> ok
    end,
    check_content(Old, New),
    update_tables(UpdatePlan, Lock, [update_xt(Table, Item, Lock, Old, New, Trigger, User, TrOpts) | Acc]).

%% ----- helper internal --------------------------------------

-define(DQuoteStrip(__T), imem_datatype:strip_dquotes(atom_to_list(__T))).
update_xt({_Table,_}, _Item, _Lock, {}, {}, _Trigger, _User, _TrOpts) ->
    ok;
update_xt({Table,_}, _Item, _Lock, Old, {}, _Trigger, _User, _TrOpts) when is_atom(Table), is_tuple(Old) ->
    ?ClientErrorNoLogging({"Truncating file is not allowed", Table}); %% ToDo: Why not?
update_xt({Table,_}, Item, Lock, {}, New, Trigger, User, TrOpts) when is_atom(Table), is_tuple(New) ->
    File = filename:join([path(),?DQuoteStrip(Table)]),
    case file_content_rec(File) of
        #ddSysConf{item = content} ->      
            write_content(File, New),
            Trigger({},New,Table,User,TrOpts),
            {Item,New};
        New ->  
            case Lock of
                none ->
                    Trigger(New,New,Table,User,TrOpts),
                    {Item,New};
                _ ->    
                    ?ConcurrencyExceptionNoLogging({"Data is already created by someone else", {File, New}})
            end;
        Current ->  
            case Lock of
                none ->
                    write_content(File, New),
                    Trigger(Current,New,Table,User,TrOpts),
                    {Item,New};
                _ ->    
                    ?ConcurrencyExceptionNoLogging({"Data is already created by someone else", {File, Current}})
            end
    end;
update_xt({Table,_}, Item, Lock, Old, Old, Trigger, User, TrOpts) when is_atom(Table), is_tuple(Old) ->
    File = filename:join([path(),?DQuoteStrip(Table)]),
    case file_content_rec(File) of
        Old ->      Trigger(Old,Old,Table,User,TrOpts),
                    {Item,Old};
        #ddSysConf{item = content} ->      
            case Lock of
                none ->
                    write_content(File, Old),
                    Trigger({},Old,Table,User,TrOpts),      
                    {Item,Old};
                _ ->    ?ConcurrencyExceptionNoLogging({"Data is deleted by someone else", {File, Old}})
            end;
        Current ->  
            case Lock of
                none ->
                    write_content(File, Old),
                    Trigger(Current,Old,Table,User,TrOpts),      
                    {Item,Old};
                _ ->    ?ConcurrencyExceptionNoLogging({"Data is modified by someone else", {File, Old, Current}})
            end
    end;
update_xt({Table,_}, Item, Lock, Old, New, Trigger, User, TrOpts) when is_atom(Table), is_tuple(Old), is_tuple(New) ->
    File = filename:join([path(),?DQuoteStrip(Table)]),
    if
        Lock == none ->
            write_content(File, New),
            Trigger(file_content_rec(File),New,Table,User,TrOpts),
            {Item,New};
        true ->
            case file_content_rec(File) of
                Old ->      
                    write_content(File, New),
                    Trigger(Old,New,Table,User,TrOpts),
                    {Item,New};
                Curr1 ->    
                    ?ConcurrencyExceptionNoLogging({"Data is modified by someone else", {File, Old, Curr1}})
            end
    end.

%% ----- media access -----------------------------------------

-define(FmtTime(__S), imem_datatype:datetime_to_io(__S)).
file_size_rec(FileInfo) ->
    #ddSysConf{item = size
               , itemStr = integer_to_binary(FileInfo#file_info.size)}.

file_time_rec(accessed, FileInfo) ->
    #ddSysConf{item = accessed
               , itemStr = ?FmtTime(FileInfo#file_info.atime)};
file_time_rec(modified, FileInfo) ->
    #ddSysConf{item = modified
               , itemStr = ?FmtTime(FileInfo#file_info.mtime)};
file_time_rec(created, FileInfo) ->
    #ddSysConf{item = created
               , itemStr = ?FmtTime(FileInfo#file_info.ctime)}.

file_content_rec(File) ->
    case file:read_file(File) of
        {ok, FileContent} ->
            case io_lib:printable_unicode_list(binary_to_list(FileContent)) of
                true -> #ddSysConf{item = content, itemStr = FileContent};
                false -> #ddSysConf{item = content, itemBin = FileContent}
            end;
        _ -> #ddSysConf{item = content}
    end.

check_content(Old, New) ->
    if
        (New#ddSysConf.itemStr =/= undefined) andalso (New#ddSysConf.itemBin =/= undefined) ->
            ?ClientErrorNoLogging({"Data provided in multiple formats"
                          , {{itemStr, New#ddSysConf.itemStr}, {itemBin, New#ddSysConf.itemBin}}});
        true -> ok
    end,
    if
        (Old#ddSysConf.itemStr =/= undefined) andalso (New#ddSysConf.itemStr =:= undefined) ->
            ?ClientErrorNoLogging({"Input and Existing data format missmatch"
                          , {{exist_str, Old#ddSysConf.itemStr}, {new_bin, New#ddSysConf.itemBin}}});
        (Old#ddSysConf.itemBin =/= undefined) andalso (New#ddSysConf.itemBin =:= undefined) ->
            ?ClientErrorNoLogging({"Input and Existing data format missmatch"
                          , {{exist_bin, Old#ddSysConf.itemBin}, {new_str, New#ddSysConf.itemStr}}});
       true -> ok
    end,
    if
        is_binary(New#ddSysConf.itemStr) andalso (byte_size(New#ddSysConf.itemStr) == 0) andalso
        is_binary(New#ddSysConf.itemBin) andalso (byte_size(New#ddSysConf.itemBin) == 0) ->
            ?ClientErrorNoLogging("Invalid operation truncate");
        true -> ok
    end.

write_content(File, Data) ->
    DataBin = case Data of
        #ddSysConf{itemStr = undefined, itemBin = DB} -> DB;
        #ddSysConf{itemStr = DB, itemBin = undefined} -> DB
    end,
    case file:write_file(File, DataBin) of
        {error, Reason} -> ?SystemExceptionNoLogging({"Failed to update", {File, Reason}});
        _ -> ok
    end.
