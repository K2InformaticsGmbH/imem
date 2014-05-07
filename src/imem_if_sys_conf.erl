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
    gen_server:start_link({local, ?MODULE}, ?MODULE, Params, [{spawn_opt, [{fullsweep_after, 0}]}]).

init(_) ->
    ?Info("~p starting...~n", [?MODULE]),
    ?Info("~p started!~n", [?MODULE]),
    {ok,#state{}}.

handle_call(path, _From, State) -> {reply, {ok, State#state.path}, State};
handle_call({add, Path, Table}, _From, State) ->
    try
        imem_meta:create_check_table({ddSysConf,Table}, {record_info(fields, ddSysConf),?ddSysConf, #ddSysConf{}}, [], system),
        ?Info("~p created!~n", [Table]),
        {reply, ok, State#state{path=Path}}
    catch
        Class:Reason ->
            ?Error("failed with ~p:~p(~p)~n", [Class,Reason,erlang:get_stacktrace()]),
            {reply, {error, Reason}, State}
    end;
handle_call(Request, From, State) ->
    ?Info("Unknown request ~p from ~p!", [Request, From]),
    {reply, ok, State}.

handle_cast(Request, State) ->
    ?Info("Unknown cast ~p!", [Request]),
    {noreply, State}.

handle_info(Info, State) ->
    ?Info("Unknown info ~p!", [Info]),
    {noreply, State}.

terminate(_Reson, _State) -> ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

format_status(_Opt, [_PDict, _State]) -> ok.

%% ----- API interface ----------------------------------------

create_sys_conf(Path) ->
    case filelib:is_dir(Path) of
        true ->
            [case gen_server:call(?MODULE, {add, Path, list_to_atom("\""++File++"\"")}) of
                 ok -> ok;
                 {error, Reason} ->
                     ?SystemException({"System Configuration setup failed",{Path,File,Reason}})
            end
            || File <- filelib:wildcard("*.*", Path)],
            ok;
        false ->
            DirPath = filname:dirname(Path),
            File = list_to_atom("\""++filname:basename(Path)++"\""),
            case gen_server:call(?MODULE, {add, DirPath, File}) of
                 ok -> ok;
                 {error, Reason} ->
                     ?SystemException({"System Configuration setup failed",{Path,Reason}})
            end
    end.

table_columns(_Table) ->
    record_info(fields, ddSysConf).

table_type(_Table) -> set.
table_record_name(_Table) -> ddSysConf.

fetch_start(Pid, Table, MatchSpec, BlockSize, Opts) when is_atom(Table) ->
    fetch_start(Pid, imem_datatype:strip_dquotes(atom_to_list(Table)), MatchSpec, BlockSize, Opts);
fetch_start(Pid, Table, _MatchSpec, _BlockSize, _Opts) ->
    File = filename:join([path(),Table]),
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
            ?SystemException({"File not found",File})
    end.

%% ----- public interface -------------------------------------

path() ->
    {ok, Path} = gen_server:call(?MODULE, path),
    Path.


update_tables(UpdatePlans, Lock) -> update_tables(UpdatePlans, Lock, []).
update_tables([], _Lock, Acc) -> lists:reverse(Acc);
update_tables([[Table, Item, Old, New]|UpdatePlan], Lock, Acc) ->
    if
        New#ddSysConf.item =/= Old#ddSysConf.item ->
            ?ClientError({"Key modification not allowed", {Old#ddSysConf.item, New#ddSysConf.item}});
        New#ddSysConf.item =/= content ->
            ?ClientError({"Readonly field", New#ddSysConf.item});
        true -> ok
    end,
    check_content(Old, New),
    update_tables(UpdatePlan, Lock, [update_xt(Table, Item, Lock, Old, New) | Acc]).

%% ----- helper internal --------------------------------------

-define(DQuoteStrip(__T), imem_datatype:strip_dquotes(atom_to_list(__T))).
update_xt({_Table,_}, _Item, _Lock, {}, {}) ->
    ok;
update_xt({Table,_}, _Item, _Lock, Old, {}) when is_atom(Table), is_tuple(Old) ->
    ?ClientError({"Truncating file is not allowed", Table});
update_xt({Table,_}, Item, Lock, {}, New) when is_atom(Table), is_tuple(New) ->
    File = filename:join([path(),?DQuoteStrip(Table)]),
    if
        Lock == none ->
            write_content(File, New);
        true ->            
            case file_content_rec(File) of
                New ->      write_content(File, New);
                Current ->  ?ConcurrencyException({"Key violation", {Item,{Current, New}}})
            end
    end,
    {Item,New};
update_xt({Table,_}, Item, none, Old, Old) when is_atom(Table), is_tuple(Old) ->
    {Item,Old};
update_xt({Table,_}, Item, _Lock, Old, Old) when is_atom(Table), is_tuple(Old) ->
    File = filename:join([path(),?DQuoteStrip(Table)]),
    case file_content_rec(File) of
        Old ->    {Item,Old};
        Current ->  ?ConcurrencyException({"Data is modified by someone else", {File, Old, Current}})
    end;
update_xt({Table,_}, Item, Lock, Old, New) when is_atom(Table), is_tuple(Old), is_tuple(New) ->
    File = filename:join([path(),?DQuoteStrip(Table)]),
    if
        Lock == none ->
            write_content(File, New);
        true ->
            case file_content_rec(File) of
                Old ->      write_content(File, New);
                Curr1 ->    ?ConcurrencyException({"Data is modified by someone else", {File, Old, Curr1}})
            end
    end,
    {Item, New}.

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
            ?ClientError({"Data provided in multiple formats"
                          , {{itemStr, New#ddSysConf.itemStr}, {itemBin, New#ddSysConf.itemBin}}});
        true -> ok
    end,
    if
        (Old#ddSysConf.itemStr =/= undefined) andalso (New#ddSysConf.itemStr =:= undefined) ->
            ?ClientError({"Input and Existing data format missmatch"
                          , {{exist_str, Old#ddSysConf.itemStr}, {new_bin, New#ddSysConf.itemBin}}});
        (Old#ddSysConf.itemBin =/= undefined) andalso (New#ddSysConf.itemBin =:= undefined) ->
            ?ClientError({"Input and Existing data format missmatch"
                          , {{exist_bin, Old#ddSysConf.itemBin}, {new_str, New#ddSysConf.itemStr}}});
       true -> ok
    end,
    if
        is_binary(New#ddSysConf.itemStr) andalso (byte_size(New#ddSysConf.itemStr) == 0) andalso
        is_binary(New#ddSysConf.itemBin) andalso (byte_size(New#ddSysConf.itemBin) == 0) ->
            ?ClientError("Invalid operation truncate");
        true -> ok
    end.

write_content(File, Data) ->
    DataBin = case Data of
        #ddSysConf{itemStr = undefined, itemBin = DB} -> DB;
        #ddSysConf{itemStr = DB, itemBin = undefined} -> DB
    end,
    case file:write_file(File, DataBin) of
        {error, Reason} -> ?SystemException({"Failed to update", {File, Reason}});
        _ -> ok
    end.
