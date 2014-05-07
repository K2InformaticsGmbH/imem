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

% public interface
-export([ create_sys_conf/1
        , table_columns/1
        , table_type/1
        , table_record_name/1
        , fetch_start/5
        , path/0
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

%% ----- interface -------------------------------------------
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

path() ->
    {ok, Path} = gen_server:call(?MODULE, path),
    Path.

table_columns(_Table) ->
    record_info(fields, ddSysConf).

table_type(_Table) -> set.
table_record_name(_Table) -> ddSysConf.

-define(FmtTime(__S), imem_datatype:datetime_to_io(__S)).

fetch_start(Pid, Table, MatchSpec, BlockSize, Opts) when is_atom(Table) ->
    fetch_start(Pid, imem_datatype:strip_dquotes(atom_to_list(Table)), MatchSpec, BlockSize, Opts);
fetch_start(Pid, Table, _MatchSpec, _BlockSize, _Opts) ->
    spawn(
        fun() ->
            Path = path(),
            File = filename:join([Path,Table]),
            {ok, FileInfo} = file:read_file_info(File, [{time, local}]),            
            FileContentRec = case file:consult(File) of
                {ok, Terms} -> #ddSysConf{item = content, itemTrm = Terms};
                _ ->
                    case file:read_file(File) of
                        {ok, FileContent} ->
                            case io_lib:printable_unicode_list(binary_to_list(FileContent)) of
                                true -> #ddSysConf{item = content, itemStr = FileContent};
                                false -> #ddSysConf{item = content, itemBin = FileContent}
                            end;
                        _ -> #ddSysConf{item = content}
                    end
            end,
            Rows = [ FileContentRec
                   , #ddSysConf{item = size,     itemTrm = FileInfo#file_info.size,  itemStr = integer_to_binary(FileInfo#file_info.size)}
                   , #ddSysConf{item = accessed, itemTrm = FileInfo#file_info.atime, itemStr = ?FmtTime(FileInfo#file_info.atime)}
                   , #ddSysConf{item = modified, itemTrm = FileInfo#file_info.mtime, itemStr = ?FmtTime(FileInfo#file_info.mtime)}
                   , #ddSysConf{item = created,  itemTrm = FileInfo#file_info.ctime, itemStr = ?FmtTime(FileInfo#file_info.ctime)}],
            receive
                abort ->    ok;
                next ->     Pid ! {row, [?sot,?eot|Rows]}
            end
        end
    ).
