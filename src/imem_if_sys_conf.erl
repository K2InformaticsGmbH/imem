-module(imem_if_sys_conf).
-behavior(gen_server).

-include("imem.hrl").
-include("imem_if_sys_conf.hrl").

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
        ]).

%% ----- gen_server -------------------------------------------

start_link(Params) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, Params, [{spawn_opt, [{fullsweep_after, 0}]}]).

init(_) ->
    ?Info("~p starting...~n", [?MODULE]),
    ?Info("~p started!~n", [?MODULE]),
    {ok,#state{}}.

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
            || File <- filelib:wildcard("*.*", Path)];
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

