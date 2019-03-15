%%% -------------------------------------------------------------------
%%% Author      : Bikram Chatterjee
%%% Description : 
%%% Version     : 
%%% Created     : 30.09.2011
%%% -------------------------------------------------------------------

-module(imem).
-behaviour(application).

-include("imem.hrl").
-include("imem_if.hrl").
-include("imem_meta.hrl").

% shell start/stop helper
-export([ start/0
        , stop/0
        ]).

% application callbacks
-export([ start/2
        , stop/1
        ]).

% library functions
-export([ get_os_memory/0
        , get_vm_memory/0
        , get_swap_space/0
        , spawn_sync_mfa/3
        , priv_dir/0
        , all_apps_version_info/0
        , all_apps_version_info/1
        ]).

-safe([get_os_memory/0, get_vm_memory/0, get_swap_space/0]).


%% ====================================================================
%% External functions
%% ====================================================================
start() ->
    {ok, _} = application:ensure_all_started(?MODULE).

stop() ->
    ok = application:stop(?MODULE).

start(_Type, StartArgs) ->
    % cluster manager node itself may not run any apps
    % it only helps to build up the cluster
    % ?Notice("---------------------------------------------------~n"),
    ?Notice(" STARTING IMEM~n"),
    case application:get_env(erl_cluster_mgrs) of
        {ok, []} -> ?Info("cluster manager node(s) not defined!~n");
        {ok, CMNs} ->
            CMNodes = lists:usort(CMNs) -- [node()],
            ?Info("joining cluster with ~p~n", [CMNodes]),
            ok = join_erl_cluster(CMNodes)
    end,
    case application:get_env(start_time) of
        undefined ->
            % Setting a start time for the first node in cluster
            % or started without CMs
            ok = application:set_env(?MODULE,start_time,{?TIMESTAMP, node()});
        _ -> ok
    end,
    % If in a cluster, wait for other IMEM nodes to complete a full boot before
    % starting mnesia to serialize IMEM start in a cluster
    wait_remote_imem(),
    % Sets max_ets_tables application env parameter with maximum value either
    % configured with ERL_MAX_ETS_TABLES OS environment variable or to default
    % 1400
    ok = application:set_env(
           ?MODULE, max_ets_tables,
           case os:getenv("ERL_MAX_ETS_TABLES") of
               false -> 1400;
               MaxEtsTablesStr -> list_to_integer(MaxEtsTablesStr)
           end),
    % Mnesia should be loaded but not started
    AppInfo = application:info(),
    RunningMnesia = lists:member(mnesia,
                                 [A || {A, _} <- proplists:get_value(running, AppInfo)]),
    if RunningMnesia ->
           ?Error("Mnesia already started~n"),
           {error, mnesia_already_started};
       true ->
           LoadedMnesia = lists:member(
                            mnesia,
                            [A || {A, _, _} <- proplists:get_value(loaded, AppInfo)]),
           if LoadedMnesia -> ok; true -> application:load(mnesia) end,
           config_start_mnesia(),
           case imem_sup:start_link(StartArgs) of
               {ok, _} = Success ->
                   ?Notice(" IMEM STARTED~n"),
                   % ?Notice("---------------------------------------------------~n"),
                   Success;
               Error ->
                   ?Error(" IMEM FAILED TO START ~p~n", [Error]),
                   Error
           end
    end.

wait_remote_imem() ->
    AllNodes = nodes(),

    % Establishing full connection too all nodes of the cluster
    [net_adm:ping(N1)   % failures (pang) can be ignored
     || N1 <- lists:usort(  % removing duplicates
                lists:flatten(
                  [rpc:call(N, erlang, nodes, [])
                   || N <- AllNodes]
                 )
               ) -- [node()] % removing self
    ],

    % Building lists of nodes already running IMEM
    RunningImemNodes =
        [N || N <- AllNodes,
              true == lists:keymember(
                        imem, 1,
                        rpc:call(N, application, which_applications, []))
        ],
    ?Info("Nodes ~p already running IMEM~n", [RunningImemNodes]),

    % Building lists of nodes already loaded IMEM but not in running state
    % (ongoing IMEM boot)
    LoadedButNotRunningImemNodes =
        [N || N <- AllNodes,
              true == lists:keymember(
                        imem, 1,
                        rpc:call(N, application, loaded_applications, []))
        ] -- RunningImemNodes,
    ?Info("Nodes ~p loaded but not running IMEM~n",
          [LoadedButNotRunningImemNodes]),

    % Wait till imem application env parameter start_time for
    % all nodes in LoadedButNotRunningImemNodes are set
    (fun WaitStartTimeSet([]) -> ok;
         WaitStartTimeSet([N|Nodes]) ->
            case rpc:call(N, application, get_env, [?MODULE, start_time]) of
                undefined -> WaitStartTimeSet(Nodes++[N]);
                _ -> WaitStartTimeSet(Nodes)
            end
    end)(LoadedButNotRunningImemNodes),

    % Create a sublist from LoadedButNotRunningImemNodes
    % with the nodes which started before this node
    {ok,{SelfStartTime,_}} = application:get_env(start_time),
    NodesToWaitFor =
        lists:foldl(
          fun(Node, Nodes) ->
                  case rpc:call(Node, application, get_env, [?MODULE, start_time]) of
                      {ok, {StartTime,_}} when StartTime < SelfStartTime ->
                          [Node|Nodes];
                      % Ignoring the node which started after this node
                      % or if RPC fails for any reason
                      _ -> Nodes
                  end
          end, [],
          LoadedButNotRunningImemNodes),
    case NodesToWaitFor of
       [] -> % first node of the cluster
             % no need to wait
            ok;
        NodesToWaitFor ->
            %set_node_status(NodesToWaitFor),
            wait_remote_imem(NodesToWaitFor)
    end.

wait_remote_imem([]) -> ok;
wait_remote_imem([Node|Nodes]) ->
    case rpc:call(Node, application, which_applications, []) of
        {badrpc,nodedown} ->
            % Nodedown moving on
            wait_remote_imem(Nodes);
        RemoteStartedApplications ->
            case lists:keymember(imem,1,RemoteStartedApplications) of
                false ->
                    % IMEM loaded but not finished starting yet,
                    % waiting 500 ms before retrying
                    ?Info("Node ~p starting IMEM, waiting for it to finish starting~n",
                          [Node]),
                    %set_node_status([Node|Nodes]),
                    timer:sleep(500),
                    wait_remote_imem(Nodes++[Node]);
                true ->
                    % IMEM loaded and started, moving on
                    wait_remote_imem(Nodes)
            end
    end.

config_start_mnesia() ->
    {ok, SchemaName} = application:get_env(mnesia_schema_name),
    SDir = atom_to_list(SchemaName) ++ "." ++ atom_to_list(node()),
    {_, SnapDir} = application:get_env(imem, imem_snapshot_dir),
    [_|Rest] = lists:reverse(filename:split(SnapDir)),
    RootParts = lists:reverse(Rest),
    SchemaDir = case ((length(RootParts) > 0) andalso
                      filelib:is_dir(filename:join(RootParts))) of
                    true -> filename:join(RootParts ++ [SDir]);
                    false ->
                        {ok, Cwd} = file:get_cwd(),
                        filename:join([Cwd, SDir])
                end,
    ?Info("schema path ~s~n", [SchemaDir]),
    application:set_env(mnesia, dir, SchemaDir),
    ok = mnesia:start().

stop(_State) ->
    imem_server:stop(),
	?Info("stopped imem_server~n"),
	?Info("stopping mnesia...~n"),
    spawn(
      fun() ->
              stopped = mnesia:stop(),
              ?Info("stopped mnesia~n")
      end),
	?Notice("SHUTDOWN IMEM~n"),
    ?Notice("---------------------------------------------------~n").

join_erl_cluster([]) -> ok;
join_erl_cluster([Node|Nodes]) ->
    case net_adm:ping(Node) of
        pong ->
            case application:get_env(start_time) of
                undefined -> set_start_time(Node);
                _ -> ok
            end,
            ?Info("joined node ~p~n", [Node]);
        pang ->
            ?Info("node ~p down!~n", [Node])
    end,
    join_erl_cluster(Nodes).

set_start_time(Node) ->
    % Setting a start time for this node in cluster requesting an unique time
    % from TimeKeeper
    case rpc:call(Node, application, get_env, [imem, start_time]) of
        {ok, {_,TimeKeeper}} ->
            ok = application:set_env(
                   ?MODULE, start_time,
                   {rpc:call(TimeKeeper, imem_if_mnesia, timestamp, []), TimeKeeper});
        undefined ->
            ok = application:set_env(
                   ?MODULE, start_time,
                   {rpc:call(Node, imem_if_mnesia, timestamp, []), Node})
    end.

-spec get_os_memory() -> {any(), integer(), integer()}.
get_os_memory() ->
    SysData = memsup:get_system_memory_data(),
    FreeMem = lists:foldl(
                fun({T, M}, A)
                      when T =:= free_memory; T =:= buffered_memory; T =:= cached_memory ->
                        A+M;
                   (_, A) -> A
                end, 0, SysData),
    TotalMemory = proplists:get_value(total_memory, SysData),
    case os:type() of
        {win32, _} = Win    -> {Win,        FreeMem,    TotalMemory};
        {unix, _} = Unix    -> {Unix,       FreeMem,    TotalMemory};
        Unknown             -> {Unknown,    FreeMem,    TotalMemory}
    end.

-spec get_vm_memory() -> {any(),integer()}.
get_vm_memory() -> get_vm_memory(os:type()).
get_vm_memory({win32, _} = Win) ->
    {Win, list_to_integer(
            re:replace(
              os:cmd("wmic process where processid=" ++ os:getpid() ++
                     " get workingsetsize | findstr /v \"WorkingSetSize\""),
              "[[:space:]]*", "", [global, {return,list}]))};
get_vm_memory({unix, _} = Unix) ->
    {Unix, erlang:round(
             element(3, get_os_memory())
                        * list_to_float(
                            re:replace(
                              os:cmd("ps -p "++os:getpid()++" -o pmem="),
                              "[[:space:]]*", "", [global, {return,list}])
                           ) / 100)}.

-spec get_swap_space() -> {integer(), integer()}.
get_swap_space() ->
    case maps:from_list(memsup:get_system_memory_data()) of
        #{free_swap := FreeSwap, total_swap := TotalSwap} ->
            {TotalSwap, FreeSwap};
        _ -> {0, 0}
    end.

% An MFA interface that is executed in a spawned short-lived process
% The result is synchronously collected and returned
% Restricts binary memory leakage within the scope of this process
spawn_sync_mfa(M,F,A) ->
    Self = self(),
    spawn(fun() -> Self ! (catch apply(M,F,A)) end),
    receive Result -> Result
    after 60000 -> {error, timeout}
    end.

priv_dir() ->
    case code:priv_dir(?MODULE) of
        {error, bad_name} ->
            filename:join(
              filename:dirname(
                filename:dirname(
                  code:which(?MODULE))), "priv");
        D -> D
    end.

all_apps_version_info() -> all_apps_version_info(_Opts = #{}).

all_apps_version_info(Opts) when is_list(Opts) ->
    all_apps_version_info(maps:from_list(Opts));
all_apps_version_info(Opts) ->
    AllModulesDetails = [
        {
            Mod,
            if is_atom(ModPath) -> atom_to_list(ModPath);
                true -> ModPath
            end
        }
        || {Mod, ModPath} <- code:all_loaded()
    ],
    AllApps = [
        {
            App, list_to_binary(AppVsn),
            element(2, application:get_key(App, modules))
        }
        || {App, _, AppVsn} <- application:which_applications()
    ],
    {FAllApps, FAllModulesDetails} =
        case Opts of
            #{apps := FilterApps} ->
                FilterAppsBin = [
                    if is_atom(A) -> atom_to_binary(A, utf8); true -> A end
                    || A <- FilterApps
                ],
                FilteredAllApps = [
                    App || {A, _, _} = App <- AllApps,
                    lists:member(atom_to_binary(A, utf8), FilterAppsBin)
                ],
                FilteredAllModules = lists:flatten(
                    [Modules || {_, _, Modules} <- FilteredAllApps]
                ),
                FilteredAllModulesDetails = lists:filter(
                    fun({Mod, _}) -> lists:member(Mod, FilteredAllModules) end,
                    AllModulesDetails
                ),
                {FilteredAllApps, FilteredAllModulesDetails};
            _ ->
                {AllApps, AllModulesDetails}
        end,
    all_apps_version_info(merge(FAllApps, FAllModulesDetails), Opts).

merge(AllApps, AllModulesDetails) ->
    merge(AllApps, AllModulesDetails, _Acc = []).

merge([], [], Acc) -> lists:usort(Acc);
merge([], ModulesDetails, Acc) ->
    merge([], [], [{undefined, undefined, ModulesDetails} | Acc]);
merge([{App, AppVsn, Modules} | AllApps], ModulesDetails, Acc) ->
    {ProcessModuleDetails, RestModulesDetails} = lists:partition(
        fun({Mod, _}) -> lists:member(Mod, Modules) end,
        ModulesDetails
    ),
    merge(AllApps, RestModulesDetails,
        [{App, AppVsn, ProcessModuleDetails} | Acc]).

all_apps_version_info(AllApps, Opts) ->
    all_apps_version_info(
        AllApps, Opts#{git => git()}, _ProcessPriv = true, _Acc = []
    ).

all_apps_version_info([], _Opts, _ProcessPriv, Acc) -> lists:usort(Acc);
all_apps_version_info([{_, _, []} | AllApps], Opts, _, Acc) ->
    all_apps_version_info(AllApps, Opts, _ProcessPriv = true, Acc);
all_apps_version_info(
    [{App, AppVsn, [{Mod, ModPath} | Rest]} | AllApps], Opts, ProcessPriv, Acc
) ->
    ModVsn = list_to_binary(
        io_lib:format(
            "vsn:~p",
            [proplists:get_value(vsn, Mod:module_info(attributes))]
        )
    ),
    DDRec = #ddVersion{
        app         = if App /= undefined -> atom_to_binary(App, utf8);
                      true -> App end,
        appVsn      = AppVsn,
        file        = atom_to_binary(Mod, utf8),
        fileVsn     = ModVsn,
        filePath    = list_to_binary(ModPath)
    },
    if ProcessPriv ->
        io:format(
            "~p: ~p modules of ~p-~s~n",
            [{?MODULE,?FUNCTION_NAME,?LINE}, length(Rest) + 1,
                App, AppVsn]
        ),
        {FileOrigin, Opts2} = case mod_gitOrigin(Mod) of
            undefined ->
                {GitRoot, Repo} = git_info(Opts, mod_source(Mod)),
                Opts1 = Opts#{gitRoot => GitRoot, gitRepo => Repo},
                {git_file(Opts1, mod_source(Mod)), Opts1};
            FileOrig -> {FileOrig, Opts}
        end,
        DDRec1 = DDRec#ddVersion{fileOrigin = FileOrigin},
        Acc1 = process_app(DDRec1, Opts2, Mod) ++ Acc,
        PrivDir = code:priv_dir(App),
        NewAcc =
            case filelib:is_dir(PrivDir) of
                true ->
                    PrivFiles = filelib:wildcard("*", PrivDir),
                    {match, [PathRoot]} = re:run(
                        PrivDir,
                        <<"(",(DDRec1#ddVersion.app)/binary, ".*)">>,
                        [{capture, [1], list}]
                    ),
                    io:format(
                        "~p: ~p files @ ~s of ~p-~s~n",
                        [{?MODULE,?FUNCTION_NAME,?LINE},
                        length(PrivFiles), PathRoot, App, AppVsn]
                    ),
                    walk_priv(
                        DDRec1#ddVersion.app,
                        DDRec1#ddVersion.appVsn,
                        PrivDir, PrivFiles, Acc1
                    );
                _ -> Acc1
            end,
        all_apps_version_info(
            [{App, AppVsn, Rest} | AllApps], Opts2, _ProcessPriv = false,
            [DDRec1 | NewAcc]
        );
    true ->
        FileOrigin = case mod_gitOrigin(Mod) of
            undefined -> git_file(Opts, mod_source(Mod));
            FileOrig -> FileOrig
        end,
        all_apps_version_info(
            [{App, AppVsn, Rest} | AllApps], Opts, _ProcessPriv = false,
            [DDRec#ddVersion{fileOrigin = FileOrigin} | Acc])
    end.

process_app(#ddVersion{app = undefined}, _Opts, _Module) -> [];
process_app(#ddVersion{app = App} = DDRec, Opts, Module) ->
    case filename:dirname(
        proplists:get_value(source, Module:module_info(compile))
    ) of
        Dir when is_list(Dir) ->
            File = <<App/binary, ".app.src">>,
            FilePath = filename:join(Dir, File),
            case file_phash2(FilePath) of
                <<>> ->
                    File1 = <<App/binary, ".app">>,
                    FilePath1 = filename:join(Dir, File1),
                    case file_phash2(FilePath1) of
                        <<>> -> [];
                        FileHash1 ->
                            FileOrigin = git_file(Opts, FilePath1),
                            [DDRec#ddVersion{
                                file        = File1,
                                fileVsn     = FileHash1,
                                filePath    = FilePath1,
                                fileOrigin  = FileOrigin
                            }]
                    end;
                FileHash ->
                    FileOrigin = git_file(Opts, FilePath),
                    [DDRec#ddVersion{
                        file        = File,
                        fileVsn     = FileHash,
                        filePath    = FilePath,
                        fileOrigin  = FileOrigin
                    }]
            end;
        _ -> []
    end.

git() ->
    case os:find_executable("git") of
        false -> false;
        GitExe when is_list(GitExe) -> true
    end.

mod_source(Module) when is_atom(Module) ->
    case proplists:get_value(source, Module:module_info(compile)) of
        Path when is_list(Path) -> Path;
        _ -> <<>>
    end.

mod_gitOrigin(Module) when is_atom(Module) ->
    case proplists:get_value(
        gitOrigin,
        proplists:get_value(
            compile_info,
            proplists:get_value(options, Module:module_info(compile)),
            []
        )
    ) of
        undefined -> undefined;
        Url when is_binary(Url) -> Url
    end.

git_info(#{git := true} = Opts, Path) when is_list(Path); is_binary(Path) ->
    case file:set_cwd(filename:dirname(Path)) of
        ok ->
            case list_to_binary(os:cmd("git rev-parse HEAD")) of
                <<"fatal:", _>> -> {<<>>, <<>>};
                Revision ->
                    case re:run(
                        os:cmd("git remote -v"),
                        "(http[^ ]+)", [{capture, [1], list}]
                    ) of
                        {match,[Url|_]} ->
                            git_info(Opts, {Url, Revision});
                        _ -> {<<>>, <<>>}
                    end
            end;
        _ -> {<<>>, <<>>}
    end;
git_info(_Opts, {Url, Revision}) ->
    CleanUrl = re:replace(Url, "\\.git", "", [{return, list}]),
    {list_to_binary([CleanUrl, "/raw/", string:trim(Revision)]),
     lists:last(filename:split(CleanUrl))};
git_info(_Opts, _) -> {<<>>, <<>>}.

git_file(#{git := true, gitRoot := <<>>}, _Path) -> <<>>;
git_file(#{git := true, gitRepo := Repo} = Opts, Path) ->
    case re:run(Path, Repo++"(.*)", [{capture, [1], list}]) of
        {match, [RelativePath]} ->
            git_file(Opts#{absPath => Path}, Path, RelativePath);
        _ -> <<>>
    end;
git_file(_Opts, _Path) -> <<>>.

git_file(#{git := true} = Opts, Path, RelativePath) ->
    BaseName = filename:basename(RelativePath),
    git_file(
        Opts, Path,
        BaseName, RelativePath
    ).
git_file(
    #{git := true, gitRoot := GitRoot} = Opts, Path, BaseName, RelativePath
) ->
    case {
        list_to_binary(os:cmd("git ls-files --error-unmatch " ++ BaseName)),
        filename:extension(RelativePath)
    } of
        {<<"error: pathspec", _/binary>>, ".erl"} ->
            git_file(
                Opts, Path,
                re:replace(
                    RelativePath, "\\.erl", "\\.yrl",
                    [{return, list}]
                )
            );
        {<<"error: pathspec", _/binary>>, ".yrl"} ->
            git_file(
                Opts, Path,
                re:replace(
                    RelativePath, "\\.yrl", "\\.xrl",
                    [{return, list}]
                )
            );
        {<<"error: pathspec", _/binary>>, _} ->
            #{absPath := AbsPath, gitRepo := Repo} = Opts,
            case re:run(
                AbsPath, "lib/"++Repo++"(.*)", [{capture, [1], list}]
            ) of
                nomatch -> notfound;
                {match, [RelativePath1]} ->
                    list_to_binary([GitRoot, RelativePath1])
            end;
        _ -> list_to_binary([GitRoot, RelativePath])
    end;
git_file(_Opts, _Path, _BaseName, _RelativePath) -> <<>>.

walk_priv(_, _, _, [], Acc) -> Acc;
walk_priv(App, AppVsn, Path, [FileOrFolder | Rest], Acc) ->
    FullPath = filename:join(Path, FileOrFolder),
    case filelib:is_dir(FullPath) of
        true ->
            PrivFiles = filelib:wildcard("*", FullPath),
            {match, [PathRoot]} = re:run(
                FullPath,
                <<"(",App/binary, ".*)">>,
                [{capture, [1], list}]
            ),
            io:format(
                "~p: ~p files @ ~s of ~s-~s~n",
                [{?MODULE,?FUNCTION_NAME,?LINE}, length(PrivFiles), PathRoot,
                 App, AppVsn]
            ),
            %log(FullPath, "dderl\-3\.2\.0", "FullPath ~p, Files ~p~n", [FullPath, Files]),
            walk_priv(
                App, AppVsn, Path, Rest,
                walk_priv(App, AppVsn, FullPath, PrivFiles, Acc)
            );
        _ ->
            walk_priv(
                App, AppVsn, Path, Rest,
                [#ddVersion{
                    app         = App,
                    appVsn      = AppVsn,
                    file        = list_to_binary(FileOrFolder),
                    fileVsn     = file_phash2(FullPath),
                    filePath    = list_to_binary(Path)
                } | Acc]
            )
    end.

file_phash2(FilePath) ->
    case file:read_file(FilePath) of
        {ok, FBin} ->
            <<"ph2:",(integer_to_binary(erlang:phash2(FBin)))/binary>>;
        _ -> <<>>
    end.

%log(T, M, F, A) ->
%    case re:run(T, M) of
%        {match, _} ->
%            io:format("~s: "++F, [?ME|A]);
%        _ -> skip
%    end.