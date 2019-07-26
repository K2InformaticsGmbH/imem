-module(imem_file).
-behavior(ssh_client_key_api).
-include_lib("public_key/include/public_key.hrl").
-include("imem.hrl").
-include("imem_config.hrl").

% behavior callbacks : ssh_client_key_api
-export([add_host_key/3, is_host_key/4, user_key/2]).

-export([connect/1, disconnect/1, list_dir/3, open/4, close/3, read/4,
         read_file/3, write/4, write_file/4, read_file_info/3, rename/4,
         is_dir/3, read_line/3, delete/3, make_dir/3, pread/5]).

% CLI Only helper fucntions
-export([pfx2pem/4]).

-type filename()    :: file:name_all().
-type modes()       :: [read | write].
-type time_out()    :: ssh_sftp:timeout().
-type handle()      :: term() | file:io_device().
-type error()       :: ssh_sftp:reason() | file:posix() | badarg | system_limit
                    | terminated | {no_translation, unicode, latin1} | term().

-define(IS_DRIVELETTER(Letter),(((Letter >= $A) andalso (Letter =< $Z)) orelse
    ((Letter >= $a) andalso (Letter =< $z)))).

-define(DEFAULT_SFTP_OPTS, [{silently_accept_hosts, true},
                            {user_interaction, false}]).

-define(SFTP_CONNECT_TIMEOUT(__HOST, __PORT),
          ?GET_CONFIG(sfhSftpConnectTimeout, [__HOST, __PORT], 3000,
                      "Delay in millisecond before waiting for connection to"
                      " sftp server after which timeout error will be returned")
       ).

-spec(connect(binary() | string() | map()) -> map() | {error, error()}).
connect([$/,$/|_] = Path) -> #{proto => cifs, path => Path};
connect(<<$/,$/,_/binary>> = Path) -> #{proto => cifs, path => Path};
connect([C,$:|_] = Path) when ?IS_DRIVELETTER(C) -> #{proto => local, path => Path};
connect(<<C,$:,_/binary>> = Path) when ?IS_DRIVELETTER(C) -> #{proto => local, path => Path};
connect(<<$/,_/binary>> = Path) -> #{proto => local, path => Path};
connect([$/|_] = Path) -> #{proto => local, path => Path};
connect(#{proto := Proto} = Ctx) when Proto == cifs; Proto == local -> Ctx;
connect(#{proto := sftp, host := Host, port := Port, user := User,
          password := Password, opts := Opts} = Ctx) ->
    EOpts = ?DEFAULT_SFTP_OPTS ++
            [{user, User},
             {password, Password},
             {timeout, ?SFTP_CONNECT_TIMEOUT(Host, Port)} | Opts],
    case ssh_sftp:start_channel(Host, Port, EOpts) of
        {ok, ChannelPid, ConnectionRef} ->
            Ctx#{chn_pid => ChannelPid, conn_ref => ConnectionRef};
        {error, _} = Error -> Error
    end;
connect(#{proto := sftp, host := Host, port := Port, key := Key, user := User,
          key_cb_mod := KeyCbMod, key_cb_args := KeyCbArgs, opts := Opts} = Ctx) ->
    JobName = get(name),
    EOpts = ?DEFAULT_SFTP_OPTS ++
            [{user, User},
             {key_cb, {KeyCbMod, [maps:merge(KeyCbArgs, Key#{name => JobName})]}},
             {timeout, ?SFTP_CONNECT_TIMEOUT(Host, Port)} | Opts],
    case ssh_sftp:start_channel(Host, Port, EOpts) of
        {ok, ChannelPid, ConnectionRef} ->
            Ctx#{chn_pid => ChannelPid, conn_ref => ConnectionRef};
        {error, _} = Error -> Error
    end;
connect(#{proto := sftp, key := _, user := _, key_cb_mod := _} = Ctx) ->
    connect(Ctx#{key_cb_args => #{}});
connect(#{proto := sftp, key := _, user := _} = Ctx) ->
    connect(Ctx#{key_cb_mod => ?MODULE});
connect(_) -> {error, badarg}.

-spec(disconnect(map()) -> ok | {error, error()}).
disconnect(#{proto := sftp, chn_pid := ChannelPid, conn_ref := ConnectionRef}) ->
    ok = ssh_sftp:stop_channel(ChannelPid),
    ssh:close(ConnectionRef);
disconnect(_Ctx) -> ok.

-spec(list_dir(map(), filename(), time_out()) ->
        {ok, [filename()]} | {error, error()}).
list_dir(#{proto := sftp, chn_pid := ChannelPid, path := Path}, Dir, Timeout) ->
    ssh_sftp:list_dir(ChannelPid, filename:join(Path, Dir), Timeout);
list_dir(#{path := Path}, Dir, _Timeout) ->
    file:list_dir(filename:join(Path, Dir)).

-spec(make_dir(map(), filename(), time_out()) -> ok | {error, error()}).
make_dir(#{proto := sftp, chn_pid := ChannelPid, path := Path}, Dir, Timeout) ->
    ssh_sftp:make_dir(ChannelPid, filename:join(Path, Dir), Timeout);
make_dir(#{path := Path}, Dir, _Timeout) ->
    file:make_dir(filename:join(Path, Dir)).

-spec(open(map(), filename(), modes(), time_out()) ->
        {ok, handle()} | {error, error()}).
open(#{proto := sftp, chn_pid := ChannelPid, path := Path}, File, Modes, Timeout) ->
    ssh_sftp:open(ChannelPid, filename:join(Path, File), lists:usort([binary | Modes]), Timeout);
open(#{path := Path}, File, Modes, _Timeout) ->
    file:open(filename:join(Path, File), lists:usort([binary | Modes])).

-spec(close(map(), handle(), time_out()) -> ok | {error, error()}).
close(#{proto := sftp, chn_pid := ChannelPid}, Handle, Timeout) ->
    ssh_sftp:close(ChannelPid, Handle, Timeout);
close(_Ctx, IoDevice, _Timeout) ->
    file:close(IoDevice).

-spec(read(map(), handle(), integer(), time_out()) ->
        {ok, binary()} | eof | {error, error()}).
read(#{proto := sftp, chn_pid := ChannelPid}, Handle, Len, Timeout) ->
    ssh_sftp:read(ChannelPid, Handle, Len, Timeout);
read(_Ctx, IoDevice, Len, _Timeout) ->
    file:read(IoDevice, Len).

-spec(pread(map(), handle(), integer(), integer(), time_out()) ->
        {ok, binary()} | eof | {error, error()}).
pread(#{proto := sftp, chn_pid := ChannelPid}, Handle, Position, Len, Timeout) ->
    ssh_sftp:pread(ChannelPid, Handle, Position, Len, Timeout);
pread(_Ctx, IoDevice, Position, Len, _Timeout) ->
    file:pread(IoDevice, Position, Len).

-spec(read_file(map(), filename(), time_out()) ->
        {ok, binary()} | {error, error()}).
read_file(#{proto := sftp, chn_pid := ChannelPid, path := Path}, File, Timeout) ->
    ssh_sftp:read_file(ChannelPid, filename:join(Path, File), Timeout);
read_file(#{path := Path}, File, _Timeout) ->
    file:read_file(filename:join(Path, File)).

-spec(read_line(map(), handle(), time_out()) ->
        {ok, binary()} | eof | {error, error()}).
read_line(#{proto := sftp}, _Handle, _Timeout) ->
    {error, unimplemented}; %% to be implemented with read and position
read_line(_Ctx, IoDevice, _Timeout) ->
    file:read_line(IoDevice).

-spec(write(map(), handle(), binary(), time_out()) -> ok | {error, error()}).
write(#{proto := sftp, chn_pid := ChannelPid}, Handle, Data, Timeout) ->
    ssh_sftp:write(ChannelPid, Handle, Data, Timeout);
write(_Ctx, IoDevice, Bytes, _Timeout) ->
    file:write(IoDevice, Bytes).

-spec(write_file(map(), filename(), binary(), time_out()) ->
        ok | {error, error()}).
write_file(#{proto := sftp, chn_pid := ChannelPid, path := Path},
           File, Iolist, Timeout) ->
    ssh_sftp:write_file(ChannelPid, filename:join(Path, File), Iolist, Timeout);
write_file(#{path := Path}, File, Bytes, _Timeout) ->
    file:write_file(filename:join(Path, File), Bytes).

-spec(read_file_info(map(), filename(), time_out()) ->
        {ok, file:file_info()} | {error, error()}).
read_file_info(#{proto := sftp, chn_pid := ChannelPid, path := Path}, File, Timeout) ->
    ssh_sftp:read_file_info(ChannelPid, filename:join(Path, File), Timeout);
read_file_info(#{path := Path}, File, _Timeout) ->
    file:read_file_info(filename:join(Path, File)).

-spec(is_dir(map(), filename(), time_out()) -> boolean()).
is_dir(#{proto := sftp, chn_pid := ChannelPid, path := Path}, Dir, Timeout) ->
    case ssh_sftp:opendir(ChannelPid, filename:join(Path, Dir), Timeout) of
        {error, _} -> false;
        {ok, Handle} ->
            ssh_sftp:close(ChannelPid, Handle, Timeout),
            true
    end;
is_dir(#{path := Path}, Dir, _Timeout) ->
    filelib:is_dir(filename:join(Path, Dir)).

-spec(rename(map(), filename(), filename(), timeout()) -> 
        ok | {error, error()}).
rename(#{proto := sftp, chn_pid := ChannelPid}, Source, Destination, Timeout) ->
    ssh_sftp:rename(ChannelPid, Source, Destination, Timeout);
rename(_Ctx, Source, Destination, _Timeout) ->
    file:rename(Source, Destination).

-spec(delete(map(), filename(), timeout()) -> ok | {error, error()}).
delete(#{proto := sftp, chn_pid := ChannelPid, path := Path}, File, Timeout) ->
    ssh_sftp:delete(ChannelPid, filename:join(Path, File), Timeout);
delete(#{path := Path}, File, _Timeout) ->
    file:delete(filename:join(Path, File)).

%------------------------------------------------------------------------------
% behavior callbacks : ssh_client_key_api
%  ssh_sftp:start_channel(..., [{key_cb, {?MODULE, Opts}}, ...])
%------------------------------------------------------------------------------
add_host_key(HostNames, PublicHostKey, _ConnectOptions) ->
    ?Debug("HostNames ~p, PublicHostKey ~p", [HostNames, PublicHostKey]),
    ok.

is_host_key(Key, Host, Algorithm, _ConnectOptions) ->
    ?Debug("Key ~p, Host ~p, Algorithm ~p", [Key, Host, Algorithm]),
    true.

user_key(Algorithm, ConnectOptions) ->
    try
        [#{name := Name} = KeyCb] = proplists:get_value(key_cb_private, ConnectOptions),
        put(name, Name),
        put(jstate, s),
        case KeyCb of
            #{Algorithm := KeyBin} ->
                ?Debug("Found for Algorithm ~p", [Algorithm]),
                {ok, decode_key(Algorithm, KeyBin)};
            Other ->
                ?Debug("no key in: ~p Algorithm : ~p", [Other, Algorithm]),
                {error, not_found}
        end
    catch
        _:Error ->
            ?Error("processing key error : ~p ~p", [Error, erlang:get_stacktrace()]),
            {error, Error}
    end.

%------------------------------------------------------------------------------

%------------------------------------------------------------------------------
% PFX file importer helper functions
% CLI only
% Assumes `openssl` in PATH
%------------------------------------------------------------------------------
pfx2pem(PfxFile, Password, OutKeyFile, OutCertFile) ->
    OpenSsl = os:find_executable("openssl"),
    if OpenSsl == false ->
            error("openssl executable is not found");
        true -> ok
    end,
    CommonArgs = ["pkcs12", "-in", PfxFile, "-nodes", "-passin", "pass:" ++ Password],

    %% Extracting the private key form a PFX to a PEM
    % openssl pkcs12 -in keystore.pfx -nocerts -passin pass:**** -nodes -out key.pem
    PassKeyFile = "pass_" ++ OutKeyFile,
    KeyArgs = CommonArgs ++ ["-nocerts", "-out", PassKeyFile],
    ?Debug("extracting private key into ~s from ~s", ["pass_" ++ OutKeyFile, PfxFile]),
    log_cmd(OpenSsl, KeyArgs,
        erlang:open_port(
              {spawn_executable, OpenSsl},
              [{line, 128},{args, KeyArgs}, exit_status,
               stderr_to_stdout, {parallelism, true}]
        )),
    case filelib:is_regular(PassKeyFile) of
        true ->
            ?Debug("extracted private key into ~s from ~s", [PassKeyFile, PfxFile]);
        false ->
            error("failed to extract private key")
    end,

    %% Removing the password from the extracted private key:
    % openssl rsa -in key.pem -out key_nopass.pem
    PassRemoveArgs = ["rsa", "-in", PassKeyFile, "-out", OutKeyFile],
    log_cmd(OpenSsl, PassRemoveArgs,
        erlang:open_port(
              {spawn_executable, OpenSsl},
              [{line, 128},{args, PassRemoveArgs}, exit_status,
               stderr_to_stdout, {parallelism, true}]
        )),
    case filelib:is_regular(OutKeyFile) of
        true ->
           ?Debug("removed password from private key ~s to ~s", [PassKeyFile, OutKeyFile]);
        false ->
            error("failed to remove password from private key")
    end,
    case file:delete(PassKeyFile) of
        ok ->
            ?Debug("deleted private key ~s", [PassKeyFile]);
        {error, Reason} ->
            error({"failed to delete private key", Reason})
    end,

    %% Exporting the certificate only
    % openssl pkcs12 -in keystore.pfx -clcerts -nokeys -passin pass:**** -nodes -out cert.pem
    CertArgs = CommonArgs ++ ["-clcerts", "-nokeys", "-out", OutCertFile],
    ?Debug("extracting certificates into ~s from ~s", [OutCertFile, PfxFile]),
    log_cmd(OpenSsl, CertArgs,
        erlang:open_port(
              {spawn_executable, OpenSsl},
              [{line, 128},{args, CertArgs}, exit_status,
               stderr_to_stdout, {parallelism, true}]
        )),
    case filelib:is_regular(OutCertFile) of
        true ->
            ?Debug("extracted private key into ~s from ~s", [PassKeyFile, PfxFile]);
        false ->
            error("failed to extract private key")
    end.

log_cmd(Cmd, Args, Port) -> log_cmd(Cmd, Args, Port, []).
log_cmd(Cmd, Args, Port, Buf) when is_port(Port) ->
    receive
        {'EXIT',Port,Reason} -> ?Error("~s terminated for ~p", [Cmd, Reason]);
        {Port,closed} -> ?Error("~s terminated", [Cmd]);
        {Port,{exit_status,Status}} ->
            if Status == 0 -> ?Debug("~s ~p finished successfully", [Cmd, Args]);
               true -> ?Error("~s ~p exit with status ~p", [Cmd, Args, Status])
            end,
            catch erlang:port_close(Port);
        {Port,{data,{F,Line}}} ->
            log_cmd(
              Cmd, Args, Port,
              if F =:= eol ->
                     ?Debug("~s", [lists:reverse([Line|Buf])]),
                     [];
                 true -> [Line|Buf]
              end);
        {Port,{data,Data}} ->
            ?Debug("~p", [Data]),
            log_cmd(Cmd, Args, Port)
    end.
%------------------------------------------------------------------------------

%% private
decode_key(_Algorithm, KeyBin) ->
    [PemEntry] = public_key:pem_decode(KeyBin),
    #'RSAPrivateKey'{} = public_key:pem_entry_decode(PemEntry).

-ifdef(TEST).

-include_lib("eunit/include/eunit.hrl").

connect_test_() ->
    {inparallel,
        [{Title, ?_assertEqual(Expected, connect(Input))}
              || {Title, Input, Expected} <- [
                {"cifs_list_str", "//localhost/share",
                 #{proto => cifs, path => "//localhost/share"}},
                {"cifs_bin_str", <<"//localhost/share">>,
                 #{proto => cifs, path => <<"//localhost/share">>}},
                {"local_list_str_big", "C:/localhost/share",
                 #{proto => local, path => "C:/localhost/share"}},
                {"local_list_str_small", "c:/localhost/share",
                 #{proto => local, path => "c:/localhost/share"}},
                {"local_bin_str_big", <<"C:/localhost/share">>,
                 #{proto => local, path => <<"C:/localhost/share">>}},
                {"local_bin_str_small", <<"c:/localhost/share">>,
                 #{proto => local, path => <<"c:/localhost/share">>}},
                {"unix_bin_str", <<"/home/user/share">>,
                 #{proto => local, path => <<"/home/user/share">>}},
                {"unix_list_str", "/home/user/share",
                 #{proto => local, path => "/home/user/share"}},
                {"bad arg test", "test", {error, badarg}}
            ]
        ]
    }.

-endif.
