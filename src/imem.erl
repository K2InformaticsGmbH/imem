%%% -------------------------------------------------------------------
%%% Author		: Bikram Chatterjee
%%% Description	: 
%%% Version		: 
%%% Created		: 30.09.2011
%%% -------------------------------------------------------------------

-module(imem).

-include("imem.hrl").

-export([start/0
        , stop/0
        , start_test_writer/1
        ]).

%% ====================================================================
%% External functions
%% ====================================================================
start() ->
    application:start(?MODULE).

stop()  ->
    application:stop(?MODULE).

start_test_writer(Param) ->
    {ok, ImemTimeout} = application:get_env(imem, imem_timeout),
    {ok, SupPid} = supervisor:start_child(imem_sup, { imem_test_writer
                                                   , {imem_test_writer, start_link, [Param]}
                                                   , permanent, ImemTimeout, worker, [imem_test_writer]}),
    [?Log("imem process ~p started pid ~p~n", [Mod, Pid]) || {Mod,Pid,_,_} <- supervisor:which_children(imem_sup)],
    {ok, SupPid}.
