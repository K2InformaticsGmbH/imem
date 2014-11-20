-module(imem_inet_tcp_dist).

-export([listen/1, accept/1, accept_connection/5,
	 setup/5, close/1, select/1, is_node_name/1]).

listen(Name) ->    
    R = inet_tcp_dist:listen(Name),
    %io:format("listen(~p) -> ~p~n", [Name, R]),
    R.

accept(Listen) ->
    R = inet_tcp_dist:accept(Listen),
    %io:format("accept(~p) -> ~p~n", [Listen, R]),
    R.

accept_connection(AcceptPid, Socket, MyNode, Allowed, SetupTime) ->
    R = inet_tcp_dist:accept_connection(AcceptPid, Socket, MyNode, Allowed, SetupTime),
    %io:format("accept_connection(~p,~p,~p,~p,~p) -> ~p~n",
    %          [AcceptPid, Socket, MyNode, Allowed, SetupTime, R]),
    R.

setup(Node, Type, MyNode, LongOrShortNames, SetupTime) ->
    R = inet_tcp_dist:setup(Node, Type, MyNode, LongOrShortNames,SetupTime),
    %io:format("setup(~p,~p,~p,~p,~p) -> ~p~n",
    %          [Node, Type, MyNode, LongOrShortNames, SetupTime, R]),
    R.

close(Socket) ->
    R = inet_tcp_dist:close(Socket),
    %io:format("close(~p) -> ~p~n", [Socket, R]),
    R.

select(Node) ->
    R = inet_tcp_dist:select(Node),
    %io:format("select(~p) -> ~p~n", [Node, R]),
    R.

is_node_name(Node) ->
    R = inet_tcp_dist:is_node_name(Node),
    %io:format("is_node_name(~p) -> ~p~n", [Node, R]),
    R.

