-module(imem_if).
-compile(export_all).

add_ram_copies(Ns, Opts) -> update_opts({ram_copies,Ns}, Opts).
add_disc_copies(Ns, Opts) -> update_opts({disc_copies,Ns}, Opts).
add_attribute(A, Opts) -> update_opts({attributes,A}, Opts).

update_opts({K,_} = T, Opts) when is_atom(K) -> lists:keystore(K, 1, Opts, T).

build_table(TableName, Columns) when is_atom(TableName), is_list(Columns) ->
    Cols = [list_to_atom(lists:flatten(io_lib:format("~p", [X]))) || X <- Columns],
    Opts0 = add_ram_copies(find_imem_nodes(imem), []),
    Opts1 = add_attribute(Cols, Opts0),
    create_table(TableName, Opts1).

create_table(Table, Opts) when is_list(Table) ->
    create_table(list_to_atom(Table), Opts);    
create_table(Table, Opts) when is_atom(Table) ->
   	case mnesia:create_table(Table, Opts) of
		{aborted, _} ->
			io:format("copying '~p' table...~n", [Table]),
			mnesia:wait_for_tables([Table], 30000),
			mnesia:add_table_copy(Table, node(), ram_copies);
		_ ->
			io:format("table '~p' created...~n", [Table]),
			mnesia:clear_table(Table)
	end.

insert_into_table(TableName, Row) when is_atom(TableName), is_tuple(Row) ->
    Row1 = case element(1, Row) of
        TableName ->
            [_|R] = tuple_to_list(Row),
            R;
        _ -> tuple_to_list(Row)
    end,
    insert_into_table(TableName, Row1);
insert_into_table(TableName, Row) when is_atom(TableName), is_list(Row) ->
    RowLen = length(Row),
    TableRowLen = length(mnesia:table_info(TableName, attributes)),
    if TableRowLen =:= RowLen ->
        mnesia:dirty_write(TableName, list_to_tuple([TableName|Row]));
        true -> {error, {"schema mismatch {table_row_len, insert_row_len} ", TableRowLen, RowLen}}
    end.

find_imem_nodes(App) ->
    [node() |
        lists:foldl(
            fun(N, Acc) ->
                    case lists:keyfind(App, 1, rpc:call(N, application, loaded_applications, [])) of
                        false -> Acc;
                        _ -> [N|Acc]
                    end
            end
            , []
            , nodes())].
find_imem_nodes() ->
    {ok, App} = application:get_application(),
    [node() |
        lists:foldl(
            fun(N, Acc) ->
                    case lists:keyfind(App, 1, rpc:call(N, application, loaded_applications, [])) of
                        false -> Acc;
                        _ -> [N|Acc]
                    end
            end
            , []
            , nodes())].



%% EXAMPLE1: create a table and add data to it
% rd(table1, {a,b,c}).
% Opts = imem_if:add_ram_copies(imem_if:find_imem_nodes(imem), []).
% Opts1 = imem_if:add_attribute(record_info(fields, table1), Opts).
% imem_if:create_table(table1, Opts1).
% mnesia:dirty_write(table1, #table1{a='change_count', b=0}).
% mnesia:dirty_write(table1, {table1, 'change_county', 3, undefined}).


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%    rr("D:/Work/Git/imem/include/imem_records.hrl").
%    Opts = imem_if:add_ram_copies(imem_if:find_imem_nodes(imem), []).
%    %imem_if:add_disc_copies(Ns, Opts).
%    Opts1 = imem_if:add_attribute(record_info(fields, sub_counter), Opts).
%    imem_if:create_table(sub_counter, Opts1).
%    imem_if:create_table(subscriber, Opts1).
%    imem_if:create_table(syncinfo, Opts1).
%	case mnesia:create_table(syncinfo, [{ram_copies, NodeList}, {attributes, record_info(fields, syncinfo)}]) of
%		{aborted, _} ->
%			io:format("copying 'syncinfo' table...~n", []),
%			mnesia:wait_for_tables([syncinfo], 30000),
%			mnesia:add_table_copy(syncinfo, node(), ram_copies);
%		_ ->
%			io:format("table syncinfo created...~n", []),
%			mnesia:clear_table(syncinfo),
%			mnesia:dirty_write(syncinfo, #syncinfo{key='change_count', val=0}),
%			mnesia:dirty_write(syncinfo, #syncinfo{key='sync_time', val=get_datetime_stamp()}),
%			mnesia:dirty_write(syncinfo, #syncinfo{key='update_time', val=get_datetime_stamp()}),
%			mnesia:dirty_write(syncinfo, #syncinfo{key='record_count', val=0})
%	end,
%	mnesia:wait_for_tables([subscriber, syncinfo], Timeout),
