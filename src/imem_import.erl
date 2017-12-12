-module(imem_import).

-export([ create_from_string/5
        ]).

-record(state,  { table      :: list()      %% table name
                , sep        :: list()      %% separator character(s)
                , names      :: list()      %% field names 
                , types      :: list()      %% field data types
                , defaults   :: list()      %% default values
                , data       :: list()      %% remaining to be imported 
        }).

create_from_string(_SKey, Str, _Opts, _Schema, _IsSec) when is_list(Str) ->
    HS = process_header(#state{data=re:split(Str,"[\n]",[{return,list}])}),       %% HeaderState
    %% ToDo: implement
    % Table = list_to_atom(HS#state.table),
    % Names = list_to_atom(HS#state.names),
    % Types = [list_to_atom(T) || T <- HS#state.types],
    % DefaultRecord = [ imem_datatype:string_to_term(T) || T <- HS#state.defaults],
    % if_call_mfa(IsSec, 'create_table', [SKey, Table, {Names,Types,DefaultRecord}, Opts]),
    import_data(HS).

process_header(_S) ->
    %% ToDo: implement
    ok.

import_data(_HS) ->
    %% ToDo: implement
    ok.


%% --Interface functions  (calling imem_if for now, not exported) ---------

% if_call_mfa(IsSec,Fun,Args) ->
%     case IsSec of
%         true -> apply(imem_sec, Fun, Args);
%         _ ->    apply(imem_meta, Fun, lists:nthtail(1, Args))
%     end.
