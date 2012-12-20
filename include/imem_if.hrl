-define(MatchAllRecords,[{'$1', [], ['$_']}]).
-define(MatchAllKeys,[{'$1', [], [{element,2,'$1'}]}]).

-define(sot,'$start_of_table').		%% defined in mnesia
-define(eot,'$end_of_table').		  %% defined in mnesia
-define(more,'$more_rows_exist').	%% defined here

-define(ClientError(Reason), throw({'ClientError',Reason})).
-define(SystemException(Reason), throw({'SystemException',Reason})).
-define(ConcurrencyException(Reason), throw({'ConcurrencyException',Reason})).
-define(UnimplementedException(Reason), throw({'UnimplementedException',Reason})).

%% HELPER FUNCTIONS (do not export!!) --------------------------------------

-define(binary_to_atom(Bin), list_to_atom(binary_to_list(Bin))).

-define(binary_to_existing_atom(Bin), 
        try
          list_to_existing_atom(binary_to_list(Bin))
        catch
          _:_ -> ?ClientError({"Name does not exist", Bin})
        end
    ).

-define(imem_test_setup, fun() ->
      application:load(imem),
      {ok, Schema} = application:get_env(imem, mnesia_schema_name),
      {ok, Cwd} = file:get_cwd(),
      NewSchema = Cwd ++ "/../" ++ atom_to_list(Schema),
      application:set_env(mnesia, dir, NewSchema),
      application:set_env(imem, mnesia_node_type, disc),
      application:start(imem)
    end
    ).

-define(imem_test_teardown, fun() ->
      application:stop(imem)
    end
    ).
