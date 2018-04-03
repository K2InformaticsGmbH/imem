-ifndef(IMEM_CONFIG_HRL).
-define(IMEM_CONFIG_HRL, true).

-define(CONFIG_TABLE,ddConfig).                    

-record(ddConfig,
    { hkl               ::list() % hierarchical key list [item,context1,context2,...]
    , val = <<"fun(_,__Rec,__Table) -> imem_config:val(__Table,__Rec) end.">> ::any()
    , owner             ::atom() % the module who owns this config
    , remark = <<"">>   ::binary() % create comments
    }
).
-define(ddConfig, [list,term,atom,binstr]).


-define(GET_CONFIG(__PName,__Context,__Default,__Documentation),
  imem_config:get_config_hlk(
    ?CONFIG_TABLE,
    {element(2,application:get_application(?MODULE)),?MODULE,__PName},
    ?MODULE, lists:flatten([__Context,node()]), __Default, __Documentation
  )
).

-define(PUT_CONFIG(__PName,__Context,__Default,__Remark),
  imem_config:put_config_hlk(
    ?CONFIG_TABLE,
    {element(2,application:get_application(?MODULE)),?MODULE,__PName},
    ?MODULE, __Context, __Default, __Remark
  )
).
-define(PUT_CONFIG(__PName,__Context,__Default,__Remark,__Documentation),
  imem_config:put_config_hlk(
    ?CONFIG_TABLE,
    {element(2,application:get_application(?MODULE)),?MODULE,__PName},
    ?MODULE, __Context, __Default, __Remark, __Documentation
  )
).

-define(LOOKUP_CONFIG(__PName,__Context),
  imem_config:lookup(
    ?CONFIG_TABLE,
    {element(2,application:get_application(?MODULE)),?MODULE,__PName},
    lists:flatten([__Context,node()])
  )
).

-endif. % IMEM_CONFIG_HRL
