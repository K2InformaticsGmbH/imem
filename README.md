imem [![Build Status](https://travis-ci.org/K2InformaticsGmbH/imem.svg?branch=rebar3)](https://travis-ci.org/K2InformaticsGmbH/imem)
====

Clustered in-memory database based on MNESIA with simple SQL layer.

* Support for application configuration management.
* Support for application control by use of MNESIA data change notifications.
* Logging mechanism for time partitioned tables with automatic data ageing.
* Snapshot/restore functions giving limited persistance gurantees, useful for consistent cold starts.

#### For changing the partition time of rolling tables

1. partition time in the dderl tables are saved in seconds.So 86400 corresponds to a day
2. on [line 14 in imem_proll.erl](https://github.com/K2InformaticsGmbH/imem/blob/master/src/imem_proll.erl#L14) change the value of GET_PROLL_CYCLE_WAIT to 1 sec (replace 100000 to 1000)
3. on [line 7 in imem_dal_skvh.erl](https://github.com/K2InformaticsGmbH/imem/blob/master/src/imem_dal_skvh.erl#L7) change the vlaue of AUDIT_SUFFIX to the partion time that you want to set. For example setting the partition time from day to a minute you have replace `-define(AUDIT_SUFFIX,"Audit_86400@_").` with `-define(AUDIT_SUFFIX,"Audit_60@_").`
4. Compile this and you can hot load this file on the running node.
5. Similar to step 3 make changes on [line 12 and 13 in imem_meta.hrl](https://github.com/K2InformaticsGmbH/imem/blob/master/include/imem_meta.hrl#L12-L13).
6. Restart of the node is rquired after the changes to the hrl files and compiling the code.
7. In dderl on table ddAlias change the row with qname as `{sbsgui,ddLog_86400@}` to `{sbsgui,ddLog_60@}` for partitioning the table every minute.
8. In the same row also change the purge time in the opts column to 5 minutes by replacing `[{record_name,ddLog},{type,ordered_set},{purge_delay,430000}]` with `[{record_name,ddLog},{type,ordered_set},{purge_delay,300}]`
9. Do the same steps as 7 and 8 for rows with qname `{sbsgui,ddMonitor_60@}`
