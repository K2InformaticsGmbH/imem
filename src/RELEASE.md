Application imem
================

Release history with new or improved features and bugfixes

Version 1.2.0 (Release Date 08.08.2013)
=======================================
	
* configurable snapshot fun
* chunked streaming format for snapshot and restore
* configurable purge fun which can act based on available os memory limits
* memory usage column added to virtual table ddSize

* fixes for binary table names in security layer imem_sec
* clean and central parsing for time partitioned tables
* teardown of imem_if in case of MNESIA down signal
* improved zip-file handling for snapshots
* fix connection probe issue with accept_once

Version 1.1.2 (Release Date 23.07.2013)
=======================================

* using lager logging for low level imem functions
* configurable monitor extensions
* configurable monitor dump to file system

* fix lock situation in log partition rollover

Version 1.1.1 (Release Date 05.07.2013)
=======================================

* extended snapshotting functions and escript access
* ddSize virtual table with row count column for joins with table names

* fix for sorting issue

Version 1.1.0 (Release Date 02.07.2013)
=======================================
