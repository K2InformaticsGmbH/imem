imem [![Build Status](https://travis-ci.org/K2InformaticsGmbH/imem.svg?branch=master)](https://travis-ci.org/K2InformaticsGmbH/imem)
====

Clustered in-memory database based on MNESIA with simple SQL layer.

* Support for application configuration management.
* Support for application control by use of MNESIA data change notifications.
* Logging mechanism for time partitioned tables with automatic data ageing.
* Snapshot/restore functions giving limited persistance gurantees, useful for consistent cold starts.
