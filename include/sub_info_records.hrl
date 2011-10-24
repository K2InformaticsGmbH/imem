
%% subscriber payment types		$$$$ see copy in aaaw_common.hrl  $$$$
-define(SUB_PAYMENT_TYPE_ERROR,-1).
-define(SUB_PAYMENT_TYPE_UNKNOWN,0).
-define(SUB_PAYMENT_TYPE_PREPAID,1).
-define(SUB_PAYMENT_TYPE_POSTPAID,2).

%% subscriber payment methods	$$$$ see copy in aaaw_common.hrl  $$$$
-define(SUB_PAYMENT_METHOD_ERROR,-1).
-define(SUB_PAYMENT_METHOD_UNKNOWN,0).
-define(SUB_PAYMENT_METHOD_PREPAID,1).
-define(SUB_PAYMENT_METHOD_POSTPAID,2).
-define(SUB_PAYMENT_METHOD_TELE2,3).
-define(SUB_PAYMENT_METHOD_TFL_POSTPAID,4).
-define(SUB_PAYMENT_METHOD_M_BUDGET,5).
-define(SUB_PAYMENT_METHOD_MIGROS,6).
-define(SUB_PAYMENT_METHOD_MUCHO,7).

-record(sub_counter, {	% subscription counter
					 counter_name		% msisdn.partitionkey.countername
					, counter_value		% integer
					}).

-record(subscriber, {	% subscription info record
					 msisdn				% integer
					, payment_type		% integer
					, payment_method	% integer
					, status			% integer
					, change_count		% integer
					}).

-record(syncinfo, {		% synchronisation info record
				   key
				  , val
				  }).

%% -define(CLUSTER_NODES, ['conn@connecta.local', 'conn@connectb.local', 'conn@connectc.local', 'conn@connectd.local']).