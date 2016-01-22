@ECHO OFF

SET /A port=9000

SET cookie=%~1
SET imem=%~2
SET imem_cm=%~3
SET kernel=%~4
SET paths=%~5
SET node=%6
SET is_first=%7

SET node_name=-name imem%node%@127.0.0.1
SET /A privnode=node-1
SET /A prt=port+node

IF DEFINED is_first (
    SET imem_cm_int=%imem_cm%
) ELSE (
    SET imem_cm_int=-imem erl_cluster_mgrs ['imem%privnode%@127.0.0.1']
)

REM IMEM start options
ECHO ------------------------------------------
ECHO Starting IMEM (Opts)
ECHO ------------------------------------------
ECHO Node Name : %node_name%
ECHO Cookie    : %cookie%
ECHO EBIN Path : %paths%
ECHO IMEM      : %imem%
ECHO IMEM_CM   : %imem_cm_int%
ECHO Kernel    : %kernel%
ECHO Port      : %prt%
ECHO ------------------------------------------

START /MAX werl %paths% %node_name% %cookie% %kernel% %imem% tcp_port %prt% %imem_cm_int% -s imem
