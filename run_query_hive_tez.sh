#!/bin/sh




containerJvm="-Xmx4600m"
containerSize=4800

echo "Start: " $(date +%s)
(time hive --hiveconf hive.execution.engine=tez --hiveconf hive.tez.container.size=$containerSize --hiveconf hive.tez.java.opts=$containerJvm -f ../hive-tpcds-tpch-workload/sample-queries-tpcds/query$1.sql --database tpcds_text_db_1_50) 2> ./output/tpcds_query$1_tez.out
echo "End: " $(date +%s)

