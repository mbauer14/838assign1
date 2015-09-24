#!/bin/sh

echo "Start: " $(date +%s)
(time hive --hiveconf hive.execution.engine=mr -f ../hive-tpcds-tpch-workload/sample-queries-tpcds/query$1.sql --database tpcds_text_db_1_50) 2> ./output/tpcds_query$1_mr.out
echo "End: " $(date +%s)

