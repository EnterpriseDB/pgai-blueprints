#!/bin/bash

# For demonstration purposes only.
#
# This script reads the output of one or more pgbench runs, and
# outputs it in JSON format, so it can be processed automatically.

j () {
    local x=$1
    local y=$2
    local z=$3
    local k=$4
    shift 4
    if [[ $x$y$z == "100" ]]; then
	echo -n "  {";
    elif [[ $x != $y && $y == $z ]]; then
	echo "  }";
	echo -n ", {";
    elif [[ $x == $y && $y != $z ]]; then
	echo " \"$k\": $@";
    else
	echo "  , \"$k\": $@";
    fi
}

f () {
    local a=$1
    shift
    local b=$1
    shift
    local c=$1
    shift
    local d=$1
    shift
    local e=$1
    shift
    if [[ $a == "pgbench" ]]; then
	x=$((1+$x))
	j $x $y $z pgbench 0
    elif [[ $a == "tps" ]]; then
	j $x $y $z pgbench_tps $c
    elif [[ "$a $b" == "start pgbench:" ]]; then
	j $x $y $z pgbench_start "\"$c $d $e $@\""
    elif [[ "$a $b" == "stop pgbench:" ]]; then
	j $x $y $z pgbench_stop "\"$c $d $e $@\""
    elif [[ "$a $b" == "postgres service:" ]]; then
	j $x $y $z postgres_service "\"$c\""
    elif [[ "$a $b" == "tpa cluster:" ]]; then
	j $x $y $z tpa_cluster "\"$c\""
    elif [[ "$a $b" == "transaction type:" ]]; then
	j $x $y $z pgbench_tx "\"$c $d $e $@\""
    elif [[ "$a $b" == "scaling factor:" ]]; then
	j $x $y $z pgbench_scale $c
    elif [[ "$a $b" == "query mode:" ]]; then
	j $x $y $z pgbench_query_mode "\"$c\""
    elif [[ "$a $b" == "transactions found:" ]]; then
	j $x $y $z found_txs "$c"
    elif [[ "$a $b" == "interval found:" ]]; then
	j $x $y $z found_interval "$c"
    elif [[ "$a $b $c" == "maximum tps found:" ]]; then
	j $x $y $z found_mtps "$d"
    elif [[ "$a $b $c" == "average tps found:" ]]; then
	j $x $y $z found_atps "$d"
    elif [[ "$a $b $e" == "latency average ms" ]]; then
	j $x $y $z pgbench_latency_avg_ms $d
    elif [[ "$a $b $c $@" == "initial connection time ms" ]]; then
	j $x $y $z pgbench_initial_conn_ms $e
    elif [[ "$a $b $c" == "number of clients:" ]]; then
	j $x $y $z pgbench_clients $d
    elif [[ "$a $b $c" == "number of threads:" ]]; then
	j $x $y $z pgbench_threads $d
    elif [[ "$a $b $c $d" == "maximum number of tries:" ]]; then
	x=$x;
    elif [[ "$a $b $c $d $e" == "number of transactions actually processed:" ]]; then
	j $x $y $z pgbench_txs "\"$@\""
    elif [[ "$a $b $c $d" == "number of failed transactions:" ]]; then
	j $x $y $z pgbench_txs_failed $e
    elif [[ "$a $b $c $d $e" == "number of transactions per client:" ]]; then
	j $x $y $z pgbench_txs_per_client $@
    elif [[ "$a $b" == "SQL script" ]]; then
	a=$a;
    elif [[ "$a" == "-" ]]; then
	a=$a;
    else
	echo "ERROR: $a $b $c $d $e $@"
	exit -1
    fi
    z=$y
    y=$x
}

out2json () {
    x=0
    y=0
    z=0
    echo "["
    while read l; do f $l; done
    echo "  }"
    echo "]"
}

json2csv () {
    jq -r -e '
      .[]   | .tpa_cluster
      + "," + .postgres_service
      + "," + (.pgbench_clients | tostring)
      + "," + (.pgbench_txs_per_client | tostring)
      + "," + (.pgbench_tps | tostring)
      + "," + (.found_mtps | tostring)
      + "," + (.found_atps | tostring)
      + "," + .pgbench_start
      '
}

prefix=$1
shift

if [[ -f $prefix.out ]]; then
    out2json < $prefix.out > $prefix.json
    json2csv < $prefix.json > $prefix.csv      
else
    echo "[]" > $prefix.json
    echo "" > $prefix.csv
fi
