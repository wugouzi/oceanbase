#!/bin/bash
PID=`ps -ef | grep observer | grep -v grep | awk '{print $2}'`
if [ ${#PID} -eq 0 ]
then
    echo "observer not running"
    exit -1
fi
echo "1:"
perf record -F 99 -g -p $PID -- sleep 70
echo "2:"
perf script -i perf.data &> perf.unfold
echo "3:"
FlameGraph/stackcollapse-perf.pl perf.unfold &> perf.folded
echo "4:"
FlameGraph/flamegraph.pl perf.folded > perf.svg
rm -rf perf.data* perf.folded perf.unfold
