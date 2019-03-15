#!/bin/bash
nodes="Ry7EnwBxQGixdqISyImR3A,3mNWRO2nQSWrPMqWo39d5Q,-yAy0dKeTpuF4yZeOWkxfA,DZ5HRTKRScuzkgA_L5LwFA"
#nodes="Zf1objE8SJCxPFdocp6TwQ,TU0XYtfSQK2CZA6mToQXJg,mIr8uzkwTgOzgv0JDVYGIg,9kgXOugDRwmALpiey8i1AA,V4FIlgx5RaijpYxlZTT5Cw,YamNoRZaRQG9bqIU2lu49w"
#M="indices.merges.current_size_in_bytes,breakers.in_flight_requests.estimated_size_in_bytes,transport.rx_size_in_bytes,thread_pool.bulk.queue,jvm.mem.heap_used_percent,indices.merges.current,
#A=",,diff,,,,"
#M="indices.merges.total_throttled_time_in_millis"
#A="diff"
M="indices.indexing.throttle_time_in_millis,indices.indexing.index_current,indices.indexing.index_failed,indices.indexing.index_total,indices.indexing.index_time_in_millis,indices.merges.current,indices.merges.current_size_in_bytes,indices.merges.total_size_in_bytes,indices.merges.total_auto_throttle_in_bytes,indices.merges.total_throttled_time_in_millis,breakers.in_flight_requests.estimated_size_in_bytes,transport.rx_size_in_bytes,thread_pool.bulk.queue,jvm.mem.heap_used_percent"
A="diff,,diff,diff,diff,,,diff,diff,diff,,diff,,,"
#timefilter="1542121612,1542144587"
timefilter="1542126570,1542144587"
file=/tmp/nodes_stats

DIR=8973a

IFS=',' read -r -a metrics <<< "$M"
IFS=',' read -r -a aggs <<< "$A"

for i in "${!metrics[@]}"
do
  CMD='~/src/main/es -metric "'${metrics[$i]}'" -nodes="'$nodes'" -option "metricvalue" 
  -a "'${aggs[$i]}'" -f "'$file'" -timeFilter "'$timefilter'" --logtostderr=1 
  > /tmp/'$DIR'/'${metrics[$i]}
  echo 'Executing '$CMD

  eval $CMD
done

