#!/bin/bash
# (c) lykke.com, 2016. All rights reserved.

for pid in `pgrep -f MatchingEngineService`; do
echo -n "Killing process "
echo $pid
kill $pid
done

echo -n "Waiting for process to die..."

one_exist=0
retry_count=0
while true;
do
  retry_count=`expr $retry_count + 1`

  for pid in `pgrep -f MatchingEngineService`; do
      if kill -0 $pid ; then
        one_exist=1
      fi
  done

  if [ $one_exist -eq 0 ] ; then
    echo ""
    echo "Done"
    exit
  else
    if [ $retry_count -gt 59 ] ; then
      echo ""
      echo "Wait exceeded"
      break
    else
      echo -n "."
      sleep 1
    fi
  fi
  one_exist=0
done

for pid in `pgrep -f MatchingEngineService`; do
  echo -n "Killing process "
  echo -n $pid
  echo " with nosignal"
  echo $pid
  kill -9 $pid
done

