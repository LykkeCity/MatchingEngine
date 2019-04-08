#!/bin/bash
# (c) lykke.com, 2016. All rights reserved.

BINDIR=$(dirname "$0")
BASEDIR=`cd "$BINDIR/.." >/dev/null; pwd`

if [ -z "$JAVACMD" ] ; then
  if [ -z "$JAVA_HOME" ] ; then
    JAVACMD="/usr/bin/java"
  else
    JAVACMD="$JAVA_HOME/bin/java"
  fi
fi

if [ ! -x "$JAVACMD" ] ; then
  echo "Error: JAVA_HOME is not defined correctly." 1>&2
  echo "  We cannot execute $JAVACMD" 1>&2
  exit 1
fi


mkdir $BASEDIR/log 2>/dev/null
cd "$BINDIR"

EXECSTR="$JAVACMD \
    -DMatchingEngineService -server -Dlog4j.configurationFile=file:///"$BASEDIR"/cfg/log4j2.xml $JAVA_OPTS \
    -Xloggc:../log/sys.gc.log -XX:+PrintGC -XX:+PrintGCDateStamps -XX:+PrintGCTimeStamps -XX:+UseGCLogFileRotation -XX:NumberOfGCLogFiles=10 -XX:GCLogFileSize=2m \
    -Dapp.name="start.sh" \
    -Dapp.pid="$$" \
    -Dapp.home="$BASEDIR" \
    -Dbasedir="$BASEDIR" \
    -jar $BASEDIR/lib/lykke-me-prototype-0.1.jar \
    $HTTP_CONFIG"


if [[ " $@ " =~ " --console " ]] ; then
    exec $EXECSTR ${@%"--console"}
else
    exec $EXECSTR $@ 1>"$BASEDIR/log/out.log" 2>"$BASEDIR/log/err.log" &
fi
