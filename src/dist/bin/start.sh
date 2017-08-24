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


CLASSPATH=$BASEDIR/lib/lykke-me-prototype-0.1.jar:$BASEDIR/lib/kotlin-stdlib-1.1.4.jar:$BASEDIR/lib/log4j-1.2.17.jar:$BASEDIR/lib/protobuf-java-3.0.0-beta-2.jar:$BASEDIR/lib/azure-storage-4.0.0.jar:$BASEDIR/lib/gson-2.6.2.jar:$BASEDIR/lib/httpclient-4.5.2.jar:$BASEDIR/lib/amqp-client-3.6.5.jar:$BASEDIR/lib/annotations-13.0.jar:$BASEDIR/lib/jackson-core-2.6.0.jar:$BASEDIR/lib/slf4j-api-1.7.12.jar:$BASEDIR/lib/commons-lang3-3.4.jar:$BASEDIR/lib/httpcore-4.4.4.jar:$BASEDIR/lib/commons-logging-1.2.jar:$BASEDIR/lib/commons-codec-1.9.jar
mkdir $BASEDIR/log 2>/dev/null
cd "$BINDIR"

EXECSTR="$JAVACMD -DMatchingEngineService -server -Dlog4j.configuration=file:///"$BASEDIR"/cfg/log4j.properties $JAVA_OPTS \
    -classpath "$CLASSPATH" \
    -Dapp.name="start.sh" \
    -Dapp.pid="$$" \
    -Dapp.home="$BASEDIR" \
    -Dbasedir="$BASEDIR" \
     com.lykke.matching.engine.AppStarterKt \
     $HTTP_CONFIG"

if [[ " $@ " =~ " --console " ]] ; then
    exec $EXECSTR ${@%"--console"}
else
    exec $EXECSTR $@ 1>"$BASEDIR/log/out.log" 2>"$BASEDIR/log/err.log" &
fi
