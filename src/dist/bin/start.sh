#! /bin/bash

set LYKKE_ME_PROTOTYPE_OPTS="-Dlog4j.configuration=file:///$BASEDIR/cfg/log4j.properties"
mkdir "$BASEDIR"/log 2>/dev/null
mkdir "$BASEDIR"/work 2>/dev/null

sh ./lykke-me-prototype "$BASEDIR"/cfg/application.properties