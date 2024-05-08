#!/bin/bash

CLASSPATH_MAIN+=$(CP="build/main:build/gen"; for lib in `find lib -type f -name "*.jar"`; do CP="$CP:$lib"; done; printf "$CP" )
JAVA_OPTIONS="-Xms6g -Xmx6g"
java -cp "${CLASSPATH_MAIN}" $JAVA_OPTIONS \
-Dorg.slf4j.simpleLogger.showDateTime=true \
-Dorg.slf4j.simpleLogger.dateTimeFormat="yyyy-MM-dd'T'HH:mm:ss.SSSZ" \
com.squareup.perf.Perf "$@"
