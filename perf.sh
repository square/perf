#!/bin/bash

CLASSPATH_MAIN+=$(CP="build"; for lib in `find lib -type f -name "*.jar"`; do CP="$CP:$lib"; done; printf "$CP" )
JAVA_OPTIONS="-Xms6g -Xmx6g"
java -cp "${CLASSPATH_MAIN}" $JAVA_OPTIONS com.squareup.perf.Perf "$@"
