#!/bin/bash

CLASSPATH_MAIN+=$(CP="build/main:build/gen"; for lib in `find lib -type f -name "*.jar"`; do CP="$CP:$lib"; done; printf "$CP" )

VERBOSE_JVM=${VERBOSE_JVM:-false}
verbose_jvm_options=""
if [[ "$VERBOSE_JVM" == "true" ]]; then
  verbose_jvm_options="-XX:+PrintCompilation -verbose:gc"
fi
JVM_OPTIONS="${verbose_jvm_options} -Xms6g -Xmx6g ${JAVA_OPTIONS}"

java -cp "${CLASSPATH_MAIN}" $JVM_OPTIONS "${PROXY_OPTIONS[@]}" \
-Dorg.slf4j.simpleLogger.showDateTime=true \
-Dorg.slf4j.simpleLogger.dateTimeFormat="yyyy-MM-dd'T'HH:mm:ss.SSSZ" \
com.squareup.perf.Perf "$@"
