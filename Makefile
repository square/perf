# The dependencies for compiling the codebase
CLASSPATH_MAIN = $(shell CP="build"; for lib in `find lib -type f -name "*.jar"`; do CP="$$CP:$$lib"; done; printf "$$CP" )

# The Java files that comprise of the PerfUtils library, which has no dependencies.
JAVA_LIBRARY_SRC = $(shell find src/com/squareup/perfutils -type f -name "*.java")

# The Java files that comprise the Perf executable, which depend on the
# libraries in `lib` and the PerfUtils library that lives at `JAVA_LIBRARY_SRC`.
JAVA_MAIN_SRC = $(shell find src/com/squareup/perf -type f -name "*.java")

# Set a default yet overridable max Java heap size for Gradle invocations.
GRADLE_OPTS ?= "-Xmx4g"

compile: build/.perf

build/.perf: lib/.compile build/.perfutils $(JAVA_MAIN_SRC)
	javac \
		-cp "$(CLASSPATH_MAIN)" \
		-d build \
		$(JAVA_MAIN_SRC)
	touch "$@"

build/.perfutils: $(JAVA_LIBRARY_SRC)
	javac \
		-cp "$(CLASSPATH_MAIN)" \
		-d build \
		$(JAVA_LIBRARY_SRC)
	touch "$@"

lib/.compile: deps.gradle
	mkdir -p lib
	GRADLE_OPTS="$(GRADLE_OPTS)" \
	gradle --no-daemon --no-configuration-cache -b deps.gradle -q copyDependencies
	touch "$@"

javadoc: javadoc/.built
javadoc/.built: $(JAVA_LIBRARY_SRC)
	rm -rf javadoc
	javadoc -Xdoclint:none -d javadoc -sourcepath src com.squareup.perfutils
	touch $@

clean:
	rm -rf build lib perfutils.jar javadoc

perfutils.jar: build/.perfutils
	# Exclude Perf.java build artifacts, which have external dependencies.
	jar cf "$@" -C build com/squareup/perfutils/
