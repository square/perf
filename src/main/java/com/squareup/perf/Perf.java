/*
 * Copyright 2024 Block Inc.
 */

package com.squareup.perf;

import picocli.CommandLine;

import com.squareup.perfutils.Box;
import com.squareup.perfutils.LoggingUtils;
import com.squareup.perfutils.PerfUtils;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.TreeMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;

/**
 * This is the main entry point for the Perf command line tool, and it serves a
 * few different and related purposes:
 * 1. New Perf tests are registered here by adding the function that defines
 *    them and their default command line arguments to `tests[]`.
 * 2. It defines the general-purpose command line options that all tests should
 *    support.
 * 3. Example perf tests live here. New Perf tests can live here or in any
 *    other file.
 * 
 * To add a new perf test, do the following steps:
 * 1. Copy and paste any existing test and rename it.
 * 2. Modify the setup code and the contents of the `operation` Function to
 *    match the operation you want to benchmark.
 * 3. Register the new test by adding it to `tests[]`.
 */
public class Perf {

  /**
   * The following class and table define available performance tests in terms of a string name, a
   * method that implements the test, and a set of default arguments to pass to that method.
   */
  private static class TestInfo {
    /**
     * The name of the test, passed on the command line to identify the test. This name is not
     * permitted to contain commas, which are used to delineate different tests.
     */
    public String name;

    /**
     * The main function for the test. It is responsible for parsing arguments and printing help as
     * well as executing and producing any output. The `String[]` that it consumes is a
     * concatenation of defaultArgs and the arguments the user gave on the command line.
     */
    public Consumer<String[]> testMethod;

    /**
     * The set of default arguments to pass to the test. Any command line options will be appended
     * after (and can overwrite) this set of arguments.
     */
    public String[] defaultArgs;

    /**
     * The human-readable description for the test.
     */
    public String description;

    /**
     * Constructor.
     */
    public TestInfo(
        String name, Consumer<String[]> testMethod, String[] defaultArgs,  String description) {
      this.name = name;
      this.testMethod = testMethod;
      this.defaultArgs = defaultArgs;
      this.description = description;
    }
  }

  /**
   * List of available tests.  We are using this array because it is both more
   * explicit and more flexible than putting annotations on test functions.
   *
   * Test names must be unique and are case-sensitive.
   */
  private static TestInfo tests[]  = new TestInfo[] {
    new TestInfo("NoOp", Perf::noopTest, new String[] {"-maxOperations", "null", "-maxDuration", "1"},
        "Benchmark of this benchmarking library and Java overheads."),
    new TestInfo("New", Perf::newTest, new String[] {"-maxOperations", "null", "-maxDuration", "1"},
        "Benchmark of simple object construction."),
    new TestInfo("DataPoint", Perf::dataPointTest, null,
        "Benchmark of creating and adding DataPoints to an ArrayList."),
    new TestInfo("LongBuffer", Perf::longBufferTest, null,
        "Benchmark of storing pairs of longs in a set of LongBuffers."),
    new TestInfo("StringFormat", Perf::stringFormat, null,
        "Benchmark of String.format."),
    new TestInfo("Synchronized", Perf::synchronizedTest, null,
        "Benchmark of assigning to an integer inside a synchronized block vs assigning to " +
        "an atomic integer."),
    // This test is used for validating that the argument overrides in TestInfo is working as expected.
    new TestInfo("NoOpThroughputShortDuration", Perf::noopTest,
        new String[] {"-maxOperations", "null", "-numThreads", "2", "-maxDuration", "0.5" },
        "Benchmark of this benchmarking library and Java overheads."),
  };
  /**
   * Options that will be useful for most of the performance tests, because they will use
   * PerfUtils.benchmarkSynchronousOperation. Individual tests are expected to inherit from this
   * class if they wish to add additional options. They can also overwrite these defaults via a
   * subclass constructor.
   */
  @CommandLine.Command(name="perf.sh <testName>")
  static class CoreOptions {
    @CommandLine.Option(names="-continuous",
      description="When set, run the specified test as an infinite loop. " +
      "When used without threadRange, write results to stdout whenever maxOperations or " +
      "maxDuration is reached. Do not use in load tests that invoke runBenchmark multiple times.")
    public boolean continuous = false;

    @CommandLine.Option(names="-maxOperations",
        description="Maximum number of operations to invoke across all threads." +
              " Only valid when numThreads == 1")
    public Optional<Long> maxOperations = Optional.of(1L);

    @CommandLine.Option(names="-maxDuration",
        description="The maximum number of seconds the benchmark should run for.")
    public Optional<Duration> maxDuration = Optional.of(Duration.ofSeconds(1));

    @CommandLine.Option(names="-numThreads",
        description="The number of threads that should attempt to run the operation simultaneously.")
    public Optional<Integer> numThreads = Optional.of(1);

    @CommandLine.Option(names="-threadRange",
        description="A comma-delimited set of thread ranges with optional step sizes." +
        " Tests that support this option should ignore numThreads when it is given." +
        "Example: 1-19,20-100:10 means thread counts from 1 to 19, 20, 30,...,100")
    public String threadRange = null;

    @CommandLine.Option(names="-numWarmupOps",
        description="The number of times the operation should be invoked in each thread before " +
        " the timing phase begins.")
    public long numWarmupOps = 100;

    /**
     * The level of verbosity of the report. The possible range is [0, 2] (values greater than 2 are
     * equivalent to 2).
     * 0: The report includes only latency percentiles and throughput in CSV format.
     * 1: The output includes latency buckets.
     * 2: The output includes latency buckets and the number of operations completed in each
     *    one-second experimental interval.
     */
    @CommandLine.Option(names="-verbosity",
        description="Control the verbosity of the output. The range of possible values is [0, 2].")
    public int verbosity = 1;

    @CommandLine.Option(names="-brief",
        description="Produce compact output. Synonymous with -verbosity=0. " +
        "This option overrides the -verbosity option when both are given.")
    public boolean brief = false;

    @CommandLine.Option(names={"-help", "--help"}, usageHelp = true, description="Print a usage message.")
    public boolean help;
  }

  /**
   * Benchmark the latency introduced by the testing library itself.
   */
  private static void noopTest(String[] args) {
    CoreOptions options = new CoreOptions();
    fillOptions(options, args);
    runBenchmark(() -> PerfUtils.Status.SUCCESS, options, TimeUnit.NANOSECONDS);
  }

  /**
   * Benchmark the latency of constructing a simple object in Java with `new`.
   */
  private static void newTest(String[] args) {
    CoreOptions options = new CoreOptions();
    fillOptions(options, args);

    /**
     * Simple class that wraps a status object and an Integer reference, to minic the construction
     * of a wrapper object if the signature of `operation` returned a wrapper instead of just a
     * status.
     */
    class Pojo {
      public Integer i;
      public PerfUtils.Status status;
      public Pojo(PerfUtils.Status status, Integer i) {
        this.status = status;
        this.i = i;
      }
    }

    {
      System.out.println("Assignment with new:");
      runBenchmark((box) -> {
            box.set(new Pojo(PerfUtils.Status.SUCCESS, 5));
            return PerfUtils.Status.SUCCESS;
          }, options, TimeUnit.NANOSECONDS);
    }
    System.gc();
    {
      System.out.println("Assignment without new:");
      final Pojo p = new Pojo(PerfUtils.Status.SUCCESS, 5);
      runBenchmark((box) -> {
            box.set(p);
            return PerfUtils.Status.SUCCESS;
          }, options, TimeUnit.NANOSECONDS);
    }
  }

  /**
   * Benchmark the cost of constructing a DataPoint and shoving into a list.
   * This test ignores any thread options provided because it is not safe to run in a multi-threaded
   * fashion.
   */
  private static void dataPointTest(String[] args) {
    class Options extends CoreOptions {
      @CommandLine.Option(names="-includeNanoTime",
          description="True means that operation should include a call to System.nanoTime()")
          public boolean includeNanoTime = false;
      /**
       * Constructor. This is used to set default values of CoreOptions that the test function
       * prefers in the absense of an override. These are overridable via TestInfo defaultArgs and
       * the cli.
       */
      public Options() {
        this.maxOperations = Optional.empty();
        this.maxDuration = Optional.of(Duration.ofSeconds(3L));
        this.numThreads = Optional.of(1);
      }
    }
    Options options = new Options();
    fillOptions(options, args);

    PerfUtils.PerfArguments perfArgs = new PerfUtils.PerfArguments()
        .setThreadInit(() -> new ArrayList<PerfUtils.DataPoint>())
        .setOperationUsingThreadInit((threadSpecificCustomData) -> {
          List<PerfUtils.DataPoint> dataPoints =
              (List<PerfUtils.DataPoint>) threadSpecificCustomData;
          dataPoints.add(new PerfUtils.DataPoint(
                options.includeNanoTime ? System.nanoTime() : 1L,
                options.includeNanoTime ? System.nanoTime() : 1L));
          return PerfUtils.Status.SUCCESS;
        });
    runBenchmark(perfArgs, options, TimeUnit.NANOSECONDS);
  }

  /**
   * Benchmark the cost of adding timing information into a wrapper around a pair of lists
   * LongBuffers.
   */
  private static void longBufferTest(String[] args) {
    class Options extends CoreOptions {
      @CommandLine.Option(names="-includeNanoTime",
          description="True means that operation should include a call to System.nanoTime()")
          public boolean includeNanoTime = false;
      /**
       * Constructor. This is used to set default values of CoreOptions that the test function
       * prefers in the absense of an override. These are overridable via TestInfo defaultArgs and
       * the cli.
       */
      public Options() {
        this.maxOperations = Optional.empty();
        this.maxDuration = Optional.of(Duration.ofSeconds(3L));
      }
    }

    Options options = new Options();
    fillOptions(options, args);

    PerfUtils.PerfArguments perfArgs = new PerfUtils.PerfArguments()
        .setThreadInit(() -> new PerfUtils.LongBufferDataPointStore())
        .setOperationUsingThreadInit((threadSpecificCustomData) -> {
          PerfUtils.LongBufferDataPointStore dataPoints =
              (PerfUtils.LongBufferDataPointStore) threadSpecificCustomData;
          dataPoints.add(
              options.includeNanoTime ? System.nanoTime() : 1L,
              options.includeNanoTime ? System.nanoTime() : 1L);
          return PerfUtils.Status.SUCCESS;
        });
    runBenchmark(perfArgs, options, TimeUnit.NANOSECONDS);
  }

  /**
   * Validate that PerfUtils produces reasonable output when failures occur. This is not a unit test
   * because it is brittle to unit test human readable output. Additionally "reasonable output" is
   * somewhat subjective.
   */
  private static void failureCaptureTest(String[] args) {
    class Options extends CoreOptions {
      @CommandLine.Option(names="-failureRate",
          description="The probability of a given request failing.")
      public Double failureRate = 1.0;
      @CommandLine.Option(names="-rethrowExceptions",
          description="True means that this operation will rethrow exceptions to PerfUtils")
      public boolean rethrowExceptions = false;
      /**
       * Constructor. This is used to set default values of CoreOptions that the test function
       * prefers in the absense of an override. These are overridable via TestInfo defaultArgs and
       * the cli.
       */
      public Options() {
        this.numWarmupOps = 0;
        this.maxOperations = Optional.empty();
        this.maxDuration = Optional.of(Duration.ofSeconds(1L));
      }
    }

    Options options = new Options();
    fillOptions(options, args);

    runBenchmark((failureBox) -> {
        try {
          if (ThreadLocalRandom.current().nextDouble() < options.failureRate) {
            throw new RuntimeException("Handcrafted exception");
          }
          return PerfUtils.Status.SUCCESS;
        } catch (RuntimeException e) {
          if (options.rethrowExceptions) {
            throw e;
          }
          failureBox.set(LoggingUtils.getStackTrace(e));
          return PerfUtils.Status.FAILURE;
        }
      }, options, TimeUnit.NANOSECONDS);
  }

  /**
   * Benchmark the cost of String.format.
   */
  public static void stringFormat(String[] args) {
    class Options extends CoreOptions {
      @CommandLine.Option(names="-format",
          description="True means that this operation will run String.format().")
      public boolean format = true;
      /**
       * Constructor. This is used to set default values of CoreOptions that the test function
       * prefers in the absense of an override. These are overridable via TestInfo defaultArgs and
       * the cli.
       */
      public Options() {
        this.maxOperations = Optional.empty();
        this.maxDuration = Optional.of(Duration.ofSeconds(3L));
      }
    }
    Options options = new Options();
    fillOptions(options, args);

    Function<Box<Object>, PerfUtils.Status> operation = (failureBox) -> {
      int d = ThreadLocalRandom.current().nextInt();
      if (options.format) {
        String.format("%d", d);
      }
      return PerfUtils.Status.SUCCESS;
    };

    runBenchmark(operation, options, TimeUnit.NANOSECONDS);
  }

  /**
   * Benchmark the cost of executing code in synchronized blocks when there is no contention.
   */
  public static void synchronizedTest(String[] args) {
    class Options extends CoreOptions {
      @CommandLine.Option(names="-format",
          description="True means that this operation will run String.format().")
      public boolean format = true;
      /**
       * Constructor. This is used to set default values of CoreOptions that the test function
       * prefers in the absense of an override. These are overridable via TestInfo defaultArgs and
       * the cli.
       */
      public Options() {
        this.maxOperations = Optional.empty();
        this.maxDuration = Optional.of(Duration.ofSeconds(3L));
      }
    }
    Options options = new Options();
    fillOptions(options, args);

    final Box<Integer> vanillaInteger = new Box<Integer>(0);
    Function<Box<Object>, PerfUtils.Status> synchronizedOperation = (failureBox) -> {
      synchronized(Perf.class) {
        vanillaInteger.set(vanillaInteger.get() + 1);
        return PerfUtils.Status.SUCCESS;
      }
    };

    final AtomicInteger atomicInteger = new AtomicInteger(0);
    Function<Box<Object>, PerfUtils.Status> atomicOperation = (failureBox) -> {
      atomicInteger.incrementAndGet();
      return PerfUtils.Status.SUCCESS;
    };

    // Run operation with synchronized
    System.out.println("Assign to a plain Integer inside a synchronized block.");
    runBenchmark(synchronizedOperation, options, TimeUnit.NANOSECONDS);

    // Run operation with atomics.
    System.out.println("Assign to an atomic integer.");
    runBenchmark(atomicOperation, options, TimeUnit.NANOSECONDS);
  }


  /**
   * A convenience mapping from test names to TestInfo objects. Given how few tests there are, it is
   * quite likely that this map is actually slower than an array scan, but it is hopefully easier to
   * read.
   */
  public static Map<String, TestInfo> testNamesMap;

  /**
   * Output a list of tests and exit.
   */
  private static void usage() {
    StringBuilder help = new StringBuilder();
    help.append("Usage:");
    help.append(
        "\n\t./perf.sh <testName> [testOptions]");
    help.append(
        "\n\t./perf.sh <testName>,<testName>,...");
    help.append(
        "\nEach test has its own options, which can be retrieved with `<testName> --help`.");
    help.append("\nAvailable tests:");
    for (TestInfo test : tests) {
      help.append(String.format("\n\t%s\t%s", test.name, test.description));
    }
    System.out.println(help.toString());
    System.exit(1);
  }

  /**
   * Do some sanity checks on TestInfo.
   */
  static {
     // Ensure that all test names are unique and do not contain commas.
     testNamesMap = new TreeMap<>();
     for (TestInfo test : tests) {
       testNamesMap.put(test.name, test);
       if (test.name.indexOf(",") != -1) {
         throw new RuntimeException(
             String.format("Test name %s contains disallowed comma!", test.name));
       }
       // Rewrite null default args to empty arrays so main() can concat without null checks.
       if (test.defaultArgs == null) {
         test.defaultArgs = new String[0];
       }
     }
     if (testNamesMap.size() != tests.length) {
       throw new RuntimeException("Test names are non-unique.");
     }
  }

  /**
   * Helper function for running benchmarks that takes simple operations which
   * do not return error distributions.
   */
  static void runBenchmark(Supplier<PerfUtils.Status> operation, CoreOptions options,
      TimeUnit displayTimeUnit) {
    runBenchmark(new PerfUtils.PerfArguments().setOperation(operation), options, displayTimeUnit);
  }

  /**
   * Helper function for running benchmarks that takes operations which return
   * error distributions but do not require custom thread initiation.
   */
  static void runBenchmark(Function<Box<Object>, PerfUtils.Status> operation, CoreOptions options,
      TimeUnit displayTimeUnit) {
    runBenchmark(new PerfUtils.PerfArguments().setOperation(operation), options, displayTimeUnit);
  }

  /**
   * Helper function to run a benchmark either for a single thread count or a range of thread
   * counts, based on the given options. This variation of runBenchmark should be used if a test
   * needs to use a custom threadInit function. The other variations are simpler to use and do not
   * require the caller to wrap their operation in args.
   *
   * This function exists for two reasons:
   * 1. To centralize the processing of CoreOptions, some of which are interpreted in this function,
   *    and some of which are passed into PerfUtils.benchmarkSynchronousOperation the threadRange
   *    option and the numThreads option.
   * 2. To centralize the set of fields that are output during thread range scans in one place.
   *
   * @param args             Incomplete arguments for PerfUtils.benchmarkSynchronousOperation, with
   *                         #operation and optionally #threadInit set. All other arguments will be
   *                         set based based on the fields in options.
   * 
   * @param options          The flags that were parsed from the command line. This function uses
   *                         a some of the flags to determine what mode to run the
   *                         benchmark in, and passes others into args.
   * @param displayTimeUnit  The time unit that should be used for displaying latencies.
   */
  static void runBenchmark(PerfUtils.PerfArguments args, CoreOptions options, TimeUnit displayTimeUnit) {
    // Construct args from options, so that each test does not need to do it.
    // This block can be extracted into a separate method if there are tests
    // that do not want to use the runBenchmark wrapper.
    if (args.operation == null) {
      System.err.println("ERROR: runBenchmark called with null operation.");
      System.exit(1);
    }
    args
      .setNumThreads(options.numThreads)
      .setMaxOperations(options.maxOperations)
      .setMaxDuration(options.maxDuration)
      .setNumWarmupOps(options.numWarmupOps);

    // Run the actual benchmark.
    do {
      String threadRange = options.threadRange;
      if (threadRange == null || threadRange.trim().isEmpty()) {
        System.out.println(PerfUtils.benchmarkSynchronousOperation(args)
            .toString(displayTimeUnit, options.verbosity));
      } else {
        List<Integer> threadCounts = PerfUtils.parseRanges(threadRange);
        System.out.println(
            "Threads,Min,Avg,P50,P99,P999,P9999,Max,Throughput,Completed Requests,Errors,Started Requests");
        // Variable is named threadCount instead of numThreads because numThreads is used for the
        // parameter.
        for (Integer threadCount: threadCounts) {
          PerfUtils.PerfReport perfReport =
              PerfUtils.benchmarkSynchronousOperation(args.setNumThreads(Optional.of(threadCount)));
          System.out.printf("%d,%d,%d,%d,%d,%d,%d,%d,%f,%d,%d,%d\n",
              threadCount,
              displayTimeUnit.convert(perfReport.minLatency),
              displayTimeUnit.convert(perfReport.averageLatency),
              displayTimeUnit.convert(perfReport.latencyPercentiles.get(0.5)),
              displayTimeUnit.convert(perfReport.latencyPercentiles.get(0.99)),
              displayTimeUnit.convert(perfReport.latencyPercentiles.get(0.999)),
              displayTimeUnit.convert(perfReport.latencyPercentiles.get(0.9999)),
              displayTimeUnit.convert(perfReport.maxLatency),
              perfReport.averageOperationsCompletedPerSecond,
              perfReport.operationsCompletedBeforeDeadline,
              perfReport.operationsFailed,
              perfReport.operationsStarted);
          // This is a condition for termination chosen somewhat arbitrarily
          // based on experimentation.
          if (perfReport.operationsFailed > 50) {
            System.out.println("Terminating experiment early due operationsFailed > 50");
            break;
          }
        }
      }
    } while(options.continuous);
  }

  /**
   * Check that the given test names exist, and run them with the given override arguments.
   *
   * @param requestedTestNames The names of tests the caller wishes to run.
   * @param args               Extra argument to pass to each test being run. The caller is
   *                           responsible for ensuring that the given arguments are valid for the
   *                           given tests.
   */
  public static void validateAndRunTests(String[] requestedTestNames, String[] args) {
    List<TestInfo> testsToRun = new ArrayList<>();
    for (String requestedTestName : requestedTestNames) {
      if (!testNamesMap.containsKey(requestedTestName)) {
        // Check if the the requested test name is a prefix of some test.
        List<String> foundTestsWithPrefix = new ArrayList<>();
        for (String testName: testNamesMap.keySet()) {
          if (testName.startsWith(requestedTestName)) {
            foundTestsWithPrefix.add(testName);
          }
        }

        if (!foundTestsWithPrefix.isEmpty()) {
          System.out.printf("Tests matching prefix '%s':\n", requestedTestName);
          for (String testName: foundTestsWithPrefix) {
            System.out.printf("\t%s\t%s\n", testName, testNamesMap.get(testName).description);
          }
        } else {
          System.out.printf("Found no tests matching prefix '%s'.\n", requestedTestName);
        }
        // Since we did not have an exact match on at least one test, do not
        // run any tests.
        return;
      }
      testsToRun.add(testNamesMap.get(requestedTestName));
    }

    for (Perf.TestInfo test : testsToRun) {
      System.out.printf("Running %s: %s\n", test.name, test.description);
      String[] testArgs = Stream.concat(
              Arrays.stream(test.defaultArgs),
              Arrays.stream(args))
          .toArray(String[]::new);
      test.testMethod.accept(testArgs);
      System.gc();

      // Sleep for 2s to let gc threads finish
      try {
        Thread.sleep(2);
      } catch (InterruptedException e) { }
    }
  }

  /**
   * A wrapper around {@link CommandLine#parseArgs} that makes any necessary
   * overrides for related flags. The current overrides are:
   * 1. Specifying -brief sets -verbosity to 0.
   *
   * @param options The options to fill.
   * @param args    The arguments to fill the options with.
   */
  static void fillOptions(CoreOptions options, String[] args) {
    CommandLine cli = new CommandLine(options);

    // Register a custom converter for parsing Durations, because most humans
    // will not know that they need to type strings prefixed with PT. Also,
    // defaulting to seconds as units is convenient.
    cli.registerConverter(Duration.class, s -> {
      s = s.toUpperCase();
      if ("NULL".equals(s) || "OPTIONAL.EMPTY".equals(s)) {
        return null;
      }
      // The person is attempting to use ISO-8601 duration format.
      if (s.startsWith("PT")) {
        return Duration.parse(s);
      }

      // If the entire string is numeric, then assume seconds.
      try {
        Double.parseDouble(s);
        return Duration.parse("PT" + s + "S");
      } catch (NumberFormatException e) {
        // If the entire string is NOT numeric, then assume the number format
        // is the ISO-8601 format without the prefix, limited to hours, minutes
        // and seconds.
        return Duration.parse("PT" + s);
      }
    });

    CommandLine.ITypeConverter<Long> nullParsingLongConverter = (String s) -> {
      s = s.toUpperCase();
      return ("NULL".equals(s) || "OPTIONAL.EMPTY".equals(s)) ? null : Long.parseLong(s);
    };

    cli.registerConverter(Long.TYPE, nullParsingLongConverter);
    cli.registerConverter(Long.class, nullParsingLongConverter);

    // Allow subsequent options to replace earlier options without warnings.
    cli.setOverwrittenOptionsAllowed(true);
    CommandLine.tracer().setLevel(CommandLine.TraceLevel.OFF);

    cli.parseArgs(args);
    if (options.help) {
      cli.usage(System.out);
      System.exit(0);
    }
    if (options.brief) {
      options.verbosity = 0;
    }
  }

  /**
   * Entry Point for Perf. Reads test names and forwards additional arguments to a test-specific
   * parser.
   *
   * When debugging performance, it is sometimes useful to set the following options as well as the
   * environment variable VERBOSE_JVM=true.
   *
   * -XX:+PrintCompilation -verbose:gc
   *
   * The easiest way to run Perf is to use the wrapper script, which constructs the classpath.
   *
   * Sample invocations:
   *    # Run a test case without overriden options
   *    ./perf.sh LongBuffer
   *
   *    # Run a test case with overriden options
   *    ./perf.sh LongBuffer --maxDuration  1
   *
   *    # Run multiple test cases sequentially.
   *    ./perf.sh New,NoOp
   *
   */
  public static void main(String[] args) {
    if (args.length == 0 || "-help".equals(args[0]) || "--help".equals(args[0])) {
      usage();
    }

    // Check if we are running one test or multiple tests.
    String[] requestedTestNames;
    if (args[0].indexOf(',') == -1) {
      // One test is being requested
      requestedTestNames = new String[]{ args[0] };
    } else {
      // Multiple tests are being requested.
      requestedTestNames = args[0].split(",");
      // Arguments are only accepted when there is a single test to run, because applying the same
      // set of arguments to different tests is potentially dangerous. The "-brief" and "-verbosity"
      // flags are exceptions because they are supported by all tests and are useful for producing
      // output in a format consumable by another program.
      // Note: If time reveals that it is useful to allow multiple tests with overriden
      // options, we could allow this testing framework to take a configuration file (likely Yaml)
      // as input.
      if ((args.length == 2 && !args[1].equals("-brief")) ||
          (args.length == 3 && !args[1].equals("-verbosity")) ||
          args.length > 3) {
        throw new IllegalArgumentException(
            "Options overrides are disallowed when multiple tests are requested, except for " +
                "-brief and -verbosity.");
      }
    }

    validateAndRunTests(requestedTestNames, Arrays.copyOfRange(args, 1, args.length));

    // The main thread exiting naturally does not appear to work
    Runtime.getRuntime().halt(0);
  }
}
