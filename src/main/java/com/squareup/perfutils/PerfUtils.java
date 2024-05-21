/*
 * Copyright 2024 Block Inc.
 */

package com.squareup.perfutils;

import java.nio.LongBuffer;
import java.time.Duration;
import java.util.Collections;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;

import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * A collection of utility functions to be used for writing multi-threaded
 * performance tests and computing statistics from them.
 */
public class PerfUtils {
  /**
   * This class wraps arguments to benchmarkSynchronousOperation; it exists to remove the need for
   * multiple variations of benchmarkSynchronousOperation with different sets of arguments.
   *
   * Fields of this object should be set using the setX() functions.
   */
  public static class PerfArguments {
    /**
     * A function used for initializing per-thread state for the benchmark. This function takes the
     * thread index of the worker thread as an argument and returns an object representing
     * test-specific data. The return value of this function will be passed into `operation` as the
     * first argument.
     */
    public Function<Integer, Object> threadInit;

    /**
     * The function that we want the performance of. When a custom error distribution is desired,
     * `operation` should put a hashable object representing the failed state into its
     * {@literal Box<Object>} argument. Always required.
     */
    public BiFunction<Object, Box<Object>, Status> operation;

    /**
     * The number of threads that will be concurrently attempting to run the operation as quickly as
     * possible. Required iff targetOpsPerSecond is empty, ignored otherwise.
     */
    public Optional<Integer> numThreads;

    /**
     * The benchmark will not run the operation more than this number of times across all threads.
     * An empty value will be treated as infinite.
     */
    public Optional<Long> maxOperations;

    /**
     * Maximum length of time to run for. An empty value will be treated as infinite.
     */
    public Optional<Duration> maxDuration;

    /**
     * The number of operations that each thread performs before synchronizing
     * with each other and starting measurement.
     */
    public long numWarmupOps;

    /**
     * The number of operations that Perf will attempt to invoke each second, using a maximum of
     * maxThreads.
     */
    public Optional<Double> targetOpsPerSecond;

    /**
     * The maximum number of threads that can be used to attempt to reach targetOpsPerSecond.
     * This is required iff targetOpsPerSecond is non-empty, to put an upper bound on resources.
     */
    public Optional<Integer> maxThreads;

    /**
     * Constructor. Sets default values for all arguments. Required arguments are intentionally set
     * to an invalid state.
     */
    public PerfArguments() {
      this.threadInit = (ignoredThreadIndex) -> new Object();
      this.operation = null;
      this.numThreads = Optional.empty();
      this.maxOperations = Optional.empty();
      this.numWarmupOps = -1;
      this.targetOpsPerSecond = Optional.empty();
      this.maxThreads = Optional.empty();
    }

    /**
     * Setter for threadInit.
     */
    public PerfArguments setThreadInit(Function<Integer, Object> threadInit) {
      this.threadInit = threadInit;
      return this;
    }

    /**
     * Setter for threadInit that does not take a thread index.
     */
    public PerfArguments setThreadInit(Supplier<Object> threadInit) {
      this.threadInit = (ignoredThreadIndex) -> threadInit.get();
      return this;
    }

    /**
     * Setter for operation that performs no transformations.
     */
    public PerfArguments setOperation(BiFunction<Object, Box<Object>, Status> operation) {
      this.operation = operation;
      return this;
    }

    /**
     * Setter for operation that wraps a Function<Object, Status> operation. This function is named
     * differently to avoid a conflict with the variation that takes a Box and does not use
     * threadInit data, because this function is expected to be less frequently used.
     */
    public PerfArguments setOperationUsingThreadInit(Function<Object, Status> operation) {
      return setOperation((threadSpecificCustomData, ignoredBox) -> operation.apply(threadSpecificCustomData));
    }

    /**
     * Setter for operation that wraps a Function<Box<Object>, Status> operation.
     */
    public PerfArguments setOperation(Function<Box<Object>, Status> operation) {
      return setOperation((ignoredThreadSpecificCustomData, box) -> operation.apply(box));
    }

    /**
     * Setter for operation that wraps a Supplier<Status> operation.
     */
    public PerfArguments setOperation(Supplier<Status> operation) {
      return setOperation((threadSpecificCustomData, ignoredBox) -> operation.get());
    }

    /**
     * Setter for numThreads.
     */
    public PerfArguments setNumThreads(Optional<Integer> numThreads) {
      this.numThreads = numThreads;
      return this;
    }

    /**
     * Setter for maxOperations.
     */
    public PerfArguments setMaxOperations(Optional<Long> maxOperations) {
      this.maxOperations = maxOperations;
      return this;
    }

    /**
     * Setter for maxDuration.
     */
    public PerfArguments setMaxDuration(Optional<Duration> maxDuration) {
      this.maxDuration = maxDuration;
      return this;
    }

    /**
     * Setter for numWarmupOps.
     */
    public PerfArguments setNumWarmupOps(long numWarmupOps) {
      this.numWarmupOps = numWarmupOps;
      return this;
    }

    /**
     * Setter for targetOpsPerSecond.
     */
    public PerfArguments setTargetOpsPerSecond(Optional<Double> targetOpsPerSecond) {
      this.targetOpsPerSecond = targetOpsPerSecond;
      return this;
    }

    /**
     * Setter for maxThreads.
     */
    public PerfArguments setMaxThreads (Optional<Integer> maxThreads) {
      this.maxThreads = maxThreads;
      return this;
    }
  }

  /**
   * Latency, throughput, completion, and error data resulting from running a performance benchmark
   * once.
   *
   * The termination condition mentioned in some of the field documentation
   * below is defined to be either a fixed number of operations being completed
   * or a fixed number of seconds having passed, depending on the options given.
   *
   * More details are available in the documentation for benchmarkSynchronousOperation.
   */
  public static class PerfReport {
    /**
     * The total number of times operation was invoked.
     * Invocations are accounted for in this count regardless of whether they completed before or
     * after the termination condition.
     */
    public long operationsStarted;

    /**
     * Total number of times that invocations of operation returned a status of SUCCESS before the
     * termination condition was reached.
     */
    public long operationsCompletedBeforeDeadline;

    /**
     * Total number of times that invocations of operation returned a status of SUCCESS after
     * maxDuration.
     */
    public long operationsCompletedPastDeadline;

    /**
     * Total number of times that invocations of operation returned a status of FAILURE. Note that
     * this includes failures observed after termination condition was reached, because many
     * failures are timeouts and we would like to account for these.
     */
    public long operationsFailed;

    /**
     * The amount of time from the first completed operation until the last measured completed
     * operation. When operationsCompletedBeforeDeadline == 0, this value will be null.
     *
     * This definition is used instead of taking a delta between the time an experiment began and
     * completed because this value is used as the denominator in the calculation of
     * averageOperationsCompletedPerSecond, making it easier to reason about the accuracy of that
     * number. Additionally, the absolute beginning and ending time of an experiment is somewhat
     * subjective, since multiple threads are involved.
     */
    public Duration measurementInterval;

    /**
     * Total operations completed in the interval between the time that the first operation completed
     * (returned) to the time when the experiment's termination condition was met, divided by the
     * length of that interval.
     *
     * We measure the interval starting from the time of the first completion, rather than at the
     * time of the first request to reduce the impact of high-latency operations artificially being
     * measured as low-throughput. For example, consider an operation that takes 10 minutes to
     * finish, but 10 invocations can finish each second in steady state. If we ran the experiment
     * for 20 minutes and started the interval from the time of the first request, this value would
     * be calculated to 5 instead of 10, which is not an accurate characterization of throughput.
     */
    public double averageOperationsCompletedPerSecond;

    ////////////////////////////////////////////////////////////////////////////////////////////////
    // All of the latency measurements are computed from successful operations that completed before
    // the end time of the experiment, when the experiment ended at maxDuration instead of
    // maxOperations.  In particular, operations that began before maxDuration seconds passed but
    // ended after are not accounted for.
    //
    // Note that excluding these slow-finishing operations can bias measured latency downwards (by
    // excluding slow operations) when running with many threads and operation latency approaches
    // maxDuration. hq6 believes this bias is preferable to the load uncertainty of measuring
    // latency for operations that ran after the end time, since it is unclear how many threads are
    // still running when such operations complete. Users of PerfUtils should reduce this bias by
    // ensuring the experiment runs for long enough that these late operations are relatively few in
    // number.
    ////////////////////////////////////////////////////////////////////////////////////////////////

    /**
     * Minimum latency measured when invoking the operation.
     */
    public Duration minLatency;

    /**
     * Average latency measured when invoking the operation.
     */
    public Duration averageLatency;

    /**
     * Maximum latency measured when invoking the operation.
     */
    public Duration maxLatency;

    /**
     * A list of data points in no particular order.
     */
    public List<DataPoint> dataPoints;

    /**
     * The number of operations that finished in each {@link PerfUtils#COMPLETION_INTERVAL_SECONDS}
     * time interval from the time when the first response was received to the time when the
     * experiment termination condition was reached.
     */
    public long[] completedOperationsInEachInterval;

    /**
     * An ordered map of upper bounds on latency histogram buckets to the number of elements in the
     * bucket. For example, if the measured latencies are [2,2,4,5,6], and the selected
     * buckets were [2 -- 4] and [4 -- 6], then this map would contain the entries (4, 2) and (6, 3).
     *
     * All buckets are exclusive of the upper bound and inclusive of the lower bound, except the
     * largest bucket which is inclusive of both bounds.
     */
    public TreeMap<Duration, Long> latencyBuckets;

    /**
     * An ordered map specifying latency at various percentile. Note that `get` on this map should
     * only ever use literal decimals, since there are precision perils to using Double as a map
     * key.  hq6 decided to use Double as key in spite of perils because the expected query pattern
     * is by literal value or by iteration.
     */
    public TreeMap<Double, Duration> latencyPercentiles;

    /**
     * The maximum number of full-width bars to display in the histogram. This is the number of
     * unicode bars printed for latency bucket with the greatest number of measurement, and equals
     * the character width of the histogram.
     */
    final int MAX_FULL_HISTOGRAM_BARS = 40;

    /**
     * Counts of "failure status description objects". These are arbitrary objects that a operation
     * can use to describe the cause for the failure, so that the caller can print different types
     * of summaries of failures, or do aggregations of failure classes if they desire.
     */
    public Map<Object, Long> failureDistributionCounts;

    /**
     * Counts of uncaught throwables thrown by an operation. The stack trace is used as the map key.
     */
    public Map<String, Long> uncaughtThrowableCounts;

    /**
     * Small helper function to add errors detected during a perftest run to a report.
     *
     * @param reportBuilder The string representation of the PerfReport to append errors to.
     *
     * @return The original StringBuilder passed in, for convenience of chained calls.
     */
    private StringBuilder appendErrors(StringBuilder reportBuilder) {
      if (!failureDistributionCounts.isEmpty()) {
        reportBuilder.append("\n\nFailure distribution: ");
        for (Object failureDescription : failureDistributionCounts.keySet()) {
          reportBuilder.append(String.format("\n\t%s: %s", failureDescription.toString(),
                failureDistributionCounts.get(failureDescription)));
        }
      }
      if (!uncaughtThrowableCounts.isEmpty()) {
        reportBuilder.append("\n\nUncaught Throwables distribution: ");
        for (String uncaughtThrowable : uncaughtThrowableCounts.keySet()) {
          reportBuilder.append(String.format("\n\t%s: %s", uncaughtThrowable,
                uncaughtThrowableCounts.get(uncaughtThrowable)));
        }
      }
      return reportBuilder;
    }

    /**
     * Generate a String representation this PerfReport suitable for human or machine consumption.
     *
     * @param timeUnit  The units to report latency measurements in.
     * @param verbosity The level of verbosity of the report. The possible range is [0, 2] (values
     *                  greater than 2 are equivalent to 2).
     *                  0: The report includes only latency percentiles and throughput in CSV
     *                     format.
     *                  1: The output includes latency buckets.
     *                  2: The output includes latency buckets and the number of operations
     *                     completed in each one-second experimental interval.
     * @return A human-readable String of this PerfReport.
     */
    public String toString(TimeUnit timeUnit, int verbosity) {
      StringBuilder reportBuilder = new StringBuilder();

      if (verbosity == 0) {
        if (operationsCompletedBeforeDeadline > 0) {
          // Produce output that can be easily copy-pasted into go/convperf.
          reportBuilder.append(
              "Req/Sec,Min,Median,Avg,P99,P999,P9999,Max," +
              "Interval,Ops start,Ops finish,Ops fail,Ops t/o\n");
          reportBuilder.append(String.format(
                "%.2f,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d",
                averageOperationsCompletedPerSecond,
                timeUnit.convert(minLatency),
                timeUnit.convert(latencyPercentiles.get(0.5)),
                timeUnit.convert(averageLatency),
                timeUnit.convert(latencyPercentiles.get(0.99)),
                timeUnit.convert(latencyPercentiles.get(0.999)),
                timeUnit.convert(latencyPercentiles.get(0.9999)),
                timeUnit.convert(maxLatency),
                timeUnit.convert(measurementInterval),
                operationsStarted,
                operationsCompletedBeforeDeadline,
                operationsFailed,
                operationsCompletedPastDeadline));
        } else {
          reportBuilder.append(
              "\nNo latency or throughput information is available because no requests completed. Detected errors follow:");
          appendErrors(reportBuilder);
        }
        return reportBuilder.toString();
      }

      reportBuilder.append("Summary:");
      if (measurementInterval != null) {
        reportBuilder.append(String.format("\n\tMeasurement interval: %d %s",
              timeUnit.convert(measurementInterval), timeUnit.toString()));
      } else {
        reportBuilder.append("\n\tMeasurement interval: N/A (Measurement interval is calculated " +
            "from operations that completed before deadline, and there were none.)");
      }
      reportBuilder.append(String.format("\n\tTotal operations started: %d", operationsStarted));
      reportBuilder.append(String.format("\n\tTotal operations finished before deadline: %d",
            operationsCompletedBeforeDeadline));
      reportBuilder.append(String.format("\n\tTotal operations failed: %d", operationsFailed));
      reportBuilder.append(String.format("\n\tTotal operations finished after deadline: %d",
            operationsCompletedPastDeadline));
      // Latency distribution data is only applicable if we have success operations to measure
      // latency of.
      if (operationsCompletedBeforeDeadline > 0) {
        reportBuilder.append(String.format(
              "\n\tMin/Median/Avg/P99/P999/P9999/Max Latency: %d/%d/%d/%d/%d/%d/%d %s",
              timeUnit.convert(minLatency),
              // This is not a pedantically correct median but the authors
              // believe it is close enough to not warrant an extra median
              // variable and computation.
              timeUnit.convert(latencyPercentiles.get(0.5)),
              timeUnit.convert(averageLatency),
              timeUnit.convert(latencyPercentiles.get(0.99)),
              timeUnit.convert(latencyPercentiles.get(0.999)),
              timeUnit.convert(latencyPercentiles.get(0.9999)),
              timeUnit.convert(maxLatency),
              timeUnit.toString()));
        // This number gets more stable as the experiment progresses because the start and tail become
        // less relevant.
        reportBuilder.append(String.format("\n\tAverage successful operations per second: %f",
              averageOperationsCompletedPerSecond));

        if (verbosity > 1) {
          reportBuilder.append(String.format("\nCompleted operations in each %d-second interval:",
                COMPLETION_INTERVAL_SECONDS));
          for (int i = 0; i < completedOperationsInEachInterval.length; i++) {
            reportBuilder.append(String.format("\n\t%d %d", i,
                  completedOperationsInEachInterval[i]));
          }

          reportBuilder.append("\nLatency percentiles:");
          for (Double percentile : latencyPercentiles.keySet()) {
            reportBuilder.append(String.format("\n\t%.4f\t%d", percentile,
                  timeUnit.convert(latencyPercentiles.get(percentile))));
          }
        }

        reportBuilder.append("\nLatency histogram:");
        Duration lowerBound = this.minLatency;
        long maxBucketCount = Collections.max(latencyBuckets.values());
        long count;
        for (Duration upperBound : latencyBuckets.keySet()) {
          count = latencyBuckets.get(upperBound);
          reportBuilder.append(String.format("\n\t%6d  -%6d [%6d] \u2502",
                timeUnit.convert(lowerBound), timeUnit.convert(upperBound), count));
          // Draw bar chart using Unicode Block Elements of different widths, using the following
          // algorithm:
          // 1. Compute the count of the bucket with greatest count and call it maxBucketCount. This
          //    will be visually represented by MAX_FULL_HISTOGRAM_BARS. Each bar represent a count of
          //    maxBucketCount / MAX_FULL_HISTOGRAM_BARS. Call this quantity barCount.
          // 2. For every other bucket, compute and draw the number a number of full-width bars based
          //    on the integer division of its count / barCount.
          // 3. Then, draw a fractional-width bar based on the fraction of barCount that count %
          //    barCount warrants.
          //
          // For example, suppose maxBucketCount = 400, MAX_FULL_HISTOGRAM_BARS = 40.
          // Then barCount = 10. A different bucket would with count = 25 would be represented by two
          // full-width bars and one half-width bar.
          //
          // The Block elements are documented here:
          // https://en.wikipedia.org/wiki/Block_Elements
          long numFullBars = count * MAX_FULL_HISTOGRAM_BARS / maxBucketCount;
          for (int i = 0; i < numFullBars; i++) {
            reportBuilder.append("\u2588");
          }

          // Subtract the count represented by the full bars already printed to get the fractional
          // count.
          count -= numFullBars * maxBucketCount / MAX_FULL_HISTOGRAM_BARS;

          // Show at least a 1/8 width bar when count is nonzero for a bucket.
          if (count * MAX_FULL_HISTOGRAM_BARS / maxBucketCount == 0 && count > 0) {
            double fractionOfBar = count * MAX_FULL_HISTOGRAM_BARS * 1.0 / maxBucketCount;
            if (fractionOfBar <= 0.125) {
              reportBuilder.append("\u258F");
            } else if (fractionOfBar <= 0.25) {
              reportBuilder.append("\u258E");
            } else if (fractionOfBar <= 0.375) {
              reportBuilder.append("\u258D");
            } else if (fractionOfBar <= 0.5) {
              reportBuilder.append("\u258C");
            } else if (fractionOfBar <= 0.625) {
              reportBuilder.append("\u258B");
            } else if (fractionOfBar <= 0.75) {
              reportBuilder.append("\u258A");
            } else if (fractionOfBar <= 0.875) {
              reportBuilder.append("\u2589");
            } else {
              // Close enough so a full bar is warranted.
              reportBuilder.append("\u2588");
            }
          }
          lowerBound = upperBound;
        }
      } else {
        reportBuilder.append(
            "\nNo latency or throughput information is available because no requests completed.");
      }
      appendErrors(reportBuilder);
      return reportBuilder.toString();
    }

    /**
     * See documentation for {@link #toString(TimeUnit, bool)}.
     */
    public String toString(TimeUnit timeUnit) {
      return this.toString(timeUnit, /*verbosity=*/1);
    }

    /**
     * See documentation for {@link #toString(TimeUnit, bool)}.
     */
    public String toString() {
      return this.toString(TimeUnit.NANOSECONDS);
    }
  }

  /**
   * The completion status of an operation being benchmarked.
   */
  public static enum Status {
    /**
     * The operation was successful and its latency should be recorded iff it completed before
     * experiment's termination condition was reached.
     */
    SUCCESS,
    /**
     * The operation failed and its latency should not be recorded. The operation may have placed a
     * failure description in the {@literal Box<Object>} that was passed in as the first argument.
     */
    FAILURE,
    /**
     * The operation failed in an unexpected, recurring, or catostraphic way and the experiment
     * should be aborted immediately. The operation may have placed failure description in the
     * {@literal Box<Object>} that was passed in as the first argument.
     */
    ABORT,
  }

  /**
   * A record of a successfully completed operation.
   */
  public static class DataPoint implements Comparable<DataPoint> {
    /**
     * Return value of System.nanoTime() before invoking the operation.
     */
    public long startNanos;

    /**
     * Return value of System.nanoTime() after invoking the operation.
     */
    public long endNanos;

    /**
     * Constructor.
     *
     * @param startNanos See {@link #startNanos}.
     * @param endNanos   See {@link #endNanos}.
     */
    public DataPoint(long startNanos, long endNanos) {
      this.startNanos = startNanos;
      this.endNanos = endNanos;
    }

    /**
     * The natural ordering of data points is by completion time.
     *
     * @param other Another data point to be ordered with this one.
     */
    public int compareTo(DataPoint other) {
      // We cannot just cast to int because long difference will overflow int.
      long diff = this.endNanos - other.endNanos;
      if (diff == 0) return 0;
      return diff < 0 ? -1 : 1;
    }

    /**
     * Alias for getting latency in nanoseconds associated with this request.
     *
     * @return The difference between the start and end of operation this DataPoint represents.
     */
    public long latency() {
      return this.endNanos - this.startNanos;
    }
  }

  /**
   * A DataPoint storage container that batches memory allocations, saving both per-object memory
   * overheads and allocations. When adding constants, this data structure is measured to have
   * 2-10x lower P9999 latency and 16-30% higher throughput than List.add(new DataPoint()).
   *
   * This class is not thread-safe.
   */
  public static class LongBufferDataPointStore {
    /**
     * Overflow list of full LongBuffers containing startNanos.
     */
    private List<LongBuffer> startNanosList;
    /**
     * Overflow list of full LongBuffers containing endNanos.
     */
    private List<LongBuffer> endNanosList;
    /**
     * Current LongBuffer that startNanos should be added to.
     */
    private LongBuffer startNanosBuffer;
    /**
     * Current LongBuffer that endNanos should be added to.
     */
    private LongBuffer endNanosBuffer;

    /**
     * The size in bytes of LongBuffers allocated to store data pointers.  Set arbitrarily to 1 MB
     * for now, which means we have two allocations every 128K data points.
     */
    private static final int CHUNK_SIZE_BYTES = 1 << 20;

    /**
     * Constructor.
     */
    public LongBufferDataPointStore() {
      startNanosList = new ArrayList<LongBuffer>();
      endNanosList = new ArrayList<LongBuffer>();
      startNanosBuffer = LongBuffer.allocate(CHUNK_SIZE_BYTES / Long.BYTES);
      endNanosBuffer = LongBuffer.allocate(CHUNK_SIZE_BYTES / Long.BYTES);
    }

    /**
     * Record the arguments.
     *
     * @param startNanos The value of System.nanoTime() immediately before a benchmarked operation
     *                   began.
     * @param endNanos The value of System.nanoTime() immediately after a benchmarked operation
     *                 began.
     */
    public void add(long startNanos, long endNanos) {
      if (!startNanosBuffer.hasRemaining()) {
        startNanosList.add(startNanosBuffer);
        endNanosList.add(endNanosBuffer);
        startNanosBuffer = LongBuffer.allocate(CHUNK_SIZE_BYTES / Long.BYTES);
        endNanosBuffer = LongBuffer.allocate(CHUNK_SIZE_BYTES / Long.BYTES);
      }
      startNanosBuffer.put(startNanos);
      endNanosBuffer.put(endNanos);
    }

    /**
     * Empty contents from this data store.
     */
    public void clear() {
      startNanosList.clear();
      endNanosList.clear();
      startNanosBuffer.clear();
      endNanosBuffer.clear();
    }

    /**
     * Copies the contents of this data store into a list of DataPoint ordered by completion time.
     *
     * @return A list of DataPoints that represent data equivalent to the data stored in this object.
     */
    public List<DataPoint> toDataPointList() {
      List<DataPoint> dataPoints = new ArrayList<DataPoint>();
      // Collect data from old chunks.
      for (int i = 0; i < startNanosList.size(); i++) {
        LongBuffer startNanosBuffer = startNanosList.get(i);
        LongBuffer endNanosBuffer = endNanosList.get(i);
        for (int j = 0; j < startNanosBuffer.position(); j++) {
          dataPoints.add(new DataPoint(startNanosBuffer.get(j), endNanosBuffer.get(j)));
        }
      }
      // Collect data from most recent chunk.
      for (int j = 0; j < startNanosBuffer.position(); j++) {
        dataPoints.add(new DataPoint(startNanosBuffer.get(j), endNanosBuffer.get(j)));
      }
      return dataPoints;
    }
  }

  /**
   * The number of roughly equal width buckets that that the latency histogram consists of.
   */
  static final int NUM_HISTOGRAM_BUCKETS = 10;

  /**
   * The number of seconds in each interval from the start of the experiment in which we count
   * completed operations for calculating {@link PerfReport#completedOperationsByInterval}.
   */
  static final long COMPLETION_INTERVAL_SECONDS = 1;

  /**
   * True means that we should print messages showing the beginning and ending of warmup and timing
   * phases.
   */
  static final boolean VERBOSE_JVM = Optional.ofNullable(System.getenv("VERBOSE_JVM"))
      .map(Boolean::parseBoolean).orElse(false);

  /**
   * State and main function for each benchmarking thread.
   */
  private static class Worker implements Runnable {
    /**
     * Data structure containing data points produced by this client. Data points are only produced
     * for successful operations.
     * This should be read after this thread has been joined, and merged with data points generated
     * by other threads.
     */
    public LongBufferDataPointStore dataPoints;

    /**
     * Number of operations started by this thread.
     */
    public long operationsStarted;

    /**
     * Number of failures observed by this client.  This should be read after this thread has been
     * joined, and aggregated with the failures experienced by other threads.
     */
    public long operationsFailed;

    /**
     * The number of operations that started and completed successfully after
     * {@link #maxDurationNanos} had passed. Such operations are excluded from latency and
     * throughput calculations because both are intended to be measured under a particular load, and
     * operations that completed after maxDurationNanos may not have completed under the same load
     * since some threads may have finished before others. This number is recorded mostly as a
     * checksum to ensure that all started operations either finished or failed.
     * For synchronous benchmarks, this number can only be 0 or 1.
     */
    public long operationsCompletedPastDeadline;

    /**
     * Counts of "failure status description objects". These are arbitrary objects that a operation
     * can use to describe the cause for the failure, so that the caller can print different types
     * of summaries of failures, or do aggregations of failure classes if they desire.
     */
    public Map<Object, Long> failureDistributionCounts;

    /**
     * Counts of uncaught throwables thrown by an operation. We use the stack trace as a map key
     * instead of the original Throwable object for the following reasons.
     * 1) The most useful bit of information from an Exception is typically the stack trace anyways.
     * 2) Exceptions do not generally override hashCode() and equals(), so keeping the throwable
     *    would amount to having to wrap it, making it less convenient to directly access.
     * 3) A caller that wants the original objection can always catch it themselves and put it into
     *    the failureDescription box.
     */
    public Map<String, Long> uncaughtThrowableCounts;

    /**
     * Function used for thread-specific state initialization. This function takes a thread index
     * and returns a user-defined Object. It is run once by this worker before running
     * {@link #operation}, and its return value is passed into {@link #operation}.
     */
    private Function<Integer, Object> threadInit;

    /**
     * The actual operation to benchmark. It returns a Status to let the benchmark client know
     * whether the operation succeeded, failed, or fatally failed in a way that merits aborting the
     * benchmark. Its first argument is the thread-specific user object that was returned from
     * {@link #threadInit}. In the case of failure, it can optionally place an object describing the
     * type of failure into the Box that is passed into it.
     */
    private BiFunction<Object, Box<Object>, Status> operation;

    /**
     * Used to ensure that all threads start at roughly the same time.
     */
    private final CyclicBarrier syncStart;

    /**
     * When present, used to rate-limit this worker when running in constant
     * arrival mode with a target QPS.
     */
    private Optional<Runnable> threadBlocker;

    /**
     * The maximum number of times this Worker will invoke operation during the timing loop. A value
     * of 0 is equivalent to infinity.
     */
    private final long maxOperations;

    /**
     * Time after which no further responses should be recorded.
     */
    private AtomicLong experimentEndNanos;

    /**
     * Maximum number of nanoseconds experiment should run for, after all threads have started.
     */
    private long maxDurationNanos;

    /**
     * Number of times to run operation before starting measurements.
     */
    private long numWarmupOps;

    /**
     * 0-based index of this worker in the workers array. Used for differentating between workers
     * when printing log messages.
     */
    private int id;

    /**
     * Constructor.
     *
     * @param threadInit         See {@link #threadInit}.
     * @param operation          See {@link #operation}.
     * @param syncStart          See {@link #syncStart}.
     * @param threadBlocker      See {@link #threadBlocker}.
     * @param maxOperations      See {@link #maxOperations}.
     * @param experimentEndNanos See {@link #experimentEndNanos}.
     * @param maxDurationNanos   See {@link #maxDurationNanos}.
     * @param numWarmupOps       See {@link #numWarmupOps}.
     * @param id                 See {@link #id}.
     */
    public Worker(
        Function<Integer, Object> threadInit,
        BiFunction<Object, Box<Object>, Status> operation,
        CyclicBarrier syncStart,
        Optional<Runnable> threadBlocker,
        long maxOperations,
        AtomicLong experimentEndNanos,
        long maxDurationNanos,
        long numWarmupOps,
        int id) {
      this.threadInit = threadInit;
      this.operation = operation;
      this.syncStart = syncStart;
      this.threadBlocker = threadBlocker;
      this.maxOperations = maxOperations;
      this.experimentEndNanos = experimentEndNanos;
      this.maxDurationNanos = maxDurationNanos;
      this.numWarmupOps = numWarmupOps;
      this.id = id;

      this.dataPoints = new LongBufferDataPointStore();
      this.operationsStarted = 0L;
      this.operationsFailed = 0L;
      this.operationsCompletedPastDeadline = 0L;
      this.failureDistributionCounts = new HashMap<Object, Long>();
      this.uncaughtThrowableCounts = new HashMap<String, Long>();
    }

    /**
     * Entry point for benchmarking thread. This function synchronizes the workers so that they
     * start roughly simultaneously and executes the benchmarked operation until either
     * maxDurationNanos have passed or maxOperations is reached.
     *
     * This function should never be invoked directly and should instead be used with
     * Thread.start().
     */
    @Override public void run() {
      Object threadSpecificCustomData = this.threadInit.apply(this.id);
      // These print statements are used together with the flags `-XX:+PrintCompilation -verbose:gc`
      // to make it easier to tell which phase of benchmarking various JVM operations occur in.
      // This approach is recommended by the following article:
      // https://stackoverflow.com/a/513259/39116
      if (VERBOSE_JVM) {
        System.out.println(String.format("WARMUP START %d", id));
      }
      // Warm up the operation before synchronizing with other threads on end time of experiment.
      // Note that the timing and dataPoints add and removal is intended to trigger JVM class
      // loading and compilation.
      for (int i = 0; i < numWarmupOps; i++) {
        Box<Object> failureStatusBox = new Box<Object>();
        long startNanos = System.nanoTime();
        Status status = null;
        try {
          status = operation.apply(threadSpecificCustomData, failureStatusBox);
        } catch(Throwable t) {
          System.err.println(String.format(
              "Operation encountered an uncaught Throwable during warmup: %s",
              LoggingUtils.getStackTrace(t)));
        }
        long endNanos = System.nanoTime();
        if (status == Status.ABORT) {
          System.err.println("Operation encountered a fatal error during warmup, aborting....");
          Object failureDescription = failureStatusBox.get();
          if (failureDescription != null) {
            System.err.println(failureDescription.toString());
          }
          Runtime.getRuntime().halt(1);
        }
        dataPoints.add(startNanos, endNanos);
      }
      dataPoints.clear();
      if (VERBOSE_JVM) {
        System.out.println(String.format("WARMUP END %d", id));
      }

      // All threads will set this, and the last thread will set the latest value.
      if (maxDurationNanos != 0) {
        experimentEndNanos.set(System.nanoTime() + maxDurationNanos);
      } else {
        experimentEndNanos.set(Long.MAX_VALUE);
      }

      // This barrier exists to wait for all other threads to be ready so all
      // threads can start at roughly the same time and also to prevent threads
      // from reading experimentEndNanos until the last write to it is done.
      //
      // Note that the actual time that each thread starts benchmarking will always differ because
      // await() cannot wake up every thread simultaneously in the general case (when we run more
      // threads than cores). There were two options discussed for dealing with this:
      //   a) Having each thread run for a fixed duration from when it wakes up; threads that wake
      //      up earlier will stop running before threads that wake up later.
      //   b) Having each thread run until a fixed end time; threads that wake up later will run for
      //      a shorter amount of time.
      //
      // Having a fixed end time for all threads rather than a fixed duration is useful because it
      // makes it easier to reason about completions during the last N intervals (when running in
      // verbose mode) because we hopefully do not have to guess which threads are still running.
      try {
        syncStart.await();
      } catch(InterruptedException|BrokenBarrierException e) {
        System.out.println("Interrupted in barrier, aborting.");
        Runtime.getRuntime().halt(1);
      }

      // The value of experimentEndNanos will never change after the barrier above, so we cache it
      // on the local stack to save an atomic operation.
      long localExpEndNanos = experimentEndNanos.get();

      // Used for failure status description objects.
      Box<Object> failureStatusBox = new Box<Object>();

      // Run until we either run out of time or operations.
      if (VERBOSE_JVM) {
        System.out.println(String.format("TIMING START %d", id));
      }
      while (true) {
        if (threadBlocker.isPresent()) {
          threadBlocker.get().run();
        }

        if (maxOperations != 0 && operationsStarted == maxOperations) {
          // We have finished all requested operations.
          break;
        }

        operationsStarted++;

        long startNanos = System.nanoTime();
        Status status = null;
        try {
          status = operation.apply(threadSpecificCustomData, failureStatusBox);
        } catch (Throwable t) {
          operationsFailed++;
          String stackTrace = LoggingUtils.getStackTrace(t);
          if (uncaughtThrowableCounts.containsKey(stackTrace)) {
            uncaughtThrowableCounts.put(stackTrace, uncaughtThrowableCounts.get(stackTrace) + 1);
          } else {
            uncaughtThrowableCounts.put(stackTrace, 1L);
          }
        }
        long endNanos = System.nanoTime();

        boolean experimentExpired = endNanos > localExpEndNanos;
        // Only record latency for successful operations.
        if (status == Status.SUCCESS) {
          if (!experimentExpired) {
            // Only count successes if the end time of the experiment has not passed.  This avoids
            // artifically lower throughput because certain threads finish much later than others and
            // expand the total duration of the experiment.
            dataPoints.add(startNanos, endNanos);
            // This is an optimization to avoid checking experimentExpired again at the bottom of
            // this loop. That check is necessary in the failure case, but we expect the common case
            // is success.
            continue;
          } else {
            operationsCompletedPastDeadline++;
          }
        } else if (status == Status.ABORT) {
          System.err.println("Operation encountered a fatal error, aborting....");
          Object failureDescription = failureStatusBox.get();
          if (failureDescription != null) {
            System.err.printf("%s\n", failureDescription.toString());
          }
          Runtime.getRuntime().halt(1);
        } else if (status == Status.FAILURE) {
          // Many failures are timeouts, so it's important to count those before checking the
          // deadline.
          operationsFailed++;
          Object failureDescription = failureStatusBox.get();
          if (failureDescription != null) {
            if (failureDistributionCounts.containsKey(failureDescription)) {
              failureDistributionCounts.put(
                  failureDescription, failureDistributionCounts.get(failureDescription) + 1L);
            } else {
              failureDistributionCounts.put(failureDescription, 1L);
            }
            failureStatusBox.set(null);
          }
        } else if (status == null) {
          // We encountered an uncaught exception above.
          // We do not currently need any special handling for this case, but this case is included
          // for completeness.
        }

        if (experimentExpired) {
          break;
        }
      }
      if (VERBOSE_JVM) {
        System.out.println(String.format("TIMING END %d", id));
      }
    }
  }

  /**
   * Count the number of operations that finished in each interval of length
   * {@link #COMPLETION_INTERVAL_SECONDS}.
   *
   * @param dataPoints List of data consisting of start and end times measured in nanoseconds,
   *                   sorted by end time in ascending order.
   * @return The number of operations that finished in each {@link #COMPLETION_INTERVAL_SECONDS},
   *         starting from the time that the first operation completed.
   */
  public static long[] getCompletedOperationsInEachInterval(List<DataPoint> dataPoints) {
    // Construct explicit time cutoffs to avoid putting completions in the wrong interval when an
    // interval has no completions.
    long completionIntervalNs = COMPLETION_INTERVAL_SECONDS * 1000L * 1000L * 1000L;
    long firstCompletionTime = dataPoints.get(0).endNanos;
    long lastCompletionTime = dataPoints.get(dataPoints.size() - 1).endNanos;
    int numIntervals = (int) ((lastCompletionTime - firstCompletionTime) / completionIntervalNs);
    if ((lastCompletionTime - firstCompletionTime) % completionIntervalNs != 0) {
      numIntervals++;
    }
    // When there is exactly one data point, we need to bump numIntervals to make sure there is no
    // crash.
    if (numIntervals == 0) {
      numIntervals++;
    }

    // This is a set of cutoff endNanos that determine which completion interval an operation is
    // counted for. A request with endNanos < intervalUpperBounds[0] is counted for completion
    // interval 0, while a request a request with whose endNanos fall in the range
    // intervalUpperBounds[i] <= endNanos < intervalUpperBounds[i+1] would be counted for completion
    // interval i.
    long[] intervalUpperBounds = new long[numIntervals];
    long[] completedOperationsInEachInterval = new long[numIntervals];
    for (int i = 0; i < intervalUpperBounds.length; i++) {
      intervalUpperBounds[i] = firstCompletionTime + (i + 1) * completionIntervalNs;
    }

    // The last intervalUpperBound is incremented to ensure that the last data point is captured
    // even if the delta between firstCompletionTime and lastCompletionTime was exactly an integer
    // number of seconds.
    intervalUpperBounds[intervalUpperBounds.length - 1]++;

    int index = 0;
    long count = 0L;
    for (DataPoint dp : dataPoints) {
      long endNanos = dp.endNanos;
      if (endNanos < intervalUpperBounds[index]) {
        count++;
      } else {
        completedOperationsInEachInterval[index] = count;
        index++;

        // Ensure that we find the correct interval for the current data point.
        while (endNanos > intervalUpperBounds[index]) {
          index++;
        }

        // Record this particular data point in the interval found above.
        count = 1L;
      }
    }
    completedOperationsInEachInterval[completedOperationsInEachInterval.length - 1] = count;
    return completedOperationsInEachInterval;
  }

  /**
   * Count the number of observed latencies falling into each of {@link #NUM_HISTOGRAM_BUCKETS}
   * roughly evenly-size buckets.
   *
   * @param latencySortedDataPoints List of data consisting of start and end times in
   *                                nanoseconds, sorted by latency in ascending order.
   * @return A map of latency bucket upper bounds 
   */
  public static TreeMap<Long, Long> getLatencyBuckets(List<DataPoint> latencySortedDataPoints) {
    long minLatency = latencySortedDataPoints.get(0).latency();
    long maxLatency = latencySortedDataPoints.get(latencySortedDataPoints.size() - 1).latency();
    long bucketSize = (maxLatency - minLatency) / NUM_HISTOGRAM_BUCKETS;
    // Impose a minimum bucket size of 1, so that if maxLatency - minLatency <
    // NUM_HISTOGRAM_BUCKETS, this function does not degenerate to a single bucket.
    if (bucketSize == 0) {
      bucketSize = 1;
    }

    // Construct explicit bucket thresholds to make it easier to handle the last bucket edge case
    // while avoiding comparing longs with doubles.
    long[] thresholds = new long[NUM_HISTOGRAM_BUCKETS];
    for (int i = 0; i < thresholds.length - 1; i++) {
      thresholds[i] = minLatency + (i + 1) * bucketSize;
    }

    // The last threshold is set to infinity to accumulate all the data points in the last count. We
    // will add maxLatency to the latencyBuckets explicitly after the loop. This bucket is only
    // needed to avoid an index out of bounds error in the loop below.
    thresholds[thresholds.length - 1] = Long.MAX_VALUE;

    TreeMap<Long, Long> latencyBuckets = new TreeMap<Long, Long>();
    int index = 0;
    long count = 0L;
    for (DataPoint dp : latencySortedDataPoints) {
      long latency = dp.latency();
      if (latency < thresholds[index]) {
        count++;
      } else {
        // Put in the count previous data points that were lower latency.
        latencyBuckets.put(thresholds[index], count);
        index++;

        // Ensure that we find the correct bucket for the current data point
        // This loop will converge as long as the max measured latency is less than Long.MAX_VALUE.
        while (latency > thresholds[index]) {
          latencyBuckets.put(thresholds[index], 0L);
          index++;
        }

        // Record this particular data point in the bucket found above.
        count = 1L;
      }
    }

    // Final bucket. Unlike the other buckets which are inclusive on the lower end and exclusive at
    // the upper end, this one is inclusive on both ends.
    if (latencyBuckets.containsKey(maxLatency)) {
      latencyBuckets.put(maxLatency, latencyBuckets.get(maxLatency) + count);
    } else {
      latencyBuckets.put(maxLatency, count);
    }
    return latencyBuckets;
  }

  /**
   * Compute the latency at various percentiles.
   *
   * @param latencySortedDataPoints List of data consisting of start and end times in
   *                                nanoseconds, sorted by latency in ascending order.
   * @return A map from decimal representations of percentiles (e.g. P99 = 0.99) to the observed
   *         latency at that percentile.
   */
  public static TreeMap<Double, Long> getLatencyPercentiles(
      List<DataPoint> latencySortedDataPoints) {
    TreeMap<Double, Long> latencyPercentiles = new TreeMap<Double, Long>();
    for (long i = 0; i < 100; i++) {
      // This hack ensures that `get` with literals like `0.01` work correctly.
      latencyPercentiles.put(Double.parseDouble(String.format("%.2f", i / 100.0)),
          latencySortedDataPoints.get((int) (latencySortedDataPoints.size() * i / 100L)).latency());
    }
    latencyPercentiles.put(0.999,
        latencySortedDataPoints.get((int) (latencySortedDataPoints.size() * 0.999)).latency());
    latencyPercentiles.put(0.9999,
        latencySortedDataPoints.get((int) (latencySortedDataPoints.size() * 0.9999)).latency());
    return latencyPercentiles;
  }

  /**
   * Compute throughput, summary statistics, latency distribution and custom error distribution for
   * the given data.  Note that this function may reorder the input order of dataPoints in-place to
   * avoid additional memory consumption.
   *
   * @param dataPoints List of data consisting of start and end times measured in nanoseconds.
   *
   * @return A PerfReport containing information about latency, throughput and errors.
   */
  public static PerfReport buildPerfReport(List<DataPoint> dataPoints) {
    if (dataPoints.isEmpty()) {
      PerfReport perfReport = new PerfReport();
      // This is the only field that makes sense to set when we do not have data.
      perfReport.operationsCompletedBeforeDeadline = 0;
      return perfReport;
    }
    // This set of operations requires dataPoints to be sorted by latency.
    Collections.sort(dataPoints, (a, b) -> {
        // We cannot just cast diff to int because the long difference will overflow int.
        long diff = a.latency() - b.latency();
        if (diff == 0) return 0;
        return diff < 0 ? -1 : 1;
    });
    long minLatency = dataPoints.get(0).latency();
    long maxLatency = dataPoints.get(dataPoints.size() - 1).latency();
    TreeMap<Long, Long> latencyBuckets = getLatencyBuckets(dataPoints);
    TreeMap<Double, Long> latencyPercentiles = getLatencyPercentiles(dataPoints);

    // This set of operations requires dataPoints to be sorted by completion time.
    Collections.sort(dataPoints);
    long averageLatency = 0L;
    for (DataPoint dp : dataPoints) {
      averageLatency += dp.latency();
    }
    averageLatency /= dataPoints.size();
    long measurementInterval =
        (dataPoints.get(dataPoints.size() - 1).endNanos - dataPoints.get(0).endNanos);
    double averageOperationsCompletedPerSecond =
        dataPoints.size() * 1000000000.0 / measurementInterval;
    long[] completedOperationsInEachInterval = getCompletedOperationsInEachInterval(dataPoints);

    PerfReport perfReport = new PerfReport();
    perfReport.operationsCompletedBeforeDeadline = dataPoints.size();
    perfReport.measurementInterval = Duration.ofNanos(measurementInterval);
    perfReport.averageOperationsCompletedPerSecond = averageOperationsCompletedPerSecond;
    perfReport.minLatency = Duration.ofNanos(minLatency);
    perfReport.averageLatency = Duration.ofNanos(averageLatency);
    perfReport.maxLatency =  Duration.ofNanos(maxLatency);
    perfReport.dataPoints = dataPoints;
    perfReport.completedOperationsInEachInterval = completedOperationsInEachInterval;
    perfReport.latencyBuckets = new TreeMap<Duration, Long>();
    for (Long upperBound : latencyBuckets.keySet()) {
      perfReport.latencyBuckets.put(Duration.ofNanos(upperBound), latencyBuckets.get(upperBound));
    }
    perfReport.latencyPercentiles = new TreeMap<Double, Duration>();
    for (Double percentile : latencyPercentiles.keySet()) {
      perfReport.latencyPercentiles.put(percentile,
          Duration.ofNanos(latencyPercentiles.get(percentile)));
    }
    return perfReport;
  }

  /**
   * Compute throughput, latency distribution and custom error distribution for the provided
   * operation. At least one of maxOperations and maxDuration must be specified. Note that using
   * maxOperations with numThreads greater than 1 is unsupported, because it is inefficient to
   * implement correctly (all threads keep consuming maxOperations until it is 0 requires all the
   * threads to ping an atomic long).  hq6 expects that multithreaded tests will always use
   * maxDuration because the maxOperations is typically only used for measuring unloaded latency.
   *
   * The `operation` interface requires that callers pass failed state description by reference,
   * rather than returning a wrapper object consisting of a Status and Object reference. This may
   * seem non-idiomatic, but its purpose is to reduce performance overheads without forcing callers
   * to take on additional complexity.
   *
   * When there is no error (expected to be the common case), returning a wrapper would naively
   * force the task to construct a new wrapper object to return. This could be fixed by having test
   * cases construct and cache a "Success" wrapper object, but now we've introduced additional
   * complexity in callers.
   *
   * When an error occurs, the operation that observed the error typically already has either an
   * Exception object or some other application-specific object describing the error. In the latter
   * case, it's easy for them to shove that object into the pre-existing Box without constructing
   * anything new. In the former case, they may have to wrap the exception in a hashable type, but
   * they would have to do that regardless of the interface because Exceptions are not hashable. If
   * we require a wrapper, the operation would then have to pay the additional cost of constructing
   * the wrapper as well.
   *
   * @param args  An object wrapping required and optional arguments; used to
   *              avoid the need to modify callers or increase the variations
   *              of this method when new options are added.
   *
   * @return A PerfReport containing information about latency, throughput and errors.
   */
  public static PerfReport benchmarkSynchronousOperation(PerfArguments args) {
    // Unpack frequently referenced args into local variables for convenience.
    int numThreads = args.numThreads.get();
    Optional<Long> maxOperations = args.maxOperations;
    Optional<Duration> maxDuration = args.maxDuration;
    // Require at least one of maxOperations and maxDuration.
    if ((maxOperations.isEmpty() || maxOperations.get() == 0) &&
        (maxDuration.isEmpty() || maxDuration.get().equals(Duration.ZERO))) {
      throw new IllegalArgumentException("One of maxOperations or maxDuration must be given.");
    }

    // We need the maxOperations == 0 check here because we cannot give null on the command line,
    // but we can give 0 to have this treated as null.
    if ((maxOperations.isPresent() && maxOperations.get() != 0) && numThreads != 1) {
      throw new IllegalArgumentException("maxOperations is only supported for numThreads == 1.");
    }

    // If we are running with targetOpsPerSecond, require maxThreads so that we
    // do not use unbounded resources. Require that there is no maxOperations.
    if (args.targetOpsPerSecond.isPresent()) {
      if (args.maxThreads.isEmpty()) {
        throw new IllegalArgumentException("maxThreads is required when using targetOpsPerSecond.");
      }
      if (maxOperations.isPresent() && maxOperations.get() != 0) {
        throw new IllegalArgumentException("maxOperations not supported when using targetOpsPerSecond.");
      }
    }

    if (args.targetOpsPerSecond.isPresent() && args.maxDuration.isEmpty()) {
      throw new IllegalArgumentException("maxDuration is required when using targetOpsPerSecond.");
    }

    // Passed into each worker and set by the last worker to start up so they can synchronize on
    // when to start without having thread startup time deducted from maxDuration.
    // We could instead compute experimentEndNanos here and pass it into each worker, but then the
    // experiment would only actually run for maxDuration - threadStartupTime. For sufficiently
    // large numbers of threads and sufficient small values of maxDuration, this can be substantial
    // and skew the experimental results.
    AtomicLong experimentEndNanos = new AtomicLong(0);

    // Create workers.
    Worker[] workers;
    Thread[] threads;
    if (args.targetOpsPerSecond.isEmpty()) {
      final CyclicBarrier syncStart = new CyclicBarrier(numThreads);
      workers = new Worker[numThreads];
      threads = new Thread[numThreads];
      for (int i = 0; i < threads.length; i++) {
        workers[i] = new Worker(args.threadInit, args.operation, syncStart, Optional.empty(),
            maxOperations.orElse(0L), experimentEndNanos, maxDuration.orElse(Duration.ZERO).toNanos(), args.numWarmupOps, i);
        threads[i] = new Thread(workers[i]);
        threads[i].start();
      }
    } else {
      // General strategy for achieving constant arrival rate:
      // 1. All the worker threads block on getting a permit from the semaphore operationsAvailable.
      // 2. A dispatch thread creates permits at the desired arrival rate.
      // 3. Whichever worker is granted a permit will perform an operation and then go back to
      //    waiting for permits.
      //
      // If the workers have fallen behind the dispatcher because the operation runs slowly, then
      // the number of permits will pile up. In this scenario, workers will never block when they
      // call operationsAvailable.acquire(), so they will run as quickly as they can, effectively
      // performing back-to-back operations.

      // We require one more thread to join the barrier, because we want the dispatch
      // thread (which is running this code)  to participate in the barrier,
      // and not read endNanos until every other thread has written it.
      int maxThreads = args.maxThreads.get();
      final CyclicBarrier syncStart = new CyclicBarrier(maxThreads + 1);
      workers = new Worker[maxThreads];
      threads = new Thread[maxThreads];

      // Unfair semaphore is more performant
      // We start with 0 permits so that none of the threads will start running (non-warmup)
      // operations until the dispatch thread starts adding permits.
      final Semaphore operationsAvailable = new Semaphore(0, false);
      Runnable threadBlocker = () -> {
        try {
          operationsAvailable.acquire();
        } catch (InterruptedException e) {
          // Should only happen on termination.
        }
      };
      for (int i = 0; i < threads.length; i++) {
        workers[i] = new Worker(args.threadInit, args.operation, syncStart,
            Optional.of(threadBlocker),
            maxOperations.orElse(0L), experimentEndNanos, maxDuration.orElse(Duration.ZERO).toNanos(), args.numWarmupOps, i);
        threads[i] = new Thread(workers[i]);
        threads[i].start();
      }
      // The main thread will feeds the Semaphore until the experiment ends.
      double targetOpsPerSecond = args.targetOpsPerSecond.get();
      long intervalNanos = (long) (1 / targetOpsPerSecond * 1e9);

      long previousTime = System.nanoTime();

      // Synchronize with the worker threads before reading experimentEndNanos.
      try {
        syncStart.await();
      } catch(InterruptedException|BrokenBarrierException e) {
        System.out.println("Interrupted in barrier, aborting.");
        Runtime.getRuntime().halt(1);
      }

      long localExpEndNanos = experimentEndNanos.get();
      while (System.nanoTime() < localExpEndNanos) {
        long targetTime = previousTime + intervalNanos;
        while (System.nanoTime() < targetTime) {
          long delta = targetTime - System.nanoTime();
          if (delta > 100000) {
            LockSupport.parkNanos(delta);
          }
          // If delta is smaller than 100 microseconds, spin loop because
          // parkNanos has bad precision.
        }
        previousTime = targetTime;
        // This condition prevents permits from increasing without bound and eventually overflowing.
        // The multiplier 10 is chosen arbitrarily.
        if (operationsAvailable.availablePermits() < maxThreads * 10) {
          operationsAvailable.release();
        }
      }

      // After the experiment ends, provide a large number of permits so all the threads can wake up
      // and exit. We provide more than the number of threads in case System.nanoTime moved
      // backwards for some threads.
      //
      // For more information on System.nanoTime moving backwards, see this post:
      //
      //    https://stackoverflow.com/questions/8853698/is-system-nanotime-system-nanotime-guaranteed-to-be-0
      //
      // On a single core, System.nanoTime is monotonically increasing. Across multiple cores, it is
      // not guaranteed.
      operationsAvailable.release(maxThreads * 10);
    }

    // Wait for all threads to finish
    for (int i = 0; i < threads.length; i++) {
      try {
        threads[i].join();
      } catch(InterruptedException e) {
        System.out.println("Interrupted while joining, aborting.");
        Runtime.getRuntime().halt(1);
      }
    }

    // Gather output from workers
    List<DataPoint> dataPoints = new ArrayList<DataPoint>();
    long operationsStarted = 0L;
    long operationsFailed = 0L;
    long operationsCompletedPastDeadline = 0L;
    Map<Object, Long> failureDistributionCounts = new HashMap<>();
    Map<String, Long> uncaughtThrowableCounts = new HashMap<>();

    for (int i = 0; i < workers.length; i++) {
      dataPoints.addAll(workers[i].dataPoints.toDataPointList());
      workers[i].dataPoints.clear();
      operationsStarted += workers[i].operationsStarted;
      operationsFailed += workers[i].operationsFailed;
      operationsCompletedPastDeadline += workers[i].operationsCompletedPastDeadline;
      workers[i].failureDistributionCounts.forEach(
          (failureDescription, count) -> failureDistributionCounts.merge(
            failureDescription, count, (count1, count2) -> count1 + count2));
      workers[i].uncaughtThrowableCounts.forEach(
          (uncaughtThrowable, count) -> failureDistributionCounts.merge(
            uncaughtThrowable, count, (count1, count2) -> count1 + count2));
    }

    PerfReport perfReport = buildPerfReport(dataPoints);
    perfReport.operationsCompletedPastDeadline = operationsCompletedPastDeadline;
    perfReport.operationsFailed = operationsFailed;
    perfReport.operationsStarted = operationsStarted;
    perfReport.failureDistributionCounts = failureDistributionCounts;
    perfReport.uncaughtThrowableCounts = uncaughtThrowableCounts;
    return perfReport;
  }

  /**
   * Utility function to expand comma-separated lists of ranges with optional steps into
   * materialized lists of integers. This function does not dedup the input.
   * Example 1: 1-3 becomes [1,2,3].
   * Example 2: 1-3,5-10:2 becomes [1,2,3,5,7,9].
   *
   * @param input A string describing a range, such as 1-3.
   *
   * @return The materialized lists of ranges, in the same order that the ranges were given in.
   */
  public static List<Integer> parseRanges(String input) {
    String errorMessage = String.format("Failed to parse range %s", input);
    List<Integer> result = new ArrayList<>();
    if (input == null || "".equals(input)) {
      return result;
    }

    String[] ranges;
    if (input.indexOf(",") == -1) {
      // This case exists only for readability, because hq6 believes many readers will not
      // immediately know that split behaves correctly when the input does not include the
      // delimiter.
      ranges = new String[] {input};
    } else {
      ranges = input.split(",");
    }
    for (String range: ranges) {
      int step = 1;
      // A step size is specified
      if (range.indexOf(":") != -1) {
        String[] rangeAndStep = range.split(":");
        if (rangeAndStep.length != 2) {
          throw new IllegalArgumentException(errorMessage);
        }
        try {
          step = Integer.parseInt(rangeAndStep[1]);
        } catch (NumberFormatException e) {
          throw new IllegalArgumentException(errorMessage, e);
        }
        range = rangeAndStep[0];
      }

      if (range.indexOf("-") != -1) {
        // This is a range and not just a single number.
        String[] bounds = range.split("-");
        if (bounds.length != 2) {
          throw new IllegalArgumentException(errorMessage);
        }
        int lower, upper;
        try {
          lower = Integer.parseInt(bounds[0]);
          upper = Integer.parseInt(bounds[1]);
        } catch (NumberFormatException e) {
          throw new IllegalArgumentException(errorMessage, e);
        }
        if (lower <= upper) {
          // The range is ascending, so we must ensure step is positive.
          if (step < 0) {
            step *= -1;
          }
          for (int i = lower; i <= upper; i+= step) {
            result.add(i);
          }
        } else {
          // The range is descending, so we must ensure step is negative.
          if (step > 0) {
            step *= -1;
          }
          for (int i = lower; i >= upper; i += step) {
            result.add(i);
          }
        }
      } else {
        // This is a single number, so we ignore step if it is specified.
        try {
          result.add(Integer.parseInt(range));
        } catch (NumberFormatException e) {
          throw new IllegalArgumentException(errorMessage, e);
        }
      }
    }
    return result;
  }
}
