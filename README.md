# Perf

Perf is a high-performance load testing library and framework for measuring the
latency and throughput of arbitrary operations in Java.

It is low enough overhead to measure operations as fast as tens of nanoseconds,
and complete millions of operations per second.

Within Square, these operations include log encryption functions, database
queries, gRPC calls, and HTTP requests.

## Getting Started

One can run the built-in example operations by running the following
commands in any shell.
```
make
# List all the available tests.
./perf.sh
# Run a particular test
./perf.sh <TestName> [options]

# Example: Measure two ways of synchronizing access to an integer.
./perf.sh Synchronized
```

## Output formats

### Default format, verbosity=1

The default format is intended for human consumption. It outputs a summary of
statistics and counts of succeeded and failed operations, as well as a visual
representation of histogram buckets. If errors were reported during execution,
their string representation is presented after the main output.
```
./perf.sh  StringFormat
Running StringFormat: Benchmark of String.format.
Summary:
        Measurement interval: 2999986583 NANOSECONDS
        Total operations started: 18755810
        Total operations finished before deadline: 18755809
        Total operations failed: 0
        Total operations finished after deadline: 1
        Min/Median/Avg/P99/P999/P9999/Max Latency: 0/125/144/1000/1417/6375/25932333 NANOSECONDS
        Average successful operations per second: 6251964.294202
Latency histogram:
             0  -2593233 [18755802] │████████████████████████████████████████
        2593233  -5186466 [     4] │▏
        5186466  -7779699 [     1] │▏
        7779699  -10372932 [     0] │
        10372932  -12966165 [     0] │
        12966165  -15559398 [     0] │
        15559398  -18152631 [     0] │
        18152631  -20745864 [     0] │
        20745864  -23339097 [     0] │
        23339097  -25932333 [     2] │▏
```

### Concise format, verbosity=0

This CSV format is intended for machine consumption, and presents latency
percentiles and QPS data in a structure that is easy for scripts to consume, and
either aggregate in a spreadsheet or send to a monitoring system such as
Datadog or SignalFx.

```
./perf.sh  StringFormat -verbosity 0
Running StringFormat: Benchmark of String.format.
Req/Sec,Min,Median,Avg,P99,P999,P9999,Max,Interval,Ops start,Ops finish,Ops fail,Ops t/o
6327665.74,0,125,142,1000,1458,6209,27535542,2999985583,18982907,18982906,0,1
```
### Verbose format, verbosity=2

The verbose format is intended to help humans debug performance, as well as
assist with debugging the Perf library itself. In addition to the output of the
default format, it includes the number of completed operations in each 1-second interval
and the values of each latency percentiles from 1 to 99.99. The sample output
is truncated for brevity below:

```
./perf.sh  StringFormat -maxDuration 5  -verbosity 2
Running StringFormat: Benchmark of String.format.
Summary:
        Measurement interval: 4999986791 NANOSECONDS
        Total operations started: 28042337
        Total operations finished before deadline: 28042336
        Total operations failed: 0
        Total operations finished after deadline: 1
        Min/Median/Avg/P99/P999/P9999/Max Latency: 0/125/161/667/1375/7708/28505333 NANOSECONDS
        Average successful operations per second: 5608482.016488
Completed operations in each 1-second interval:
        0 5653198
        1 7256567
        2 5743653
        3 4334194
        4 5054724
Latency percentiles:
        0.0000  0
        0.0100  42
        0.0200  83
        ...
        0.5000  125
				...
        0.9500  250
        0.9600  292
        0.9700  334
        0.9800  458
        0.9900  667
        0.9990  1375
        0.9999  7708
Latency histogram:
             0  -2850533 [28042328] │████████████████████████████████████████
        2850533  -5701066 [     4] │▏
        5701066  -8551599 [     1] │▏
        8551599  -11402132 [     0] │
        11402132  -14252665 [     0] │
        14252665  -17103198 [     1] │▏
        17103198  -19953731 [     0] │
        19953731  -22804264 [     0] │
        22804264  -25654797 [     0] │
        25654797  -28505333 [     2] │▏
```


## Usage

There are two ways to consume Perf.
 * **Use Perf's main method**. You can fork the repo and add tests to Perf.java which are shaped like the
   existing examples.
 * **Use PerfUtils library methods**. You can `make perfutils.jar`, add it to
   your classpath, and invoke one of the variations of
   `PerfUtils.benchmarkSynchronousOperation`.

## Limitations
 * The maximum precision of latency measurements depends on the implementation
   of `System.nanoTime()` on the system where Perf is run.
 * There is currently no support for synchronizing multiple instances of Perf
   to measure throughput together, although launching two instances together in
   a `tmux` window and adding the measured throughputs gives a reasonable
   approximation.
