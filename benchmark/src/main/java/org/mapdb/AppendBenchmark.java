package org.mapdb;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

@State(Scope.Benchmark)
@Warmup(iterations = 4)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public abstract class AppendBenchmark {

  Store store;

  abstract protected Store initialize();

  protected String name() {
    return getClass().getSimpleName();
  }

  @Setup(Level.Iteration)
  public void setup() throws IOException {
    store = initialize();
  }

  @TearDown(Level.Iteration)
  public void tearDown() throws IOException {
    store.commit();
    store.close();
    store = null;
  }

  @Benchmark
  @Fork(jvmArgs = {"-server", "-XX:+AggressiveOpts", "-dsa", "-Xbatch", "-Xms1g", "-Xmx2g"})
  @Measurement(iterations = 10)
  @BenchmarkMode(Mode.Throughput)
  public void putZero() throws IOException {
    store.put(0L, Serializer.LONG);
  }

  @Benchmark
  @Fork(jvmArgs = {"-server", "-XX:+AggressiveOpts", "-dsa", "-Xbatch", "-Xms3g", "-Xmx4g"})
  @Measurement(iterations = 10)
  @BenchmarkMode(Mode.Throughput)
  public void putSmall() throws IOException {
    store.put(new byte[1024], Serializer.BYTE_ARRAY);
  }

  @Benchmark
  @Fork(jvmArgs = {"-server", "-XX:+AggressiveOpts", "-dsa", "-Xbatch", "-Xms7g", "-Xmx8g"})
  @Measurement(iterations = 8)
  @BenchmarkMode(Mode.Throughput)
  public void putMedium() throws IOException {
    store.put(new byte[1024*64], Serializer.BYTE_ARRAY);
  }

  @Benchmark
  @Fork(jvmArgs = {"-server", "-XX:+AggressiveOpts", "-dsa", "-Xbatch", "-Xms9g", "-Xmx10g"})
  @Measurement(iterations = 5)
  @BenchmarkMode(Mode.Throughput)
  public void putBig() throws IOException {
    store.put(new byte[1024*128], Serializer.BYTE_ARRAY);
  }
}
