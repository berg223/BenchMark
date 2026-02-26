/**
 * Copyright 2026 Minjian Cai
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package apache.pulsar.map;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

@BenchmarkMode(Mode.Throughput)
@State(Scope.Benchmark)
@Warmup(iterations = 5, time = 1000, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(iterations = 10, time = 1000, timeUnit = TimeUnit.MILLISECONDS)
@Threads(8)
@Fork(1)
public class ConcurrentHashMapBenchMark {

  private static final Object VALUE = new Object();

  @Param({"1024"})
  private int initialCapacity;

  @Param({"4", "16"})
  private int concurrencyLevel;

  @Param({"1000", "10000", "100000"})
  private int initialSize;

  @Param({"ConcurrentHashMap"})
  private String type;

  private ConcurrentHashMap<Long, Object> map;

  private AtomicLong keyGenerator;

  @Setup(Level.Iteration)
  public void setup() {
    switch (type) {
      case "ConcurrentHashMap":
        map = new ConcurrentHashMap<>(initialCapacity, 0.75f, concurrencyLevel);
        break;
    }

    // Pre-populate maps with data
    for (int i = 0; i < initialSize; i++) {
      long key = ThreadLocalRandom.current().nextLong();
      map.put(key, VALUE);
    }

    keyGenerator = new AtomicLong(initialSize);
  }

  @TearDown(Level.Iteration)
  public void tearDown() {
    map.clear();
  }

  @Benchmark
  public void getPresent(Blackhole blackhole) {
    long key = ThreadLocalRandom.current().nextLong(Math.max(keyGenerator.get(), 1));
    blackhole.consume(map.get(key));
  }

  @Benchmark
  public void getAbsent(Blackhole blackhole) {
    long key =
        ThreadLocalRandom.current().nextLong(Math.max(keyGenerator.get(), 1)) + keyGenerator.get();
    blackhole.consume(map.get(key));
  }

  @Benchmark
  public void putAbsent() {
    long key = keyGenerator.getAndIncrement();
    map.put(key, VALUE);
  }

  @Benchmark
  public void putPresent() {
    long key = ThreadLocalRandom.current().nextLong(Math.max(keyGenerator.get(), 1));
    map.put(key, VALUE);
  }

  @Benchmark
  public void removePresent() {
    long key = ThreadLocalRandom.current().nextLong(Math.max(keyGenerator.get(), 1));
    map.remove(key);
  }

  @Benchmark
  public void removeAbsent() {
    long key =
        ThreadLocalRandom.current().nextLong(Math.max(keyGenerator.get(), 1)) + keyGenerator.get();
    map.remove(key);
  }

  @Benchmark
  @Threads(8)
  public void testMixedOperations(Blackhole blackhole) {
    long key = keyGenerator.getAndIncrement();
    long operationType = key % 5; // 0-4
    if (operationType == 0) {
      // 20% write
      map.put(key, VALUE);
    } else {
      // 80% read
      Object value = map.get(key);
      blackhole.consume(value);
    }
  }
}
