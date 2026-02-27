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
import org.apache.pulsar.common.util.collections.ConcurrentLongHashMap;
import org.jctools.maps.NonBlockingHashMapLong;
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
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.results.format.ResultFormatType;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

@BenchmarkMode(Mode.Throughput)
@State(Scope.Benchmark)
@Warmup(iterations = 2, time = 1000, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(iterations = 6, time = 1000, timeUnit = TimeUnit.MILLISECONDS)
@Threads(8)
@Fork(1)
public class ConcurrentHashMapBenchMark {

  private static final Object VALUE = new Object();

  private int initialCapacity = 1024;

  private int concurrencyLevel = 16;

  @Param({"1000", "10000", "100000"})
  private int initialSize;

  @Param({
    "ConcurrentHashMap",
    "NonBlockingHashMapLong",
    "ConcurrentLongHashMap",
    "TrialConcurrentLongHashMap"
  })
  private String type;

  private MapAccessor mapAccessor;

  private AtomicLong keyGenerator;

  @Setup(Level.Iteration)
  public void setup() {
    mapAccessor = createMapAccessor();

    // Pre-populate maps with data
    for (int i = 0; i < initialSize; i++) {
      long key = ThreadLocalRandom.current().nextLong();
      mapAccessor.put(key, VALUE);
    }

    keyGenerator = new AtomicLong(initialSize);
  }

  @Benchmark
  public void getPresent(Blackhole blackhole) {
    long key = ThreadLocalRandom.current().nextLong(Math.max(keyGenerator.get(), 1));
    blackhole.consume(mapAccessor.get(key));
  }

  @Benchmark
  public void getAbsent(Blackhole blackhole) {
    long key =
        ThreadLocalRandom.current().nextLong(Math.max(keyGenerator.get(), 1)) + keyGenerator.get();
    blackhole.consume(mapAccessor.get(key));
  }

  @Benchmark
  public void putAbsent() {
    long key = keyGenerator.getAndIncrement();
    mapAccessor.put(key, VALUE);
  }

  @Benchmark
  public void putPresent() {
    long key = ThreadLocalRandom.current().nextLong(Math.max(keyGenerator.get(), 1));
    mapAccessor.put(key, VALUE);
  }

  @Benchmark
  public void removePresent() {
    long key = ThreadLocalRandom.current().nextLong(Math.max(keyGenerator.get(), 1));
    mapAccessor.remove(key);
  }

  @Benchmark
  public void removeAbsent() {
    long key =
        ThreadLocalRandom.current().nextLong(Math.max(keyGenerator.get(), 1)) + keyGenerator.get();
    mapAccessor.remove(key);
  }

  @Benchmark
  @Threads(8)
  public void testMixedOperations(Blackhole blackhole) {
    long key = keyGenerator.getAndIncrement();
    long operationType = key % 5; // 0-4
    if (operationType == 0) {
      // 20% write
      mapAccessor.put(key, VALUE);
    } else {
      // 80% read
      Object value = mapAccessor.get(key);
      blackhole.consume(value);
    }
  }

  private MapAccessor createMapAccessor() {
    return switch (type) {
      case "ConcurrentHashMap" -> {
        var concurrentHashMap = new ConcurrentHashMap<>(initialCapacity, 0.75f, concurrencyLevel);
        yield new MapAccessor() {
          public void put(long key, Object v) {
            concurrentHashMap.put(key, v);
          }

          public Object get(long key) {
            return concurrentHashMap.get(key);
          }

          public void remove(long key) {
            concurrentHashMap.remove(key);
          }
        };
      }

      case "NonBlockingHashMapLong" -> {
        var nonBlockingHashMapLong = new NonBlockingHashMapLong<>(initialCapacity);
        yield new MapAccessor() {
          public void put(long key, Object v) {
            nonBlockingHashMapLong.put(key, v);
          }

          public Object get(long key) {
            return nonBlockingHashMapLong.get(key);
          }

          public void remove(long key) {
            nonBlockingHashMapLong.remove(key);
          }
        };
      }
      case "ConcurrentLongHashMap" -> {
        var concurrentLongHashMap =
            new ConcurrentLongHashMap.Builder<>()
                .expectedItems(initialCapacity)
                .concurrencyLevel(concurrencyLevel)
                .expandFactor(2)
                .mapFillFactor(0.66f)
                .mapIdleFactor(0.25f)
                .shrinkFactor(2.0f)
                .autoShrink(true)
                .build();
        yield new MapAccessor() {
          public void put(long key, Object v) {
            concurrentLongHashMap.put(key, v);
          }

          public Object get(long key) {
            return concurrentLongHashMap.get(key);
          }

          public void remove(long key) {
            concurrentLongHashMap.remove(key);
          }
        };
      }
      case "TrialConcurrentLongHashMap" -> {
        var trialConcurrentLongHashMap =
            new TrialConcurrentLongHashMap.Builder<>()
                .expectedItems(initialCapacity)
                .concurrencyLevel(concurrencyLevel)
                .expandFactor(2)
                .mapFillFactor(0.66f)
                .mapIdleFactor(0.25f)
                .shrinkFactor(2.0f)
                .autoShrink(true)
                .build();
        yield new MapAccessor() {
          public void put(long key, Object v) {
            trialConcurrentLongHashMap.put(key, v);
          }

          public Object get(long key) {
            return trialConcurrentLongHashMap.get(key);
          }

          public void remove(long key) {
            trialConcurrentLongHashMap.remove(key);
          }
        };
      }
      default -> throw new IllegalArgumentException("Invalid map type: " + type);
    };
  }

  public static void main(String[] args) throws RunnerException {
    Options opt =
        new OptionsBuilder()
            .addProfiler("gc")
            .include(ConcurrentHashMapBenchMark.class.getSimpleName())
            .shouldFailOnError(true)
            .result("benchmark-concurrent-hashmap.json")
            .resultFormat(ResultFormatType.JSON)
            .build();
    new Runner(opt).run();
  }
}
