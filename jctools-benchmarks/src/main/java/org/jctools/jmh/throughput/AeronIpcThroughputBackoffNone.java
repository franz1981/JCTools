/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.jctools.jmh.throughput;

import io.aeron.Aeron;
import io.aeron.CommonContext;
import io.aeron.Publication;
import io.aeron.Subscription;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import io.aeron.logbuffer.FragmentHandler;
import org.agrona.BitUtil;
import org.agrona.BufferUtil;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.BusySpinIdleStrategy;
import org.agrona.concurrent.UnsafeBuffer;
import org.jctools.util.PortableJvmInfo;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;

@State(Scope.Group)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 10, time = 1)
@Measurement(iterations = 10, time = 1)
public class AeronIpcThroughputBackoffNone {

   public static final int STREAM_ID = 1;
   public static final int FRAGMENT_LIMIT = 128;
   static final long DELAY_PRODUCER = Long.getLong("delay.p", 0L);
   static final long DELAY_CONSUMER = Long.getLong("delay.c", 0L);
   FragmentHandler onFragment;
   MediaDriver.Context ctx;
   MediaDriver mediaDriver;
   Aeron aeron;
   Publication publication;
   Subscription subscription;

   @Param(value = {"0","8"})
   int msgSize;

   @Param(value = {"131072"})
   int capacity;

   DirectBuffer msg;

   @Setup()
   public void init(Blackhole msgConsumer) {
      System.setProperty("agrona.disable.bounds.checks","true");
      onFragment = (buffer, offset, length, header) -> {
         msgConsumer.consume(buffer);
      };
      //not completly fair: the term length will receive the headers too: would be better to include them while calculating the term length
      final int ipcTermLength = BitUtil.findNextPositivePowerOfTwo(msgSize > 0 ? (capacity * msgSize) : capacity);
      ctx = new MediaDriver.Context()
          .termBufferSparseFile(false)
          .ipcTermBufferLength(ipcTermLength)
          .threadingMode(ThreadingMode.SHARED)
          .sharedIdleStrategy(new BusySpinIdleStrategy())
          .dirDeleteOnStart(true);
      mediaDriver = MediaDriver.launch(ctx);
      aeron = Aeron.connect();
      publication = aeron.addPublication(CommonContext.IPC_CHANNEL, STREAM_ID);
      subscription = aeron.addSubscription(CommonContext.IPC_CHANNEL, STREAM_ID);
      final ByteBuffer msgContent = BufferUtil.allocateDirectAligned(
          BitUtil.align(msgSize, PortableJvmInfo.CACHE_LINE_SIZE * 2),
          PortableJvmInfo.CACHE_LINE_SIZE * 2);
      final UnsafeBuffer unsafeBuffer = new UnsafeBuffer(msgContent);
      msg = unsafeBuffer;
   }

   @AuxCounters
   @State(Scope.Thread)
   public static class PollCounters {

      public int pollsFailed;
      public int pollsMade;

      @Setup(Level.Iteration)
      public void clean() {
         pollsFailed = pollsMade = 0;
      }
   }

   @AuxCounters
   @State(Scope.Thread)
   public static class OfferCounters {

      public int offersFailed;
      public int offersMade;

      @Setup(Level.Iteration)
      public void clean() {
         offersFailed = offersMade = 0;
      }
   }

   @Benchmark
   @Group("tpt")
   @GroupThreads(2)
   public void offer(OfferCounters counters)
   {
      if (publication.offer(msg, 0, msgSize) < 0)
      {
         counters.offersFailed++;
         backoff();
      }
      else
      {
         counters.offersMade++;
      }
      if (DELAY_PRODUCER != 0)
      {
         Blackhole.consumeCPU(DELAY_PRODUCER);
      }
   }

   protected void backoff() {
   }

   @Benchmark
   @Group("tpt")
   public void poll(PollCounters counters) {
      final int elements = subscription.poll(onFragment, FRAGMENT_LIMIT);
      if (elements == 0) {
         counters.pollsFailed++;
         backoff();
      } else {
         counters.pollsMade += elements;
      }
      if (DELAY_CONSUMER != 0) {
         Blackhole.consumeCPU(DELAY_CONSUMER);
      }
   }

   @TearDown(Level.Iteration)
   public void emptyQ() {
      synchronized (subscription) {
         while (subscription.poll((buffer, offset, length, header) -> {
         }, Integer.MAX_VALUE) > 0) {

         }
      }
   }

   public static void main(String[] args) throws RunnerException {
      final Options opt = new OptionsBuilder()
          .include(AeronIpcThroughputBackoffNone.class.getSimpleName())
          .jvmArgs("-Dagrona.disable.bounds.checks=true")
          .warmupIterations(5)
          .measurementIterations(5)
          .forks(1).build();
      new Runner(opt).run();
   }
}
