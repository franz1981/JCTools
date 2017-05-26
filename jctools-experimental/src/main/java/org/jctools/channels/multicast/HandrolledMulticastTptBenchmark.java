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

package org.jctools.channels.multicast;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Stream;

import org.jctools.util.UnsafeAccess;

public class HandrolledMulticastTptBenchmark {

   public static void main(String[] args) throws InterruptedException {
      final boolean zeroCopy = false;
      final boolean useRaw = false;
      final int messages = 100_000_000;
      final int tests = 10;
      final int readers = 1;
      final int messageSize = 8;
      final int capacity = 1 << 17;
      final int requiredSize = ChannelLayout.getRequiredBufferSize(capacity, messageSize);
      final ByteBuffer byteBuffer = ByteBuffer.allocateDirect(requiredSize);
      byteBuffer.order(ByteOrder.nativeOrder());
      final Thread[] readerTasks = new Thread[readers];
      final CountDownLatch readerStarted = new CountDownLatch(readers);
      for (int i = 0; i < readers; i++) {
         readerTasks[i] = new Thread(() -> {
            long maxBatch = 0;
            long readMessages = 0;
            final long lostMessages;
            long batch = 0;
            final OffHeapFixedMessageSizeTailer reader = new OffHeapFixedMessageSizeTailer(byteBuffer, capacity, messageSize);
            readerStarted.countDown();
            while (!Thread.currentThread().interrupted()) {
               if (zeroCopy && !useRaw) {
                  long listenedSequence;
                  while ((listenedSequence = reader.readNext()) >= 0) {
                     final long value = UnsafeAccess.UNSAFE.getLong(reader.elementAddressOf(listenedSequence));
                     if (reader.validateRawAcquire(listenedSequence)) {
                        readMessages++;
                        batch++;
                     }
                  }
                  maxBatch = Math.max(batch, maxBatch);
                  batch = 0;
               } else if (zeroCopy && useRaw) {
                  long listenedSequence;
                  while ((listenedSequence = reader.readRawAcquire()) >= 0) {
                     final long value = UnsafeAccess.UNSAFE.getLong(reader.elementAddressOf(listenedSequence));
                     if (reader.validateRawAcquire(listenedSequence)) {
                        readMessages++;
                        batch++;
                        //release only if is valid!
                        reader.readRelease(listenedSequence);
                     }
                  }
                  maxBatch = Math.max(batch, maxBatch);
                  batch = 0;
               } else {
                  int read;
                  if ((read = reader.read(addr -> {
                     final long value = UnsafeAccess.UNSAFE.getLong(addr);
                  }, Integer.MAX_VALUE)) > 0) {
                     readMessages += read;
                     batch += read;
                  }
                  maxBatch = Math.max(batch, maxBatch);
                  batch = 0;
               }
            }
            lostMessages = reader.lost();

            final long expectedTotal = (long) messages * tests;
            final long perceivedTotal = readMessages + lostMessages;
            if (expectedTotal != perceivedTotal) {
               System.err.println("expected total = " + expectedTotal + " perceived total = " + perceivedTotal + " mismatch!!!");
            }
            System.out.println(Thread.currentThread() + " read = " + readMessages + "/" + expectedTotal);
            System.out.println("max batch size = " + maxBatch);
         });
         readerTasks[i].start();
      }
      readerStarted.await();
      Thread.sleep(2000);
      final OffHeapFixedMessageSizeAppender sampleWriter = new OffHeapFixedMessageSizeAppender(byteBuffer, capacity, messageSize);
      for (int t = 0; t < tests; t++) {
         final long start = System.nanoTime();
         for (int i = 0; i < messages; i++) {
            final long sequence = sampleWriter.writeAcquire();
            UnsafeAccess.UNSAFE.putLong(sampleWriter.messageOffset(sequence), 1);
            sampleWriter.writeRelease(sequence);
         }
         final long elapsed = System.nanoTime() - start;
         System.out.println(((messages * 1000_000_000L) / 1000_000L) / elapsed + "M msg/sec");
      }
      Stream.of(readerTasks).forEach(Thread::interrupt);
   }

}
