/*
 * Copyright 2015-2017 Real Logic Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.jctools.jmh.executors;

import org.jctools.queues.SpscArrayQueue;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.Arrays;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;


@State(Scope.Benchmark)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
public class ArrayBlockingQueueBenchmark
{
    public static final int MAX_THREAD_COUNT = 4;
    public static final int SEND_QUEUE_CAPACITY = 64 * 1024;
    public static final int RESPONSE_QUEUE_CAPACITY = 128;

    @State(Scope.Benchmark)
    public static class SharedState
    {
        @Param( {"1", "100"})
        int burstLength;
        Integer[] values;

        final AtomicBoolean running = new AtomicBoolean(true);
        final AtomicInteger threadId = new AtomicInteger();
        final Queue<Integer> sendQueue = new ArrayBlockingQueue<>(SEND_QUEUE_CAPACITY);

        @SuppressWarnings("unchecked")
        final Queue<Integer>[] responseQueues = new SpscArrayQueue[MAX_THREAD_COUNT];

        Thread consumerThread;

        @Setup
        public synchronized void setup()
        {
            for (int i = 0; i < MAX_THREAD_COUNT; i++)
            {
                responseQueues[i] = new SpscArrayQueue<>(RESPONSE_QUEUE_CAPACITY);
            }

            values = new Integer[burstLength];
            for (int i = 0; i < burstLength; i++)
            {
                values[i] = -(burstLength - i);
            }
            //System.out.println(Arrays.toString(values));
            consumerThread = new Thread(
                () ->
                {
                    while (true)
                    {
                        final Integer value = sendQueue.poll();
                        if (null == value)
                        {
                            if (!running.get())
                            {
                                break;
                            }
                        }
                        else
                        {
                            final int intValue = value;
                            if (intValue >= 0)
                            {
                                final Queue<Integer> responseQueue = responseQueues[value];
                                while (!responseQueue.offer(value))
                                {
                                    // busy spin
                                }
                            }
                        }
                    }
                }
            );

            consumerThread.setName("consumer");
            consumerThread.start();
        }

        @TearDown
        public synchronized void tearDown() throws Exception
        {
            running.set(false);
            consumerThread.join();
        }
    }

    @State(Scope.Thread)
    public static class PerThreadState
    {
        int id;
        Integer[] values;
        Queue<Integer> sendQueue;
        Queue<Integer> responseQueue;

        @Setup
        public void setup(final SharedState sharedState)
        {
            id = sharedState.threadId.getAndIncrement();
            values = Arrays.copyOf(sharedState.values, sharedState.values.length);
            values[values.length - 1] = id;
            //System.out.println("["+id+"] -> " + Arrays.toString(values));
            sendQueue = sharedState.sendQueue;
            responseQueue = sharedState.responseQueues[id];
        }
    }

    @Benchmark
    @BenchmarkMode(Mode.SampleTime)
    @Threads(1)
    public Integer test1Producer(final PerThreadState state)
    {
        return sendBurst(state);
    }

    @Benchmark
    @BenchmarkMode(Mode.SampleTime)
    @Threads(2)
    public Integer test2Producers(final PerThreadState state)
    {
        return sendBurst(state);
    }

    @Benchmark
    @BenchmarkMode(Mode.SampleTime)
    @Threads(3)
    public Integer test3Producers(final PerThreadState state)
    {
        return sendBurst(state);
    }

    private Integer sendBurst(final PerThreadState state)
    {
        final Queue<Integer> sendQueue = state.sendQueue;

        for (final Integer value : state.values)
        {
            while (!sendQueue.offer(value))
            {
                // busy spin
            }
        }

        Integer value;
        do
        {
            value = state.responseQueue.poll();
        }
        while (null == value);

        return value;
    }

    public static void main(String[] args) throws RunnerException
    {
        Options opt = new OptionsBuilder()
            .shouldDoGC(true)
            .include(ArrayBlockingQueue.class.getSimpleName())
            .warmupIterations(5)
            .measurementIterations(5)
            .forks(1)
            .build();
        new Runner(opt).run();
    }
}
