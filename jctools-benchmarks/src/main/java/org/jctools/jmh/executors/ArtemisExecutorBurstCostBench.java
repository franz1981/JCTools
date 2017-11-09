/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.jctools.jmh.executors;

import org.jctools.queues.MpscUnboundedArrayQueue;
import org.jctools.queues.SpscArrayQueue;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.Arrays;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

@State(Scope.Benchmark)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
public class ArtemisExecutorBurstCostBench
{

    public static final int DUMMY_OPERATION_CONSUME = 1;
    public static final int MAX_THREAD_COUNT = 4;
    public static final int EXECUTOR_QUEUE_CAPACITY = 64 * 1024;
    public static final int RESPONSE_QUEUE_CAPACITY = 128;


    @State(Scope.Benchmark)
    public static class SharedState
    {
        @Param( {"1", "100"})
        int burstLength;

        @Param( {"linked", "chunked"})
        String qType;

        @Param( {"blocking", "lock-free"})
        String executorType;

        final AtomicBoolean running = new AtomicBoolean(true);
        final AtomicInteger threadId = new AtomicInteger();
        //use a mpsc chunked queue
        ArtemisExecutor artemisExecutor;
        Queue<Runnable> executorQueue;

        @SuppressWarnings("unchecked")
        final Queue<Integer>[] responseQueues = new SpscArrayQueue[MAX_THREAD_COUNT];

        Thread consumerThread;

        @Setup
        public synchronized void setup()
        {
            switch (executorType)
            {
                case "lock-free":
                    executorQueue = new MpscUnboundedArrayQueue<>(EXECUTOR_QUEUE_CAPACITY);
                    break;
                case "blocking":
                    executorQueue = new LinkedBlockingQueue<>();
                    break;
            }


            Executor executor = executorQueue::offer;
            Queue<Runnable> tasks = null;

            switch (qType)
            {
                case "linked":
                    tasks = new ConcurrentLinkedQueue<>();
                    break;
                case "chunked":
                    tasks = new MpscUnboundedArrayQueue<>(Math.max(128, burstLength * MAX_THREAD_COUNT));
                    break;
            }

            artemisExecutor = new OrderedExecutor(tasks, executor);

            for (int i = 0; i < MAX_THREAD_COUNT; i++)
            {
                responseQueues[i] = new SpscArrayQueue<>(RESPONSE_QUEUE_CAPACITY);
            }

            consumerThread = new Thread(
                () ->
                {
                    while (running.get())
                    {
                        Runnable task;
                        while ((task = executorQueue.poll()) != null)
                        {
                            task.run();
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
        Runnable[] tasks;
        ArtemisExecutor sendExecutor;
        Queue<Integer> responseQueue;
        SharedState sharedState;

        @Setup
        public void setup(final SharedState sharedState)
        {
            this.sharedState = sharedState;
            id = sharedState.threadId.getAndIncrement();
            final Integer value = id;
            tasks = new Runnable[sharedState.burstLength];
            //stuff to be done
            Arrays.fill(tasks, (Runnable) () ->
            {
                Blackhole.consumeCPU(DUMMY_OPERATION_CONSUME);
            });
            final Queue<Integer> responseQ = sharedState.responseQueues[id];
            //specific for the producer thread to close the burst
            tasks[tasks.length - 1] = () ->
            {
                //dummy operation to emulate something to do
                Blackhole.consumeCPU(DUMMY_OPERATION_CONSUME);

                if (!responseQ.offer(value))
                {
                    System.err.println("ERROR!!!!");
                    throw new RuntimeException("CAN'T HAPPEN!");
                }
            };
            sendExecutor = sharedState.artemisExecutor;
            responseQueue = responseQ;
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
        final Executor sendExecutor = state.sendExecutor;

        for (final Runnable value : state.tasks)
        {
            sendExecutor.execute(value);
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
            .include(ArtemisExecutorBurstCostBench.class.getSimpleName())
            .warmupIterations(5)
            .measurementIterations(5)
            .forks(1)
            .build();
        new Runner(opt).run();
    }

}
