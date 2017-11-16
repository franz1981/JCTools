/*
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
package org.jctools.jmh.throughput;

import org.jctools.queues.MessagePassingQueue;
import org.jctools.queues.MessagePassingQueue.Consumer;
import org.jctools.queues.QueueByTypeFactory;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.concurrent.TimeUnit;

@State(Scope.Group)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 10, time = 1)
@Measurement(iterations = 10, time = 1)
public class MessagePassingQueueThroughputBackoffNone
{
    static final long DELAY_PRODUCER = Long.getLong("delay.p", 0L);
    static final long DELAY_CONSUMER = Long.getLong("delay.c", 0L);
    public static final int DRAIN_LIMIT = 128;
    static final Integer TEST_ELEMENT = 1;
    Integer element = TEST_ELEMENT;
    Consumer<Integer> onDrain;
    Integer escape;
    MessagePassingQueue<Integer> q;

    @Param(value = {"MpscRelaxedArrayQueue", "MpscArrayQueue"})
    String qType;

    @Param(value = {"131072"})
    String qCapacity;

    @Setup()
    public void createQandPrimeCompilation(Blackhole msgConsumer)
    {
        final String qType = this.qType;
        //it is calling the object version one
        onDrain = msgConsumer::consume;
        q = (MessagePassingQueue) QueueByTypeFactory.createQueue(qType, 128 * 1024);
        // stretch the queue to the limit, working through resizing and full
        for (int i = 0; i < 128 + 100; i++)
        {
            q.relaxedOffer(element);
        }
        for (int i = 0; i < 128 + 100; i++)
        {
            q.relaxedPoll();

        }
        // make sure the important common case is exercised
        for (int i = 0; i < 20000; i++)
        {
            q.relaxedOffer(element);
            q.relaxedPoll();
        }
        final String qCapacity = this.qCapacity;

        this.q = (MessagePassingQueue) QueueByTypeFactory.buildQ(qType, qCapacity);
    }

    @AuxCounters
    @State(Scope.Thread)
    public static class PollCounters
    {
        public int pollsFailed;
        public int pollsMade;

        @Setup(Level.Iteration)
        public void clean()
        {
            pollsFailed = pollsMade = 0;
        }
    }

    @AuxCounters
    @State(Scope.Thread)
    public static class OfferCounters
    {
        public int offersFailed;
        public int offersMade;

        @Setup(Level.Iteration)
        public void clean()
        {
            offersFailed = offersMade = 0;
        }
    }

    @Benchmark
    @Group("tpt")
    @GroupThreads(1)
    public void offer(OfferCounters counters)
    {
        if (!q.relaxedOffer(element))
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

    protected void backoff()
    {
    }

    @Benchmark
    @Group("tpt")
    public void poll(PollCounters counters)
    {
        final int consumed = q.drain(onDrain, DRAIN_LIMIT);
        if (consumed == 0)
        {
            counters.pollsFailed++;
            backoff();
        }
        else
        {
            counters.pollsMade += consumed;
        }
        if (DELAY_CONSUMER != 0)
        {
            Blackhole.consumeCPU(DELAY_CONSUMER);
        }
    }

    @TearDown(Level.Iteration)
    public void emptyQ()
    {
        synchronized (q)
        {
            q.clear();
        }
    }

    public static void main(String[] args) throws RunnerException
    {
        final Options opt = new OptionsBuilder()
            .include(MessagePassingQueueThroughputBackoffNone.class.getSimpleName())
            .warmupIterations(5)
            .measurementIterations(5)
            .forks(1)
            .build();
        new Runner(opt).run();
    }
}
