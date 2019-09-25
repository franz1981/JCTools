package org.jctools.jmh.latency;

import org.jctools.queues.FAAArrayQueue;
import org.openjdk.jmh.annotations.*;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 10, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 10, time = 1, timeUnit = TimeUnit.SECONDS)
@SuppressWarnings("serial")
public class FAABurstCost
{
    static final Go GO = new Go();
    @Param( {"SpscArrayQueue", "MpscArrayQueue", "SpmcArrayQueue", "MpmcArrayQueue"})
    int burstSize;

    int consumerCount = 1;
    FAAArrayQueue<AbstractEvent> q;
    private Thread[] consumerThreads;
    private Consumer consumer;

    @Setup(Level.Trial)
    public void setupQueueAndConsumers()
    {
        q = new FAAArrayQueue<AbstractEvent>(128 * 1024);
        consumer = new Consumer(q);
        consumerThreads = new Thread[consumerCount];
        for (int i = 0; i < consumerCount; i++)
        {
            consumerThreads[i] = new Thread(consumer);
            consumerThreads[i].start();
        }
    }

    @Benchmark
    public void burstCost(Stop stop)
    {
        final int burst = burstSize;
        final FAAArrayQueue<AbstractEvent> q = this.q;
        final Go go = GO;
        stop.lazySet(false);
        for (int i = 0; i < burst - 1; i++)
        {
            while (!q.enqueue(go))
            {
                ;
            }
        }

        while (!q.enqueue(stop))
        {
            ;
        }

        while (!stop.get())
        {
            ;
        }
    }

    @TearDown
    public void killConsumer() throws InterruptedException
    {
        consumer.isRunning = false;
        for (int i = 0; i < consumerCount; i++)
        {
            consumerThreads[i].join();
        }
    }

    abstract static class AbstractEvent extends AtomicBoolean
    {
        abstract void handle();
    }

    static class Go extends AbstractEvent
    {
        @Override
        void handle()
        {
            // do nothing
        }
    }

    @State(Scope.Thread)
    public static class Stop extends AbstractEvent
    {
        @Override
        void handle()
        {
            lazySet(true);
        }
    }

    static class Consumer implements Runnable
    {
        final FAAArrayQueue<AbstractEvent> q;
        volatile boolean isRunning = true;

        public Consumer(FAAArrayQueue<AbstractEvent> q)
        {
            this.q = q;
        }

        @Override
        public void run()
        {
            final FAAArrayQueue<AbstractEvent> q = this.q;
            while (isRunning)
            {
                AbstractEvent e = null;
                if ((e = q.dequeue()) == null)
                {
                    continue;
                }
                e.handle();
            }
        }

    }
}
