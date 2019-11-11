package org.jctools.jmh.throughput;

import org.jctools.queues.MpmcUnboundedXaddArrayQueue;
import org.jctools.queues.blocking.BlockingQueueFactory;
import org.jctools.queues.blocking.McParkTakeStrategy;
import org.jctools.queues.blocking.YieldPutStrategy;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.BenchmarkParams;
import org.openjdk.jmh.infra.Blackhole;

import java.util.concurrent.*;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.LockSupport;

@State(Scope.Group)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 10, time = 1)
@Measurement(iterations = 10, time = 1)
@Fork(2)
public class ExecutorThroughput
{

    static final long DELAY_PRODUCER = Long.getLong("delay.p", 0L);
    static final long DELAY_CONSUMER = Long.getLong("delay.c", 0L);
    //no need to busy spin while collecting stats
    static final long REFRESH_STATS_NS = Long.getLong("stats.ns", 10L);

    @Param(value = {"LTQ", "XADD", "FJ", "BLQ"})
    String eType;

    @Param(value = {"132000"})
    String initialCapacity;

    @Param(value = {"2"})
    int consumers;
    ExecutorService executorService;
    //these are quite good to scale, but are slow to be read, that's ok for this test :)
    LongAdder offers = new LongAdder();
    LongAdder polls = new LongAdder();
    Runnable offerTask;

    @Setup
    public void createExecutor(BenchmarkParams params)
    {
        LongAdder polls = new LongAdder();
        offerTask = () -> polls.increment();
        this.polls = polls;
        this.offers = new LongAdder();
        switch (eType)
        {
            case "XADD":
                final BlockingQueue<Runnable> blockingQueue =
                    BlockingQueueFactory.getBlockingQueueFrom(MpmcUnboundedXaddArrayQueue.class,
                        McParkTakeStrategy.class, YieldPutStrategy.class, Integer.parseInt(initialCapacity));
                executorService = new ThreadPoolExecutor(
                    consumers,
                    consumers,
                    1,
                    TimeUnit.DAYS,
                    blockingQueue);
                break;
            case "LTQ":
                executorService =
                    new ThreadPoolExecutor(consumers, consumers, 1, TimeUnit.DAYS, new LinkedTransferQueue<>());
                break;
            case "BLQ":
                executorService =
                    new ThreadPoolExecutor(consumers, consumers, 1, TimeUnit.DAYS, new LinkedBlockingQueue<>());
                break;
            case "FJ":
                //TODO JDK 11 allows more fine tuning that cannot be ignored
                executorService = new ForkJoinPool(consumers);
                break;
            default:
                throw new IllegalStateException("Unexpected value: " + eType);
        }
    }

    @Setup(Level.Iteration)
    public void resetCounters()
    {
        polls.reset();
        offers.reset();
    }

    @State(Scope.Thread)
    @AuxCounters
    public static class ExecutorCounters
    {
        public long polls;
    }

    @Benchmark
    @Group("tpt")
    public void offer()
    {
        offers.increment();
        executorService.execute(offerTask);
        if (DELAY_PRODUCER != 0)
        {
            Blackhole.consumeCPU(DELAY_PRODUCER);
        }
    }

    @Benchmark
    @Group("tpt")
    public void pollStats(ExecutorCounters counter)
    {
        //NOTE: pollStats won't be called anymore while emptyExecutor is being called
        counter.polls = polls.sum();
        LockSupport.parkNanos(REFRESH_STATS_NS);
    }

    @TearDown(Level.Iteration)
    public void emptyExecutor()
    {
        while (polls.sum() != offers.sum())
        {
            Thread.yield();
        }
    }

    @TearDown(Level.Trial)
    public void shutdown()
    {
        executorService.shutdown();
        try
        {
            while (!executorService.awaitTermination(1, TimeUnit.MINUTES))
            {
                Thread.yield();
            }
        }
        catch (InterruptedException e)
        {
            e.printStackTrace();
        }
    }
}

