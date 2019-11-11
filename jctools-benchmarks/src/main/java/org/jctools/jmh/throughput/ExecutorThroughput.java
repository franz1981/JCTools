package org.jctools.jmh.throughput;

import org.jctools.queues.MpmcUnboundedXaddArrayQueue;
import org.jctools.queues.blocking.BlockingQueueFactory;
import org.jctools.queues.blocking.McParkTakeStrategy;
import org.jctools.queues.blocking.YieldPutStrategy;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.BenchmarkParams;
import org.openjdk.jmh.infra.Blackhole;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
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
    AtomicLong[] offers;
    AtomicLong[] polls;
    Runnable offerTask;
    long startIter;
    AtomicLong producerId;
    AtomicLong consumerId;
    ThreadLocal<AtomicLong> tlCounters;

    @Setup
    public void createExecutor(BenchmarkParams params)
    {
        consumerId = new AtomicLong();
        producerId = new AtomicLong();
        polls = new AtomicLong[consumers];
        for (int i = 0; i < polls.length; i++)
        {
            polls[i] = new AtomicLong();
        }
        tlCounters = ThreadLocal.withInitial(() -> {
            final int id = (int) consumerId.getAndIncrement();
            final AtomicLong poll = polls[id];
            if (poll.get() != 0)
            {
                throw new IllegalStateException("we have a problem!");
            }
            return poll;
        });
        offers = new AtomicLong[params.getThreadGroups()[0]];
        for (int i = 0; i < offers.length; i++)
        {
            offers[i] = new AtomicLong();
        }
        final ThreadLocal<AtomicLong> counters = this.tlCounters;
        offerTask = () -> {
            final Thread t = Thread.currentThread();
            final AtomicLong counter = counters.get();
            counter.lazySet(counter.get() + 1);
            if (DELAY_CONSUMER != 0)
            {
                Blackhole.consumeCPU(DELAY_CONSUMER);
            }
        };
        switch (eType)
        {
            case "XADD":
                final BlockingQueue<Runnable> blockingQueue =
                    BlockingQueueFactory.getBlockingQueueFrom(MpmcUnboundedXaddArrayQueue.class,
                        McParkTakeStrategy.class, YieldPutStrategy.class, Integer.parseInt(initialCapacity));
                executorService = new ThreadPoolExecutor(consumers, consumers, 1, TimeUnit.DAYS, blockingQueue);
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

    @State(Scope.Thread)
    public static class OfferId
    {
        int offerId;

        @Setup(Level.Iteration)
        public void init(ExecutorThroughput tpt)
        {
            offerId = (int) (tpt.producerId.getAndIncrement() % tpt.offers.length);
        }
    }

    @Setup(Level.Iteration)
    public void resetCounters()
    {
        for (AtomicLong o : polls)
        {
            o.set(0);
        }
        for (AtomicLong o : offers)
        {
            o.set(0);
        }
        startIter = System.nanoTime();
    }

    @Benchmark
    @Group("tpt")
    public void offer(OfferId offerId)
    {
        final AtomicLong offered = offers[offerId.offerId];
        offered.lazySet(offered.get() + 1);
        executorService.execute(offerTask);
        if (DELAY_PRODUCER != 0)
        {
            Blackhole.consumeCPU(DELAY_PRODUCER);
        }
    }

    @State(Scope.Thread)
    @AuxCounters
    public static class ExecutorCounters
    {
        public long polls;
    }

    @Benchmark
    @Group("tpt")
    public void pollStats(ExecutorCounters counter)
    {
        //NOTE: pollStats won't be called anymore while emptyExecutor is being called
        counter.polls = sum(polls);
        LockSupport.parkNanos(REFRESH_STATS_NS);
    }

    private static long sum(AtomicLong[] longs)
    {
        long total = 0;
        for (AtomicLong l : longs)
        {
            total += l.get();
        }
        return total;
    }

    @TearDown(Level.Iteration)
    public void emptyExecutor(BenchmarkParams params)
    {
        long total;
        while ((total = sum(polls)) != sum(offers))
        {
            Thread.yield();
        }
        /*
        final long elapsedNanos = System.nanoTime() - startIter;
        final TimeUnit timeUnit = params.getTimeUnit();
        final double tpt = total * ((double) TimeUnit.NANOSECONDS.convert(1, timeUnit) / (double) elapsedNanos);
        System.out.println(
            "End2End Throughput: " + String.format("%.3f", tpt) + " ops/" + timeUnit.toString().toLowerCase());
         */
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

