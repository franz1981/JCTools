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
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
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
    AtomicLong producerId;
    //it would be accessed from different threads, but not a big deal :)
    OfferCounters[] counters;

    @Setup
    public void createExecutor(BenchmarkParams params)
    {
        producerId = new AtomicLong();
        final int[] threadGroups = params.getThreadGroups();
        final int producers = threadGroups[0];
        //System.out.println("total producers = " + producers);
        counters = new OfferCounters[producers];
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
                    blockingQueue,
                    r -> {
                        final Thread t = new Thread(r);
                        t.setDaemon(true);
                        return t;
                    });
                break;
            case "LTQ":
                executorService =
                    new ThreadPoolExecutor(consumers, consumers, 1, TimeUnit.DAYS, new LinkedTransferQueue<>(), r -> {
                        final Thread t = new Thread(r);
                        t.setDaemon(true);
                        return t;
                    });
                break;
            case "BLQ":
                executorService =
                    new ThreadPoolExecutor(consumers, consumers, 1, TimeUnit.DAYS, new LinkedBlockingQueue<>(), r -> {
                        final Thread t = new Thread(r);
                        t.setDaemon(true);
                        return t;
                    });
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
    public static class OfferCounters
    {
        final AtomicLongFieldUpdater<OfferCounters> POLLS_UPDATER =
            AtomicLongFieldUpdater.newUpdater(OfferCounters.class, "polls");
        public volatile long polls;
        public int producerId;
        private Runnable execute;
        private OfferCounters[] counters;

        @Setup(Level.Iteration)
        public void initCounter(ExecutorThroughput cfg)
        {
            producerId = (int) (cfg.producerId.getAndIncrement() % cfg.counters.length);
            //System.out.println("Setting up " + producerId);
            cfg.counters[producerId] = this;
            counters = cfg.counters;
            polls = 0;
            execute = () -> {
                //different consumers could be hit this
                POLLS_UPDATER.getAndIncrement(OfferCounters.this);
                if (DELAY_CONSUMER != 0)
                {
                    Blackhole.consumeCPU(DELAY_CONSUMER);
                }
            };
        }

        @TearDown(Level.Iteration)
        public void closeCounter()
        {
            counters[producerId] = null;
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
    public void offer(OfferCounters counters)
    {
        executorService.execute(counters.execute);
        if (DELAY_PRODUCER != 0)
        {
            Blackhole.consumeCPU(DELAY_PRODUCER);
        }
    }

    @Benchmark
    @Group("tpt")
    public void pollStats(ExecutorCounters counter)
    {
        long total = 0;
        for (OfferCounters c : counters)
        {
            if (c != null)
            {
                total += c.polls;
            }
        }
        counter.polls = total;
        LockSupport.parkNanos(REFRESH_STATS_NS);
    }

    @TearDown(Level.Iteration)
    public void emptyExecutor()
    {
        try
        {
            executorService.submit(() -> {

            }).get(1, TimeUnit.MINUTES);
        }
        catch (InterruptedException e)
        {
            e.printStackTrace();
        }
        catch (ExecutionException e)
        {
            e.printStackTrace();
        }
        catch (TimeoutException e)
        {
            e.printStackTrace();
        }
    }
}

