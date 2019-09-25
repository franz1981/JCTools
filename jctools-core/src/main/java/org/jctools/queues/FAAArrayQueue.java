package org.jctools.queues;

import java.lang.reflect.Field;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReferenceArray;

public class FAAArrayQueue<E>
{

    static class Node<E>
    {
        final AtomicInteger deqidx = new AtomicInteger(0);
        final AtomicReferenceArray<E> items;
        final AtomicInteger enqidx = new AtomicInteger(1);
        volatile Node<E> next = null;

        // Start with the first entry pre-filled and enqidx at 1
        Node(final E item, int size)
        {
            items = new AtomicReferenceArray<E>(size);
            items.lazySet(0, item);
        }

        /**
         * @param cmp Previous {@code next}
         * @param val New {@code next}
         * @return {@code true} if CAS was successful
         */
        boolean casNext(Node<E> cmp, Node<E> val)
        {
            return UNSAFE.compareAndSwapObject(this, nextOffset, cmp, val);
        }

        // Unsafe mechanics
        private static final sun.misc.Unsafe UNSAFE;
        private static final long nextOffset;

        static
        {
            try
            {
                Field f = sun.misc.Unsafe.class.getDeclaredField("theUnsafe");
                f.setAccessible(true);
                UNSAFE = (sun.misc.Unsafe) f.get(null);
                nextOffset = UNSAFE.objectFieldOffset(Node.class.getDeclaredField("next"));
            }
            catch (Exception e)
            {
                throw new Error(e);
            }
        }
    }

    @sun.misc.Contended
    private volatile Node<E> head;
    @sun.misc.Contended
    private volatile Node<E> tail;
    private final int size;

    final E taken = (E) new Object(); // Muuuahahah !


    public FAAArrayQueue(int size)
    {
        final Node<E> sentinelNode = new Node<E>(null, size);
        sentinelNode.enqidx.set(0);
        head = sentinelNode;
        tail = sentinelNode;
        this.size = size;
    }


    /**
     * Progress Condition: Lock-Free
     *
     * @param item must not be null
     */
    public boolean enqueue(E item)
    {
        if (item == null)
        {
            throw new NullPointerException();
        }
        while (true)
        {
            final Node<E> ltail = tail;
            final int idx = ltail.enqidx.getAndIncrement();
            if (idx > size - 1)
            { // This node is full
                if (ltail != tail)
                {
                    continue;
                }
                final Node<E> lnext = ltail.next;
                if (lnext == null)
                {
                    final Node<E> newNode = new Node<E>(item, size);
                    if (ltail.casNext(null, newNode))
                    {
                        casTail(ltail, newNode);
                        return true;
                    }
                }
                else
                {
                    casTail(ltail, lnext);
                }
                continue;
            }
            if (ltail.items.compareAndSet(idx, null, item))
            {
                return true;
            }
        }
    }


    /**
     * Progress condition: lock-free
     */
    public E dequeue()
    {
        while (true)
        {
            Node<E> lhead = head;
            if (lhead.deqidx.get() >= lhead.enqidx.get() && lhead.next == null)
            {
                return null;
            }
            final int idx = lhead.deqidx.getAndIncrement();
            if (idx > size - 1)
            { // This node has been drained, check if there is another one
                if (lhead.next == null)
                {
                    return null;  // No more nodes in the queue
                }
                casHead(lhead, lhead.next);
                continue;
            }
            final E item = lhead.items.getAndSet(idx, taken); // We can use a CAS instead
            if (item != null)
            {
                return item;
            }
        }
    }


    private boolean casTail(Node<E> cmp, Node<E> val)
    {
        return UNSAFE.compareAndSwapObject(this, tailOffset, cmp, val);
    }

    private boolean casHead(Node<E> cmp, Node<E> val)
    {
        return UNSAFE.compareAndSwapObject(this, headOffset, cmp, val);
    }

    // Unsafe mechanics
    private static final sun.misc.Unsafe UNSAFE;
    private static final long tailOffset;
    private static final long headOffset;

    static
    {
        try
        {
            Field f = sun.misc.Unsafe.class.getDeclaredField("theUnsafe");
            f.setAccessible(true);
            UNSAFE = (sun.misc.Unsafe) f.get(null);
            tailOffset = UNSAFE.objectFieldOffset(FAAArrayQueue.class.getDeclaredField("tail"));
            headOffset = UNSAFE.objectFieldOffset(FAAArrayQueue.class.getDeclaredField("head"));
        }
        catch (Exception e)
        {
            throw new Error(e);
        }
    }
}
