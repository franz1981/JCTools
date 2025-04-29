package org.jctools.queues.varhandle;

import java.lang.invoke.VarHandle;
import org.jctools.queues.MessagePassingQueue;
import org.jctools.queues.QueueProgressIndicators;
import org.jctools.queues.IndexedQueueSizeUtil;
import org.jctools.queues.IndexedQueueSizeUtil.IndexedQueue;
import org.jctools.util.Pow2;
import org.jctools.util.SpscLookAheadUtil;

import java.util.AbstractQueue;
import java.util.Iterator;
import java.util.NoSuchElementException;

import static org.jctools.queues.varhandle.VarHandleQueueUtil.*;

abstract class SpscVarHandleArrayQueueColdField<E> extends AbstractQueue<E>
      implements MessagePassingQueue<E>, IndexedQueue, QueueProgressIndicators {

   protected final long mask;
   protected final E[] buffer;
   final int lookAheadStep;

   SpscVarHandleArrayQueueColdField(int capacity) {
      int actualCapacity = Pow2.roundToPowerOfTwo(capacity);
      mask = actualCapacity - 1;
      buffer = allocateRefArray(actualCapacity);
      lookAheadStep = SpscLookAheadUtil.computeLookAheadStep(actualCapacity);
   }

   @Override
   public int capacity() {
      return (int) (mask + 1);
   }

   @Override
   public int size() {
      return IndexedQueueSizeUtil.size(this, IndexedQueueSizeUtil.PLAIN_DIVISOR);
   }

   @Override
   public boolean isEmpty() {
      return IndexedQueueSizeUtil.isEmpty(this);
   }

   @Override
   public void clear() {
      while (poll() != null) {
         // empty poll
      }
   }

   @Override
   public String toString() {
      return this.getClass().getName();
   }

   @Override
   public long currentProducerIndex() {
      return lvProducerIndex();
   }

   @Override
   public long currentConsumerIndex() {
      return lvConsumerIndex();
   }
}

abstract class SpscVarHandleArrayQueueL1Pad<E> extends SpscVarHandleArrayQueueColdField<E> {
   byte b000,b001,b002,b003,b004,b005,b006,b007;//  8b
   byte b010,b011,b012,b013,b014,b015,b016,b017;// 16b
   byte b020,b021,b022,b023,b024,b025,b026,b027;// 24b
   byte b030,b031,b032,b033,b034,b035,b036,b037;// 32b
   byte b040,b041,b042,b043,b044,b045,b046,b047;// 40b
   byte b050,b051,b052,b053,b054,b055,b056,b057;// 48b
   byte b060,b061,b062,b063,b064,b065,b066,b067;// 56b
   byte b070,b071,b072,b073,b074,b075,b076,b077;// 64b
   byte b100,b101,b102,b103,b104,b105,b106,b107;// 72b
   byte b110,b111,b112,b113,b114,b115,b116,b117;// 80b
   byte b120,b121,b122,b123,b124,b125,b126,b127;// 88b
   byte b130,b131,b132,b133,b134,b135,b136,b137;// 96b
   byte b140,b141,b142,b143,b144,b145,b146,b147;//104b
   byte b150,b151,b152,b153,b154,b155,b156,b157;//112b
   byte b160,b161,b162,b163,b164,b165,b166,b167;//120b
   byte b170,b171,b172,b173,b174,b175,b176,b177;//128b

   SpscVarHandleArrayQueueL1Pad(int capacity) {
      super(capacity);
   }
}

abstract class SpscVarHandleArrayQueueProducerIndexFields<E> extends SpscVarHandleArrayQueueL1Pad<E> {
   private static final VarHandle P_INDEX_HANDLE = VarHandleHelper.findVarHandle(
         SpscVarHandleArrayQueueProducerIndexFields.class, "producerIndex", long.class);

   private volatile long producerIndex;
   protected long producerLimit;

   SpscVarHandleArrayQueueProducerIndexFields(int capacity) {
      super(capacity);
   }

   @Override
   public final long lvProducerIndex() {
      return producerIndex;
   }

   final long lpProducerIndex() {
      return producerIndex;
   }

   final void soProducerIndex(final long newValue) {
      P_INDEX_HANDLE.setRelease(this, newValue);
   }
}

abstract class SpscVarHandleArrayQueueL2Pad<E> extends SpscVarHandleArrayQueueProducerIndexFields<E> {
   byte b000,b001,b002,b003,b004,b005,b006,b007;//  8b
   byte b010,b011,b012,b013,b014,b015,b016,b017;// 16b
   byte b020,b021,b022,b023,b024,b025,b026,b027;// 24b
   byte b030,b031,b032,b033,b034,b035,b036,b037;// 32b
   byte b040,b041,b042,b043,b044,b045,b046,b047;// 40b
   byte b050,b051,b052,b053,b054,b055,b056,b057;// 48b
   byte b060,b061,b062,b063,b064,b065,b066,b067;// 56b
   byte b070,b071,b072,b073,b074,b075,b076,b077;// 64b
   byte b100,b101,b102,b103,b104,b105,b106,b107;// 72b
   byte b110,b111,b112,b113,b114,b115,b116,b117;// 80b
   byte b120,b121,b122,b123,b124,b125,b126,b127;// 88b
   byte b130,b131,b132,b133,b134,b135,b136,b137;// 96b
   byte b140,b141,b142,b143,b144,b145,b146,b147;//104b
   byte b150,b151,b152,b153,b154,b155,b156,b157;//112b
   byte b160,b161,b162,b163,b164,b165,b166,b167;//120b
   byte b170,b171,b172,b173,b174,b175,b176,b177;//128b

   SpscVarHandleArrayQueueL2Pad(int capacity) {
      super(capacity);
   }
}

abstract class SpscVarHandleArrayQueueConsumerIndexField<E> extends SpscVarHandleArrayQueueL2Pad<E> {
   private static final VarHandle C_INDEX_HANDLE = VarHandleHelper.findVarHandle(
         SpscVarHandleArrayQueueConsumerIndexField.class, "consumerIndex", long.class);

   private volatile long consumerIndex;

   SpscVarHandleArrayQueueConsumerIndexField(int capacity) {
      super(capacity);
   }

   public final long lvConsumerIndex() {
      return consumerIndex;
   }

   final long lpConsumerIndex() {
      return consumerIndex;
   }

   final void soConsumerIndex(final long newValue) {
      C_INDEX_HANDLE.setRelease(this, newValue);
   }
}

abstract class SpscVarHandleArrayQueueL3Pad<E> extends SpscVarHandleArrayQueueConsumerIndexField<E> {
   byte b000,b001,b002,b003,b004,b005,b006,b007;//  8b
   byte b010,b011,b012,b013,b014,b015,b016,b017;// 16b
   byte b020,b021,b022,b023,b024,b025,b026,b027;// 24b
   byte b030,b031,b032,b033,b034,b035,b036,b037;// 32b
   byte b040,b041,b042,b043,b044,b045,b046,b047;// 40b
   byte b050,b051,b052,b053,b054,b055,b056,b057;// 48b
   byte b060,b061,b062,b063,b064,b065,b066,b067;// 56b
   byte b070,b071,b072,b073,b074,b075,b076,b077;// 64b
   byte b100,b101,b102,b103,b104,b105,b106,b107;// 72b
   byte b110,b111,b112,b113,b114,b115,b116,b117;// 80b
   byte b120,b121,b122,b123,b124,b125,b126,b127;// 88b
   byte b130,b131,b132,b133,b134,b135,b136,b137;// 96b
   byte b140,b141,b142,b143,b144,b145,b146,b147;//104b
   byte b150,b151,b152,b153,b154,b155,b156,b157;//112b
   byte b160,b161,b162,b163,b164,b165,b166,b167;//120b
   byte b170,b171,b172,b173,b174,b175,b176,b177;//128b

   SpscVarHandleArrayQueueL3Pad(int capacity) {
      super(capacity);
   }
}

/**
 * A Single-Producer-Single-Consumer queue backed by a pre-allocated buffer using VarHandles for memory access.
 * <p>
 * This implementation is wait free.
 */
public class SpscVarHandleArrayQueue<E> extends SpscVarHandleArrayQueueL3Pad<E> {

   public SpscVarHandleArrayQueue(final int capacity) {
      super(Math.max(capacity, 4));
   }

   /**
    * {@inheritDoc}
    * <p>
    * This implementation is correct for single producer thread use only.
    */
   @Override
   public boolean offer(final E e) {
      if (null == e) {
         throw new NullPointerException();
      }
      // local load of field to avoid repeated loads after volatile reads
      final E[] buffer = this.buffer;
      final long mask = this.mask;
      final long producerIndex = this.lpProducerIndex();

      if (producerIndex >= producerLimit && !offerSlowPath(buffer, mask, producerIndex)) {
         return false;
      }
      final int offset = calcCircularRefElementOffset(producerIndex, mask);

      soRefElement(buffer, offset, e);
      soProducerIndex(producerIndex + 1); // ordered store -> atomic and ordered for size()
      return true;
   }

   private boolean offerSlowPath(final E[] buffer, final long mask, final long producerIndex) {
      final int lookAheadStep = this.lookAheadStep;
      if (null == lvRefElement(buffer, calcCircularRefElementOffset(producerIndex + lookAheadStep, mask))) {
         producerLimit = producerIndex + lookAheadStep;
      } else {
         final int offset = calcCircularRefElementOffset(producerIndex, mask);
         if (null != lvRefElement(buffer, offset)) {
            return false;
         }
      }
      return true;
   }

   /**
    * {@inheritDoc}
    * <p>
    * This implementation is correct for single consumer thread use only.
    */
   @Override
   public E poll() {
      final long consumerIndex = this.lpConsumerIndex();
      final int offset = calcCircularRefElementOffset(consumerIndex, mask);
      // local load of field to avoid repeated loads after volatile reads
      final E[] buffer = this.buffer;
      final E e = lvRefElement(buffer, offset);
      if (null == e) {
         return null;
      }
      soRefElement(buffer, offset, null);
      soConsumerIndex(consumerIndex + 1); // ordered store -> atomic and ordered for size()
      return e;
   }

   /**
    * {@inheritDoc}
    * <p>
    * This implementation is correct for single consumer thread use only.
    */
   @Override
   public E peek() {
      return lvRefElement(buffer, calcCircularRefElementOffset(lpConsumerIndex(), mask));
   }

   @Override
   public boolean relaxedOffer(final E message) {
      return offer(message);
   }

   @Override
   public E relaxedPoll() {
      return poll();
   }

   @Override
   public E relaxedPeek() {
      return peek();
   }

   @Override
   public int drain(final Consumer<E> c) {
      return drain(c, capacity());
   }

   @Override
   public int fill(final Supplier<E> s) {
      return fill(s, capacity());
   }

   @Override
   public int drain(final Consumer<E> c, final int limit) {
      if (null == c)
         throw new IllegalArgumentException("c is null");
      if (limit < 0)
         throw new IllegalArgumentException("limit is negative: " + limit);
      if (limit == 0)
         return 0;

      final E[] buffer = this.buffer;
      final long mask = this.mask;
      final long consumerIndex = this.lpConsumerIndex();

      for (int i = 0; i < limit; i++) {
         final long index = consumerIndex + i;
         final int offset = calcCircularRefElementOffset(index, mask);
         final E e = lvRefElement(buffer, offset);
         if (null == e) {
            return i;
         }
         soRefElement(buffer, offset, null);
         soConsumerIndex(index + 1); // ordered store -> atomic and ordered for size()
         c.accept(e);
      }
      return limit;
   }

   @Override
   public int fill(final Supplier<E> s, final int limit) {
      if (null == s)
         throw new IllegalArgumentException("supplier is null");
      if (limit < 0)
         throw new IllegalArgumentException("limit is negative:" + limit);
      if (limit == 0)
         return 0;

      final E[] buffer = this.buffer;
      final long mask = this.mask;
      final int lookAheadStep = this.lookAheadStep;
      final long producerIndex = this.lpProducerIndex();

      for (int i = 0; i < limit; i++) {
         final long index = producerIndex + i;
         final int lookAheadElementOffset = calcCircularRefElementOffset(index + lookAheadStep, mask);
         if (null == lvRefElement(buffer, lookAheadElementOffset)) {
            int lookAheadLimit = Math.min(lookAheadStep, limit - i);
            for (int j = 0; j < lookAheadLimit; j++) {
               final int offset = calcCircularRefElementOffset(index + j, mask);
               soRefElement(buffer, offset, s.get());
               soProducerIndex(index + j + 1); // ordered store -> atomic and ordered for size()
            }
            i += lookAheadLimit - 1;
         } else {
            final int offset = calcCircularRefElementOffset(index, mask);
            if (null != lvRefElement(buffer, offset)) {
               return i;
            }
            soRefElement(buffer, offset, s.get());
            soProducerIndex(index + 1); // ordered store -> atomic and ordered for size()
         }
      }
      return limit;
   }

   @Override
   public void drain(final Consumer<E> c, final WaitStrategy w, final ExitCondition exit) {
      if (null == c)
         throw new IllegalArgumentException("c is null");
      if (null == w)
         throw new IllegalArgumentException("wait is null");
      if (null == exit)
         throw new IllegalArgumentException("exit condition is null");

      final E[] buffer = this.buffer;
      final long mask = this.mask;
      long consumerIndex = this.lpConsumerIndex();

      int counter = 0;
      while (exit.keepRunning()) {
         for (int i = 0; i < 4096; i++) {
            final int offset = calcCircularRefElementOffset(consumerIndex, mask);
            final E e = lvRefElement(buffer, offset);
            if (null == e) {
               counter = w.idle(counter);
               continue;
            }
            consumerIndex++;
            counter = 0;
            soRefElement(buffer, offset, null);
            soConsumerIndex(consumerIndex); // ordered store -> atomic and ordered for size()
            c.accept(e);
         }
      }
   }

   @Override
   public void fill(final Supplier<E> s, final WaitStrategy w, final ExitCondition e) {
      if (null == w)
         throw new IllegalArgumentException("waiter is null");
      if (null == e)
         throw new IllegalArgumentException("exit condition is null");
      if (null == s)
         throw new IllegalArgumentException("supplier is null");

      final E[] buffer = this.buffer;
      final long mask = this.mask;
      final int lookAheadStep = this.lookAheadStep;
      long producerIndex = this.lpProducerIndex();
      int counter = 0;
      while (e.keepRunning()) {
         final int lookAheadElementOffset = calcCircularRefElementOffset(producerIndex + lookAheadStep, mask);
         if (null == lvRefElement(buffer, lookAheadElementOffset)) {
            for (int j = 0; j < lookAheadStep; j++) {
               final int offset = calcCircularRefElementOffset(producerIndex, mask);
               producerIndex++;
               soRefElement(buffer, offset, s.get());
               soProducerIndex(producerIndex); // ordered store -> atomic and ordered for size()
            }
         } else {
            final int offset = calcCircularRefElementOffset(producerIndex, mask);
            if (null != lvRefElement(buffer, offset)) {
               counter = w.idle(counter);
               continue;
            }
            producerIndex++;
            counter = 0;
            soRefElement(buffer, offset, s.get());
            soProducerIndex(producerIndex); // ordered store -> atomic and ordered for size()
         }
      }
   }

   @Override
   public Iterator<E> iterator() {
      final long cIndex = lvConsumerIndex();
      final long pIndex = lvProducerIndex();

      return new WeakIterator<>(cIndex, pIndex, mask, buffer);
   }

   private static class WeakIterator<E> implements Iterator<E> {
      private final long pIndex;
      private final long mask;
      private final E[] buffer;
      private long nextIndex;
      private E nextElement;

      WeakIterator(long cIndex, long pIndex, long mask, E[] buffer) {
         this.nextIndex = cIndex;
         this.pIndex = pIndex;
         this.mask = mask;
         this.buffer = buffer;
         nextElement = getNext();
      }

      @Override
      public void remove() {
         throw new UnsupportedOperationException("remove");
      }

      @Override
      public boolean hasNext() {
         return nextElement != null;
      }

      @Override
      public E next() {
         final E e = nextElement;
         if (e == null)
            throw new NoSuchElementException();
         nextElement = getNext();
         return e;
      }

      private E getNext() {
         while (nextIndex < pIndex) {
            int offset = calcCircularRefElementOffset(nextIndex++, mask);
            E e = lvRefElement(buffer, offset);
            if (e != null) {
               return e;
            }
         }
         return null;
      }
   }
}