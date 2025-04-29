package org.jctools.queues.varhandle;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;

public final class VarHandleQueueUtil {
   // VarHandles for array access
   private static final VarHandle ARRAY_HANDLE = MethodHandles.arrayElementVarHandle(Object[].class);
   private static final VarHandle LONG_ARRAY_HANDLE = MethodHandles.arrayElementVarHandle(long[].class);

   @SuppressWarnings("unchecked")
   public static <E> E lvRefElement(E[] buffer, int offset) {
      return (E) ARRAY_HANDLE.getVolatile(buffer, offset);
   }

   @SuppressWarnings("unchecked")
   public static <E> E lpRefElement(E[] buffer, int offset) {
      return (E) ARRAY_HANDLE.get(buffer, offset);
   }

   public static <E> void spRefElement(E[] buffer, int offset, E value) {
      ARRAY_HANDLE.setRelease(buffer, offset, value);
   }

   public static void soRefElement(Object[] buffer, int offset, Object value) {
      ARRAY_HANDLE.setRelease(buffer, offset, value);
   }

   public static <E> void svRefElement(E[] buffer, int offset, E value) {
      ARRAY_HANDLE.set(buffer, offset, value);
   }

   public static int calcRefElementOffset(long index) {
      return (int) index;
   }

   public static int calcCircularRefElementOffset(long index, long mask) {
      return (int) (index & mask);
   }

   @SuppressWarnings("unchecked")
   public static <E> E[] allocateRefArray(int capacity) {
      return (E[]) new Object[capacity];
   }

   public static void spLongElement(long[] buffer, int offset, long e) {
      LONG_ARRAY_HANDLE.setRelease(buffer, offset, e);
   }

   public static void soLongElement(long[] buffer, int offset, long e) {
      LONG_ARRAY_HANDLE.setRelease(buffer, offset, e);
   }

   public static long lpLongElement(long[] buffer, int offset) {
      return (long) LONG_ARRAY_HANDLE.get(buffer, offset);
   }

   public static long lvLongElement(long[] buffer, int offset) {
      return (long) LONG_ARRAY_HANDLE.getVolatile(buffer, offset);
   }

   public static int calcLongElementOffset(long index) {
      return (int) index;
   }

   public static int calcCircularLongElementOffset(long index, int mask) {
      return (int) (index & mask);
   }

   public static long[] allocateLongArray(int capacity) {
      return new long[capacity];
   }

   public static int length(Object[] buf) {
      return buf.length;
   }

   /**
    * This method assumes index is actually (index << 1) because lower bit is used for resize hence the >> 1
    */
   public static int modifiedCalcCircularRefElementOffset(long index, long mask) {
      return (int) (index & mask) >> 1;
   }

   public static int nextArrayOffset(Object[] curr) {
      return curr.length - 1;
   }
}