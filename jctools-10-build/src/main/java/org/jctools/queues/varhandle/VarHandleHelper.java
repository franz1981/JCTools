package org.jctools.queues.varhandle;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;

public final class VarHandleHelper {
   /**
    * Safely finds a VarHandle for a class field, converting checked exceptions to runtime exceptions.
    *
    * @param declaringClass the class that declares the field
    * @param fieldName the name of the field
    * @param fieldType the type of the field
    * @return a VarHandle for the field
    * @throws RuntimeException if the field cannot be found or accessed
    */
   public static VarHandle findVarHandle(Class<?> declaringClass, String fieldName, Class<?> fieldType) {
      try {
         MethodHandles.Lookup lookup = MethodHandles.privateLookupIn(declaringClass, MethodHandles.lookup());
         return lookup.findVarHandle(declaringClass, fieldName, fieldType);
      } catch (NoSuchFieldException | IllegalAccessException | IllegalArgumentException e) {
         throw new RuntimeException("Cannot get VarHandle for " +
               declaringClass.getSimpleName() + "." + fieldName, e);
      }
   }
}