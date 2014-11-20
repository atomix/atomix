package net.kuujo.copycat.internal.util;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;

/**
 * Reflection related utilities.
 */
public final class Reflection {
  private Reflection() {
  }

  /**
   * Finds an annotation on the method or one of its parent classes or interfaces.
   */
  public static <A extends Annotation> A findAnnotation(Method method, Class<A> annotationType) {
    A annotation = method.getAnnotation(annotationType);
    if (annotation != null) {
      return annotation;
    }

    Class<?> clazz = method.getDeclaringClass().getSuperclass();
    while (clazz != Object.class) {
      annotation = findMethodAnnotationInInterface(method, clazz, annotationType);
      if (annotation != null) {
        return annotation;
      }
      clazz = clazz.getSuperclass();
    }
    return null;
  }

  /**
   * Finds an annotation in an interface.
   */
  public static <A extends Annotation> A findMethodAnnotationInInterface(Method method,
    Class<?> clazz, Class<A> annotationType) {
    try {
      Method matchMethod = clazz.getDeclaredMethod(method.getName(), method.getParameterTypes());
      A annotation = matchMethod.getAnnotation(annotationType);
      if (annotation != null) {
        return annotation;
      }
    } catch (NoSuchMethodException e) {
    }

    for (Class<?> i : clazz.getInterfaces()) {
      A annotation = findMethodAnnotationInInterface(method, i, annotationType);
      if (annotation != null) {
        return annotation;
      }
    }
    return null;
  }
}
