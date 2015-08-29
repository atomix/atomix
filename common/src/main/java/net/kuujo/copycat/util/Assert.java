package net.kuujo.copycat.util;

/**
 * Assertion utilities.
 */
public final class Assert {
  private Assert() {
  }

  public static void arg(boolean expression, String errorMessageFormat, Object... args) {
    if (!expression)
      throw new IllegalArgumentException(String.format(errorMessageFormat, args));
  }
  
  public static <T> T arg(T argument, boolean expression, String errorMessageFormat, Object... args) {
    arg(expression, errorMessageFormat, args);
    return argument;
  }
  
  public static <T> T argNot(T argument, boolean expression, String errorMessageFormat, Object... args) {
    return arg(argument, !expression, errorMessageFormat, args);
  }

  public static <T> T notNull(T reference, String parameterName) {
    if (reference == null)
      throw new NullPointerException(parameterName + " cannot be null");
    return reference;
  }

  public static void state(boolean expression, String errorMessageFormat, Object... args) {
    if (!expression)
      throw new IllegalStateException(String.format(errorMessageFormat, args));
  }
  
  public static void stateNot(boolean expression, String errorMessageFormat, Object... args) {
    state(!expression, errorMessageFormat, args);
  }
}