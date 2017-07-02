/*
 * Copyright 2017-present Open Networking Laboratory
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.atomix.utils;

import com.google.common.base.MoreObjects;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.Marker;

import static com.google.common.base.MoreObjects.toStringHelper;

/**
 * Contextual logger.
 */
public class ContextualLogger extends DelegatingLogger {

  /**
   * Returns a new contextual logger builder.
   *
   * @param name the logger name
   * @return the logger builder
   */
  public static Builder builder(String name) {
    return new Builder(LoggerFactory.getLogger(name));
  }

  /**
   * Returns a new contextual logger builder.
   *
   * @param clazz the logger class
   * @return the logger builder
   */
  public static Builder builder(Class clazz) {
    return new Builder(LoggerFactory.getLogger(clazz));
  }

  /**
   * Returns a new contextual logger builder.
   *
   * @param logger the logger to contextualize
   * @return the logger builder
   */
  public static Builder builder(Logger logger) {
    return new Builder(logger);
  }

  /**
   * Contextual logger builder.
   */
  public static class Builder implements io.atomix.utils.Builder<ContextualLogger> {
    private final Logger logger;
    private final MoreObjects.ToStringHelper toStringHelper = toStringHelper("");

    public Builder(Logger logger) {
      this.logger = logger;
    }

    /**
     * Configures the {@link MoreObjects.ToStringHelper} so {@link #toString()} will ignore properties with null
     * value. The order of calling this method, relative to the {@code add()}/{@code addValue()}
     * methods, is not significant.
     */
    @CanIgnoreReturnValue
    public Builder omitNullValues() {
      toStringHelper.omitNullValues();
      return this;
    }

    /**
     * Adds a name/value pair to the formatted output in {@code name=value} format. If {@code value}
     * is {@code null}, the string {@code "null"} is used, unless {@link #omitNullValues()} is
     * called, in which case this name/value pair will not be added.
     */
    @CanIgnoreReturnValue
    public Builder add(String name, Object value) {
      toStringHelper.add(name, value);
      return this;
    }

    /**
     * Adds a name/value pair to the formatted output in {@code name=value} format.
     */
    @CanIgnoreReturnValue
    public Builder add(String name, boolean value) {
      toStringHelper.add(name, value);
      return this;
    }

    /**
     * Adds a name/value pair to the formatted output in {@code name=value} format.
     */
    @CanIgnoreReturnValue
    public Builder add(String name, char value) {
      toStringHelper.add(name, value);
      return this;
    }

    /**
     * Adds a name/value pair to the formatted output in {@code name=value} format.
     */
    @CanIgnoreReturnValue
    public Builder add(String name, double value) {
      toStringHelper.add(name, value);
      return this;
    }

    /**
     * Adds a name/value pair to the formatted output in {@code name=value} format.
     */
    @CanIgnoreReturnValue
    public Builder add(String name, float value) {
      toStringHelper.add(name, value);
      return this;
    }

    /**
     * Adds a name/value pair to the formatted output in {@code name=value} format.
     */
    @CanIgnoreReturnValue
    public Builder add(String name, int value) {
      toStringHelper.add(name, value);
      return this;
    }

    /**
     * Adds a name/value pair to the formatted output in {@code name=value} format.
     */
    @CanIgnoreReturnValue
    public Builder add(String name, long value) {
      toStringHelper.add(name, value);
      return this;
    }

    /**
     * Adds an unnamed value to the formatted output.
     *
     * <p>It is strongly encouraged to use {@link #add(String, Object)} instead and give value a
     * readable name.
     */
    @CanIgnoreReturnValue
    public Builder addValue(Object value) {
      toStringHelper.addValue(value);
      return this;
    }

    /**
     * Adds an unnamed value to the formatted output.
     *
     * <p>It is strongly encouraged to use {@link #add(String, boolean)} instead and give value a
     * readable name.
     */
    @CanIgnoreReturnValue
    public Builder addValue(boolean value) {
      toStringHelper.addValue(value);
      return this;
    }

    /**
     * Adds an unnamed value to the formatted output.
     *
     * <p>It is strongly encouraged to use {@link #add(String, char)} instead and give value a
     * readable name.
     */
    @CanIgnoreReturnValue
    public Builder addValue(char value) {
      toStringHelper.addValue(value);
      return this;
    }

    /**
     * Adds an unnamed value to the formatted output.
     *
     * <p>It is strongly encouraged to use {@link #add(String, double)} instead and give value a
     * readable name.
     */
    @CanIgnoreReturnValue
    public Builder addValue(double value) {
      toStringHelper.addValue(value);
      return this;
    }

    /**
     * Adds an unnamed value to the formatted output.
     *
     * <p>It is strongly encouraged to use {@link #add(String, float)} instead and give value a
     * readable name.
     */
    @CanIgnoreReturnValue
    public Builder addValue(float value) {
      toStringHelper.addValue(value);
      return this;
    }

    /**
     * Adds an unnamed value to the formatted output.
     *
     * <p>It is strongly encouraged to use {@link #add(String, int)} instead and give value a
     * readable name.
     */
    @CanIgnoreReturnValue
    public Builder addValue(int value) {
      toStringHelper.addValue(value);
      return this;
    }

    /**
     * Adds an unnamed value to the formatted output.
     *
     * <p>It is strongly encouraged to use {@link #add(String, long)} instead and give value a
     * readable name.
     */
    @CanIgnoreReturnValue
    public Builder addValue(long value) {
      toStringHelper.addValue(value);
      return this;
    }

    @Override
    public ContextualLogger build() {
      return new ContextualLogger(logger, toStringHelper.toString());
    }
  }

  private static final String SEPARATOR = " - ";
  private final String context;

  public ContextualLogger(Logger delegate, String context) {
    super(delegate);
    this.context = context + SEPARATOR;
  }

  /**
   * Returns a contextualized version of the given string.
   *
   * @param msg the message to contextualize
   * @return the contextualized message
   */
  private String contextualize(String msg) {
    return context + msg;
  }

  @Override
  public void trace(String msg) {
    if (isTraceEnabled()) {
      super.trace(contextualize(msg));
    }
  }

  @Override
  public void trace(String format, Object arg) {
    if (isTraceEnabled()) {
      super.trace(contextualize(format), arg);
    }
  }

  @Override
  public void trace(String format, Object arg1, Object arg2) {
    if (isTraceEnabled()) {
      super.trace(contextualize(format), arg1, arg2);
    }
  }

  @Override
  public void trace(String format, Object... arguments) {
    if (isTraceEnabled()) {
      super.trace(contextualize(format), arguments);
    }
  }

  @Override
  public void trace(String msg, Throwable t) {
    if (isTraceEnabled()) {
      super.trace(contextualize(msg), t);
    }
  }

  @Override
  public void trace(Marker marker, String msg) {
    if (isTraceEnabled()) {
      super.trace(marker, contextualize(msg));
    }
  }

  @Override
  public void trace(Marker marker, String format, Object arg) {
    if (isTraceEnabled()) {
      super.trace(marker, contextualize(format), arg);
    }
  }

  @Override
  public void trace(Marker marker, String format, Object arg1, Object arg2) {
    if (isTraceEnabled()) {
      super.trace(marker, contextualize(format), arg1, arg2);
    }
  }

  @Override
  public void trace(Marker marker, String format, Object... argArray) {
    if (isTraceEnabled()) {
      super.trace(marker, contextualize(format), argArray);
    }
  }

  @Override
  public void trace(Marker marker, String msg, Throwable t) {
    if (isTraceEnabled()) {
      super.trace(marker, contextualize(msg), t);
    }
  }

  @Override
  public void debug(String msg) {
    if (isDebugEnabled()) {
      super.debug(contextualize(msg));
    }
  }

  @Override
  public void debug(String format, Object arg) {
    if (isDebugEnabled()) {
      super.debug(contextualize(format), arg);
    }
  }

  @Override
  public void debug(String format, Object arg1, Object arg2) {
    if (isDebugEnabled()) {
      super.debug(contextualize(format), arg1, arg2);
    }
  }

  @Override
  public void debug(String format, Object... arguments) {
    if (isDebugEnabled()) {
      super.debug(contextualize(format), arguments);
    }
  }

  @Override
  public void debug(String msg, Throwable t) {
    if (isDebugEnabled()) {
      super.debug(contextualize(msg), t);
    }
  }

  @Override
  public void debug(Marker marker, String msg) {
    if (isDebugEnabled()) {
      super.debug(marker, contextualize(msg));
    }
  }

  @Override
  public void debug(Marker marker, String format, Object arg) {
    if (isDebugEnabled()) {
      super.debug(marker, contextualize(format), arg);
    }
  }

  @Override
  public void debug(Marker marker, String format, Object arg1, Object arg2) {
    if (isDebugEnabled()) {
      super.debug(marker, contextualize(format), arg1, arg2);
    }
  }

  @Override
  public void debug(Marker marker, String format, Object... arguments) {
    if (isDebugEnabled()) {
      super.debug(marker, contextualize(format), arguments);
    }
  }

  @Override
  public void debug(Marker marker, String msg, Throwable t) {
    if (isDebugEnabled()) {
      super.debug(marker, contextualize(msg), t);
    }
  }

  @Override
  public void info(String msg) {
    if (isInfoEnabled()) {
      super.info(contextualize(msg));
    }
  }

  @Override
  public void info(String format, Object arg) {
    if (isInfoEnabled()) {
      super.info(contextualize(format), arg);
    }
  }

  @Override
  public void info(String format, Object arg1, Object arg2) {
    if (isInfoEnabled()) {
      super.info(contextualize(format), arg1, arg2);
    }
  }

  @Override
  public void info(String format, Object... arguments) {
    if (isInfoEnabled()) {
      super.info(contextualize(format), arguments);
    }
  }

  @Override
  public void info(String msg, Throwable t) {
    if (isInfoEnabled()) {
      super.info(contextualize(msg), t);
    }
  }

  @Override
  public void info(Marker marker, String msg) {
    if (isInfoEnabled()) {
      super.info(marker, contextualize(msg));
    }
  }

  @Override
  public void info(Marker marker, String format, Object arg) {
    if (isInfoEnabled()) {
      super.info(marker, contextualize(format), arg);
    }
  }

  @Override
  public void info(Marker marker, String format, Object arg1, Object arg2) {
    if (isInfoEnabled()) {
      super.info(marker, contextualize(format), arg1, arg2);
    }
  }

  @Override
  public void info(Marker marker, String format, Object... arguments) {
    if (isInfoEnabled()) {
      super.info(marker, contextualize(format), arguments);
    }
  }

  @Override
  public void info(Marker marker, String msg, Throwable t) {
    if (isInfoEnabled()) {
      super.info(marker, contextualize(msg), t);
    }
  }

  @Override
  public void warn(String msg) {
    if (isWarnEnabled()) {
      super.warn(contextualize(msg));
    }
  }

  @Override
  public void warn(String format, Object arg) {
    if (isWarnEnabled()) {
      super.warn(contextualize(format), arg);
    }
  }

  @Override
  public void warn(String format, Object... arguments) {
    if (isWarnEnabled()) {
      super.warn(contextualize(format), arguments);
    }
  }

  @Override
  public void warn(String format, Object arg1, Object arg2) {
    if (isWarnEnabled()) {
      super.warn(contextualize(format), arg1, arg2);
    }
  }

  @Override
  public void warn(String msg, Throwable t) {
    if (isWarnEnabled()) {
      super.warn(contextualize(msg), t);
    }
  }

  @Override
  public void warn(Marker marker, String msg) {
    if (isWarnEnabled()) {
      super.warn(marker, contextualize(msg));
    }
  }

  @Override
  public void warn(Marker marker, String format, Object arg) {
    if (isWarnEnabled()) {
      super.warn(marker, contextualize(format), arg);
    }
  }

  @Override
  public void warn(Marker marker, String format, Object arg1, Object arg2) {
    if (isWarnEnabled()) {
      super.warn(marker, contextualize(format), arg1, arg2);
    }
  }

  @Override
  public void warn(Marker marker, String format, Object... arguments) {
    if (isWarnEnabled()) {
      super.warn(marker, contextualize(format), arguments);
    }
  }

  @Override
  public void warn(Marker marker, String msg, Throwable t) {
    if (isWarnEnabled()) {
      super.warn(marker, contextualize(msg), t);
    }
  }

  @Override
  public void error(String msg) {
    if (isErrorEnabled()) {
      super.error(contextualize(msg));
    }
  }

  @Override
  public void error(String format, Object arg) {
    if (isErrorEnabled()) {
      super.error(contextualize(format), arg);
    }
  }

  @Override
  public void error(String format, Object arg1, Object arg2) {
    if (isErrorEnabled()) {
      super.error(contextualize(format), arg1, arg2);
    }
  }

  @Override
  public void error(String format, Object... arguments) {
    if (isErrorEnabled()) {
      super.error(contextualize(format), arguments);
    }
  }

  @Override
  public void error(String msg, Throwable t) {
    if (isErrorEnabled()) {
      super.error(contextualize(msg), t);
    }
  }

  @Override
  public void error(Marker marker, String msg) {
    if (isErrorEnabled()) {
      super.error(marker, contextualize(msg));
    }
  }

  @Override
  public void error(Marker marker, String format, Object arg) {
    if (isErrorEnabled()) {
      super.error(marker, contextualize(format), arg);
    }
  }

  @Override
  public void error(Marker marker, String format, Object arg1, Object arg2) {
    if (isErrorEnabled()) {
      super.error(marker, contextualize(format), arg1, arg2);
    }
  }

  @Override
  public void error(Marker marker, String format, Object... arguments) {
    if (isErrorEnabled()) {
      super.error(marker, contextualize(format), arguments);
    }
  }

  @Override
  public void error(Marker marker, String msg, Throwable t) {
    if (isErrorEnabled()) {
      super.error(marker, contextualize(msg), t);
    }
  }
}
