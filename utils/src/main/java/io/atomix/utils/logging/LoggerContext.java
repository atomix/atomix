// SPDX-FileCopyrightText: 2017-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.utils.logging;

import com.google.common.base.MoreObjects;
import com.google.errorprone.annotations.CanIgnoreReturnValue;

import java.util.function.Supplier;

/**
 * Logger context.
 */
public class LoggerContext {

  /**
   * Returns a new contextual logger builder.
   *
   * @param name the logger name
   * @return the logger builder
   */
  public static Builder builder(String name) {
    return new Builder(name);
  }

  /**
   * Returns a new contextual logger builder.
   *
   * @param clazz the logger class
   * @return the logger builder
   */
  public static Builder builder(Class clazz) {
    return new Builder(clazz.getSimpleName());
  }

  private final Supplier<String> stringProvider;

  public LoggerContext(Supplier<String> stringProvider) {
    this.stringProvider = stringProvider;
  }

  @Override
  public String toString() {
    return stringProvider.get();
  }

  /**
   * Contextual logger builder.
   */
  public static class Builder implements io.atomix.utils.Builder<LoggerContext> {
    private final MoreObjects.ToStringHelper identityStringHelper;
    private MoreObjects.ToStringHelper argsStringHelper;
    private boolean omitNullValues = false;

    public Builder(String name) {
      this.identityStringHelper = MoreObjects.toStringHelper(name);
    }

    /**
     * Initializes the arguments string helper.
     */
    private void initializeArgs() {
      if (argsStringHelper == null) {
        argsStringHelper = MoreObjects.toStringHelper("");
      }
    }

    /**
     * Configures the {@link MoreObjects.ToStringHelper} so {@link #toString()} will ignore properties with null
     * value. The order of calling this method, relative to the {@code add()}/{@code addValue()}
     * methods, is not significant.
     */
    @CanIgnoreReturnValue
    public Builder omitNullValues() {
      this.omitNullValues = true;
      return this;
    }

    /**
     * Adds a name/value pair to the formatted output in {@code name=value} format. If {@code value}
     * is {@code null}, the string {@code "null"} is used, unless {@link #omitNullValues()} is
     * called, in which case this name/value pair will not be added.
     */
    @CanIgnoreReturnValue
    public Builder add(String name, Object value) {
      initializeArgs();
      argsStringHelper.add(name, value);
      return this;
    }

    /**
     * Adds a name/value pair to the formatted output in {@code name=value} format.
     */
    @CanIgnoreReturnValue
    public Builder add(String name, boolean value) {
      initializeArgs();
      argsStringHelper.add(name, value);
      return this;
    }

    /**
     * Adds a name/value pair to the formatted output in {@code name=value} format.
     */
    @CanIgnoreReturnValue
    public Builder add(String name, char value) {
      initializeArgs();
      argsStringHelper.add(name, value);
      return this;
    }

    /**
     * Adds a name/value pair to the formatted output in {@code name=value} format.
     */
    @CanIgnoreReturnValue
    public Builder add(String name, double value) {
      initializeArgs();
      argsStringHelper.add(name, value);
      return this;
    }

    /**
     * Adds a name/value pair to the formatted output in {@code name=value} format.
     */
    @CanIgnoreReturnValue
    public Builder add(String name, float value) {
      initializeArgs();
      argsStringHelper.add(name, value);
      return this;
    }

    /**
     * Adds a name/value pair to the formatted output in {@code name=value} format.
     */
    @CanIgnoreReturnValue
    public Builder add(String name, int value) {
      initializeArgs();
      argsStringHelper.add(name, value);
      return this;
    }

    /**
     * Adds a name/value pair to the formatted output in {@code name=value} format.
     */
    @CanIgnoreReturnValue
    public Builder add(String name, long value) {
      initializeArgs();
      argsStringHelper.add(name, value);
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
      identityStringHelper.addValue(value);
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
      identityStringHelper.addValue(value);
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
      identityStringHelper.addValue(value);
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
      identityStringHelper.addValue(value);
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
      identityStringHelper.addValue(value);
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
      identityStringHelper.addValue(value);
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
      identityStringHelper.addValue(value);
      return this;
    }

    @Override
    public LoggerContext build() {
      MoreObjects.ToStringHelper identityStringHelper = this.identityStringHelper;
      MoreObjects.ToStringHelper argsStringHelper = this.argsStringHelper;
      if (omitNullValues) {
        identityStringHelper.omitNullValues();
        if (argsStringHelper != null) {
          argsStringHelper.omitNullValues();
        }
      }
      return new LoggerContext(() -> {
        if (argsStringHelper == null) {
          return identityStringHelper.toString();
        } else {
          return identityStringHelper.toString() + argsStringHelper.toString();
        }
      });
    }
  }
}
