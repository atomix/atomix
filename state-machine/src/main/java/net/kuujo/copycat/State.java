/*
 * Copyright 2014 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package net.kuujo.copycat;

import net.kuujo.copycat.internal.DefaultState;

import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * Copycat state machine state.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public interface State {

  /**
   * Returns a new state builder.
   *
   * @return A new state builder.
   */
  static Builder builder() {
    return new DefaultState.Builder();
  }

  /**
   * Returns the unique state ID.
   *
   * @return The unique state ID.
   */
  String id();

  /**
   * Executes a command for the state.
   *
   * @param command The command to submit.
   * @param arg The command argument.
   * @param context The current state context.
   * @return The command output.
   */
  Object execute(String command, Object arg, StateContext context);

  /**
   * Takes a snapshot of the state.
   *
   * @return A snapshot of the state.
   */
  byte[] takeSnapshot();

  /**
   * Installs a snapshot of the state.
   *
   * @param snapshot A snapshot of the state.
   */
  void installSnapshot(byte[] snapshot);

  /**
   * State builder.
   */
  static interface Builder {

    /**
     * Sets the unique state ID.
     *
     * @param id The unique state ID.
     * @return The state builder.
     */
    Builder withId(String id);

    /**
     * Adds a command to the state.
     *
     * @param command The name of the command to add.
     * @param function The command function.
     * @param <T> The command input type.
     * @param <U> The command output type.
     * @return The state builder.
     */
    <T, U> Builder addCommand(String command, BiFunction<T, StateContext, U> function);

    /**
     * Sets a snapshot provider on the state.
     *
     * @param provider The state snapshot provider.
     * @return The state builder.
     */
    Builder withSnapshotProvider(Supplier<byte[]> provider);

    /**
     * Sets a snapshot installer on the state.
     *
     * @param installer The state snapshot installer.
     * @return The state builder.
     */
    Builder withSnapshotInstaller(Consumer<byte[]> installer);

  }

}
