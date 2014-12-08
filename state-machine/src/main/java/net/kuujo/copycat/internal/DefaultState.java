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
package net.kuujo.copycat.internal;

import net.kuujo.copycat.State;
import net.kuujo.copycat.StateContext;

import java.util.HashMap;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class DefaultState implements State {
  private String id;
  private final Map<String, BiFunction> commands = new HashMap<>();
  private Supplier<byte[]> snapshotProvider;
  private Consumer<byte[]> snapshotInstaller;

  @Override
  public String id() {
    return id;
  }

  @Override
  @SuppressWarnings({"unchecked", "rawtypes"})
  public Object execute(String command, Object arg, StateContext context) {
    BiFunction function = commands.get(command);
    if (function != null) {
      return function.apply(arg, context);
    }
    return null;
  }

  @Override
  public byte[] takeSnapshot() {
    if (snapshotProvider != null) {
      return snapshotProvider.get();
    }
    return new byte[0];
  }

  @Override
  public void installSnapshot(byte[] snapshot) {
    if (snapshotInstaller != null) {
      snapshotInstaller.accept(snapshot);
    }
  }

  /**
   * Default state builder.
   */
  public static class Builder implements State.Builder {
    private final DefaultState state = new DefaultState();

    @Override
    public State.Builder withId(String id) {
      state.id = id;
      return this;
    }

    @Override
    public <T, U> State.Builder addCommand(String command, BiFunction<T, StateContext, U> function) {
      state.commands.put(command, function);
      return this;
    }

    @Override
    public State.Builder withSnapshotProvider(Supplier<byte[]> provider) {
      state.snapshotProvider = provider;
      return this;
    }

    @Override
    public State.Builder withSnapshotInstaller(Consumer<byte[]> installer) {
      state.snapshotInstaller = installer;
      return this;
    }
  }

}
