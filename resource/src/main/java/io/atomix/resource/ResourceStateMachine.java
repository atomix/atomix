/*
 * Copyright 2015 the original author or authors.
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
 * limitations under the License
 */
package io.atomix.resource;

import io.atomix.catalyst.buffer.BufferInput;
import io.atomix.catalyst.buffer.BufferOutput;
import io.atomix.catalyst.serializer.CatalystSerializable;
import io.atomix.catalyst.serializer.Serializer;
import io.atomix.catalyst.util.Assert;
import io.atomix.copycat.client.Command;
import io.atomix.copycat.client.session.Session;
import io.atomix.copycat.server.Commit;
import io.atomix.copycat.server.StateMachine;
import io.atomix.copycat.server.StateMachineExecutor;
import io.atomix.copycat.server.session.SessionListener;

import java.util.Properties;

/**
 * Base resource state machine.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public abstract class ResourceStateMachine extends StateMachine implements SessionListener {
  private final ResourceType type;
  private Commit<ConfigureCommand> configureCommit;

  protected ResourceStateMachine(ResourceType type) {
    this.type = Assert.notNull(type, "type");
  }

  @Override
  public final void init(StateMachineExecutor executor) {
    try {
      executor.serializer().resolve(type.typeResolver().newInstance());
    } catch (InstantiationException | IllegalAccessException e) {
      throw new ResourceException("failed to instantiate resource type resolver");
    }

    executor.serializer().register(ResourceCommand.class, -50);
    executor.serializer().register(ResourceQuery.class, -51);
    executor.serializer().register(ConfigureCommand.class, -52);
    executor.serializer().register(DeleteCommand.class, -53);

    executor.<DeleteCommand>register(DeleteCommand.class, this::delete);
    executor.<ConfigureCommand>register(ConfigureCommand.class, this::configure);
    super.init(new ResourceStateMachineExecutor(executor));
  }

  /**
   * Handles a configure command.
   */
  @SuppressWarnings("unchecked")
  private void configure(Commit<ConfigureCommand> commit) {
    if (configureCommit != null)
      configureCommit.close();
    configureCommit = commit;
    configure(configureCommit.operation().config());
  }

  /**
   * Configures the resource.
   *
   * @param config The resource configuration.
   */
  public void configure(Properties config) {
  }

  @Override
  public void register(Session session) {
  }

  @Override
  public void unregister(Session session) {
  }

  @Override
  public void expire(Session session) {
  }

  @Override
  public void close(Session session) {
  }

  /**
   * Handles a delete command.
   */
  private void delete(Commit<DeleteCommand> commit) {
    try {
      delete();
    } finally {
      commit.close();
    }
  }

  /**
   * Deletes state machine state.
   */
  public void delete() {
  }

  /**
   * Resource configure command.
   */
  public static class ConfigureCommand implements Command<Void>, CatalystSerializable {
    private Properties config;

    public ConfigureCommand() {
    }

    public ConfigureCommand(Properties config) {
      this.config = Assert.notNull(config, "config");
    }

    /**
     * Returns the resource configuration.
     *
     * @return The resource configuration.
     */
    public Properties config() {
      return config;
    }

    @Override
    public void writeObject(BufferOutput<?> buffer, Serializer serializer) {
      serializer.writeObject(config, buffer);
    }

    @Override
    public void readObject(BufferInput<?> buffer, Serializer serializer) {
      config = serializer.readObject(buffer);
    }
  }

  /**
   * Resource delete command.
   */
  public static class DeleteCommand implements Command<Void>, CatalystSerializable {
    @Override
    public void writeObject(BufferOutput<?> buffer, Serializer serializer) {
    }
    @Override
    public void readObject(BufferInput<?> buffer, Serializer serializer) {
    }
  }

}
