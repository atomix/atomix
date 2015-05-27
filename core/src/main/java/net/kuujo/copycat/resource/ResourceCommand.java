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
 * limitations under the License.
 */
package net.kuujo.copycat.resource;

import net.kuujo.copycat.raft.Command;
import net.kuujo.copycat.raft.Operation;

/**
 * Resource command.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class ResourceCommand<T extends Command<U>, U> extends ResourceOperation<T, U> implements Command<U> {

  /**
   * Returns a new resource command builder.
   *
   * @return A new resource command builder.
   */
  @SuppressWarnings("unchecked")
  public static <T extends Command<U>, U> Builder<T, U> builder() {
    return Operation.builder(Builder.class);
  }

  /**
   * Resource command builder.
   */
  public static class Builder<T extends Command<U>, U> extends Command.Builder<Builder<T, U>, ResourceCommand<T, U>> {

    public Builder(ResourceCommand<T, U> operation) {
      super(operation);
    }

    /**
     * Sets the resource ID.
     *
     * @param resource The resource ID.
     * @return The resource command builder.
     */
    public Builder withResource(long resource) {
      command.resource = resource;
      return this;
    }

    /**
     * Sets the resource command.
     *
     * @param command The resource command.
     * @return The resource command builder.
     */
    public Builder withCommand(T command) {
      this.command.operation = command;
      return this;
    }
  }

}
