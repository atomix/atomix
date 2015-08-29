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

import net.kuujo.copycat.io.serializer.SerializeWith;
import net.kuujo.copycat.raft.protocol.Command;
import net.kuujo.copycat.raft.protocol.Operation;
import net.kuujo.copycat.util.Assert;
import net.kuujo.copycat.util.BuilderPool;

/**
 * Resource command.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
@SerializeWith(id=400)
public class ResourceCommand<T extends Command<U>, U> extends ResourceOperation<T, U> implements Command<U> {

  /**
   * Returns a new resource command builder.
   *
   * @return A new resource command builder.
   */
  @SuppressWarnings("unchecked")
  public static <T extends Command<U>, U> Builder<T, U> builder() {
    return Operation.builder(Builder.class, Builder::new);
  }

  @Override
  public String toString() {
    return String.format("%s[resource=%d, command=%s]", getClass().getSimpleName(), resource, operation);
  }

  /**
   * Resource command builder.
   */
  public static class Builder<T extends Command<U>, U> extends Command.Builder<Builder<T, U>, ResourceCommand<T, U>, U> {

    private Builder(BuilderPool<Builder<T, U>, ResourceCommand<T, U>> pool) {
      super(pool);
    }

    @Override
    protected ResourceCommand<T, U> create() {
      return new ResourceCommand<>();
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
     * @throws NullPointerException if {@code command} is null
     */
    public Builder withCommand(T command) {
      this.command.operation = Assert.notNull(command, "command");
      return this;
    }
  }

}
