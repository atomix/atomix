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
package io.atomix.manager;

import io.atomix.catalyst.buffer.BufferInput;
import io.atomix.catalyst.buffer.BufferOutput;
import io.atomix.catalyst.serializer.SerializationException;
import io.atomix.catalyst.serializer.SerializeWith;
import io.atomix.catalyst.serializer.Serializer;
import io.atomix.catalyst.util.Assert;
import io.atomix.catalyst.util.BuilderPool;
import io.atomix.copycat.client.Command;
import io.atomix.copycat.client.Operation;
import io.atomix.copycat.server.StateMachine;

/**
 * Create resource command.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
@SerializeWith(id=414)
public class CreateResource extends PathOperation<Long> implements Command<Long> {

  /**
   * Returns a new CreateResource builder.
   *
   * @return A new CreateResource command builder.
   */
  public static Builder builder() {
    return Operation.builder(CreateResource.Builder.class, CreateResource.Builder::new);
  }

  private Class<? extends StateMachine> type;

  public CreateResource() {
  }

  /**
   * @throws NullPointerException if {@code path} or {@code type} are null
   */
  public CreateResource(String path, Class<? extends StateMachine> type) {
    super(path);
    this.type = Assert.notNull(type, "type");
  }

  /**
   * Returns the resource state machine class.
   *
   * @return The resource state machine class.
   */
  public Class<? extends StateMachine> type() {
    return type;
  }

  @Override
  public void writeObject(BufferOutput<?> buffer, Serializer serializer) {
    super.writeObject(buffer, serializer);
    buffer.writeInt(type.getName().getBytes().length).write(type.getName().getBytes());
  }

  @Override
  @SuppressWarnings("unchecked")
  public void readObject(BufferInput<?> buffer, Serializer serializer) {
    super.readObject(buffer, serializer);
    byte[] bytes = new byte[buffer.readInt()];
    buffer.read(bytes);
    String typeName = new String(bytes);
    try {
      type = (Class<? extends StateMachine>) Class.forName(typeName);
    } catch (ClassNotFoundException e) {
      throw new SerializationException(e);
    }
  }

  /**
   * Create resource builder.
   */
  public static class Builder extends PathOperation.Builder<Builder, CreateResource, Long> {
    public Builder(BuilderPool<Builder, CreateResource> pool) {
      super(pool);
    }

    @Override
    protected CreateResource create() {
      return new CreateResource();
    }

    /**
     * Sets the resource state machine type.
     *
     * @param type The resource state machine type.
     * @return The create resource command builder.
     * @throws NullPointerException if {@code type} is null
     */
    public Builder withType(Class<? extends StateMachine> type) {
      operation.type = Assert.notNull(type, "type");
      return this;
    }
  }

}
