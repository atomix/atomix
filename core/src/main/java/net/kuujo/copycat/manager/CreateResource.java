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
package net.kuujo.copycat.manager;

import net.kuujo.copycat.BuilderPool;
import net.kuujo.copycat.Command;
import net.kuujo.copycat.Operation;
import net.kuujo.copycat.io.BufferInput;
import net.kuujo.copycat.io.BufferOutput;
import net.kuujo.copycat.io.serializer.SerializationException;
import net.kuujo.copycat.io.serializer.SerializeWith;
import net.kuujo.copycat.io.serializer.Serializer;
import net.kuujo.copycat.server.StateMachine;

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

  public CreateResource(String path, Class<? extends StateMachine> type) {
    super(path);
    this.type = type;
  }

  /**
   * Returns the resource class.
   *
   * @return The resource class.
   */
  public Class<? extends StateMachine> type() {
    return type;
  }

  @Override
  public void writeObject(BufferOutput buffer, Serializer serializer) {
    super.writeObject(buffer, serializer);
    buffer.writeInt(type.getName().getBytes().length).write(type.getName().getBytes());
  }

  @Override
  @SuppressWarnings("unchecked")
  public void readObject(BufferInput buffer, Serializer serializer) {
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
     * Sets the command type.
     *
     * @param type The command type.
     * @return The command builder.
     */
    public Builder withType(Class<? extends StateMachine> type) {
      operation.type = type;
      return this;
    }
  }

}
