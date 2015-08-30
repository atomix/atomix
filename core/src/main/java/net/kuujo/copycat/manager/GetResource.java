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

import net.kuujo.copycat.io.BufferInput;
import net.kuujo.copycat.io.BufferOutput;
import net.kuujo.copycat.io.serializer.SerializationException;
import net.kuujo.copycat.io.serializer.SerializeWith;
import net.kuujo.copycat.io.serializer.Serializer;
import net.kuujo.copycat.raft.StateMachine;
import net.kuujo.copycat.raft.protocol.ConsistencyLevel;
import net.kuujo.copycat.raft.protocol.Operation;
import net.kuujo.copycat.raft.protocol.Query;
import net.kuujo.copycat.util.Assert;
import net.kuujo.copycat.util.BuilderPool;

/**
 * Get resource command.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
@SerializeWith(id=416)
public class GetResource extends PathOperation<Long> implements Query<Long> {

  /**
   * Returns a new GetResource builder.
   *
   * @return A new GetResource command builder.
   */
  public static Builder builder() {
    return Operation.builder(GetResource.Builder.class, GetResource.Builder::new);
  }

  private Class<? extends StateMachine> type;

  public GetResource() {
  }

  /**
   * @throws NullPointerException if {@code path} or {@code type} are null
   */
  public GetResource(String path, Class<? extends StateMachine> type) {
    super(path);
    this.type = Assert.notNull(type, "type");
  }

  @Override
  public ConsistencyLevel consistency() {
    return ConsistencyLevel.LINEARIZABLE;
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
  public static class Builder extends PathOperation.Builder<Builder, GetResource, Long> {
    public Builder(BuilderPool<Builder, GetResource> pool) {
      super(pool);
    }

    @Override
    protected GetResource create() {
      return new GetResource();
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
