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
import io.atomix.copycat.client.Query;
import io.atomix.resource.ResourceStateMachine;

/**
 * Get resource command.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
@SerializeWith(id=35)
public class GetResource extends KeyOperation<Long> implements Query<Long> {
  private Class<? extends ResourceStateMachine> type;

  public GetResource() {
  }

  /**
   * @throws NullPointerException if {@code path} or {@code type} are null
   */
  public GetResource(String path, Class<? extends ResourceStateMachine> type) {
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
  public Class<? extends ResourceStateMachine> type() {
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
      type = (Class<? extends ResourceStateMachine>) Class.forName(typeName);
    } catch (ClassNotFoundException e) {
      throw new SerializationException(e);
    }
  }

}
