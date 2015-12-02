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

import java.util.Set;

import io.atomix.catalyst.buffer.BufferInput;
import io.atomix.catalyst.buffer.BufferOutput;
import io.atomix.catalyst.serializer.CatalystSerializable;
import io.atomix.catalyst.serializer.SerializationException;
import io.atomix.catalyst.serializer.SerializeWith;
import io.atomix.catalyst.serializer.Serializer;
import io.atomix.copycat.client.Query;
import io.atomix.resource.ResourceStateMachine;

/**
 * Get resource keys query.
 */
@SerializeWith(id=41)
public class GetResourceKeys implements Query<Set<String>>, CatalystSerializable {

  Class<? extends ResourceStateMachine> type;

  public GetResourceKeys() {
    type = null;
  }

  public GetResourceKeys(Class<? extends ResourceStateMachine> type) {
    this.type = type;
  }

  /**
   * Returns the resource state machine class.
   *
   * @return The resource state machine class
   */
  public Class<? extends ResourceStateMachine> type() {
    return type;
  }

  @Override
  public void writeObject(BufferOutput<?> buffer, Serializer serializer) {
    if (type == null) {
      buffer.writeInt(0);
    } else {
      buffer.writeInt(type.getName().getBytes().length).write(type.getName().getBytes());
    }
  }

  @Override
  public void readObject(BufferInput<?> buffer, Serializer serializer) {
    int classNameSize = buffer.readInt();
    if (classNameSize == 0) {
      type = null;
      return;
    }
    byte[] bytes = new byte[classNameSize];
    buffer.read(bytes);
    String typeName = new String(bytes);
    try {
      type = (Class<? extends ResourceStateMachine>) Class.forName(typeName);
    } catch (ClassNotFoundException e) {
      throw new SerializationException(e);
    }
  }
}
