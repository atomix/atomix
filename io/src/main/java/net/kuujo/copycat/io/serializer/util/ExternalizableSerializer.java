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
package net.kuujo.copycat.io.serializer.util;

import net.kuujo.copycat.io.BufferInput;
import net.kuujo.copycat.io.BufferObjectInput;
import net.kuujo.copycat.io.BufferObjectOutput;
import net.kuujo.copycat.io.BufferOutput;
import net.kuujo.copycat.io.serializer.SerializationException;
import net.kuujo.copycat.io.serializer.Serializer;
import net.kuujo.copycat.io.serializer.TypeSerializer;

import java.io.Externalizable;
import java.io.IOException;

/**
 * Externalizable serializer.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class ExternalizableSerializer implements TypeSerializer<Externalizable> {

  @Override
  public void write(Externalizable externalizable, BufferOutput buffer, Serializer serializer) {
    try {
      externalizable.writeExternal(new BufferObjectOutput(buffer, serializer));
    } catch (IOException e) {
      throw new SerializationException("failed to serialize externalizable type: " + externalizable.getClass(), e);
    }
  }

  @Override
  public Externalizable read(Class<Externalizable> type, BufferInput buffer, Serializer serializer) {
    try {
      Externalizable externalizable = type.newInstance();
      externalizable.readExternal(new BufferObjectInput(buffer, serializer));
      return externalizable;
    } catch (InstantiationException | IllegalAccessException e) {
      throw new SerializationException("failed to instantiate externalizable type: " + type, e);
    } catch (IOException | ClassNotFoundException e) {
      throw new SerializationException("failed to deserialize externalizable type: " + type, e);
    }
  }

}
