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
package net.kuujo.copycat.io.serializer;

import net.kuujo.copycat.io.Buffer;

import java.util.HashSet;
import java.util.Set;

/**
 * Set serializer.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
@Serialize(@Serialize.Type(id=9, type=Set.class))
public class SetWriter implements ObjectWriter<Set> {

  @Override
  public void write(Set object, Buffer buffer, Serializer serializer) {
    buffer.writeInt(object.size());
    for (Object value : object) {
      serializer.writeObject(value, buffer);
    }
  }

  @Override
  @SuppressWarnings("unchecked")
  public Set read(Class<Set> type, Buffer buffer, Serializer serializer) {
    int size = buffer.readInt();
    Set object = new HashSet<>(size);
    for (int i = 0; i < size; i++) {
      object.add(serializer.readObject(buffer));
    }
    return object;
  }

}
