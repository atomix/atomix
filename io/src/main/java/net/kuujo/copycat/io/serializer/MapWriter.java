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

import java.util.HashMap;
import java.util.Map;

/**
 * Map serializer.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
@Serialize(@Serialize.Type(id=11, type=Map.class))
public class MapWriter implements ObjectWriter<Map> {

  @Override
  public void write(Map object, Buffer buffer, Serializer serializer) {
    buffer.writeUnsignedShort(object.size());
    for (Map.Entry entry : ((Map<?, ?>) object).entrySet()) {
      serializer.writeObject(entry.getKey(), buffer);
      serializer.writeObject(entry.getValue(), buffer);
    }
  }

  @Override
  @SuppressWarnings("unchecked")
  public Map read(Class<Map> type, Buffer buffer, Serializer serializer) {
    int size = buffer.readUnsignedShort();
    Map object = new HashMap<>(size);
    for (int i = 0; i < size; i++) {
      Object key = serializer.readObject(buffer);
      Object value = serializer.readObject(buffer);
      object.put(key, value);
    }
    return object;
  }

}
