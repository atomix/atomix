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

import net.kuujo.copycat.io.BufferInput;
import net.kuujo.copycat.io.BufferOutput;
import net.kuujo.copycat.io.serializer.Serializer;
import net.kuujo.copycat.io.serializer.TypeSerializer;

/**
 * Integer array serializer.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class IntegerArraySerializer implements TypeSerializer<int[]> {

  @Override
  public void write(int[] ints, BufferOutput buffer, Serializer serializer) {
    buffer.writeUnsignedShort(ints.length);
    for (int i : ints) {
      buffer.writeInt(i);
    }
  }

  @Override
  public int[] read(Class<int[]> type, BufferInput buffer, Serializer serializer) {
    int[] ints = new int[buffer.readUnsignedShort()];
    for (int i = 0; i < ints.length; i++) {
      ints[i] = buffer.readInt();
    }
    return ints;
  }

}
