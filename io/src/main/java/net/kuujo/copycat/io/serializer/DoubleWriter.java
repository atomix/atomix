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

/**
 * Double serializer.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
@Serialize(@Serialize.Type(id=2, type=Double.class))
public class DoubleWriter implements ObjectWriter<Double> {

  @Override
  public void write(Double object, Buffer buffer, Serializer serializer) {
    buffer.writeDouble(object);
  }

  @Override
  public Double read(Class<Double> type, Buffer buffer, Serializer serializer) {
    return buffer.readDouble();
  }

}
