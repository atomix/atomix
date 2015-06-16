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

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;

/**
 * Big decimal serializer.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
@Serialize(@Serialize.Type(id=8, type=BigDecimal.class))
public class BigDecimalWriter implements ObjectWriter<BigDecimal> {

  @Override
  public void write(BigDecimal object, Buffer buffer, Serializer serializer) {
    byte[] bytes = object.toPlainString().getBytes(StandardCharsets.UTF_8);
    buffer.writeInt(bytes.length).write(bytes);
  }

  @Override
  public BigDecimal read(Class<BigDecimal> type, Buffer buffer, Serializer serializer) {
    byte[] bytes = new byte[buffer.readInt()];
    buffer.read(bytes);
    return new BigDecimal(new String(bytes, StandardCharsets.UTF_8));
  }

}
