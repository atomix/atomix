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
 * Provides an interface for serializable types.
 * <p>
 * Classes can implement this interface as an alternative to providing a separate {@link Serializer} instance. Note,
 * however, that {@link Writable} classes must still be registered via {@link CopycatSerializer#register(Class)}.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public interface Writable {

  /**
   * Writes the object to the given buffer.
   *
   * @param buffer The buffer to which to write the object.
   */
  void writeObject(Buffer buffer);

  /**
   * Reads the object from the given buffer.
   *
   * @param buffer The buffer from which to read the object.
   */
  void readObject(Buffer buffer);

}
