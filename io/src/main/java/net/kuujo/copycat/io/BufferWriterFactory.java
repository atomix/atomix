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
package net.kuujo.copycat.io;

import net.kuujo.copycat.io.util.ReferenceCounted;
import net.kuujo.copycat.io.util.ReferenceManager;

/**
 * Buffer writer factory.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
@FunctionalInterface
public interface BufferWriterFactory<T extends BufferWriter<?, U>, U extends Buffer & ReferenceCounted<? extends Buffer>> {

  /**
   * Creates a new buffer writer.
   *
   * @param buffer The underlying buffer.
   * @param offset The starting offset for the writer.
   * @param limit The limit for the writer.
   * @param referenceManager The writer reference manager.
   * @return The new writer instance.
   */
  T createWriter(U buffer, long offset, long limit, ReferenceManager<T> referenceManager);

}
