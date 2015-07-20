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
package net.kuujo.copycat;

/**
 * Object builder.
 * <p>
 * This is a base interface for building objects in Copycat.
 * <p>
 * Throughout Copycat, builders are used to build a variety of objects. In most cases, builders are thread local and
 * <em>not</em> designed to be thread safe and it is assumed that {@link #build()} will be called from the same thread
 * in which the builder was created.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public interface Builder<T> {

  /**
   * Builds the object.
   * <p>
   * The returned object may be a new instance of the built class or a recycled instance, depending on the semantics
   * of the builder implementation. Users should never assume that a builder allocates a new instance.
   *
   * @return The built object.
   */
  T build();

}
