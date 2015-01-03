/*
 * Copyright 2014 the original author or authors.
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
package net.kuujo.copycat.collections;

import net.kuujo.copycat.ResourceConfig;
import net.kuujo.copycat.internal.util.Assert;
import net.kuujo.copycat.protocol.Consistency;

import java.util.Map;

/**
 * Asynchronous collection configuration.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public abstract class AsyncCollectionConfig<T extends AsyncCollectionConfig<T>> extends ResourceConfig<T> {
  public static final String ASYNC_COLLECTION_CONSISTENCY = "consistency";

  private static final String DEFAULT_ASYNC_COLLECTION_CONSISTENCY = "default";

  protected AsyncCollectionConfig() {
  }

  protected AsyncCollectionConfig(Map<String, Object> config) {
    super(config);
  }

  protected AsyncCollectionConfig(T config) {
    super(config);
  }

  /**
   * Sets the collection read consistency.
   *
   * @param consistency The collection read consistency.
   * @throws java.lang.NullPointerException If the consistency is {@code null}
   */
  public void setConsistency(String consistency) {
    put(ASYNC_COLLECTION_CONSISTENCY, Consistency.parse(Assert.isNotNull(consistency, "consistency")).toString());
  }

  /**
   * Sets the collection read consistency.
   *
   * @param consistency The collection read consistency.
   * @throws java.lang.NullPointerException If the consistency is {@code null}
   */
  public void setConsistency(Consistency consistency) {
    put(ASYNC_COLLECTION_CONSISTENCY, Assert.isNotNull(consistency, "consistency").toString());
  }

  /**
   * Returns the collection read consistency.
   *
   * @return The collection read consistency.
   */
  public Consistency getConsistency() {
    return Consistency.parse(get(ASYNC_COLLECTION_CONSISTENCY, DEFAULT_ASYNC_COLLECTION_CONSISTENCY));
  }

  /**
   * Sets the collection read consistency, returning the configuration for method chaining.
   *
   * @param consistency The collection read consistency.
   * @return The collection configuration.
   * @throws java.lang.NullPointerException If the consistency is {@code null}
   */
  @SuppressWarnings("unchecked")
  public T withConsistency(String consistency) {
    setConsistency(consistency);
    return (T) this;
  }

  /**
   * Sets the collection read consistency, returning the configuration for method chaining.
   *
   * @param consistency The collection read consistency.
   * @return The collection configuration.
   * @throws java.lang.NullPointerException If the consistency is {@code null}
   */
  @SuppressWarnings("unchecked")
  public T withConsistency(Consistency consistency) {
    setConsistency(consistency);
    return (T) this;
  }

}
