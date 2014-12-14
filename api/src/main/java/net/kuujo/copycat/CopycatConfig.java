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
package net.kuujo.copycat;

import net.kuujo.copycat.internal.util.Assert;
import net.kuujo.copycat.log.LogConfig;
import net.kuujo.copycat.util.serializer.JavaSerializer;
import net.kuujo.copycat.util.serializer.Serializer;

/**
 * Copycat configuration.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class CopycatConfig extends LogConfig {
  private Serializer serializer = new JavaSerializer();

  public CopycatConfig() {
    super();
  }

  public CopycatConfig(String resource) {
    super(resource);
  }

  /**
   * Sets the copycat log serializer.
   *
   * @param serializer The copycat log serializer.
   */
  public void setSerializer(Serializer serializer) {
    this.serializer = Assert.isNotNull(serializer, "serializer");
  }

  /**
   * Returns the copycat log serializer.
   *
   * @return The copycat log serializer.
   */
  public Serializer getSerializer() {
    return serializer;
  }

  /**
   * Sets the copycat log serializer, returning the configuration for method chaining.
   *
   * @param serializer The copycat log serializer.
   * @return The copycat configuration.
   */
  public CopycatConfig withSerializer(Serializer serializer) {
    this.serializer = Assert.isNotNull(serializer, "serializer");
    return this;
  }

}
