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
 * limitations under the License
 */
package io.atomix.manager.options;

import io.atomix.catalyst.serializer.Serializer;
import io.atomix.catalyst.util.PropertiesReader;
import io.atomix.catalyst.util.QualifiedProperties;

import java.util.Properties;

/**
 * Atomix options.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public abstract class AtomixOptions {
  public static final String SERIALIZER = "serializer";

  protected final PropertiesReader reader;

  protected AtomixOptions(Properties properties) {
    this.reader = new PropertiesReader(properties);
  }

  /**
   * Returns the Atomix serializer.
   *
   * @return The Atomix serializer.
   */
  public Serializer serializer() {
    return new Serializer(new QualifiedProperties(reader.properties(), SERIALIZER));
  }

}
