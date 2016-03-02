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
package io.atomix.resource;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Annotation for specifying resource state and serialization information.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
public @interface ResourceTypeInfo {

  /**
   * The 8-bit resource type ID.
   * <p>
   * This identifier must be unique across all resource types and should be within the range
   * {@code -128} to {@code 127}. Core Atomix resource types are typically identified with
   * negative numbers, so the range {@code 1} through {@code 127} is reserved for user-provided
   * resource types.
   */
  int id();

  /**
   * The resource factory class.
   * <p>
   * Resources must provide a factory class for constructing instances of the resource for a
   * given configuration. The factory will define the resource instance type and state machine
   * to construct based on the configuration.
   */
  Class<? extends ResourceFactory<?>> factory();

}
