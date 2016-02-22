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

import io.atomix.catalyst.serializer.SerializableTypeResolver;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Annotation for specifying resource state and serialization information.
 * <p>
 * {@link Resource}s <em>must</em> be annotated with this annotation to be managed by the Atomix cluster.
 * This annotation provides information about the resource's {@link ResourceStateMachine state machine}
 * and {@link SerializableTypeResolver serialization} of resource commands. Atomix will use the annotation
 * at runtime to create a {@link ResourceType} with which it can identify the resource type and create
 * server-side replicated state machines and register serializable types relevant to the resource.
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
   * The resource state machine class.
   * <p>
   * This is the state machine class that Atomix will construct on each server in the cluster.
   * The state machine is responsible for managing state on each server. Each resource instance
   * has an associated state machine which will last throughout its lifetime.
   */
  Class<? extends ResourceStateMachine> stateMachine();

  /**
   * The resource serializable type resolver.
   * <p>
   * Resource types should provided a {@link SerializableTypeResolver} to specify how to serialize
   * {@link io.atomix.copycat.Command commands} and {@link io.atomix.copycat.Query queries} and any
   * other types of objects required to operate the resource. Atomix will query the type resolver
   * to ensure serializable types are registered on both clients and servers prior to usage of the
   * resource.
   */
  Class<? extends SerializableTypeResolver> typeResolver();

}
