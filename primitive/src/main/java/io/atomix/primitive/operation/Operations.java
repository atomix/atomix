/*
 * Copyright 2018-present Open Networking Foundation
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
package io.atomix.primitive.operation;

import io.atomix.utils.config.ConfigurationException;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;

/**
 * Operation utilities.
 */
public final class Operations {

  /**
   * Returns the collection of operations provided by the given service interface.
   *
   * @param serviceInterface the service interface
   * @return the operations provided by the given service interface
   */
  public static Map<Method, OperationId> getMethodMap(Class<?> serviceInterface) {
    if (!serviceInterface.isInterface()) {
      throw new ConfigurationException("Service type must be an interface");
    }
    return findMethods(serviceInterface);
  }

  /**
   * Recursively finds operations defined by the given type and its implemented interfaces.
   *
   * @param type the type for which to find operations
   * @return the operations defined by the given type and its parent interfaces
   */
  private static Map<Method, OperationId> findMethods(Class<?> type) {
    Map<Method, OperationId> operations = new HashMap<>();
    for (Method method : type.getDeclaredMethods()) {
      Operation operation = method.getAnnotation(Operation.class);
      if (operation != null) {
        String name = operation.value().equals("") ? method.getName() : operation.value();
        operations.put(method, OperationId.from(name, operation.type()));
      }
    }
    for (Class<?> iface : type.getInterfaces()) {
      operations.putAll(findMethods(iface));
    }
    return operations;
  }

  /**
   * Returns the collection of operations provided by the given service interface.
   *
   * @param serviceInterface the service interface
   * @return the operations provided by the given service interface
   */
  public static Map<OperationId, Method> getOperationMap(Class<?> serviceInterface) {
    if (!serviceInterface.isInterface()) {
      throw new ConfigurationException("Service type must be an interface");
    }
    return findOperations(serviceInterface);
  }

  /**
   * Recursively finds operations defined by the given type and its implemented interfaces.
   *
   * @param type the type for which to find operations
   * @return the operations defined by the given type and its parent interfaces
   */
  private static Map<OperationId, Method> findOperations(Class<?> type) {
    Map<OperationId, Method> operations = new HashMap<>();
    for (Method method : type.getDeclaredMethods()) {
      Operation operation = method.getAnnotation(Operation.class);
      if (operation != null) {
        String name = operation.value().equals("") ? method.getName() : operation.value();
        operations.put(OperationId.from(name, operation.type()), method);
      }
    }
    for (Class<?> iface : type.getInterfaces()) {
      operations.putAll(findOperations(iface));
    }
    return operations;
  }

  private Operations() {
  }
}
