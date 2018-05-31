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
      Map<Method, OperationId> operations = new HashMap<>();
      for (Class<?> iface : serviceInterface.getInterfaces()) {
        operations.putAll(findMethods(iface));
      }
      return operations;
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
      OperationId operationId = getOperationId(method);
      if (operationId != null) {
        if (operations.values().stream().anyMatch(operation -> operation.id().equals(operationId.id()))) {
          throw new IllegalStateException("Duplicate operation name '" + operationId.id() + "'");
        }
        operations.put(method, operationId);
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
      Map<OperationId, Method> operations = new HashMap<>();
      for (Class<?> iface : serviceInterface.getInterfaces()) {
        operations.putAll(findOperations(iface));
      }
      return operations;
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
      OperationId operationId = getOperationId(method);
      if (operationId != null) {
        if (operations.keySet().stream().anyMatch(operation -> operation.id().equals(operationId.id()))) {
          throw new IllegalStateException("Duplicate operation name '" + operationId.id() + "'");
        }
        operations.put(operationId, method);
      }
    }
    for (Class<?> iface : type.getInterfaces()) {
      operations.putAll(findOperations(iface));
    }
    return operations;
  }

  /**
   * Returns the operation ID for the given method.
   *
   * @param method the method for which to lookup the operation ID
   * @return the operation ID for the given method or null if the method is not annotated
   */
  private static OperationId getOperationId(Method method) {
    Command command = method.getAnnotation(Command.class);
    if (command != null) {
      String name = command.value().equals("") ? method.getName() : command.value();
      return OperationId.from(name, OperationType.COMMAND);
    }
    Query query = method.getAnnotation(Query.class);
    if (query != null) {
      String name = query.value().equals("") ? method.getName() : query.value();
      return OperationId.from(name, OperationType.QUERY);
    }
    Operation operation = method.getAnnotation(Operation.class);
    if (operation != null) {
      String name = operation.value().equals("") ? method.getName() : operation.value();
      return OperationId.from(name, operation.type());
    }
    return null;
  }

  private Operations() {
  }
}
