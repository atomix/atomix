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
package io.atomix.primitive.impl;

import com.google.common.base.Defaults;
import com.google.common.collect.Maps;
import io.atomix.primitive.AsyncPrimitive;
import io.atomix.primitive.PrimitiveException;
import io.atomix.primitive.PrimitiveRegistry;
import io.atomix.primitive.operation.OperationId;
import io.atomix.primitive.operation.Operations;
import io.atomix.primitive.operation.PrimitiveOperation;
import io.atomix.primitive.partition.PartitionId;
import io.atomix.primitive.proxy.PartitionProxy;
import io.atomix.primitive.proxy.PrimitiveProxy;
import io.atomix.utils.concurrent.Futures;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Stream;

/**
 * Abstract asynchronous primitive that provides proxies.
 */
public abstract class AbstractAsyncPrimitiveProxy<A extends AsyncPrimitive, S> extends AbstractAsyncPrimitive<A> {
  private final Map<PartitionId, ServiceProxy<S>> serviceProxies = Maps.newConcurrentMap();

  @SuppressWarnings("unchecked")
  public AbstractAsyncPrimitiveProxy(Class<S> serviceType, PrimitiveProxy proxy, PrimitiveRegistry registry) {
    super(proxy, registry);
    for (PartitionProxy partition : proxy.getPartitions()) {
      ServiceProxyHandler serviceProxyHandler = new ServiceProxyHandler(serviceType, partition);
      S serviceProxy = (S) java.lang.reflect.Proxy.newProxyInstance(getClass().getClassLoader(), new Class[]{serviceType}, serviceProxyHandler);
      serviceProxies.put(partition.partitionId(), new ServiceProxy<>(serviceProxy, serviceProxyHandler));
    }
  }

  /**
   * Returns the collection of service proxies.
   *
   * @return the collection of service proxies
   */
  private Collection<ServiceProxy<S>> getServiceProxies() {
    return serviceProxies.values();
  }

  /**
   * Returns the service proxy for the given partition ID.
   *
   * @param partitionId the partition ID for which to return the service proxy
   * @return the service proxy for the given partition ID
   */
  private ServiceProxy<S> getServiceProxy(PartitionId partitionId) {
    return serviceProxies.get(partitionId);
  }

  /**
   * Returns the service proxy for the given key.
   *
   * @param key the key for which to return the service proxy
   * @return the service proxy for the given key
   */
  private ServiceProxy<S> getServiceProxy(String key) {
    return getServiceProxy(getProxy().getPartitionId(key));
  }

  /**
   * Submits an empty operation to all partitions.
   *
   * @param operation the operation identifier
   * @return A completable future to be completed with the operation result. The future is guaranteed to be completed after all
   * {@link PrimitiveOperation} submission futures that preceded it.
   * @throws NullPointerException if {@code operation} is null
   */
  protected CompletableFuture<Void> acceptAll(Consumer<S> operation) {
    return Futures.allOf(getServiceProxies().stream().map(proxy -> proxy.accept(operation))).thenApply(v -> null);
  }

  /**
   * Submits an empty operation to all partitions.
   *
   * @param operation the operation identifier
   * @param <R>       the operation result type
   * @return A completable future to be completed with the operation result. The future is guaranteed to be completed after all
   * {@link PrimitiveOperation} submission futures that preceded it.
   * @throws NullPointerException if {@code operation} is null
   */
  protected <R> CompletableFuture<Stream<R>> applyAll(Function<S, R> operation) {
    return Futures.allOf(getServiceProxies().stream().map(proxy -> proxy.apply(operation)));
  }

  /**
   * Submits an empty operation to the given partition.
   *
   * @param partitionId the partition in which to execute the operation
   * @param operation   the operation identifier
   * @return A completable future to be completed with the operation result. The future is guaranteed to be completed after all
   * {@link PrimitiveOperation} submission futures that preceded it.
   * @throws NullPointerException if {@code operation} is null
   */
  protected CompletableFuture<Void> acceptOn(PartitionId partitionId, Consumer<S> operation) {
    return getServiceProxy(partitionId).accept(operation);
  }

  /**
   * Submits an empty operation to the given partition.
   *
   * @param partitionId the partition in which to execute the operation
   * @param operation   the operation identifier
   * @param <R>         the operation result type
   * @return A completable future to be completed with the operation result. The future is guaranteed to be completed after all
   * {@link PrimitiveOperation} submission futures that preceded it.
   * @throws NullPointerException if {@code operation} is null
   */
  protected <R> CompletableFuture<R> applyOn(PartitionId partitionId, Function<S, R> operation) {
    return getServiceProxy(partitionId).apply(operation);
  }

  /**
   * Submits an empty operation to the owning partition for the given key.
   *
   * @param key       the key for which to submit the operation
   * @param operation the operation
   * @return A completable future to be completed with the operation result. The future is guaranteed to be completed after all
   * {@link PrimitiveOperation} submission futures that preceded it.
   * @throws NullPointerException if {@code operation} is null
   */
  protected CompletableFuture<Void> acceptBy(String key, Consumer<S> operation) {
    return getServiceProxy(key).accept(operation);
  }

  /**
   * Submits an empty operation to the owning partition for the given key.
   *
   * @param key       the key for which to submit the operation
   * @param operation the operation
   * @param <R>       the operation result type
   * @return A completable future to be completed with the operation result. The future is guaranteed to be completed after all
   * {@link PrimitiveOperation} submission futures that preceded it.
   * @throws NullPointerException if {@code operation} is null
   */
  protected <R> CompletableFuture<R> applyBy(String key, Function<S, R> operation) {
    return getServiceProxy(key).apply(operation);
  }

  /**
   * Service proxy container.
   */
  private class ServiceProxy<S> {
    private final S proxy;
    private final ServiceProxyHandler handler;

    ServiceProxy(S proxy, ServiceProxyHandler handler) {
      this.proxy = proxy;
      this.handler = handler;
    }

    /**
     * Invokes a void method on the underlying proxy.
     *
     * @param operation the operation to perform on the proxy
     * @return the resulting void future
     */
    CompletableFuture<Void> accept(Consumer<S> operation) {
      operation.accept(proxy);
      return handler.getResultFuture();
    }

    /**
     * Invokes a function on the underlying proxy.
     *
     * @param operation the operation to perform on the proxy
     * @param <T>       the operation return type
     * @return the future result
     */
    <T> CompletableFuture<T> apply(Function<S, T> operation) {
      operation.apply(proxy);
      return handler.getResultFuture();
    }
  }

  /**
   * Service proxy invocation handler.
   * <p>
   * The invocation handler
   */
  private class ServiceProxyHandler implements InvocationHandler {
    private final PartitionProxy proxy;
    private final ThreadLocal<CompletableFuture> future = new ThreadLocal<>();
    private final Map<Method, OperationId> operations = new ConcurrentHashMap<>();

    private ServiceProxyHandler(Class<?> type, PartitionProxy proxy) {
      this.proxy = proxy;
      this.operations.putAll(Operations.getMethodMap(type));
    }

    @Override
    public Object invoke(Object object, Method method, Object[] args) throws Throwable {
      OperationId operationId = operations.get(method);
      if (operationId != null) {
        future.set(proxy.execute(PrimitiveOperation.operation(operationId, encode(args)))
            .thenApply(AbstractAsyncPrimitiveProxy.this::decode));
      } else {
        throw new PrimitiveException("Unknown primitive operation: " + method.getName());
      }
      return Defaults.defaultValue(method.getReturnType());
    }

    /**
     * Returns the result future for the operation.
     *
     * @param <T> the future result type
     * @return the result future
     */
    @SuppressWarnings("unchecked")
    <T> CompletableFuture<T> getResultFuture() {
      return future.get();
    }
  }
}
