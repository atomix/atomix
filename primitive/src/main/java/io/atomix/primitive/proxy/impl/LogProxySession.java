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
package io.atomix.primitive.proxy.impl;

import com.google.common.base.Defaults;
import com.google.common.collect.Maps;
import io.atomix.primitive.PrimitiveException;
import io.atomix.primitive.PrimitiveState;
import io.atomix.primitive.PrimitiveType;
import io.atomix.primitive.event.EventType;
import io.atomix.primitive.event.Events;
import io.atomix.primitive.log.LogSession;
import io.atomix.primitive.operation.OperationId;
import io.atomix.primitive.operation.Operations;
import io.atomix.primitive.partition.PartitionId;
import io.atomix.primitive.proxy.ProxySession;
import io.atomix.primitive.service.PrimitiveService;
import io.atomix.primitive.service.ServiceConfig;
import io.atomix.utils.concurrent.Futures;
import io.atomix.utils.concurrent.ThreadContext;
import io.atomix.utils.serializer.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Function;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Log proxy session.
 */
public class LogProxySession<S> implements ProxySession<S> {
  private final Logger log = LoggerFactory.getLogger(getClass());
  private final String name;
  private final PrimitiveType type;
  private final PrimitiveService service;
  private final Serializer serializer;
  private final LogSession session;
  private final ServiceProxy<S> proxy;
  private final AtomicLong operationIndex = new AtomicLong();
  private final Map<EventType, Method> eventMethods = Maps.newConcurrentMap();
  private final Map<Long, CompletableFuture> futures = Maps.newConcurrentMap();

  @SuppressWarnings("unchecked")
  public LogProxySession(String name, PrimitiveType type, Class<S> serviceType, ServiceConfig serviceConfig, Serializer serializer, LogSession session) {
    this.name = checkNotNull(name, "name cannot be null");
    this.type = checkNotNull(type, "type cannot be null");
    this.service = type.newService(serviceConfig);
    this.serializer = checkNotNull(serializer, "serializer cannot be null");
    this.session = checkNotNull(session, "session cannot be null");
    ServiceProxyHandler serviceProxyHandler = new ServiceProxyHandler(serviceType);
    S serviceProxy = (S) java.lang.reflect.Proxy.newProxyInstance(serviceType.getClassLoader(), new Class[]{serviceType}, serviceProxyHandler);
    proxy = new ServiceProxy<>(serviceProxy, serviceProxyHandler);
  }

  @Override
  public String name() {
    return name;
  }

  @Override
  public PrimitiveType type() {
    return type;
  }

  @Override
  public PartitionId partitionId() {
    return session.partitionId();
  }

  @Override
  public ThreadContext context() {
    return session.context();
  }

  @Override
  public PrimitiveState getState() {
    return session.getState();
  }

  @Override
  public void register(Object client) {
    Events.getEventMap(client.getClass()).forEach((eventType, method) -> eventMethods.put(eventType, method));
  }

  @Override
  public CompletableFuture<Void> accept(Consumer<S> operation) {
    if (session.getState() == PrimitiveState.CLOSED) {
      return Futures.exceptionalFuture(new PrimitiveException.ClosedSession());
    }
    return proxy.accept(operation);
  }

  @Override
  public <R> CompletableFuture<R> apply(Function<S, R> operation) {
    if (session.getState() == PrimitiveState.CLOSED) {
      return Futures.exceptionalFuture(new PrimitiveException.ClosedSession());
    }
    return proxy.apply(operation);
  }

  @Override
  public void addStateChangeListener(Consumer<PrimitiveState> listener) {
    session.addStateChangeListener(listener);
  }

  @Override
  public void removeStateChangeListener(Consumer<PrimitiveState> listener) {
    session.removeStateChangeListener(listener);
  }

  @Override
  public CompletableFuture<ProxySession<S>> connect() {
    return CompletableFuture.completedFuture(this);
  }

  @Override
  public CompletableFuture<Void> close() {
    return CompletableFuture.completedFuture(null);
  }

  @Override
  public CompletableFuture<Void> delete() {
    return CompletableFuture.completedFuture(null);
  }

  /**
   * Returns the serializer for the primitive operations.
   *
   * @return the serializer for the primitive operations
   */
  protected Serializer serializer() {
    return serializer;
  }

  /**
   * Encodes the given object using the configured {@link #serializer()}.
   *
   * @param object the object to encode
   * @param <T>    the object type
   * @return the encoded bytes
   */
  protected <T> byte[] encode(T object) {
    return object != null ? serializer().encode(object) : null;
  }

  /**
   * Decodes the given object using the configured {@link #serializer()}.
   *
   * @param bytes the bytes to decode
   * @param <T>   the object type
   * @return the decoded object
   */
  protected <T> T decode(byte[] bytes) {
    return bytes != null ? serializer().decode(bytes) : null;
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
    private final ThreadLocal<CompletableFuture> future = new ThreadLocal<>();
    private final Map<Method, OperationId> operations = new ConcurrentHashMap<>();

    private ServiceProxyHandler(Class<?> type) {
      this.operations.putAll(Operations.getMethodMap(type));
    }

    @Override
    public Object invoke(Object object, Method method, Object[] args) throws Throwable {
      OperationId operationId = operations.get(method);
      if (operationId != null) {
        CompletableFuture future = new CompletableFuture();
        this.future.set(future);
        long index = operationIndex.incrementAndGet();
        futures.put(index, future);
        session.producer().append(encode(new LogOperation(session.sessionId(), index, operationId, encode(args))));
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
