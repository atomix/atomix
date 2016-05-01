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
package io.atomix.variables;

import io.atomix.copycat.client.CopycatClient;
import io.atomix.resource.AbstractResource;
import io.atomix.resource.ReadConsistency;
import io.atomix.variables.internal.ValueCommands;

import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;

/**
 * Base class for distributed atomic variables.
 * <p>
 * This class provides functionality shared by {@link DistributedValue} and {@link DistributedLong}, including
 * {@link #set(Object) set}, {@link #get() get}, and atomic {@link #compareAndSet(Object, Object) compare-and-set}
 * operations. The methods are closely modeled on those of Java's {@link java.util.concurrent.atomic.AtomicReference}.
 * Operations that modify the resource state are atomic. Operations that read resource state may be atomic
 * depending on the configured {@link ReadConsistency read consistency level}.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
@SuppressWarnings("unchecked")
public abstract class AbstractDistributedValue<T extends AbstractDistributedValue<T, U>, U> extends AbstractResource<T> {

  protected AbstractDistributedValue(CopycatClient client, Properties options) {
    super(client, options);
  }

  /**
   * Gets the current value.
   * <p>
   * The value will be read using the configured {@link ReadConsistency read consistency} level for
   * the resource. For {@link ReadConsistency#ATOMIC atomic reads}, reads will be forwarded to the cluster
   * leader. Weaker reads may be evaluated on non-leader nodes. However, consistency constraints guarantee
   * that state will never go back in time.
   *
   * @return A completable future to be completed with the current value.
   */
  public CompletableFuture<U> get() {
    return client.submit(new ValueCommands.Get<>());
  }

  /**
   * Gets the current value.
   * <p>
   * The value will be read using the provided {@link ReadConsistency read consistency} level.
   * For {@link ReadConsistency#ATOMIC atomic reads}, reads will be forwarded to the cluster leader.
   * Weaker reads may be evaluated on non-leader nodes. However, consistency constraints guarantee
   * that state will never go back in time.
   *
   * @param consistency The read consistency level.
   * @return A completable future to be completed with the current value.
   */
  public CompletableFuture<U> get(ReadConsistency consistency) {
    return client.submit(new ValueCommands.Get<>(consistency.level()));
  }

  /**
   * Sets the value.
   * <p>
   * This is an atomic operation that is guaranteed to take place exactly once between the invocation
   * of the method call and the completion of the returned {@link CompletableFuture}. Once the returned
   * {@link CompletableFuture} is completed, all other instances of the resource can see the state change
   * when writing or reading using {@link ReadConsistency#ATOMIC atomic consistency}.
   *
   * @param value The current value.
   * @return A completable future to be completed once the value has been set.
   */
  public CompletableFuture<Void> set(U value) {
    return client.submit(new ValueCommands.Set(value));
  }

  /**
   * Sets the value with a time-to-live.
   * <p>
   * This is an atomic operation that is guaranteed to take place exactly once between the invocation
   * of the method call and the completion of the returned {@link CompletableFuture}. Once the returned
   * {@link CompletableFuture} is completed, all other instances of the resource can see the state change
   * when writing or reading using {@link ReadConsistency#ATOMIC atomic consistency}.
   * <p>
   * The value set will be expired after the given {@link Duration} so long as it's not overridden
   * by a later call from this or another instance of the resource. The time-to-live on the value
   * is coarse grained, and the value is only guaranteed to expire up to a session timeout after
   * the given time-to-live has expired.
   *
   * @param value The value to set.
   * @param ttl The time after which to expire the value.
   * @return A completable future to be completed once the value has been set.
   */
  public CompletableFuture<Void> set(U value, Duration ttl) {
    return client.submit(new ValueCommands.Set(value, ttl.toMillis()));
  }

  /**
   * Gets the current value and updates it.
   * <p>
   * This is an atomic operation that is guaranteed to take place exactly once between the invocation
   * of the method call and the completion of the returned {@link CompletableFuture}. Once the returned
   * {@link CompletableFuture} is completed, all other instances of the resource can see the state change
   * when writing or reading using {@link ReadConsistency#ATOMIC atomic consistency}.
   * <p>
   * Once the operation is complete, the returned {@link CompletableFuture} will be completed with the
   * value of the resource prior to setting the updated value.
   *
   * @param value The updated value.
   * @return A completable future to be completed with the previous value.
   */
  public CompletableFuture<U> getAndSet(U value) {
    return client.submit(new ValueCommands.GetAndSet<>(value));
  }

  /**
   * Gets the current value and updates it, setting a time-to-live on the updated value.
   * <p>
   * This is an atomic operation that is guaranteed to take place exactly once between the invocation
   * of the method call and the completion of the returned {@link CompletableFuture}. Once the returned
   * {@link CompletableFuture} is completed, all other instances of the resource can see the state change
   * when writing or reading using {@link ReadConsistency#ATOMIC atomic consistency}.
   * <p>
   * Once the operation is complete, the returned {@link CompletableFuture} will be completed with the
   * value of the resource prior to setting the updated value. The value set will be expired after the
   * given {@link Duration} so long as it's not overridden by a later call from this or another instance
   * of the resource. The time-to-live on the value is coarse grained, and the value is only guaranteed
   * to expire up to a session timeout after the given time-to-live has expired.
   *
   * @param value The updated value.
   * @param ttl The time after which to expire the value.
   * @return A completable future to be completed with the previous value.
   */
  public CompletableFuture<U> getAndSet(U value, Duration ttl) {
    return client.submit(new ValueCommands.GetAndSet<>(value, ttl.toMillis()));
  }

  /**
   * Compares the current value and updated it if expected value equals the current value.
   * <p>
   * This is an atomic operation that is guaranteed to take place exactly once between the invocation
   * of the method call and the completion of the returned {@link CompletableFuture}. If the {@code expect}
   * value is equal to the current resource value, the value will be updated and the returned
   * {@link CompletableFuture} will be completed {@code true}, otherwise the value will not be updated and
   * the returned {@link CompletableFuture} will be completed {@code false}. In contrast to Java's atomic
   * variables, value comparisons are done with {@link Object#equals(Object)} rather than {@code ==}. This
   * is necessary because of serialization. Once the returned future is completed, all other instances of
   * the resource can see the state change when writing or reading using
   * {@link ReadConsistency#ATOMIC atomic consistency}.
   *
   * @param expect The expected value.
   * @param update The updated value.
   * @return A completable future to be completed with a boolean value indicating whether the value was updated.
   */
  public CompletableFuture<Boolean> compareAndSet(U expect, U update) {
    return client.submit(new ValueCommands.CompareAndSet(expect, update));
  }

  /**
   * Compares the current value and updated it if expected value equals the current value, setting a time-to-live
   * on the updated value.
   * <p>
   * This is an atomic operation that is guaranteed to take place exactly once between the invocation
   * of the method call and the completion of the returned {@link CompletableFuture}. If the {@code expect}
   * value is equal to the current resource value, the value will be updated and the returned
   * {@link CompletableFuture} will be completed {@code true}, otherwise the value will not be updated and
   * the returned {@link CompletableFuture} will be completed {@code false}. If the value is not updated then
   * the provided time-to-live will not apply to the existing value. In contrast to Java's atomic
   * variables, value comparisons are done with {@link Object#equals(Object)} rather than {@code ==}. This
   * is necessary because of serialization. Once the returned future is completed, all other instances of
   * the resource can see the state change when writing or reading using
   * {@link ReadConsistency#ATOMIC atomic consistency}.
   * <p>
   * If the value is changed, the updated value will be expired after the given {@link Duration} so long
   * as it's not overridden by a later call from this or another instance of the resource. The time-to-live
   * on the value is coarse grained, and the value is only guaranteed to expire up to a session timeout after
   * the given time-to-live has expired.
   *
   * @param expect The expected value.
   * @param update The updated value.
   * @param ttl The time after which to expire the value.
   * @return A completable future to be completed with a boolean value indicating whether the value was updated.
   */
  public CompletableFuture<Boolean> compareAndSet(U expect, U update, Duration ttl) {
    return client.submit(new ValueCommands.CompareAndSet(expect, update, ttl.toMillis()));
  }

}
