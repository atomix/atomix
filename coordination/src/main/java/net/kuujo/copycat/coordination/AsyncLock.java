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
 * limitations under the License.
 */
package net.kuujo.copycat.coordination;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * Asynchronous lock.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public interface AsyncLock {

  /**
   * Acquires the lock.
   *
   * @return A completable future to be completed once the lock has been acquired.
   */
  CompletableFuture<Void> lock();

  /**
   * Acquires the lock if it's free.
   *
   * @return A completable future to be completed with a boolean indicating whether the lock was acquired.
   */
  CompletableFuture<Boolean> tryLock();

  /**
   * Acquires the lock if it's free within the given timeout.
   *
   * @param time The time within which to acquire the lock in milliseconds.
   * @return A completable future to be completed with a boolean indicating whether the lock was acquired.
   */
  CompletableFuture<Boolean> tryLock(long time);

  /**
   * Acquires the lock if it's free within the given timeout.
   *
   * @param time The time within which to acquire the lock.
   * @param unit The time unit.
   * @return A completable future to be completed with a boolean indicating whether the lock was acquired.
   */
  CompletableFuture<Boolean> tryLock(long time, TimeUnit unit);

  /**
   * Releases the lock.
   *
   * @return A completable future to be completed once the lock has been released.
   */
  CompletableFuture<Void> unlock();

}
