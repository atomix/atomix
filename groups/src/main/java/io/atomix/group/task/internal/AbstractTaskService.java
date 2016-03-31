/*
 * Copyright 2016 the original author or authors.
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
package io.atomix.group.task.internal;

import io.atomix.group.task.TaskConsumer;
import io.atomix.group.task.TaskService;
import io.atomix.group.util.Submitter;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Abstract message service.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public abstract class AbstractTaskService extends AbstractTaskClient implements TaskService {
  private final Map<String, AbstractTaskConsumer> consumers = new ConcurrentHashMap<>();

  public AbstractTaskService(Submitter submitter) {
    super(submitter);
  }

  protected abstract <T> AbstractTaskConsumer<T> createConsumer(String name, TaskConsumer.Options options);

  @Override
  public <T> AbstractTaskConsumer<T> consumer(String name) {
    return consumer(name, null);
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T> AbstractTaskConsumer<T> consumer(String name, TaskConsumer.Options options) {
    AbstractTaskConsumer<T> consumer = consumers.get(name);
    if (consumer == null) {
      synchronized (consumers) {
        consumer = consumers.get(name);
        if (consumer == null) {
          consumer = createConsumer(name, options != null ? options : new TaskConsumer.Options());
          consumers.put(name, consumer);
        }
      }
    }

    if (options != null) {
      consumer.setOptions(options);
    }
    return consumer;
  }

  /**
   * Closes the given consumer.
   *
   * @param consumer The consumer to close.
   */
  void close(AbstractTaskConsumer consumer) {
    consumers.remove(consumer.name());
  }

}
