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
package net.kuujo.copycat.cluster.internal;

/**
 * Internal topic constants.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
final class InternalTopics {
  public static final String JOIN = "join";
  public static final String PROMOTE = "promote";
  public static final String LEAVE = "leave";
  public static final String SYNC = "sync";
  public static final String POLL = "poll";
  public static final String VOTE = "vote";
  public static final String APPEND = "append";
  public static final String READ = "read";
  public static final String WRITE = "write";
  public static final String DELETE = "delete";
}
