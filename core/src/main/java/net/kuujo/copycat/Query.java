/*
 * Copyright 2014 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package net.kuujo.copycat;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * State machine read-only operation annotation.<p>
 *
 * This annotation is used to identify read-only operations in the state machine. It is strongly recommended that
 * users use this annotation for read-only state machine methods. This annotation allows Copycat to identify
 * operations that do not need to be logged or replicated and thus use of this annotation can significantly
 * improve read performance. Alternatively, this annotation should <em>never</em> be used on any method that
 * modifies the state machine's state.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
public @interface Query {

  /**
   * The query name. Defaults to the method name.
   */
  String name() default "";

}
