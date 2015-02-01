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
package net.kuujo.copycat.state;

import net.kuujo.copycat.log.BufferedLog;
import net.kuujo.copycat.test.ResourceTest;
import net.kuujo.copycat.test.TestCluster;

/**
 * State log test.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class StateLogTest extends ResourceTest<StateLog<String>> {

  @Override
  protected TestCluster<StateLog<String>> createCluster() {
    return TestCluster.of((uri, config) -> StateLog.<String>create("test", uri, config, new StateLogConfig().withLog(new BufferedLog())));
  }

}
