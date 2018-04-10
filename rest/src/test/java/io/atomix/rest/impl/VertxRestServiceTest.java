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
package io.atomix.rest.impl;

import io.atomix.cluster.Node;
import io.atomix.core.Atomix;
import io.atomix.protocols.backup.partition.PrimaryBackupPartitionGroup;
import io.atomix.rest.ManagedRestService;
import io.atomix.utils.net.Address;
import io.restassured.builder.RequestSpecBuilder;
import io.restassured.filter.log.RequestLoggingFilter;
import io.restassured.filter.log.ResponseLoggingFilter;
import io.restassured.http.ContentType;
import io.restassured.specification.RequestSpecification;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.net.ServerSocket;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.equalTo;

/**
 * Vert.x REST service test.
 */
public class VertxRestServiceTest {
  private static final int BASE_PORT = 5000;

  private Atomix atomix;
  private ManagedRestService restService;
  private RequestSpecification spec;

  @Test
  public void testCluster() throws Exception {
    given()
        .spec(spec)
        .when()
        .get("cluster/node")
        .then()
        .statusCode(200)
        .assertThat()
        .body("id", equalTo("1"))
        .body("type", equalTo("DATA"))
        .body("host", equalTo(atomix.clusterService().getLocalNode().address().host()))
        .body("port", equalTo(atomix.clusterService().getLocalNode().address().port()))
        .body("status", equalTo("ACTIVE"));

    given()
        .spec(spec)
        .when()
        .get("cluster/nodes")
        .then()
        .statusCode(200)
        .assertThat()
        .body("[0].id", equalTo("1"));
  }

  @Before
  public void beforeTest() throws Exception {
    deleteData();
    atomix = buildAtomix();
    atomix.start().get(30, TimeUnit.SECONDS);
    restService = new VertxRestService(atomix, Address.from("localhost", findAvailablePort(BASE_PORT + 1)));
    restService.start().get(30, TimeUnit.SECONDS);
    spec = new RequestSpecBuilder()
        .setContentType(ContentType.JSON)
        .setBaseUri(String.format("http://%s/v1/", restService.address().toString()))
        .addFilter(new ResponseLoggingFilter())
        .addFilter(new RequestLoggingFilter())
        .build();
  }

  @After
  public void afterTest() throws Exception {
    restService.stop().get(30, TimeUnit.SECONDS);
    atomix.stop().get(30, TimeUnit.SECONDS);
    deleteData();
  }

  protected static Atomix buildAtomix() {
    Node localNode = Node.builder(String.valueOf(1))
        .withType(Node.Type.DATA)
        .withAddress("localhost", findAvailablePort(BASE_PORT))
        .build();

    Collection<Node> nodes = Collections.singletonList(localNode);

    return Atomix.builder()
        .withClusterName("test")
        .withDataDirectory(new File("target/test-logs/1"))
        .withLocalNode(localNode)
        .withNodes(nodes)
        .addPartitionGroup(PrimaryBackupPartitionGroup.builder("data")
            .withNumPartitions(3)
            .build())
        .build();
  }

  protected static int findAvailablePort(int defaultPort) {
    try {
      ServerSocket socket = new ServerSocket(0);
      socket.setReuseAddress(true);
      int port = socket.getLocalPort();
      socket.close();
      return port;
    } catch (IOException ex) {
      return defaultPort;
    }
  }

  protected static void deleteData() throws Exception {
    Path directory = Paths.get("target/test-logs/");
    if (Files.exists(directory)) {
      Files.walkFileTree(directory, new SimpleFileVisitor<Path>() {
        @Override
        public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
          Files.delete(file);
          return FileVisitResult.CONTINUE;
        }

        @Override
        public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
          Files.delete(dir);
          return FileVisitResult.CONTINUE;
        }
      });
    }
  }
}
