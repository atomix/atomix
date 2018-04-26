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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import io.atomix.cluster.Member;
import io.atomix.core.Atomix;
import io.atomix.protocols.backup.partition.PrimaryBackupPartitionGroup;
import io.atomix.rest.ManagedRestService;
import io.atomix.rest.RestService;
import io.atomix.utils.net.Address;
import io.restassured.builder.RequestSpecBuilder;
import io.restassured.filter.log.RequestLoggingFilter;
import io.restassured.filter.log.ResponseLoggingFilter;
import io.restassured.http.ContentType;
import io.restassured.specification.RequestSpecification;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.ServerSocket;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.equalTo;

/**
 * Vert.x REST service test.
 */
public class VertxRestServiceTest {
  private static final int BASE_PORT = 5000;

  private List<Atomix> instances;
  private List<RestService> services;
  private List<RequestSpecification> specs;

  @Test
  public void testStatus() throws Exception {
    given()
        .spec(new RequestSpecBuilder()
            .setContentType(ContentType.TEXT)
            .setBaseUri(String.format("http://%s/", services.get(0).address().toString()))
            .addFilter(new ResponseLoggingFilter())
            .addFilter(new RequestLoggingFilter())
            .build())
        .when()
        .get("status")
        .then()
        .statusCode(200);
  }

  @Test
  public void testCluster() throws Exception {
    given()
        .spec(specs.get(0))
        .when()
        .get("cluster/node")
        .then()
        .statusCode(200)
        .assertThat()
        .body("id", equalTo("1"))
        .body("type", equalTo("DATA"))
        .body("host", equalTo(instances.get(0).membershipService().getLocalMember().address().host()))
        .body("port", equalTo(instances.get(0).membershipService().getLocalMember().address().port()))
        .body("status", equalTo("ACTIVE"));

    given()
        .spec(specs.get(0))
        .when()
        .get("cluster/nodes")
        .then()
        .statusCode(200)
        .assertThat()
        .body("[0].id", equalTo("1"));
  }

  @Test
  public void testEvents() throws Exception {
    String id = given()
        .spec(specs.get(0))
        .when()
        .post("events/test/subscribers")
        .then()
        .statusCode(200)
        .extract()
        .body()
        .asString();

    given()
        .spec(specs.get(1))
        .body("Hello world!")
        .when()
        .post("events/test")
        .then()
        .statusCode(200);

    given()
        .spec(specs.get(0))
        .when()
        .get("events/test/subscribers/" + id)
        .then()
        .statusCode(200)
        .assertThat()
        .body(equalTo("Hello world!"));
  }

  @Test
  public void testMessages() throws Exception {
    String id = given()
        .spec(specs.get(0))
        .when()
        .post("messages/test/subscribers")
        .then()
        .statusCode(200)
        .extract()
        .body()
        .asString();

    given()
        .spec(specs.get(1))
        .body("Hello world!")
        .when()
        .post("messages/test")
        .then()
        .statusCode(200);

    given()
        .spec(specs.get(0))
        .when()
        .get("messages/test/subscribers/" + id)
        .then()
        .statusCode(200)
        .assertThat()
        .body(equalTo("Hello world!"));

    given()
        .spec(specs.get(1))
        .body("Hello world again!")
        .when()
        .post("messages/test/" + instances.get(0).membershipService().getLocalMember().id())
        .then()
        .statusCode(200);

    given()
        .spec(specs.get(0))
        .when()
        .get("messages/test/subscribers/" + id)
        .then()
        .statusCode(200)
        .assertThat()
        .body(equalTo("Hello world again!"));
  }

  @Test
  public void testMap() throws Exception {
    JsonNode json = JsonNodeFactory.withExactBigDecimals(true).objectNode()
        .put("type", "consistent-map")
        .put("cache-enabled", true)
        .put("null-values", false);

    given()
        .spec(specs.get(0))
        .contentType(ContentType.JSON)
        .body(json)
        .when()
        .post("primitives/test")
        .then()
        .statusCode(200);

    given()
        .spec(specs.get(1))
        .when()
        .get("primitives/test/foo")
        .then()
        .statusCode(200);

    given()
        .spec(specs.get(0))
        .body("Hello world!")
        .when()
        .put("primitives/test/foo")
        .then()
        .statusCode(200);

    given()
        .spec(specs.get(1))
        .when()
        .get("primitives/test/foo")
        .then()
        .statusCode(200)
        .assertThat()
        .body("value", equalTo("Hello world!"));
  }

  @Before
  public void beforeTest() throws Exception {
    deleteData();

    List<CompletableFuture<Atomix>> instanceFutures = new ArrayList<>(3);
    instances = new ArrayList<>(3);
    for (int i = 1; i <= 3; i++) {
      Atomix atomix = buildAtomix(i);
      instanceFutures.add(atomix.start());
      instances.add(atomix);
    }
    CompletableFuture.allOf(instanceFutures.toArray(new CompletableFuture[instanceFutures.size()])).get(30, TimeUnit.SECONDS);

    List<CompletableFuture<RestService>> serviceFutures = new ArrayList<>(3);
    services = new ArrayList<>(3);
    for (int i = 0; i < 3; i++) {
      ManagedRestService restService = new VertxRestService(instances.get(i), Address.from("localhost", findAvailablePort(BASE_PORT)));
      serviceFutures.add(restService.start());
      services.add(restService);
    }
    CompletableFuture.allOf(serviceFutures.toArray(new CompletableFuture[serviceFutures.size()])).get(30, TimeUnit.SECONDS);

    specs = new ArrayList<>(3);
    for (int i = 0; i < 3; i++) {
      RequestSpecification spec = new RequestSpecBuilder()
          .setContentType(ContentType.TEXT)
          .setBaseUri(String.format("http://%s/v1/", services.get(i).address().toString()))
          .addFilter(new ResponseLoggingFilter())
          .addFilter(new RequestLoggingFilter())
          .build();
      specs.add(spec);
    }
  }

  @After
  public void afterTest() throws Exception {
    List<CompletableFuture<Void>> serviceFutures = new ArrayList<>(3);
    for (RestService service : services) {
      serviceFutures.add(((ManagedRestService) service).stop());
    }
    CompletableFuture.allOf(serviceFutures.toArray(new CompletableFuture[serviceFutures.size()])).get(30, TimeUnit.SECONDS);

    List<CompletableFuture<Void>> instanceFutures = new ArrayList<>(3);
    for (Atomix instance : instances) {
      instanceFutures.add(instance.stop());
    }
    CompletableFuture.allOf(instanceFutures.toArray(new CompletableFuture[instanceFutures.size()])).get(30, TimeUnit.SECONDS);
    deleteData();
  }

  protected Atomix buildAtomix(int memberId) {
    Member localMember = Member.builder(String.valueOf(memberId))
        .withType(Member.Type.EPHEMERAL)
        .withAddress("localhost", findAvailablePort(BASE_PORT))
        .build();

    Collection<Member> members = Stream.concat(Stream.of(localMember), instances.stream().map(instance -> instance.membershipService().getLocalMember()))
        .collect(Collectors.toList());

    return Atomix.builder()
        .withClusterName("test")
        .withLocalMember(localMember)
        .withMembers(members)
        .withSystemPartitionGroup(PrimaryBackupPartitionGroup.builder("system")
            .withNumPartitions(1)
            .build())
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
