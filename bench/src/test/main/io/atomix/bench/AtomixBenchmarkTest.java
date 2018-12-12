/*
 * Copyright 2018-present Open Networking Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.atomix.bench;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.atomix.cluster.Node;
import io.atomix.cluster.discovery.BootstrapDiscoveryProvider;
import io.atomix.core.Atomix;
import io.atomix.protocols.raft.partition.RaftPartitionGroup;
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

import java.io.File;
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
import java.util.stream.IntStream;

import static io.restassured.RestAssured.given;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * Benchmark test.
 */
public class AtomixBenchmarkTest {
  private static final int BASE_PORT = 5000;

  private List<Atomix> atomixInstances;
  private List<Atomix> benchInstances;
  private List<RestService> services;
  private List<RequestSpecification> specs;

  @Test
  public void testMapBench() throws Exception {
    JsonNodeFactory jsonFactory = JsonNodeFactory.withExactBigDecimals(true);
    ObjectNode json = jsonFactory.objectNode();
    json.set("type", jsonFactory.textNode("map"));
    json.set("operations", jsonFactory.numberNode(1000));

    testBench(json);
  }

  @Test
  public void testMessagingBench() throws Exception {
    given()
        .spec(specs.get(0))
        .when()
        .get("bench/types")
        .then()
        .statusCode(200);

    given()
        .spec(specs.get(0))
        .when()
        .get("bench/types/map")
        .then()
        .statusCode(200);

    JsonNodeFactory jsonFactory = JsonNodeFactory.withExactBigDecimals(true);
    ObjectNode json = jsonFactory.objectNode();
    json.set("type", jsonFactory.textNode("messaging"));
    json.set("operations", jsonFactory.numberNode(10000));
    json.set("message-size", jsonFactory.numberNode(128));

    testBench(json);
  }

  private void testBench(JsonNode json) throws Exception {
    String testId = given()
        .spec(specs.get(0))
        .contentType(ContentType.JSON)
        .body(json)
        .when()
        .post("bench")
        .then()
        .statusCode(200)
        .extract()
        .body()
        .asString();

    assertNotNull(testId);

    JsonNode progress = given()
        .spec(specs.get(0))
        .when()
        .get("bench/{testId}/progress", testId)
        .then()
        .statusCode(200)
        .extract()
        .body()
        .as(JsonNode.class);

    assertEquals(BenchmarkStatus.RUNNING.name(), progress.get("status").asText());

    do {
      Thread.sleep(1000);
      progress = given()
          .spec(specs.get(0))
          .when()
          .get("bench/{testId}/progress", testId)
          .then()
          .statusCode(200)
          .extract()
          .body()
          .as(JsonNode.class);
    } while (progress.get("status").asText().equals(BenchmarkStatus.RUNNING.name()));

    JsonNode result = given()
        .spec(specs.get(0))
        .when()
        .get("bench/{testId}/result", testId)
        .then()
        .statusCode(200)
        .extract()
        .body()
        .as(JsonNode.class);
    assertEquals(3, result.get("processes").size());
  }

  @Before
  public void beforeTest() throws Exception {
    deleteData();

    // Set up three server nodes.
    List<CompletableFuture<Atomix>> atomixFutures = new ArrayList<>(3);
    atomixInstances = new ArrayList<>(3);
    for (int i = 1; i <= 3; i++) {
      Atomix atomix = buildServer(i, 3);
      atomixFutures.add(atomix.start().thenApply(v -> atomix));
      atomixInstances.add(atomix);
    }
    CompletableFuture.allOf(atomixFutures.toArray(new CompletableFuture[0])).get(30, TimeUnit.SECONDS);

    // Set up three benchmark executor nodes.
    List<CompletableFuture<Atomix>> benchFutures = new ArrayList<>(3);
    benchInstances = new ArrayList<>(3);
    for (int i = 1; i <= 3; i++) {
      Atomix atomix = buildBench(i + 3, 3);
      benchFutures.add(atomix.start().thenApply(v -> atomix));
      benchInstances.add(atomix);
    }
    CompletableFuture.allOf(benchFutures.toArray(new CompletableFuture[0])).get(30, TimeUnit.SECONDS);

    benchInstances.forEach(atomix -> new AtomixBenchmark(atomix).start());

    // Only setup REST servers for the three benchmark nodes.
    List<CompletableFuture<RestService>> serviceFutures = new ArrayList<>(3);
    services = new ArrayList<>(3);
    for (int i = 0; i < 3; i++) {
      ManagedRestService restService = new BenchmarkRestService(benchInstances.get(i), Address.from("localhost", findAvailablePort(BASE_PORT)));
      serviceFutures.add(restService.start());
      services.add(restService);
    }
    CompletableFuture.allOf(serviceFutures.toArray(new CompletableFuture[0])).get(30, TimeUnit.SECONDS);

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
    try {
      List<CompletableFuture<Void>> serviceFutures = new ArrayList<>(3);
      for (RestService service : services) {
        serviceFutures.add(((ManagedRestService) service).stop());
      }
      CompletableFuture.allOf(serviceFutures.toArray(new CompletableFuture[0])).get(30, TimeUnit.SECONDS);
    } finally {
      List<CompletableFuture<Void>> atomixFutures = new ArrayList<>(3);
      for (Atomix instance : atomixInstances) {
        atomixFutures.add(instance.stop());
      }
      CompletableFuture.allOf(atomixFutures.toArray(new CompletableFuture[0])).get(30, TimeUnit.SECONDS);

      List<CompletableFuture<Void>> benchFutures = new ArrayList<>(3);
      for (Atomix instance : atomixInstances) {
        benchFutures.add(instance.stop());
      }
      CompletableFuture.allOf(benchFutures.toArray(new CompletableFuture[0])).get(30, TimeUnit.SECONDS);
      deleteData();
    }
  }

  private Atomix buildServer(int memberId, int members) {
    Collection<Node> nodes = IntStream.range(0, members)
        .mapToObj(i -> Node.builder()
            .withId(String.valueOf(i))
            .withHost("localhost")
            .withPort(5000 + i + 1)
            .build())
        .collect(Collectors.toList());
    return Atomix.builder()
        .withClusterId("test")
        .withMemberId(String.valueOf(memberId))
        .withHost("localhost")
        .withPort(5000 + memberId)
        .withMulticastEnabled()
        .withMembershipProvider(new BootstrapDiscoveryProvider(nodes))
        .withManagementGroup(RaftPartitionGroup.builder("system")
            .withNumPartitions(1)
            .withDataDirectory(new File("target/test-logs/system/" + memberId))
            .withMembers("1", "2", "3")
            .build())
        .addPartitionGroup(RaftPartitionGroup.builder("data")
            .withNumPartitions(3)
            .withPartitionSize(3)
            .withDataDirectory(new File("target/test-logs/data/" + memberId))
            .withMembers("1", "2", "3")
            .build())
        .build();
  }

  private Atomix buildBench(int memberId, int members) {
    Collection<Node> nodes = IntStream.range(0, members)
        .mapToObj(i -> Node.builder()
            .withId(String.valueOf(i))
            .withHost("localhost")
            .withPort(5000 + i + 1)
            .build())
        .collect(Collectors.toList());
    return Atomix.builder()
        .withClusterId("test")
        .withMemberId(String.valueOf(memberId))
        .withHost("localhost")
        .withPort(5000 + memberId)
        .withMulticastEnabled()
        .withMembershipProvider(new BootstrapDiscoveryProvider(nodes))
        .withProperty(BenchmarkConstants.BENCH_NODE_TYPE, Boolean.TRUE.toString())
        .build();
  }

  private static int findAvailablePort(int defaultPort) {
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
