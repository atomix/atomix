/*
 * Copyright 2017-present Open Networking Foundation
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
package io.atomix.agent;

import io.atomix.cluster.BootstrapDiscoveryProvider;
import io.atomix.cluster.MulticastDiscoveryProvider;
import io.atomix.core.Atomix;
import io.atomix.core.AtomixConfig;
import io.atomix.rest.ManagedRestService;
import io.atomix.rest.RestService;
import io.atomix.utils.net.Address;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.impl.action.StoreTrueArgumentAction;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.ArgumentType;
import net.sourceforge.argparse4j.inf.Namespace;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Atomix agent runner.
 */
public class AtomixAgent {
  private static final Logger LOGGER = LoggerFactory.getLogger(AtomixAgent.class);

  public static void main(String[] args) throws Exception {
    ArgumentType<Address> addressArgumentType = (argumentParser, argument, value) -> Address.from(value);

    ArgumentParser parser = ArgumentParsers.newArgumentParser("AtomixServer")
        .defaultHelp(true)
        .description("Atomix server");
    parser.addArgument("--member", "-m")
        .type(String.class)
        .nargs("?")
        .required(false)
        .help("The local member identifier, used in intra-cluster communication.");
    parser.addArgument("--address", "-a")
        .type(addressArgumentType)
        .metavar("HOST:PORT")
        .nargs("?")
        .required(false)
        .help("The address for the local member. If no address is specified, the first public interface will be used.");
    parser.addArgument("--host")
        .type(String.class)
        .nargs("?")
        .required(false)
        .help("The host on which this member runs, used for host-aware partition management.");
    parser.addArgument("--rack")
        .type(String.class)
        .nargs("?")
        .required(false)
        .help("The rack on which this member runs, used for rack-aware partition management.");
    parser.addArgument("--zone")
        .type(String.class)
        .nargs("?")
        .required(false)
        .help("The zone in which this member runs, used for zone-aware partition management.");
    parser.addArgument("--config", "-c")
        .metavar("FILE|JSON|YAML")
        .required(false)
        .help("The Atomix configuration. Can be specified as a file path or JSON/YAML string.");
    parser.addArgument("--bootstrap", "-b")
        .nargs("*")
        .type(addressArgumentType)
        .metavar("HOST:PORT")
        .required(false)
        .help("The set of bootstrap members, if any. If bootstrap members are provided then the bootstrap location provider will be used");
    parser.addArgument("--multicast")
        .action(new StoreTrueArgumentAction())
        .setDefault(false)
        .help("Enables multicast discovery. Note that the network must support multicast for this feature to work.");
    parser.addArgument("--multicast-address")
        .type(addressArgumentType)
        .metavar("HOST:PORT")
        .help("Sets the multicast discovery address. Defaults to 230.0.0.1:54321");
    parser.addArgument("--http-port", "-p")
        .type(Integer.class)
        .metavar("PORT")
        .required(false)
        .setDefault(5678)
        .help("Sets the port on which to run the HTTP server. Defaults to 5678");

    Namespace namespace = null;
    try {
      namespace = parser.parseArgs(args);
    } catch (ArgumentParserException e) {
      parser.handleError(e);
      System.exit(1);
    }

    final String configString = namespace.get("config");
    final String memberId = namespace.getString("member");
    final Address address = namespace.get("address");
    final String host = namespace.getString("host");
    final String rack = namespace.getString("rack");
    final String zone = namespace.getString("zone");
    final List<Address> bootstrap = namespace.getList("bootstrap");
    final boolean multicastEnabled = namespace.getBoolean("multicast");
    final Address multicastAddress = namespace.get("multicast_address");
    final Integer httpPort = namespace.getInt("http_port");

    // If a configuration was provided, merge the configuration's member information with the provided command line arguments.
    AtomixConfig config;
    if (configString != null) {
      config = Atomix.config(configString);
    } else {
      config = Atomix.config();
    }

    if (memberId != null) {
      config.getClusterConfig().setMemberId(memberId);
    }

    if (address != null) {
      config.getClusterConfig().setAddress(address);
    }

    if (host != null) {
      config.getClusterConfig().setHost(host);
    }
    if (rack != null) {
      config.getClusterConfig().setRack(rack);
    }
    if (zone != null) {
      config.getClusterConfig().setZone(zone);
    }

    if (bootstrap != null && !bootstrap.isEmpty()) {
      config.getClusterConfig().setMembershipProviderConfig(new BootstrapDiscoveryProvider.Config().setLocations(bootstrap));
    }

    if (multicastEnabled) {
      config.getClusterConfig().setMulticastEnabled(true);
      config.getClusterConfig().setMulticastAddress(multicastAddress);
      if (bootstrap == null || bootstrap.isEmpty()) {
        config.getClusterConfig().setMembershipProviderConfig(new MulticastDiscoveryProvider.Config());
      }
    }

    Atomix atomix = Atomix.builder(config).withShutdownHookEnabled().build();

    atomix.start().join();

    LOGGER.info("Atomix listening at {}:{}", atomix.getMembershipService().getLocalMember().address().host(), atomix.getMembershipService().getLocalMember().address().port());

    ManagedRestService rest = RestService.builder()
        .withAtomix(atomix)
        .withAddress(Address.from(atomix.getMembershipService().getLocalMember().address().host(), httpPort))
        .build();

    rest.start().join();

    LOGGER.info("HTTP server listening at {}:{}", atomix.getMembershipService().getLocalMember().address().address().getHostAddress(), httpPort);

    synchronized (Atomix.class) {
      while (atomix.isRunning()) {
        Atomix.class.wait();
      }
    }
  }
}
