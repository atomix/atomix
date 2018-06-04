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

import io.atomix.cluster.MemberConfig;
import io.atomix.cluster.MemberId;
import io.atomix.core.Atomix;
import io.atomix.core.AtomixConfig;
import io.atomix.rest.ManagedRestService;
import io.atomix.rest.RestService;
import io.atomix.utils.net.Address;
import io.atomix.utils.net.MalformedAddressException;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.impl.action.StoreTrueArgumentAction;
import net.sourceforge.argparse4j.inf.Argument;
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
    ArgumentType<MemberConfig> memberArgumentType = (ArgumentParser argumentParser, Argument argument, String value) -> new MemberConfig()
        .setId(parseMemberId(value))
        .setAddress(parseAddress(value));

    ArgumentType<Address> addressArgumentType = (argumentParser, argument, value) -> Address.from(value);

    ArgumentParser parser = ArgumentParsers.newArgumentParser("AtomixServer")
        .defaultHelp(true)
        .description("Atomix server");
    parser.addArgument("member")
        .type(String.class)
        .nargs("?")
        .metavar("NAME@HOST:PORT")
        .required(false)
        .help("The member info for the local member. This should be in the format [NAME@]HOST[:PORT]. " +
            "If no name is provided, the member name will default to the host. " +
            "If no port is provided, the port will default to 5679.");
    parser.addArgument("--config", "-c")
        .metavar("FILE|JSON|YAML")
        .required(false)
        .help("The Atomix configuration. Can be specified as a file path or JSON/YAML string.");
    parser.addArgument("--bootstrap", "-b")
        .nargs("*")
        .type(memberArgumentType)
        .metavar("NAME@HOST:PORT")
        .required(false)
        .help("The set of core members, if any. When bootstrapping a new cluster, if the local member is a core member " +
            "then it should be present in the core configuration as well.");
    parser.addArgument("--multicast", "-m")
        .action(new StoreTrueArgumentAction())
        .setDefault(false)
        .help("Enables multicast discovery. Note that the network must support multicast for this feature to work.");
    parser.addArgument("--multicast-address", "-a")
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
    final String localMemberInfo = namespace.get("member");
    final List<MemberConfig> bootstrapMembers = namespace.getList("bootstrap");
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

    // If the local member info is specified, attempt to look up the member in the members list.
    // Otherwise, create a new member.
    MemberConfig localMember = null;
    if (localMemberInfo != null) {
      MemberId localMemberId = parseMemberId(localMemberInfo);
      if (localMemberId != null) {
        localMember = config.getClusterConfig().getMembers().stream()
            .filter(member -> member.getId().equals(localMemberId))
            .findFirst()
            .orElse(null);
        if (localMember == null && bootstrapMembers != null) {
          localMember = bootstrapMembers.stream()
              .filter(member -> member.getId().equals(localMemberId))
              .findFirst()
              .orElse(null);
        }
      } else {
        Address localMemberAddress = parseAddress(localMemberInfo);
        localMember = config.getClusterConfig().getMembers().stream()
            .filter(member -> member.getAddress().equals(localMemberAddress))
            .findFirst()
            .orElse(null);
        if (localMember == null && bootstrapMembers != null) {
          localMember = bootstrapMembers.stream()
              .filter(member -> member.getAddress().equals(localMemberAddress))
              .findFirst()
              .orElse(null);
        }
      }

      if (localMember == null) {
        localMember = new MemberConfig()
            .setId(parseMemberId(localMemberInfo))
            .setAddress(parseAddress(localMemberInfo));
      }
    }

    if (localMember != null) {
      config.getClusterConfig().setLocalMember(localMember);
    }

    if (bootstrapMembers != null) {
      config.getClusterConfig().setMembers(bootstrapMembers);
    }

    if (multicastEnabled) {
      config.getClusterConfig().setMulticastEnabled(true);
      config.getClusterConfig().setMulticastAddress(multicastAddress);
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

  static MemberId parseMemberId(String address) {
    int endIndex = address.indexOf('@');
    if (endIndex > 0) {
      return MemberId.from(address.substring(0, endIndex));
    }
    return null;
  }

  static Address parseAddress(String address) {
    int startIndex = address.indexOf('@');
    if (startIndex == -1) {
      try {
        return Address.from(address);
      } catch (MalformedAddressException e) {
        return Address.local();
      }
    } else {
      return Address.from(address.substring(startIndex + 1));
    }
  }
}
