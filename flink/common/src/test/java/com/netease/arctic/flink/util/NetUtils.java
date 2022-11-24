/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.netease.arctic.flink.util;

import java.io.IOException;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.UnknownHostException;

/**
 * Copy some useful code from flink to avoid flink dependency.
 */
public class NetUtils {

  /**
   * Find a non-occupied port.
   *
   * @return A non-occupied port.
   */
  public static int getAvailablePort() {
    for (int i = 0; i < 50; i++) {
      try (ServerSocket serverSocket = new ServerSocket(0)) {
        int port = serverSocket.getLocalPort();
        if (port != 0) {
          return port;
        }
      } catch (IOException ignored) {
      }
    }

    throw new RuntimeException("Could not find a free permitted port on the machine.");
  }

  /**
   * Encodes an IP address properly as a URL string. This method makes sure that IPv6 addresses
   * have the proper formatting to be included in URLs.
   *
   * @param address The IP address to encode.
   * @return The proper URL string encoded IP address.
   */
  public static String ipAddressToUrlString(InetAddress address) {
    if (address == null) {
      throw new NullPointerException("address is null");
    } else if (address instanceof Inet4Address) {
      return address.getHostAddress();
    } else {
      throw new IllegalArgumentException("Unrecognized type of InetAddress: " + address);
    }
  }

  /**
   * Normalizes and encodes a hostname and port to be included in URL. In particular, this method
   * makes sure that IPv6 address literals have the proper formatting to be included in URLs.
   *
   * @param host The address to be included in the URL.
   * @param port The port for the URL address.
   * @return The proper URL string encoded IP address and port.
   * @throws java.net.UnknownHostException Thrown, if the hostname cannot be translated into a
   *                                       URL.
   */
  public static String hostAndPortToUrlString(String host, int port) throws UnknownHostException {
    return ipAddressAndPortToUrlString(InetAddress.getByName(host), port);
  }

  /**
   * Encodes an IP address and port to be included in URL. in particular, this method makes sure
   * that IPv6 addresses have the proper formatting to be included in URLs.
   *
   * @param address The address to be included in the URL.
   * @param port    The port for the URL address.
   * @return The proper URL string encoded IP address and port.
   */
  public static String ipAddressAndPortToUrlString(InetAddress address, int port) {
    return ipAddressToUrlString(address) + ':' + port;
  }

}
