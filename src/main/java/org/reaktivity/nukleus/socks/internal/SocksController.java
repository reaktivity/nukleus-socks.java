/**
 * Copyright 2016-2019 The Reaktivity Project
 *
 * The Reaktivity Project licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package org.reaktivity.nukleus.socks.internal;

import static java.nio.ByteBuffer.allocateDirect;
import static java.nio.ByteOrder.nativeOrder;

import java.util.Arrays;
import java.util.concurrent.CompletableFuture;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.reaktivity.nukleus.Controller;
import org.reaktivity.nukleus.ControllerSpi;
import org.reaktivity.nukleus.route.RouteKind;
import org.reaktivity.nukleus.socks.internal.types.Flyweight;
import org.reaktivity.nukleus.socks.internal.types.OctetsFW;
import org.reaktivity.nukleus.socks.internal.types.control.Role;
import org.reaktivity.nukleus.socks.internal.types.control.RouteFW;
import org.reaktivity.nukleus.socks.internal.types.control.SocksRouteExFW;
import org.reaktivity.nukleus.socks.internal.types.control.UnrouteFW;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

public final class SocksController implements Controller
{
    private static final int MAX_SEND_LENGTH = 1024;

    private final RouteFW.Builder routeRW = new RouteFW.Builder();
    private final UnrouteFW.Builder unrouteRW = new UnrouteFW.Builder();

    private final SocksRouteExFW.Builder routeExRW = new SocksRouteExFW.Builder();

    private final OctetsFW extensionRO = new OctetsFW().wrap(new UnsafeBuffer(new byte[0]), 0, 0);

    private final ControllerSpi controllerSpi;
    private final MutableDirectBuffer commandBuffer;
    private final MutableDirectBuffer extensionBuffer;
    private final Gson gson;
    private static final Pattern IPV4_ADDRESS_PATTERN =
        Pattern.compile("(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)" +
            "\\.(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)" +
            "\\.(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)" +
            "\\.(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)");
    private static final Pattern IPV6_STD_ADDRESS_PATTERN =
        Pattern.compile("([0-9a-f]{1,4})\\:([0-9a-f]{1,4})\\:" +
            "([0-9a-f]{1,4})\\:([0-9a-f]{1,4})\\:" +
            "([0-9a-f]{1,4})\\:([0-9a-f]{1,4})\\:" +
            "([0-9a-f]{1,4})\\:([0-9a-f]{1,4})");
    private static final Pattern IPV6_HEX_COMPRESSED_VALIDATE_PATTERN =
        Pattern.compile("^((?:[0-9A-Fa-f]{1,4}(?::[0-9A-Fa-f]{1,4})*)?)" +
            "::((?:[0-9A-Fa-f]{1,4}(?::[0-9A-Fa-f]{1,4})*)?)$");
    private static final Pattern IPV6_HEX_COMPRESSED_MATCH_PATTERN =
        Pattern.compile("(?:([0-9a-f]{1,4})\\:?)?(?:([0-9a-f]{1,4})\\:?)?" +
            "(?:([0-9a-f]{1,4})\\:?)?(?:([0-9a-f]{1,4})\\:?)?" +
            "(?:([0-9a-f]{1,4})\\:?)?(?:([0-9a-f]{1,4})\\:?)?" +
            "([0-9a-f]{1,4})?(::)" +
            "(?:(?:([0-9a-f]{1,4})\\:?)??)(?:(?:([0-9a-f]{1,4})\\:?)??)" +
            "(?:(?:([0-9a-f]{1,4})\\:?)??)(?:(?:([0-9a-f]{1,4})\\:?)??)" +
            "(?:(?:([0-9a-f]{1,4})\\:?)??)(?:(?:([0-9a-f]{1,4})\\:?)??)" +
            "(?:(?:([0-9a-f]{1,4})?))");
    private static final Pattern DOMAIN_NAME_MATCH_PATTERN =
        Pattern.compile("^[a-zA-Z]([a-zA-Z0-9-\\.]{0,62}[a-zA-Z0-9])?$");
    private static final ThreadLocal<Matcher> IPV4_ADDRESS_MATCHER =
        ThreadLocal.withInitial(() -> IPV4_ADDRESS_PATTERN.matcher(""));
    private static final ThreadLocal<Matcher> IPV6_STD_ADDRESS_MATCHER =
        ThreadLocal.withInitial(() -> IPV6_STD_ADDRESS_PATTERN.matcher(""));
    private static final ThreadLocal<Matcher> IPV6_HEX_COMPRESSED_VALIDATE_MATCHER =
        ThreadLocal.withInitial(() -> IPV6_HEX_COMPRESSED_VALIDATE_PATTERN.matcher(""));
    private static final ThreadLocal<Matcher> IPV6_HEX_COMPRESSED_MATCHER =
        ThreadLocal.withInitial(() -> IPV6_HEX_COMPRESSED_MATCH_PATTERN.matcher(""));
    private static final ThreadLocal<Matcher> DOMAIN_NAME_MATCHER =
        ThreadLocal.withInitial(() -> DOMAIN_NAME_MATCH_PATTERN.matcher(""));
    private static final ThreadLocal<byte[]> IPV4_ADDRESS_BYTES =
        ThreadLocal.withInitial(() -> new byte[4]);
    private static final ThreadLocal<byte[]> IPV6_ADDRESS_BYTES =
        ThreadLocal.withInitial(() -> new byte[16]);

    public SocksController(
        ControllerSpi controllerSpi)
    {
        this.controllerSpi = controllerSpi;
        this.commandBuffer = new UnsafeBuffer(allocateDirect(MAX_SEND_LENGTH).order(nativeOrder()));
        this.extensionBuffer = new UnsafeBuffer(allocateDirect(MAX_SEND_LENGTH).order(nativeOrder()));
        this.gson = new Gson();
    }

    @Override
    public int process()
    {
        return controllerSpi.doProcess();
    }

    @Override
    public void close() throws Exception
    {
        controllerSpi.doClose();
    }

    @Override
    public Class<SocksController> kind()
    {
        return SocksController.class;
    }

    @Override
    public String name()
    {
        return SocksNukleus.NAME;
    }

    public CompletableFuture<Long> route(
        RouteKind kind,
        String localAddress,
        String remoteAddress)
    {
        return route(kind, localAddress, remoteAddress, null);
    }

    public CompletableFuture<Long> route(
        RouteKind kind,
        String localAddress,
        String remoteAddress,
        String extension)
    {
        Flyweight routeEx = extensionRO;

        if (extension != null)
        {
            final JsonParser parser = new JsonParser();
            final JsonElement element = parser.parse(extension);
            if (element.isJsonObject())
            {
                final JsonObject object = (JsonObject) element;
                final String address = gson.fromJson(object.get("address"), String.class);
                final int port = gson.fromJson(object.get("port"), Integer.class);

                if (IPV4_ADDRESS_MATCHER.get().reset(address).matches())
                {
                    final Matcher ipv4Matcher = IPV4_ADDRESS_MATCHER.get();
                    final byte[] ipv4AddressBytes = IPV4_ADDRESS_BYTES.get();
                    encodeIpv4AddressBytes(ipv4Matcher, ipv4AddressBytes);
                    routeEx = routeExRW.wrap(extensionBuffer, 0, extensionBuffer.capacity())
                        .address(b -> b.ipv4Address(s -> s.set(ipv4AddressBytes)))
                        .port(port)
                        .build();
                }
                else if (IPV6_STD_ADDRESS_MATCHER.get().reset(address).matches())
                {
                    final byte[] addressBytes = IPV6_ADDRESS_BYTES.get();
                    final Matcher ipv6Matcher = IPV6_STD_ADDRESS_MATCHER.get();
                    encodeStandardIpv6AddressBytes(ipv6Matcher, addressBytes);
                    routeEx = routeExRW.wrap(extensionBuffer, 0, extensionBuffer.capacity())
                        .address(b -> b.ipv6Address(s -> s.set(addressBytes)))
                        .port(port)
                        .build();
                }
                else if (IPV6_HEX_COMPRESSED_MATCHER.get().reset(address).matches() &&
                    IPV6_HEX_COMPRESSED_VALIDATE_MATCHER.get().reset(address).matches())
                {
                    final byte[] addressBytes = IPV6_ADDRESS_BYTES.get();
                    final Matcher ipv6Matcher = IPV6_HEX_COMPRESSED_MATCHER.get();
                    encodeCompressedIpv6AddressBytes(ipv6Matcher, addressBytes);
                    routeEx = routeExRW.wrap(extensionBuffer, 0, extensionBuffer.capacity())
                        .address(b -> b.ipv6Address(s -> s.set(addressBytes)))
                        .port(port)
                        .build();
                }
                else if (DOMAIN_NAME_MATCHER.get().reset(address).matches())
                {
                    routeEx = routeExRW.wrap(extensionBuffer, 0, extensionBuffer.capacity())
                        .address(b -> b.domainName(address))
                        .port(port)
                        .build();
                }
            }
        }
        return doRoute(kind, localAddress, remoteAddress, routeEx);
    }

    public CompletableFuture<Void> unroute(
        long routeId)
    {
        final long correlationId = controllerSpi.nextCorrelationId();

        final UnrouteFW unroute = unrouteRW.wrap(commandBuffer, 0, commandBuffer.capacity())
                                           .correlationId(correlationId)
                                           .nukleus(name())
                                           .routeId(routeId)
                                           .build();

        return controllerSpi.doUnroute(unroute.typeId(), unroute.buffer(), unroute.offset(), unroute.sizeof());
    }

    private CompletableFuture<Long> doRoute(
        RouteKind kind,
        String localAddress,
        String remoteAddress,
        Flyweight extension)
    {
        final long correlationId = controllerSpi.nextCorrelationId();
        final Role role = Role.valueOf(kind.ordinal());

        final RouteFW route = routeRW.wrap(commandBuffer, 0, commandBuffer.capacity())
                                     .correlationId(correlationId)
                                     .nukleus(name())
                                     .role(b -> b.set(role))
                                     .localAddress(localAddress)
                                     .remoteAddress(remoteAddress)
                                     .extension(extension.buffer(), extension.offset(), extension.sizeof())
                                     .build();

        return controllerSpi.doRoute(route.typeId(), route.buffer(), route.offset(), route.sizeof());
    }

    private static void fillInBytes(
        byte[] addressBytes,
        int index,
        String groupHex)
    {
        short groupBytes = parseShort(groupHex, 16);
        addressBytes[index++] = (byte) ((groupBytes >> 8) & 0xff);
        addressBytes[index] = (byte) (groupBytes & 0xff);
    }

    private static void encodeCompressedIpv6AddressBytes(
        Matcher ipv6Matcher,
        byte[] addressBytes)
    {
        Arrays.fill(addressBytes, (byte) 0);
        for (int group = 1; group < 8; group++)
        {
            String groupHex = ipv6Matcher.group(group);
            if (groupHex == null)
            {
                break;
            }
            fillInBytes(addressBytes, 2 * (group - 1), groupHex);
        }
        for (int group = 15; group > 8; group--)
        {
            String groupHex = ipv6Matcher.group(group);
            if (groupHex == null)
            {
                break;
            }
            fillInBytes(addressBytes, 2 * (group - 8), groupHex);
        }
    }

    private static void encodeStandardIpv6AddressBytes(
        Matcher ipv6Matcher,
        byte[] addressBytes)
    {
        for (int group = 1; group < ipv6Matcher.groupCount() + 1; group++)
        {
            String groupHex = ipv6Matcher.group(group);
            fillInBytes(addressBytes, 2 * (group - 1), groupHex);
        }
    }

    private static void encodeIpv4AddressBytes(
        Matcher ipv4Matcher,
        byte[] ipv4AddressBytes)
    {
        for (int group = 1; group < ipv4AddressBytes.length + 1; group++)
        {
            ipv4AddressBytes[group - 1] = parseByte(ipv4Matcher.group(group), 10);
        }
    }

    static byte parseByte(
        String s,
        int radix)
    {
        assert s.length() > 0 && s.length() <= 3;
        return (byte) Integer.parseInt(s, radix);
    }

    static short parseShort(
        String s,
        int radix)
    {
        assert s.length() > 0 && s.length() <= 4;
        return (short) Integer.parseInt(s, radix);
    }
}
