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

import java.net.Inet4Address;
import java.net.InetAddress;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.reaktivity.nukleus.Controller;
import org.reaktivity.nukleus.ControllerSpi;

import org.reaktivity.nukleus.route.RouteKind;
import org.reaktivity.nukleus.socks.internal.types.Flyweight;
import org.reaktivity.nukleus.socks.internal.types.OctetsFW;
import org.reaktivity.nukleus.socks.internal.types.control.Role;
import org.reaktivity.nukleus.socks.internal.types.control.RouteFW;
import org.reaktivity.nukleus.socks.internal.types.control.UnrouteFW;
import org.reaktivity.nukleus.socks.internal.types.control.SocksRouteExFW;
import org.reaktivity.nukleus.socks.internal.types.SocksAddressFW.Builder;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import static org.reaktivity.nukleus.socks.internal.stream.SocksServerFactory.lookupName;

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
                routeEx = routeExRW.wrap(extensionBuffer, 0, extensionBuffer.capacity())
                                   .address(addressBuilder(lookupName((address))))
                                   .port(port)
                                   .build();
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

    public static Consumer<Builder> addressBuilder(
        InetAddress inet)
    {
        byte[] ip = inet.getAddress();
        Consumer<Builder> addressBuilder = inet instanceof Inet4Address ?
            b -> b.ipv4Address(s -> s.put(ip)):
            b -> b.ipv6Address(s -> s.put(ip));
        return addressBuilder;
    }
}
