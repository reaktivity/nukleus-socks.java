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
package org.reaktivity.nukleus.socks.internal.control;

import static org.reaktivity.nukleus.route.RouteKind.CLIENT;
import static org.reaktivity.nukleus.route.RouteKind.SERVER;

import java.util.concurrent.TimeUnit;

import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.DisableOnDebug;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.junit.rules.Timeout;
import org.kaazing.k3po.junit.annotation.Specification;
import org.kaazing.k3po.junit.rules.K3poRule;
import org.reaktivity.nukleus.socks.internal.SocksController;
import org.reaktivity.reaktor.test.ReaktorRule;

import com.google.gson.Gson;
import com.google.gson.JsonObject;

public class ControllerIT
{
    private final K3poRule k3po = new K3poRule()
        .addScriptRoot("route", "org/reaktivity/specification/nukleus/socks/control/route")
        .addScriptRoot("unroute", "org/reaktivity/specification/nukleus/socks/control/unroute");

    private final TestRule timeout = new DisableOnDebug(new Timeout(5L, TimeUnit.SECONDS));

    private final ReaktorRule reaktor = new ReaktorRule()
        .directory("target/nukleus-itests")
        .commandBufferCapacity(1024)
        .responseBufferCapacity(1024)
        .counterValuesBufferCapacity(4096)
        .controller("socks"::equals);

    @Rule
    public final TestRule chain = RuleChain.outerRule(k3po).around(timeout).around(reaktor);

    private final Gson gson = new Gson();

    @Test
    @Specification({"${route}/server/routed.domain/nukleus"})
    public void shouldRouteServerWithDomainAddress() throws Exception
    {
        k3po.start();

        final JsonObject extension = new JsonObject();
        extension.addProperty("address", "example.com");
        extension.addProperty("port", "8080");

        reaktor.controller(SocksController.class)
               .route(SERVER, "socks#0", "target#0", gson.toJson(extension))
               .get();

        k3po.finish();
    }

    @Test
    @Specification({"${route}/server/routed.ipv4/nukleus"})
    public void shouldRouteServerWithIpv4Address() throws Exception
    {
        k3po.start();

        final JsonObject extension = new JsonObject();
        extension.addProperty("address", "192.168.0.1");
        extension.addProperty("port", "8080");

        reaktor.controller(SocksController.class)
               .route(SERVER, "socks#0", "target#0", gson.toJson(extension))
               .get();

        k3po.finish();
    }

    @Test
    @Specification({"${route}/server/routed.ipv6/nukleus"})
    public void shouldRouteServerWithIpv6Address() throws Exception
    {
        k3po.start();

        final JsonObject extension = new JsonObject();
        extension.addProperty("address", "fd12:3456:789a:1::c6a8:1");
        extension.addProperty("port", "8080");

        reaktor.controller(SocksController.class)
               .route(SERVER, "socks#0", "target#0", gson.toJson(extension))
               .get();

        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/server/routed.domain/nukleus",
        "${unroute}/server/nukleus"})
    public void shouldUnrouteServerWithDomainAddress() throws Exception
    {
        k3po.start();

        final JsonObject extension = new JsonObject();
        extension.addProperty("address", "example.com");
        extension.addProperty("port", "8080");

        long routeId = reaktor.controller(SocksController.class)
                              .route(SERVER, "socks#0", "target#0", gson.toJson(extension))
                              .get();

        k3po.notifyBarrier("ROUTED_SERVER");

        reaktor.controller(SocksController.class)
               .unroute(routeId)
               .get();

        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/server/routed.ipv4/nukleus",
        "${unroute}/server/nukleus"})
    public void shouldUnrouteServerWithIpv4Address() throws Exception
    {
        k3po.start();

        final JsonObject extension = new JsonObject();
        extension.addProperty("address", "192.168.0.1");
        extension.addProperty("port", "8080");

        long routeId = reaktor.controller(SocksController.class)
                              .route(SERVER, "socks#0", "target#0", gson.toJson(extension))
                              .get();

        k3po.notifyBarrier("ROUTED_SERVER");

        reaktor.controller(SocksController.class)
               .unroute(routeId)
               .get();

        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/server/routed.ipv6/nukleus",
        "${unroute}/server/nukleus"})
    public void shouldUnrouteServerWithIpv6Address() throws Exception
    {
        k3po.start();

        final JsonObject extension = new JsonObject();
        extension.addProperty("address", "fd12:3456:789a:1::c6a8:1");
        extension.addProperty("port", "8080");

        long routeId = reaktor.controller(SocksController.class)
                              .route(SERVER, "socks#0", "target#0", gson.toJson(extension))
                              .get();

        k3po.notifyBarrier("ROUTED_SERVER");

        reaktor.controller(SocksController.class)
               .unroute(routeId)
               .get();

        k3po.finish();
    }

    @Test
    @Specification({"${route}/client/routed.domain/nukleus"})
    public void shouldRouteClientWithDomainAddress() throws Exception
    {
        k3po.start();

        final JsonObject extension = new JsonObject();
        extension.addProperty("address", "example.com");
        extension.addProperty("port", "8080");

        reaktor.controller(SocksController.class)
               .route(CLIENT, "socks#0", "target#0", gson.toJson(extension))
               .get();

        k3po.finish();
    }

    @Test
    @Specification({"${route}/client/routed.ipv4/nukleus"})
    public void shouldRouteClientWithIpv4Address() throws Exception
    {
        k3po.start();

        final JsonObject extension = new JsonObject();
        extension.addProperty("address", "192.168.0.1");
        extension.addProperty("port", "8080");

        reaktor.controller(SocksController.class)
               .route(CLIENT, "socks#0", "target#0", gson.toJson(extension))
               .get();

        k3po.finish();
    }

    @Test
    @Specification({"${route}/client/routed.ipv6/nukleus"})
    public void shouldRouteClientWithIpv6Address() throws Exception
    {
        k3po.start();

        final JsonObject extension = new JsonObject();
        extension.addProperty("address", "fd12:3456:789a:1::c6a8:1");
        extension.addProperty("port", "8080");

        reaktor.controller(SocksController.class)
               .route(CLIENT, "socks#0", "target#0", gson.toJson(extension))
               .get();

        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/client/routed.domain/nukleus",
        "${unroute}/client/nukleus"})
    public void shouldUnrouteClientWithDomainAddress() throws Exception
    {
        k3po.start();

        final JsonObject extension = new JsonObject();
        extension.addProperty("address", "example.com");
        extension.addProperty("port", "8080");

        long routeId = reaktor.controller(SocksController.class)
                              .route(CLIENT, "socks#0", "target#0", gson.toJson(extension))
                              .get();

        k3po.notifyBarrier("ROUTED_CLIENT");

        reaktor.controller(SocksController.class)
               .unroute(routeId)
               .get();

        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/client/routed.ipv4/nukleus",
        "${unroute}/client/nukleus"})
    public void shouldUnrouteClientWithIpv4Address() throws Exception
    {
        k3po.start();

        final JsonObject extension = new JsonObject();
        extension.addProperty("address", "192.168.0.1");
        extension.addProperty("port", "8080");

        long routeId = reaktor.controller(SocksController.class)
                              .route(CLIENT, "socks#0", "target#0", gson.toJson(extension))
                              .get();

        k3po.notifyBarrier("ROUTED_CLIENT");

        reaktor.controller(SocksController.class)
               .unroute(routeId)
               .get();

        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/client/routed.ipv6/nukleus",
        "${unroute}/client/nukleus"})
    public void shouldUnrouteClientWithIpv6Address() throws Exception
    {
        k3po.start();

        final JsonObject extension = new JsonObject();
        extension.addProperty("address", "fd12:3456:789a:1::c6a8:1");
        extension.addProperty("port", "8080");

        long routeId = reaktor.controller(SocksController.class)
                              .route(CLIENT, "socks#0", "target#0", gson.toJson(extension))
                              .get();

        k3po.notifyBarrier("ROUTED_CLIENT");

        reaktor.controller(SocksController.class)
               .unroute(routeId)
               .get();

        k3po.finish();
    }
}
