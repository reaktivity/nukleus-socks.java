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

import com.google.gson.Gson;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.DisableOnDebug;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.junit.rules.Timeout;
import org.kaazing.k3po.junit.annotation.Specification;
import org.kaazing.k3po.junit.rules.K3poRule;
import org.reaktivity.nukleus.route.RouteKind;
import org.reaktivity.nukleus.socks.internal.SocksController;
import org.reaktivity.reaktor.test.ReaktorRule;

import java.util.concurrent.TimeUnit;

public class ControllerIT
{
    private final K3poRule k3po = new K3poRule()
        .addScriptRoot("route", "org/reaktivity/specification/nukleus/socks/control/route")
        .addScriptRoot("unroute", "org/reaktivity/specification/nukleus/socks/control/unroute");
    private final TestRule timeout = new DisableOnDebug(new Timeout(5L, TimeUnit.SECONDS));
    private final ReaktorRule reaktor = (new ReaktorRule()).directory("target/nukleus-itests")
        .commandBufferCapacity(1024)
        .responseBufferCapacity(1024)
        .counterValuesBufferCapacity(4096)
        .controller("socks"::equals);
    private final Gson gson = new Gson();

    @Rule
    public final TestRule chain = RuleChain.outerRule(this.k3po).around(this.timeout).around(this.reaktor);

    @Test
    @Specification({"${route}/server/routed.domain/nukleus"})
    public void shouldRouteServerWithDomainAddress() throws Exception
    {
        this.k3po.start();
        String addressPort = "{\"address\":\"example.com\", \"port\": 8080}";
        this.reaktor.controller(SocksController.class).route(RouteKind.SERVER,
            "socks#0", "target#0", addressPort).get();
        this.k3po.finish();
    }

    @Test
    @Specification({"${route}/server/routed.ipv4/nukleus"})
    public void shouldRouteServerWithIpv4Address() throws Exception
    {
        this.k3po.start();
        String addressPort = "{\"address\":\"192.168.0.1\", \"port\": 8080}";
        this.reaktor.controller(SocksController.class).route(RouteKind.SERVER,
            "socks#0", "target#0", addressPort).get();
        this.k3po.finish();
    }

    @Test
    @Specification({"${route}/server/routed.ipv6/nukleus"})
    public void shouldRouteServerWithIpv6Address() throws Exception
    {
        this.k3po.start();
        String addressPort = "{\"address\":\"fd12:3456:789a:1::c6a8:1\", \"port\": 8080}";
        this.reaktor.controller(SocksController.class).route(RouteKind.SERVER,
            "socks#0", "target#0", addressPort).get();
        this.k3po.finish();
    }

    @Test
    @Specification({
        "${route}/server/routed.domain/nukleus",
        "${unroute}/server/nukleus"})
    public void shouldUnrouteServerWithDomainAddress() throws Exception
    {
        this.k3po.start();
        String addressPort = "{\"address\":\"example.com\", \"port\": 8080}";
        long routeId = this.reaktor.controller(SocksController.class).route(RouteKind.SERVER,
            "socks#0", "target#0", addressPort).get();
        this.k3po.notifyBarrier("ROUTED_SERVER");
        this.reaktor.controller(SocksController.class).unroute(routeId).get();
        this.k3po.finish();
    }

    @Test
    @Specification({
        "${route}/server/routed.ipv4/nukleus",
        "${unroute}/server/nukleus"})
    public void shouldUnrouteServerWithIpv4Address() throws Exception
    {
        this.k3po.start();
        String addressPort = "{\"address\":\"192.168.0.1\", \"port\": 8080}";
        long routeId = this.reaktor.controller(SocksController.class).route(RouteKind.SERVER,
            "socks#0", "target#0", addressPort).get();
        this.k3po.notifyBarrier("ROUTED_SERVER");
        this.reaktor.controller(SocksController.class).unroute(routeId).get();
        this.k3po.finish();
    }

    @Test
    @Specification({
        "${route}/server/routed.ipv6/nukleus",
        "${unroute}/server/nukleus"})
    public void shouldUnrouteServerWithIpv6Address() throws Exception
    {
        this.k3po.start();
        String addressPort = "{\"address\":\"fd12:3456:789a:1::c6a8:1\", \"port\": 8080}";
        long routeId = this.reaktor.controller(SocksController.class).route(RouteKind.SERVER,
            "socks#0", "target#0", addressPort).get();
        this.k3po.notifyBarrier("ROUTED_SERVER");
        this.reaktor.controller(SocksController.class).unroute(routeId).get();
        this.k3po.finish();
    }

    @Test
    @Specification({"${route}/client/routed.domain/nukleus"})
    public void shouldRouteClientWithDomainAddress() throws Exception
    {
        this.k3po.start();
        String addressPort = "{\"address\":\"example.com\", \"port\": 8080}";
        this.reaktor.controller(SocksController.class).route(RouteKind.CLIENT,
            "socks#0", "target#0", addressPort).get();
        this.k3po.finish();
    }

    @Test
    @Specification({"${route}/client/routed.ipv4/nukleus"})
    public void shouldRouteClientWithIpv4Address() throws Exception
    {
        this.k3po.start();
        String addressPort = "{\"address\":\"192.168.0.1\", \"port\": 8080}";
        this.reaktor.controller(SocksController.class).route(RouteKind.CLIENT,
            "socks#0", "target#0", addressPort).get();
        this.k3po.finish();
    }

    @Ignore
    @Test
    @Specification({"${route}/client/routed.ipv6/nukleus"})
    public void shouldRouteClientWithIpv6Address() throws Exception
    {
        this.k3po.start();
        String addressPort = "{\"address\":\"fd12:3456:789a:1::c6a8:1\", \"port\": 8080}";
        this.reaktor.controller(SocksController.class).route(RouteKind.CLIENT,
            "socks#0", "target#0", addressPort).get();
        this.k3po.finish();
    }

    @Test
    @Specification({
        "${route}/client/routed.domain/nukleus",
        "${unroute}/client/nukleus"})
    public void shouldUnrouteClientWithDomainAddress() throws Exception
    {
        this.k3po.start();
        String addressPort = "{\"address\":\"example.com\", \"port\": 8080}";
        long routeId = this.reaktor.controller(SocksController.class).route(RouteKind.CLIENT,
            "socks#0", "target#0", addressPort).get();
        this.k3po.notifyBarrier("ROUTED_CLIENT");
        this.reaktor.controller(SocksController.class).unroute(routeId).get();
        this.k3po.finish();
    }

    @Test
    @Specification({
        "${route}/client/routed.ipv4/nukleus",
        "${unroute}/client/nukleus"})
    public void shouldUnrouteClientWithIpv4Address() throws Exception
    {
        this.k3po.start();
        String addressPort = "{\"address\":\"192.168.0.1\", \"port\": 8080}";
        long routeId = this.reaktor.controller(SocksController.class).route(RouteKind.CLIENT,
            "socks#0", "target#0", addressPort).get();
        this.k3po.notifyBarrier("ROUTED_CLIENT");
        this.reaktor.controller(SocksController.class).unroute(routeId).get();
        this.k3po.finish();
    }

    @Ignore
    @Test
    @Specification({
        "${route}/client/routed.ipv6/nukleus",
        "${unroute}/client/nukleus"})
    public void shouldUnrouteClientWithIpv6Address() throws Exception
    {
        this.k3po.start();
        String addressPort = "{\"address\":\"fd12:3456:789a:1::c6a8:1\", \"port\": 8080}";
        long routeId = this.reaktor.controller(SocksController.class).route(RouteKind.CLIENT,
            "socks#0", "target#0", addressPort).get();
        this.k3po.notifyBarrier("ROUTED_CLIENT");
        this.reaktor.controller(SocksController.class).unroute(routeId).get();
        this.k3po.finish();
    }
}
