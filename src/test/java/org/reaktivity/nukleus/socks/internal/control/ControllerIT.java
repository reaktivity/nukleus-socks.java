/**
 * Copyright 2016-2017 The Reaktivity Project
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

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.hamcrest.Matchers.either;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.rules.RuleChain.outerRule;

import java.util.Random;
import java.util.concurrent.ExecutionException;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.DisableOnDebug;
import org.junit.rules.ExpectedException;
import org.junit.rules.TestRule;
import org.junit.rules.Timeout;
import org.kaazing.k3po.junit.annotation.Specification;
import org.kaazing.k3po.junit.rules.K3poRule;
import org.reaktivity.nukleus.socks.internal.SocksController;
import org.reaktivity.reaktor.test.ReaktorRule;

public class ControllerIT
{

    private final K3poRule k3po = new K3poRule()
        .addScriptRoot("route", "org/reaktivity/specification/nukleus/socks/control/route")
        .addScriptRoot("unroute", "org/reaktivity/specification/nukleus/socks/control/unroute");

    private final TestRule timeout = new DisableOnDebug(new Timeout(5, SECONDS));

    private final ReaktorRule reaktor = new ReaktorRule()
        .directory("target/nukleus-itests")
        .commandBufferCapacity(1024)
        .responseBufferCapacity(1024)
        .counterValuesBufferCapacity(1024)
        .controller(SocksController.class::isAssignableFrom);

    @Rule
    public final ExpectedException thrown = ExpectedException.none();

    @Rule
    public final TestRule chain = outerRule(k3po).around(timeout)
        .around(reaktor);

    @Test
    @Specification({
        "${route}/server/domain/nukleus"
    })
    public void shouldRouteServerDomain() throws Exception
    {
        long targetRef = new Random().nextLong();

        k3po.start();

        reaktor.controller(SocksController.class)
            .routeServer("source", 0L, "target", targetRef, "example.com:8080")
            .get();

        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/server/ipv4/nukleus"
    })
    public void shouldRouteServerIpv4() throws Exception
    {
        long targetRef = new Random().nextLong();

        k3po.start();

        reaktor.controller(SocksController.class)
            .routeServer("source", 0L, "target", targetRef, "127.0.0.1:8080")
            .get();

        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/server/ipv6/nukleus"
    })
    public void shouldRouteServerIpv6() throws Exception
    {
        long targetRef = new Random().nextLong();

        k3po.start();

        reaktor.controller(SocksController.class)
            .routeServer("source", 0L, "target", targetRef, "::1:8080")
            .get();

        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/client/domain/nukleus"
    })
    public void shouldRouteClientDomain() throws Exception
    {
        long targetRef = new Random().nextLong();

        k3po.start();

        reaktor.controller(SocksController.class)
            .routeClient("source", 0L, "target", targetRef, "example.com:8080")
            .get();

        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/client/ipv4/nukleus"
    })
    public void shouldRouteClientIpv4() throws Exception
    {
        long targetRef = new Random().nextLong();

        k3po.start();

        reaktor.controller(SocksController.class)
            .routeClient("source", 0L, "target", targetRef, "127.0.0.1:8080")
            .get();

        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/client/ipv6/nukleus"
    })
    public void shouldRouteClientIpv6() throws Exception
    {
        long targetRef = new Random().nextLong();

        k3po.start();

        reaktor.controller(SocksController.class)
            .routeClient("source", 0L, "target", targetRef, "::1:8080")
            .get();

        k3po.finish();
    }
    
    @Test
    @Specification({
        "${route}/server/domain/nukleus",
        "${unroute}/server/domain/nukleus"
    })
    public void shouldUnrouteServerDomain() throws Exception
    {
        long targetRef = new Random().nextLong();

        k3po.start();

        long sourceRef = reaktor.controller(SocksController.class)
            .routeServer("source", 0L, "target", targetRef, "example.com:8080")
            .get();

        k3po.notifyBarrier("ROUTED_SERVER");

        reaktor.controller(SocksController.class)
            .unrouteServer("source", sourceRef, "target", targetRef, "example.com:8080")
            .get();

        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/server/ipv4/nukleus",
        "${unroute}/server/ipv4/nukleus"
    })
    public void shouldUnrouteServerIpv4() throws Exception
    {
        long targetRef = new Random().nextLong();

        k3po.start();

        long sourceRef = reaktor.controller(SocksController.class)
            .routeServer("source", 0L, "target", targetRef, "127.0.0.1:8080")
            .get();

        k3po.notifyBarrier("ROUTED_SERVER");

        reaktor.controller(SocksController.class)
            .unrouteServer("source", sourceRef, "target", targetRef, "127.0.0.1:8080")
            .get();

        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/server/ipv6/nukleus",
        "${unroute}/server/ipv6/nukleus"
    })
    public void shouldUnrouteServerIpv6() throws Exception
    {
        long targetRef = new Random().nextLong();

        k3po.start();

        long sourceRef = reaktor.controller(SocksController.class)
            .routeServer("source", 0L, "target", targetRef, "::1:8080")
            .get();

        k3po.notifyBarrier("ROUTED_SERVER");

        reaktor.controller(SocksController.class)
            .unrouteServer("source", sourceRef, "target", targetRef, "::1:8080")
            .get();

        k3po.finish();
    }

    @Test
    @Specification({
        "${unroute}/server/fails.unknown.route/nukleus"
    })
    public void shouldFailToUnrouteServerWithUnknownAcceptRouteRef() throws Exception
    {
        thrown.expect(either(is(instanceOf(IllegalStateException.class)))
            .or(is(instanceOf(ExecutionException.class))));
        thrown.expectCause(either(nullValue(Exception.class)).or(is(instanceOf(IllegalStateException.class))));
        k3po.start();
        long sourceRef = new Random().nextLong();
        long targetRef = new Random().nextLong();
        reaktor.controller(SocksController.class)
            .unrouteServer("source", sourceRef, "target", targetRef, "example.com:8080")
            .get();

        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/client/domain/nukleus",
        "${unroute}/client/domain/nukleus"
    })
    public void shouldUnrouteClientDomain() throws Exception
    {
        long targetRef = new Random().nextLong();

        k3po.start();

        long sourceRef = reaktor.controller(SocksController.class)
            .routeClient("source", 0L, "target", targetRef, "example.com:8080")
            .get();

        k3po.notifyBarrier("ROUTED_CLIENT");

        reaktor.controller(SocksController.class)
            .unrouteClient("source", sourceRef, "target", targetRef, "example.com:8080")
            .get();

        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/client/ipv4/nukleus",
        "${unroute}/client/ipv4/nukleus"
    })
    public void shouldUnrouteClientIpv4() throws Exception
    {
        long targetRef = new Random().nextLong();

        k3po.start();

        long sourceRef = reaktor.controller(SocksController.class)
            .routeClient("source", 0L, "target", targetRef, "127.0.0.1:8080")
            .get();

        k3po.notifyBarrier("ROUTED_CLIENT");

        reaktor.controller(SocksController.class)
            .unrouteClient("source", sourceRef, "target", targetRef, "127.0.0.1:8080")
            .get();

        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/client/ipv6/nukleus",
        "${unroute}/client/ipv6/nukleus"
    })
    public void shouldUnrouteClientIpv6() throws Exception
    {
        long targetRef = new Random().nextLong();

        k3po.start();

        long sourceRef = reaktor.controller(SocksController.class)
            .routeClient("source", 0L, "target", targetRef, "::1:8080")
            .get();

        k3po.notifyBarrier("ROUTED_CLIENT");

        reaktor.controller(SocksController.class)
            .unrouteClient("source", sourceRef, "target", targetRef, "::1:8080")
            .get();

        k3po.finish();
    }

    @Test
    @Specification({
        "${unroute}/client/fails.unknown.route/nukleus"
    })
    public void shouldFailToUnrouteClientWithUnknownAcceptRouteRef() throws Exception
    {
        thrown.expect(either(is(instanceOf(IllegalStateException.class)))
            .or(is(instanceOf(ExecutionException.class))));
        thrown.expectCause(either(nullValue(Exception.class)).or(is(instanceOf(IllegalStateException.class))));
        k3po.start();
        long sourceRef = new Random().nextLong();
        long targetRef = new Random().nextLong();
        reaktor.controller(SocksController.class)
            .unrouteClient("source", sourceRef, "target", targetRef, "example.com:8080")
            .get();

        k3po.finish();
    }

}
