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

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.DisableOnDebug;
import org.junit.rules.TestRule;
import org.junit.rules.Timeout;
import org.kaazing.k3po.junit.annotation.Specification;
import org.kaazing.k3po.junit.rules.K3poRule;
import org.reaktivity.reaktor.test.ReaktorRule;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.rules.RuleChain.outerRule;

public class ControlIT
{
    private final K3poRule k3po = new K3poRule()
        .addScriptRoot("route", "org/reaktivity/specification/nukleus/socks/control/route")
        .addScriptRoot("unroute", "org/reaktivity/specification/nukleus/socks/control/unroute");
    private final TestRule timeout = new DisableOnDebug(new Timeout(5, SECONDS));

    private final ReaktorRule reaktor = new ReaktorRule()
        .directory("target/nukleus-itests")
        .commandBufferCapacity(1024)
        .responseBufferCapacity(1024)
        .counterValuesBufferCapacity(4096)
        .nukleus("socks"::equals);
    @Rule
    public final TestRule chain = outerRule(k3po).around(timeout).around(reaktor);

    @Test
    @Specification({
        "${route}/server/routed.domain/controller"
    })
    public void shouldRouteServerWithDomainAddress() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/server/routed.ipv4/controller"
    })
    public void shouldRouteServerWithIPv4Address() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/server/routed.ipv6/controller"
    })
    public void shouldRouteServerWithIPv6Address() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/client/routed.ipv4/controller"
    })
    public void shouldRouteClientWithIPv4Address() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/client/routed.ipv6/controller"
    })
    public void shouldRouteClientWithIPv6Address() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/server.reverse/routed.domain/controller"
    })
    public void shouldRouteReverseServerWithDomainAddress() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
            "${route}/client.reverse/routed.domain/controller"
    })
    public void shouldRouteReverseClientWithDomainAddress() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
            "${route}/client.reverse/routed.ipv4/controller"
    })
    public void shouldRouteReverseClientWithIPv4Address() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
            "${route}/client.reverse/routed.ipv6/controller"
    })
    public void shouldRouteReverseClientWithIPv6Address() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
            "${unroute}/server/controller",
            "${unroute}/server/nukleus"
    })
    public void shouldUnrouteServer() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_SERVER");
        k3po.finish();
    }

    @Test
    @Specification({
            "${unroute}/unknown/controller"
    })
    public void shouldNotUnrouteUnknown() throws Exception
    {
        k3po.start();
        k3po.finish();
    }

    @Test
    @Specification({
            "${unroute}/client/controller",
            "${unroute}/client/nukleus"
    })
    public void shouldUnrouteClient() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_CLIENT");
        k3po.finish();
    }

    @Test
    @Specification({
            "${unroute}/server.reverse/controller",
            "${unroute}/server.reverse/nukleus"
    })
    public void shouldUnrouteReverseServer() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_SERVER_REVERSE");
        k3po.finish();
    }

    @Test
    @Specification({
            "${unroute}/client.reverse/nukleus",
            "${unroute}/client.reverse/controller"
    })
    public void shouldUnrouteReverseClient() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_CLIENT_REVERSE");
        k3po.finish();
    }


}
