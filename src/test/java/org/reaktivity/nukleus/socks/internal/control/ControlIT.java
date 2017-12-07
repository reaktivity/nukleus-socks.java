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
import static org.junit.rules.RuleChain.outerRule;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.DisableOnDebug;
import org.junit.rules.TestRule;
import org.junit.rules.Timeout;
import org.kaazing.k3po.junit.annotation.Specification;
import org.kaazing.k3po.junit.rules.K3poRule;
import org.reaktivity.reaktor.test.ReaktorRule;

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
        .counterValuesBufferCapacity(1024)
        .nukleus("socks"::equals);

    @Rule
    public final TestRule chain = outerRule(k3po).around(timeout).around(reaktor);

    @Test
    @Specification({
        "${route}/server/domain/controller"
    })
    public void shouldRouteServerDomain() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/server/ipv4/controller"
    })
    public void shouldRouteServerIpv4() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/server/ipv6/controller"
    })
    public void shouldRouteServerIpv6() throws Exception
    {
        k3po.finish();
    }
    
    @Test
    @Specification({
        "${route}/client/domain/controller"
    })
    public void shouldRouteClientDomain() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/client/ipv4/controller"
    })
    public void shouldRouteClientIpv4() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/client/ipv6/controller"
    })
    public void shouldRouteClientIpv6() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/server/domain/controller",
        "${unroute}/server/domain/controller"
    })
    public void shouldUnrouteServerDomain() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/server/ipv4/controller",
        "${unroute}/server/ipv4/controller"
    })
    public void shouldUnrouteServerIpv4() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/server/ipv6/controller",
        "${unroute}/server/ipv6/controller"
    })
    public void shouldUnrouteServerIpv6() throws Exception
    {
        k3po.finish();
    }


    @Test
    @Specification({
        "${unroute}/server/fails.unknown.route/controller"
    })
    public void shouldUnrouteServerUnknownRoute() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/client/domain/controller",
        "${unroute}/client/domain/controller"
    })
    public void shouldUnrouteClientDomain() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/client/ipv4/controller",
        "${unroute}/client/ipv4/controller"
    })
    public void shouldUnrouteClientIpv4() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/client/ipv6/controller",
        "${unroute}/client/ipv6/controller"
    })
    public void shouldUnrouteClientIpv6() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${unroute}/client/fails.unknown.route/controller"
    })
    public void shouldUnrouteClientUnknownRoute() throws Exception
    {
        k3po.finish();
    }
}
