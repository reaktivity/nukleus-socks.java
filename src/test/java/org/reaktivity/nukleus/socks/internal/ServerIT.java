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

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.rules.RuleChain.outerRule;
import static org.reaktivity.reaktor.test.ReaktorRule.EXTERNAL_AFFINITY_MASK;

import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.DisableOnDebug;
import org.junit.rules.TestRule;
import org.junit.rules.Timeout;
import org.kaazing.k3po.junit.annotation.Specification;
import org.kaazing.k3po.junit.rules.K3poRule;
import org.reaktivity.reaktor.test.ReaktorRule;

public class ServerIT
{
    private final K3poRule k3po = new K3poRule()
        .addScriptRoot("route", "org/reaktivity/specification/nukleus/socks/control/route")
        .addScriptRoot("forward", "org/reaktivity/specification/nukleus/socks/streams/forward")
        .addScriptRoot("client", "org/reaktivity/specification/socks");

    private final TestRule timeout = new DisableOnDebug(new Timeout(10, SECONDS));

    private final ReaktorRule reaktor = new ReaktorRule()
        .directory("target/nukleus-itests")
        .commandBufferCapacity(1024)
        .responseBufferCapacity(1024)
        .counterValuesBufferCapacity(4096)
        .nukleus("socks"::equals)
        .affinityMask("target#0", EXTERNAL_AFFINITY_MASK)
        .clean();

    @Rule
    public final TestRule chain = outerRule(reaktor).around(k3po).around(timeout);

    @Test
    @Specification({
        "${route}/server/routed.domain/controller",
        "${forward}/connected.domain/server",
        "${client}/rfc1928/connect/connected.domain/client"
    })
    public void shouldForwardConnectedDomain() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/server/routed.ipv4/controller",
        "${forward}/connected.ipv4/server",
        "${client}/rfc1928/connect/connected.ipv4/client"
    })
    public void shouldConnectIpv4() throws Exception
    {
        k3po.finish();
    }

    @Ignore
    @Test
    @Specification({
        "${route}/server/routed.ipv6/controller",
        "${forward}/connected.ipv6/server",
        "${client}/rfc1928/connect/connected.ipv6/client"
    })
    public void shouldConnectIpv6() throws Exception
    {
        k3po.finish();
    }

    @Ignore
    @Test
    @Specification({
        "${route}/server/controller",
        "${forward}/connected.then.client.abort/server",
        "${client}/rfc1928/connect/connected.then.client.abort/clint"
    })
    public void shouldConnectThenClientAbort() throws Exception
    {
        k3po.finish();
    }

    @Ignore
    @Test
    @Specification({
        "${route}/server/controller",
        "${forward}/connected.then.client.close/server",
        "${client}/rfc1928/connect/connected.then.client.close/client"
    })
    public void shouldConnectThenClientClose() throws Exception
    {
        k3po.finish();
    }

    @Ignore
    @Test
    @Specification({
        "${route}/server/controller",
        "${forward}/connected.then.client.reset/server",
        "${client}/rfc1928/connect/connected.then.client.reset/client"
    })
    public void shouldConnectThenClientReset() throws Exception
    {
        k3po.finish();
    }

    @Ignore
    @Test
    @Specification({
        "${route}/server/controller",
        "${forward}/connected.then.client.write.data/server",
        "${client}/rfc1928/connect/connected.then.client.write.data/client"
    })
    public void shouldConnectThenClientWriteData() throws Exception
    {
        k3po.finish();
    }

    @Ignore
    @Test
    @Specification({
        "${route}/server/controller",
        "${forward}/connected.then.server.abort/server",
        "${client}/rfc1928/connect/connected.then.server.abort/client"
    })
    public void shouldConnectThenServerAbort() throws Exception
    {
        k3po.finish();
    }

    @Ignore
    @Test
    @Specification({
        "${route}/server/controller",
        "${forward}/connected.then.server.close/server",
        "${client}/rfc1928/connect/connected.then.server.close/client"
    })
    public void shouldConnectThenServerClose() throws Exception
    {
        k3po.finish();
    }

    @Ignore
    @Test
    @Specification({
        "${route}/server/controller",
        "${forward}/connected.then.server.reset/server",
        "${client}/rfc1928/connect/connected.then.server.reset/client"
    })
    public void shouldConnectThenServerReset() throws Exception
    {
        k3po.finish();
    }

    @Ignore
    @Test
    @Specification({
        "${route}/server/controller",
        "${forward}/connected.then.server.write.data/server",
        "${client}/rfc1928/connect/connected.then.server.write.data/client"
    })
    public void shouldConnectThenServerWritesData() throws Exception
    {
        k3po.finish();
    }

    @Ignore
    @Test
    @Specification({
        "${route}/server/controller",
        "${client}/rfc1928/connect/rejected.address.type.not.supported/client"
    })
    public void shouldRejectAddressTypeNotSupported() throws Exception
    {
        k3po.finish();
    }

    @Ignore
    @Test
    @Specification({
        "${route}/server/controller",
        "${client}/rfc1928/connect/rejected.connection.not.allowed.by.ruleset/client"
    })
    public void shouldRejectConnectionNotAllowedByRuleset() throws Exception
    {
        k3po.finish();
    }

    @Ignore
    @Test
    @Specification({
        "${route}/server/controller",
        "${client}/rfc1928/connect/rejected.connection.refused/client"
    })
    public void shouldRejectConnectionRefused() throws Exception
    {
        k3po.finish();
    }
}
