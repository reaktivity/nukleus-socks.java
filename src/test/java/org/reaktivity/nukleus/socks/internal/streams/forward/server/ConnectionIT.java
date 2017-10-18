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
package org.reaktivity.nukleus.socks.internal.streams.forward.server;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.rules.RuleChain.outerRule;

import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.DisableOnDebug;
import org.junit.rules.TestRule;
import org.junit.rules.Timeout;
import org.kaazing.k3po.junit.annotation.Specification;
import org.kaazing.k3po.junit.rules.K3poRule;
import org.reaktivity.reaktor.test.ReaktorRule;

public class ConnectionIT
{
    private final K3poRule k3po = new K3poRule()
        .addScriptRoot("route", "org/reaktivity/specification/nukleus/socks/control/route")
        .addScriptRoot("client", "org/reaktivity/specification/socks/rfc1928/forward")
        .addScriptRoot("server", "org/reaktivity/specification/nukleus/socks/streams/forward");
    private final TestRule timeout = new DisableOnDebug(new Timeout(10, SECONDS));

    private final ReaktorRule reaktor = new ReaktorRule()
        .directory("target/nukleus-itests")
        .commandBufferCapacity(1024)
        .responseBufferCapacity(1024)
        .counterValuesBufferCapacity(1024)
        .nukleus("socks"::equals)
        .clean();

    @Rule
    public final TestRule chain = outerRule(reaktor).around(k3po)
        .around(timeout);

    @Test
    @Specification({
        "${route}/server/controller",
        "${client}/client.connect.send.data/client",
        "${server}/client.connect.send.data/server"})
    public void shouldAcceptAndSendDataBothWays() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/server/controller",
        "${client}/client.connect.send.data.throttling/client",
        "${server}/client.connect.send.data.throttling/server"})
    public void shouldAcceptAndSendDataBothWaysWithThrottling() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/server/controller",
        "${client}/client.does.not.connect.no.acceptable.methods/client"})
    public void shouldNotEstablishConnectionNoAcceptableMethods() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/server/controller",
        "${client}/client.connect.fallback.to.no.authentication/client",
        "${server}/client.connect.fallback.to.no.authentication/server"})
    public void shouldEstablishConnectionFallbackToNoAuthentication() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/server/controller",
        "${client}/client.connect.request.with.command.not.supported/client"})
    public void shouldNotEstablishConnectionCommandNotSupported() throws Exception
    {
        k3po.finish();
    }
}
