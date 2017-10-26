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
package org.reaktivity.nukleus.socks.internal.streams.forward.client;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.rules.RuleChain.outerRule;

import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.DisableOnDebug;
import org.junit.rules.TestRule;
import org.junit.rules.Timeout;
import org.kaazing.k3po.junit.annotation.ScriptProperty;
import org.kaazing.k3po.junit.annotation.Specification;
import org.kaazing.k3po.junit.rules.K3poRule;
import org.reaktivity.reaktor.test.ReaktorRule;

/*
 * TODO Externalize bytes sent as Socks version/method/command in implementation
 * Use a mocking tool to enforce client sending wrong values in order to test proper connection close
 */
public class ConnectionIT
{
    private final K3poRule k3po = new K3poRule()
        .addScriptRoot("route", "org/reaktivity/specification/nukleus/socks/control/route")
        .addScriptRoot("client", "org/reaktivity/specification/nukleus/socks/streams/forward")
        .addScriptRoot("server", "org/reaktivity/specification/socks/rfc1928/forward");
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
    @ScriptProperty("serverAccept 'nukleus://target/streams/socks#source'")
    @Specification({
        "${route}/client/controller",
        "${client}/client.connect.send.data/client",
        "${server}/client.connect.send.data/server"
    })
    public void shouldConnectAndSendDataBothWays() throws Exception
    {
        k3po.finish();
    }

    @Test
    @ScriptProperty("serverAccept 'nukleus://target/streams/socks#source'")
    @Specification({
        "${route}/client/controller",
        "${client}/client.connect.send.data.throttling.server.smaller/client",
        "${server}/client.connect.send.data.throttling.server.smaller/server"
    })
    public void shouldAcceptAndSendDataBothWaysWithThrottlingServerSmaller() throws Exception
    {
        k3po.finish();
    }

    @Test
    @ScriptProperty("serverAccept 'nukleus://target/streams/socks#source'")
    @Specification({
        "${route}/client/controller",
        "${client}/client.connect.send.data.throttling.client.smaller/client",
        "${server}/client.connect.send.data.throttling.client.smaller/server"
    })
    public void shouldAcceptAndSendDataBothWaysWithThrottlingClientSmaller() throws Exception
    {
        k3po.finish();
    }

    @Test
    @ScriptProperty("serverAccept 'nukleus://target/streams/socks#source'")
    @Specification({
        "${route}/client/controller",
        "${client}/client.connect.send.data.throttling.window.1/client",
        "${server}/client.connect.send.data.throttling.window.1/server"
    })
    public void shouldAcceptAndSendDataBothWaysWithThrottlingWindow1() throws Exception
    {
        k3po.finish();
    }

    @Test
    @ScriptProperty("serverAccept 'nukleus://target/streams/socks#source'")
    @Specification({
        "${route}/client/controller",
        "${client}/client.connect.send.data.throttling.window.8.padding.7/client",
        "${server}/client.connect.send.data.throttling.window.8.padding.7/server"
    })
    public void shouldAcceptAndSendDataBothWaysWithThrottlingWindow8Padding7() throws Exception
    {
        k3po.finish();
    }

    @Ignore("Sending other method than 0x00 not supported in client")
    @Test
    @ScriptProperty("serverAccept 'nukleus://target/streams/socks#source'")
    @Specification({
        "${route}/client/controller",
        "${client}/client.does.not.connect.no.acceptable.methods/client",
        "${server}/client.does.not.connect.no.acceptable.methods/server"
    })
    public void shouldNotEstablishConnectionNoAcceptableMethods() throws Exception
    {
        k3po.finish();
    }

    @Ignore("Multiple authentication methods not supported in client")
    @Test
    @ScriptProperty("serverAccept 'nukleus://target/streams/socks#source'")
    @Specification({
        "${route}/client/controller",
        "${client}/client.connect.fallback.to.no.authentication/client",
        "${server}/client.connect.fallback.to.no.authentication/server"
    })
    public void shouldEstablishConnectionFallbackToNoAuthentication() throws Exception
    {
        k3po.finish();
    }

    @Ignore("Sending other command than 0x01 not supported in client")
    @Test
    @ScriptProperty("serverAccept 'nukleus://target/streams/socks#source'")
    @Specification({
        "${route}/client/controller",
        "${client}/client.connect.request.with.command.not.supported/client",
        "${server}/client.connect.request.with.command.not.supported/server"
    })
    public void shouldNotEstablishConnectionCommandNotSupported() throws Exception
    {
        k3po.finish();
    }

}
