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
package org.reaktivity.nukleus.socks.internal;

import static java.nio.ByteBuffer.allocateDirect;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import java.net.InetAddress;
import java.net.UnknownHostException;

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.Assert;
import org.junit.Test;
import org.reaktivity.nukleus.socks.internal.types.Flyweight;
import org.reaktivity.nukleus.socks.internal.types.control.SocksRouteExFW;

public class SocksControllerTest
{

    private final MutableDirectBuffer buffer = new UnsafeBuffer(allocateDirect(100))
    {
        {
            // Make sure the code is not secretly relying upon memory being initialized to 0
            setMemory(0, capacity(), (byte) 0xFF);
        }
    };
    private final SocksRouteExFW socksRouteExRO = new SocksRouteExFW();

    @Test
    public void shouldParseIPv6Address() throws UnknownHostException
    {
        SocksController socksController = new SocksController(null);
        Flyweight.Builder.Visitor visitRouteEx = socksController.visitRouteEx("[::1]:1234");
        visitRouteEx.visit(buffer, 0, 100);
        socksRouteExRO.wrap(buffer, 0, 100);
        assertEquals(1234, socksRouteExRO.socksPort());
        byte[] extracted = new byte[16];
        socksRouteExRO.socksAddress().ipv6Address().buffer().getBytes(1, extracted);
        assertArrayEquals(InetAddress.getByName("::1").getAddress(), extracted);
    }

    @Test
    public void shouldParseIPv6AddressNoBrackets() throws UnknownHostException
    {
        SocksController socksController = new SocksController(null);
        Flyweight.Builder.Visitor visitRouteEx = socksController.visitRouteEx("0:0:0:0:0:0:0:1:1234");
        visitRouteEx.visit(buffer, 0, 100);
        socksRouteExRO.wrap(buffer, 0, 100);
        assertEquals(1234, socksRouteExRO.socksPort());
        byte[] extracted = new byte[16];
        socksRouteExRO.socksAddress().ipv6Address().buffer().getBytes(1, extracted);
        assertArrayEquals(InetAddress.getByName("::1").getAddress(), extracted);
    }

    @Test
    public void shouldParseIPv4Address() throws UnknownHostException
    {
        SocksController socksController = new SocksController(null);
        Flyweight.Builder.Visitor visitRouteEx = socksController.visitRouteEx("127.0.0.1:1234");
        visitRouteEx.visit(buffer, 0, 100);
        socksRouteExRO.wrap(buffer, 0, 100);
        assertEquals(1234, socksRouteExRO.socksPort());
        byte[] extracted = new byte[4];
        socksRouteExRO.socksAddress().ipv4Address().buffer().getBytes(1, extracted);
        assertArrayEquals(InetAddress.getByName("127.0.0.1").getAddress(), extracted);
    }


    @Test
    public void shouldNotParseIPv4AddressWithBrackets() throws UnknownHostException
    {
        SocksController socksController = new SocksController(null);
        Flyweight.Builder.Visitor visitRouteEx = socksController.visitRouteEx("[127.0.0.1]:1234");
        visitRouteEx.visit(buffer, 0, 100);
        socksRouteExRO.wrap(buffer, 0, 100);
        assertEquals(1234, socksRouteExRO.socksPort());
        assertEquals(3, socksRouteExRO.socksAddress().kind());
    }

    @Test
    public void shouldParseDomainName() throws UnknownHostException
    {
        SocksController socksController = new SocksController(null);
        Flyweight.Builder.Visitor visitRouteEx = socksController.visitRouteEx("example.com:1234");
        visitRouteEx.visit(buffer, 0, 100);
        socksRouteExRO.wrap(buffer, 0, 100);
        assertEquals(1234, socksRouteExRO.socksPort());
        assertEquals("example.com", socksRouteExRO.socksAddress().domainName().asString());
    }

    @Test
    public void shouldParseDomainNameWithSpecialChars() throws UnknownHostException
    {
        SocksController socksController = new SocksController(null);
        Flyweight.Builder.Visitor visitRouteEx = socksController.visitRouteEx("[example.com~]:1234");
        visitRouteEx.visit(buffer, 0, 100);
        socksRouteExRO.wrap(buffer, 0, 100);
        assertEquals(1234, socksRouteExRO.socksPort());
        assertEquals("[example.com~]", socksRouteExRO.socksAddress().domainName().asString());
    }
}
