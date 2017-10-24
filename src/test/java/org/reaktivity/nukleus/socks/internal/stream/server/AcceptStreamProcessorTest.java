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
package org.reaktivity.nukleus.socks.internal.stream.server;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.net.Inet4Address;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.collections.Long2ObjectHashMap;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.reaktivity.nukleus.Configuration;
import org.reaktivity.nukleus.buffer.BufferPool;
import org.reaktivity.nukleus.function.MessageConsumer;
import org.reaktivity.nukleus.route.RouteManager;
import org.reaktivity.nukleus.socks.internal.stream.Context;
import org.reaktivity.nukleus.socks.internal.stream.types.SocksCommandRequestFW;
import org.reaktivity.nukleus.socks.internal.stream.types.SocksNegotiationRequestFW;
import org.reaktivity.nukleus.socks.internal.stream.types.SocksNegotiationResponseFW;
import org.reaktivity.nukleus.socks.internal.types.OctetsFW;
import org.reaktivity.nukleus.socks.internal.types.control.Role;
import org.reaktivity.nukleus.socks.internal.types.control.RoleFW;
import org.reaktivity.nukleus.socks.internal.types.control.RouteFW;
import org.reaktivity.nukleus.socks.internal.types.stream.BeginFW;
import org.reaktivity.nukleus.socks.internal.types.stream.DataFW;
import org.reaktivity.nukleus.socks.internal.types.stream.TcpBeginExFW;
import org.reaktivity.nukleus.socks.internal.types.stream.WindowFW;
import org.reaktivity.reaktor.internal.buffer.DefaultBufferPool;

public class AcceptStreamProcessorTest
{
    MessageConsumer acceptThrottle;
    MessageConsumer acceptReply;

    MessageConsumer connect;
    Context context;

    MessageConsumer connectReplyThrottle;

    @Before
    public void init()
    {
        Configuration configuration = mock(Configuration.class);
        RouteManager routeManager = mock(RouteManager.class);
        MutableDirectBuffer writeBuffer = new UnsafeBuffer(ByteBuffer.allocateDirect(1024 * 1024));
        BufferPool bufferPool = new DefaultBufferPool(1024 * 1024, 1024);
        AtomicLong streamId = new AtomicLong(1);
        AtomicLong correlationId = new AtomicLong(1);
        Long2ObjectHashMap correlations = new Long2ObjectHashMap();

        context = new Context(configuration,
            routeManager,
            writeBuffer,
            bufferPool,
            streamId::getAndIncrement,
            correlationId::getAndIncrement,
            correlations,
            this::wrapRoute);

        acceptThrottle = mock(MessageConsumer.class);
        acceptReply = mock(MessageConsumer.class);
        connect = mock(MessageConsumer.class);
        connectReplyThrottle = mock(MessageConsumer.class);

        MutableDirectBuffer routeBuffer = new UnsafeBuffer(ByteBuffer.allocateDirect(1024));
        RouteFW routeFW = new RouteFW.Builder()
            .wrap(routeBuffer, 0, routeBuffer.capacity())
            .correlationId(9L)
            .role(new Consumer<RoleFW.Builder>()
            {
                @Override
                public void accept(RoleFW.Builder builder)
                {
                    builder.set(Role.SERVER);
                }
            })
            .source("source")
            .sourceRef(1001L)
            .target("target")
            .targetRef(500L)

            .build();

        when(routeManager.resolve(any(), any())).thenReturn(routeFW);
        when(routeManager.supplyTarget("source")).thenReturn(acceptReply);
        when(routeManager.supplyTarget("target")).thenReturn(connect);
    }

    private RouteFW wrapRoute(
        int msgTypeId,
        DirectBuffer buffer,
        int index,
        int length)
    {
        return context.routeRO.wrap(buffer, index, index + length);
    }

    @Test
    public void shouldConnectAndSendDataUsingBuffering()
    {
        AcceptStreamProcessor acceptStreamProcessor = new AcceptStreamProcessor(
            context,
            acceptThrottle,
            1,
            1001,
            "source",
            9
        );
        MutableDirectBuffer tmpBuffer = new UnsafeBuffer(ByteBuffer.allocateDirect(1024));
        BeginFW beginFW = new BeginFW.Builder()
            .wrap(tmpBuffer, 0, tmpBuffer.capacity())
            .streamId(1)
            .source("source")
            .sourceRef(1001) // Bi-directional reply
            .correlationId(9)
            .extension(e -> e.reset())
            .build();
        acceptStreamProcessor.handleStream(
            beginFW.typeId(),
            beginFW.buffer(),
            0,
            beginFW.sizeof()
                                          );
        verify(acceptReply).accept(
            anyInt(),
            any(),
            eq(0),
            anyInt()
                                  );
        WindowFW windowFWFromAcceptReply = new WindowFW.Builder()
            .wrap(tmpBuffer, 0, tmpBuffer.capacity())
            .streamId(acceptStreamProcessor.correlation.acceptReplyStreamId())
            .update(AcceptStreamProcessor.MAX_WRITABLE_BYTES)
            .frames(AcceptStreamProcessor.MAX_WRITABLE_FRAMES)
            .build();
        acceptStreamProcessor.handleAcceptReplyThrottle(
            windowFWFromAcceptReply.typeId(),
            windowFWFromAcceptReply.buffer(),
            windowFWFromAcceptReply.offset(),
            windowFWFromAcceptReply.sizeof()
                                                       );

        ArgumentCaptor<Integer> argumentType = ArgumentCaptor.forClass(Integer.class);
        ArgumentCaptor<DirectBuffer> argumentBuffer = ArgumentCaptor.forClass(DirectBuffer.class);
        ArgumentCaptor<Integer> argumentOffset = ArgumentCaptor.forClass(Integer.class);
        ArgumentCaptor<Integer> argumentLength = ArgumentCaptor.forClass(Integer.class);
        verify(acceptThrottle).accept(
            argumentType.capture(),
            argumentBuffer.capture(),
            argumentOffset.capture(),
            argumentLength.capture()
                                     );
        WindowFW windowFW = new WindowFW();
        windowFW.wrap(
            argumentBuffer.getValue(),
            argumentOffset.getValue(),
            argumentLength.getValue());
        assertEquals(AcceptStreamProcessor.MAX_WRITABLE_BYTES, windowFW.update());
        assertEquals(AcceptStreamProcessor.MAX_WRITABLE_FRAMES, windowFW.frames());

        SocksNegotiationRequestFW socksNegotiationRequestFW = new SocksNegotiationRequestFW.Builder()
            .wrap(tmpBuffer, DataFW.FIELD_OFFSET_PAYLOAD, tmpBuffer.capacity())
            .version((byte) 0x05)
            .nmethods((byte) 0x01)
            .method(new byte[]{0x00})
            .build();
        DataFW dataNegotiationRequestFW = new DataFW.Builder()
            .wrap(tmpBuffer, 0, tmpBuffer.capacity())
            .streamId(1)
            .payload(p -> p.set(
                socksNegotiationRequestFW.buffer(),
                socksNegotiationRequestFW.offset(),
                socksNegotiationRequestFW.sizeof()))
            .extension(e -> e.reset())
            .build();
        acceptStreamProcessor.handleStream(
            dataNegotiationRequestFW.typeId(),
            dataNegotiationRequestFW.buffer(),
            dataNegotiationRequestFW.offset(),
            dataNegotiationRequestFW.sizeof()
                                          );

        verify(acceptReply, times(2)).accept(
            argumentType.capture(),
            argumentBuffer.capture(),
            argumentOffset.capture(),
            argumentLength.capture()
                                            );
        DataFW dataFW = new DataFW()
            .wrap(argumentBuffer.getValue(), argumentOffset.getValue(), argumentLength.getValue());

        OctetsFW payload = dataFW.payload();
        DirectBuffer buffer = payload.buffer();
        int limit = payload.limit();
        int offset = payload.offset();

        final SocksNegotiationResponseFW socksNegotiationResponse =
            new SocksNegotiationResponseFW().wrap(buffer, offset, limit);
        assertEquals((byte) 0x05, socksNegotiationResponse.version());
        assertEquals((byte) 0x00, socksNegotiationResponse.method());

        // Reply with Socks version 5 and "NO AUTHENTICATION REQUIRED"
        SocksCommandRequestFW socksConnectRequestFW = new SocksCommandRequestFW.Builder()
            .wrap(tmpBuffer, DataFW.FIELD_OFFSET_PAYLOAD, tmpBuffer.capacity())
            .version((byte) 0x05)
            .command((byte) 0x01) // CONNECT
            .destination("example.com:1234")
            .build();
        DataFW dataConnectRequestFW = context.dataRW.wrap(tmpBuffer, 0, tmpBuffer.capacity())
            .streamId(acceptStreamProcessor.correlation.connectStreamId())
            .payload(p -> p.set(
                socksConnectRequestFW.buffer(),
                socksConnectRequestFW.offset(),
                socksConnectRequestFW.sizeof()))
            .extension(e -> e.reset())
            .build();
        acceptStreamProcessor.handleStream(
            dataConnectRequestFW.typeId(),
            dataConnectRequestFW.buffer(),
            dataConnectRequestFW.offset(),
            dataConnectRequestFW.sizeof()
                                          );

        verify(connect).accept(
            argumentType.capture(),
            argumentBuffer.capture(),
            argumentOffset.capture(),
            argumentLength.capture()
                              );
        BeginFW connectBeginFrame = new BeginFW()
            .wrap(argumentBuffer.getValue(), argumentOffset.getValue(), argumentLength.getValue());

        // System.out.println(connectBeginFrame);

        TcpBeginExFW tcpBeginExFW = new TcpBeginExFW.Builder()
            .wrap(tmpBuffer, 0, tmpBuffer.capacity())
            .localAddress(builder -> builder.ipv4Address(builder1 ->
            {
                try
                {
                    builder1.set(Inet4Address.getByName("127.0.0.1").getAddress());
                } catch (UnknownHostException e)
                {
                    e.printStackTrace();
                }
            }))
            .localPort(65320)
            .remoteAddress(builder -> builder.ipv4Address(builder1 ->
            {
                try
                {
                    builder1.set(Inet4Address.getByName("127.0.0.1").getAddress());
                } catch (UnknownHostException e)
                {
                    e.printStackTrace();
                }
            }))
            .remotePort(8080)
            .build();
        acceptStreamProcessor.correlation.connectReplyThrottle(connectReplyThrottle);
        acceptStreamProcessor.correlation.connectReplyStreamId(10);

        acceptStreamProcessor.transitionToConnectionReady(Optional.of(tcpBeginExFW));


        WindowFW windowFWFromConnect = new WindowFW.Builder()
            .wrap(tmpBuffer, 0, tmpBuffer.capacity())
            .streamId(acceptStreamProcessor.correlation.connectStreamId())
            .update(AcceptStreamProcessor.MAX_WRITABLE_BYTES)
            .frames(AcceptStreamProcessor.MAX_WRITABLE_FRAMES)
            .build();
        acceptStreamProcessor.handleConnectThrottle(
            windowFWFromConnect.typeId(),
            windowFWFromConnect.buffer(),
            windowFWFromConnect.offset(),
            windowFWFromConnect.sizeof());
    }
}
