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

import java.util.function.Consumer;

import org.agrona.DirectBuffer;
import org.reaktivity.nukleus.function.MessageConsumer;
import org.reaktivity.nukleus.socks.internal.metadata.State;
import org.reaktivity.nukleus.socks.internal.stream.AbstractStreamHandler;
import org.reaktivity.nukleus.socks.internal.stream.Context;
import org.reaktivity.nukleus.socks.internal.stream.Correlation;
import org.reaktivity.nukleus.socks.internal.stream.types.SocksCommandResponseFW;
import org.reaktivity.nukleus.socks.internal.types.stream.AbortFW;
import org.reaktivity.nukleus.socks.internal.types.stream.BeginFW;
import org.reaktivity.nukleus.socks.internal.types.stream.DataFW;
import org.reaktivity.nukleus.socks.internal.types.stream.EndFW;
import org.reaktivity.nukleus.socks.internal.types.stream.ResetFW;
import org.reaktivity.nukleus.socks.internal.types.stream.TcpBeginExFW;
import org.reaktivity.nukleus.socks.internal.types.stream.WindowFW;

final class ConnectReplyStreamHandler extends AbstractStreamHandler
{
    private final MessageConsumer connectReplyThrottle;
    private final long connectReplyId;

    private MessageConsumer acceptReplyEndpoint;
    private long acceptReplyStreamId;

    private MessageConsumer streamState;

    private int targetWindowBytes;
    private int targetWindowFrames;
    private int targetWindowBytesAdjustment;
    private int targetWindowFramesAdjustment;

    private Consumer<WindowFW> windowHandler;

    ConnectReplyStreamHandler(
        Context context,
        MessageConsumer connectReplyThrottle,
        long connectReplyId)
    {
        super(context);
        this.connectReplyThrottle = connectReplyThrottle;
        this.connectReplyId = connectReplyId;
        this.streamState = this::beforeBegin;
    }

    protected void handleStream(
        int msgTypeId,
        DirectBuffer buffer,
        int index,
        int length)
    {
        streamState.accept(msgTypeId, buffer, index, length);
    }

    @State
    private void beforeBegin(
        int msgTypeId,
        DirectBuffer buffer,
        int index,
        int length)
    {
        if (msgTypeId == BeginFW.TYPE_ID)
        {
            final BeginFW begin = context.beginRO.wrap(buffer, index, index + length);
            handleBegin(begin);
        }
        else
        {
            doReset(connectReplyThrottle, connectReplyId);
        }
    }

    @State
    private void afterBeginOrData(
        int msgTypeId,
        DirectBuffer buffer,
        int index,
        int length)
    {
        switch (msgTypeId)
        {
        case DataFW.TYPE_ID:
            final DataFW data = context.dataRO.wrap(buffer, index, index + length);
            handleData(data);
            break;
        case EndFW.TYPE_ID:
            final EndFW end = context.endRO.wrap(buffer, index, index + length);
            handleEnd(end);
            break;
        case AbortFW.TYPE_ID:
            final AbortFW abort = context.abortRO.wrap(buffer, index, index + length);
            handleAbort(abort);
            break;
        default:
            doReset(connectReplyThrottle, connectReplyId);
            break;
        }
    }

    private void handleBegin(BeginFW begin)
    {
        final long connectRef = begin.sourceRef();
        final long correlationId = begin.correlationId();

        final Correlation correlation = context.correlations.remove(correlationId);

        if (connectRef == 0L && correlation != null)
        {
            final String acceptReplyName = correlation.acceptSourceName();
            System.out.println(acceptReplyName);
            // this.acceptReplyEndpoint = context.router.supplyTarget(acceptReplyName);
            final MessageConsumer newAcceptReply = context.router.supplyTarget(acceptReplyName);
            final long newAcceptReplyId = context.supplyStreamId.getAsLong();
            final long newCorrelationId = correlation.acceptCorrelationId();

            // TODO get the SocksCommandRequest from the Correlation
            // TODO use it to populate a SocksCommandResponse and sent it on the acceptReplyStream
            final TcpBeginExFW tcpBeginEx = begin.extension()
                .get(context.tcpBeginExRO::wrap);

            System.out.println("localAddress: " + tcpBeginEx.localAddress()
                .toString());
            System.out.println("localPort: " + tcpBeginEx.localPort());
            // TODO transform the TcpBeginExFW into the bound address and port of SocksCommandResponseFW

            byte socksAtyp;
            byte[] socksAddr;
            int socksPort = tcpBeginEx.localPort();
            if (tcpBeginEx.localAddress().kind() == 1)
            {
                socksAtyp = (byte) 0x01;
                socksAddr = new byte[4];
            }
            else
            {
                socksAtyp = (byte) 0x04;
                socksAddr = new byte[16];
            }
            tcpBeginEx.localAddress()
                .ipv4Address()
                .get((directBuffer, offset, limit) ->
                {
                    directBuffer.getBytes(offset, socksAddr);
                    return null;
                });

            long connectStreamId = correlation.connectStreamId();
            MessageConsumer connectEndpoint = correlation.connectEndpoint();

            SocksCommandResponseFW socksConnectResponseFW = context.socksConnectionResponseRW
                .wrap(context.writeBuffer, DataFW.FIELD_OFFSET_PAYLOAD, context.writeBuffer.capacity())
                .version((byte) 0x05)
                .reply((byte) 0x00) // CONNECT
                .bind(socksAtyp, socksAddr, socksPort)
                .build();
            DataFW dataReplyFW = context.dataRW.wrap(context.writeBuffer, 0, context.writeBuffer.capacity())
                .streamId(newAcceptReplyId)
                .payload(p -> p.set(
                    socksConnectResponseFW.buffer(),
                    socksConnectResponseFW.offset(),
                    socksConnectResponseFW.sizeof()))
                .extension(e -> e.reset())
                .build();
            newAcceptReply.accept(
                dataReplyFW.typeId(),
                dataReplyFW.buffer(),
                dataReplyFW.offset(),
                dataReplyFW.sizeof());

            // doWindow(connectReplyThrottle, connectReplyStreamId, 1024, 1024); // TODO remove hardcoded values

            this.acceptReplyEndpoint = newAcceptReply;
            this.acceptReplyStreamId = newAcceptReplyId;

            this.streamState = this::afterBeginOrData;
            this.windowHandler = this::processInitialWindow;
        }
        else
        {
            doReset(connectReplyThrottle, connectReplyId);
        }
    }

    private void handleData(DataFW data)
    {
    }

    private void handleEnd(EndFW end)
    {
    }

    private void handleAbort(AbortFW abort)
    {
    }

    private void handleThrottle(
        int msgTypeId,
        DirectBuffer buffer,
        int index,
        int length)
    {
        switch (msgTypeId)
        {
        case WindowFW.TYPE_ID:
            final WindowFW window = context.windowRO.wrap(buffer, index, index + length);
            this.windowHandler.accept(window);
            break;
        case ResetFW.TYPE_ID:
            final ResetFW reset = context.resetRO.wrap(buffer, index, index + length);
            handleReset(reset);
            break;
        default:
            // ignore
            break;
        }
    }

    private void processInitialWindow(WindowFW window)
    {
        final int sourceWindowBytesDelta = window.update();

        targetWindowBytesAdjustment -= sourceWindowBytesDelta * 20 / 100;

        this.windowHandler = this::processWindow;
        this.windowHandler.accept(window);
    }

    private void processWindow(WindowFW window)
    {
        final int sourceWindowBytesDelta = window.update();
        final int sourceWindowFramesDelta = window.frames();

        final int targetWindowBytesDelta = sourceWindowBytesDelta + targetWindowBytesAdjustment;
        final int targetWindowFramesDelta = sourceWindowFramesDelta + targetWindowFramesAdjustment;

        targetWindowBytes += Math.max(targetWindowBytesDelta, 0);
        targetWindowBytesAdjustment = Math.min(targetWindowBytesDelta, 0);

        targetWindowFrames += Math.max(targetWindowFramesDelta, 0);
        targetWindowFramesAdjustment = Math.min(targetWindowFramesDelta, 0);

        if (targetWindowBytesDelta > 0 || targetWindowFramesDelta > 0)
        {
            doWindow(connectReplyThrottle, connectReplyId, Math.max(targetWindowBytesDelta, 0),
                Math.max(targetWindowFramesDelta, 0));
        }
    }

    private void handleReset(ResetFW reset)
    {
        doReset(connectReplyThrottle, connectReplyId);
    }
}
