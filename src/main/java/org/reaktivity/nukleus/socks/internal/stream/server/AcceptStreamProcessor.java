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

import static org.reaktivity.nukleus.buffer.BufferPool.NO_SLOT;

import java.util.Optional;

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.reaktivity.nukleus.function.MessageConsumer;
import org.reaktivity.nukleus.function.MessagePredicate;
import org.reaktivity.nukleus.socks.internal.metadata.Signal;
import org.reaktivity.nukleus.socks.internal.metadata.State;
import org.reaktivity.nukleus.socks.internal.stream.AbstractStreamProcessor;
import org.reaktivity.nukleus.socks.internal.stream.AcceptTransitionListener;
import org.reaktivity.nukleus.socks.internal.stream.Context;
import org.reaktivity.nukleus.socks.internal.stream.Correlation;
import org.reaktivity.nukleus.socks.internal.stream.types.FragmentedFlyweight;
import org.reaktivity.nukleus.socks.internal.stream.types.SocksCommandRequestFW;
import org.reaktivity.nukleus.socks.internal.stream.types.SocksCommandResponseFW;
import org.reaktivity.nukleus.socks.internal.stream.types.SocksNegotiationRequestFW;
import org.reaktivity.nukleus.socks.internal.stream.types.SocksNegotiationResponseFW;
import org.reaktivity.nukleus.socks.internal.types.OctetsFW;
import org.reaktivity.nukleus.socks.internal.types.control.RouteFW;
import org.reaktivity.nukleus.socks.internal.types.control.SocksRouteExFW;
import org.reaktivity.nukleus.socks.internal.types.stream.AbortFW;
import org.reaktivity.nukleus.socks.internal.types.stream.BeginFW;
import org.reaktivity.nukleus.socks.internal.types.stream.DataFW;
import org.reaktivity.nukleus.socks.internal.types.stream.EndFW;
import org.reaktivity.nukleus.socks.internal.types.stream.ResetFW;
import org.reaktivity.nukleus.socks.internal.types.stream.TcpBeginExFW;
import org.reaktivity.nukleus.socks.internal.types.stream.WindowFW;

public final class AcceptStreamProcessor extends AbstractStreamProcessor implements AcceptTransitionListener<TcpBeginExFW>
{
    public static final int MAX_WRITABLE_BYTES = 65535;
    public static final int MAX_WRITABLE_FRAMES = 65535;

    private MessageConsumer streamState;
    private MessageConsumer acceptReplyThrottleState;
    private MessageConsumer connectThrottleState;

    private int acceptReplyWindowBytes = 0;
    private int acceptReplyWindowFrames = 0;

    private int connectWindowBytes = 0;
    private int connectWindowFrames = 0;

    private int receivedBytes = 0;
    private int receivedFrames = 0;

    private byte socksAtyp;
    private byte[] socksAddr;
    private int socksPort;

    final Correlation correlation;

    public AcceptStreamProcessor(
        Context context,
        MessageConsumer acceptThrottle,
        long acceptStreamId,
        long acceptSourceRef,
        String acceptSourceName,
        long acceptCorrelationId)
    {
        super(context);
        final long acceptReplyStreamId = context.supplyStreamId.getAsLong();
        this.streamState = this::beforeBegin;
        this.acceptReplyThrottleState = this::handleAcceptReplyThrottleBeforeHandshake;
        this.connectThrottleState = this::handleConnectThrottleBeforeHandshake;
        context.router.setThrottle(
            acceptSourceName,
            acceptReplyStreamId,
            this::handleAcceptReplyThrottle);
        MessageConsumer acceptReplyEndpoint = context.router.supplyTarget(acceptSourceName);
        correlation = new Correlation(
            acceptThrottle,
            acceptStreamId,
            acceptSourceRef,
            acceptSourceName,
            acceptReplyStreamId,
            acceptCorrelationId,
            this
        );
        correlation.acceptReplyEndpoint(acceptReplyEndpoint);
        correlation.connectStreamId(context.supplyStreamId.getAsLong());
        correlation.connectCorrelationId(context.supplyCorrelationId.getAsLong());
        correlation.nextAcceptSignal(this::noop);
    }

    protected void handleStream(
        int msgTypeId,
        DirectBuffer buffer,
        int index,
        int length)
    {
        streamState.accept(msgTypeId, buffer, index, length);
    }

    protected void handleAcceptReplyThrottle(
        int msgTypeId,
        DirectBuffer buffer,
        int index,
        int length)
    {
        acceptReplyThrottleState.accept(msgTypeId, buffer, index, length);
    }

    protected void handleConnectThrottle(
        int msgTypeId,
        DirectBuffer buffer,
        int index,
        int length)
    {
        connectThrottleState.accept(msgTypeId, buffer, index, length);
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
            initializeNegotiation(begin);
        }
        else
        {
            doReset(correlation.acceptThrottle(), correlation.acceptStreamId());
        }
    }

    private void initializeNegotiation(
        BeginFW begin)
    {
        // System.out.println("ACCEPT/initializeNegotiation");
        // System.out.println("\tACCEPT: created stream " + begin.streamId());
        // System.out.println("\tACCEPT: send back begin frame " + correlation.acceptReplyStreamId());
        doBegin(
            correlation.acceptReplyEndpoint(),
            correlation.acceptReplyStreamId(),
            0L,
            correlation.acceptCorrelationId());
        doWindow(
            correlation.acceptThrottle(),
            correlation.acceptStreamId(),
            MAX_WRITABLE_BYTES,
            MAX_WRITABLE_FRAMES
        );
        this.streamState = this::afterNegotiationInitialized;
    }

    @State
    private void afterNegotiationInitialized(
        int msgTypeId,
        DirectBuffer buffer,
        int index,
        int length)
    {
        switch (msgTypeId)
        {
        case DataFW.TYPE_ID:
            final DataFW data = context.dataRO.wrap(buffer, index, index + length);
            receivedBytes += data.payload().sizeof();
            receivedFrames++;
            handleNegotiationData(data);
            break;
        case EndFW.TYPE_ID:
        case AbortFW.TYPE_ID:
            doAbort(correlation.acceptReplyEndpoint(), correlation.acceptReplyStreamId());
            break;
        default:
            doReset(correlation.acceptThrottle(), correlation.acceptStreamId());
            break;
        }
    }

    private void handleNegotiationFlyweight(
        FragmentedFlyweight<SocksNegotiationRequestFW> flyweight,
        DirectBuffer buffer,
        int offset,
        int limit)
    {
        final SocksNegotiationRequestFW socksNegotiation = flyweight.wrap(buffer, offset, limit);
        if (socksNegotiation.version() != 0x05)
        {
            doReset(correlation.acceptThrottle(), correlation.acceptStreamId());
        }
        int nmethods = socksNegotiation.nmethods();
        byte i = 0;
        for (; i < nmethods; i++)
        {
            if (socksNegotiation.methods()[0] == (byte) 0x00)
            {
                break;
            }
        }
        if (i == nmethods)
        {
            correlation.nextAcceptSignal(this::attemptFailedNegotiationResponse);
            correlation.nextAcceptSignal().accept(true);
        }
        else
        {
            correlation.nextAcceptSignal(this::attemptNegotiationResponse);
            correlation.nextAcceptSignal().accept(true);
        }
    }

    private void handleNegotiationData(
        DataFW data)
    {
        handleFragmentedData(
            data,
            context.socksNegotiationRequestRO,
            this::handleNegotiationFlyweight,
            correlation.acceptThrottle(),
            correlation.acceptStreamId());
    }

    @Signal
    private void attemptNegotiationResponse(
        boolean isReadyState)
    {
        // System.out.println("ACCEPT/attemptNegotiationResponse");
        SocksNegotiationResponseFW socksNegotiationResponseFW = context.socksNegotiationResponseRW
            .wrap(context.writeBuffer, DataFW.FIELD_OFFSET_PAYLOAD, context.writeBuffer.capacity())
            .version((byte) 0x05)
            .method((byte) 0x00)
            .build();
        DataFW dataNegotiationResponseFW = context.dataRW.wrap(context.writeBuffer, 0, context.writeBuffer.capacity())
            .streamId(correlation.acceptReplyStreamId())
            .payload(p -> p.set(
                socksNegotiationResponseFW.buffer(),
                socksNegotiationResponseFW.offset(),
                socksNegotiationResponseFW.sizeof()))
            .extension(e -> e.reset())
            .build();
        if (acceptReplyWindowBytes > dataNegotiationResponseFW.sizeof() &&
            acceptReplyWindowFrames > 0)
        {
            this.streamState = this::afterNegotiation;
            correlation.nextAcceptSignal(this::noop);
            correlation.acceptReplyEndpoint().accept(
                dataNegotiationResponseFW.typeId(),
                dataNegotiationResponseFW.buffer(),
                dataNegotiationResponseFW.offset(),
                dataNegotiationResponseFW.sizeof());
            acceptReplyWindowBytes -= dataNegotiationResponseFW.payload().sizeof();
            acceptReplyWindowFrames--;
        }
    }

    @Signal
    private void attemptFailedNegotiationResponse(
        boolean isReadyState)
    {
        // System.out.println("ACCEPT/attemptFailedNegotiationResponse");
        SocksNegotiationResponseFW socksNegotiationResponseFW = context.socksNegotiationResponseRW
            .wrap(context.writeBuffer, DataFW.FIELD_OFFSET_PAYLOAD, context.writeBuffer.capacity())
            .version((byte) 0x05)
            .method((byte) 0xFF)
            .build();
        DataFW dataNegotiationResponseFW = context.dataRW.wrap(context.writeBuffer, 0, context.writeBuffer.capacity())
            .streamId(correlation.acceptReplyStreamId())
            .payload(p -> p.set(
                socksNegotiationResponseFW.buffer(),
                socksNegotiationResponseFW.offset(),
                socksNegotiationResponseFW.sizeof()))
            .extension(e -> e.reset())
            .build();
        if (acceptReplyWindowBytes > dataNegotiationResponseFW.sizeof() &&
            acceptReplyWindowFrames > 0)
        {
            this.streamState = this::afterFailedHandshake; // Reuse state that will trigger reset
            correlation.nextAcceptSignal(this::noop);
            correlation.acceptReplyEndpoint().accept(
                dataNegotiationResponseFW.typeId(),
                dataNegotiationResponseFW.buffer(),
                dataNegotiationResponseFW.offset(),
                dataNegotiationResponseFW.sizeof());
            acceptReplyWindowBytes -= dataNegotiationResponseFW.payload().sizeof();
            acceptReplyWindowFrames--;
        }
    }

    @State
    private void afterFailedHandshake(
        int msgTypeId,
        DirectBuffer buffer,
        int index,
        int length)
    {
        // Only EndFrame should come back
    }

    @Signal
    private void noop(
        boolean isReadyState)
    {
    }

    @State
    private void afterNegotiation(
        int msgTypeId,
        DirectBuffer buffer,
        int index,
        int length)
    {
        switch (msgTypeId)
        {
        case DataFW.TYPE_ID:
            final DataFW data = context.dataRO.wrap(buffer, index, index + length);
            receivedBytes += data.payload().sizeof();
            receivedFrames++;
            handleConnectRequestData(data);
            break;
        case EndFW.TYPE_ID:
        case AbortFW.TYPE_ID:
            doAbort(correlation.acceptReplyEndpoint(), correlation.acceptReplyStreamId());
            break;
        default:
            doReset(correlation.acceptThrottle(), correlation.acceptStreamId());
            break;
        }
    }

    private void handleConnectRequestFlyweight(
        FragmentedFlyweight<SocksCommandRequestFW> flyweight,
        DirectBuffer buffer,
        int offset,
        int limit)
    {
        final SocksCommandRequestFW socksCommandRequestFW = flyweight.wrap(buffer, offset, limit);
        if (socksCommandRequestFW.version() != 0x05)
        {
            doReset(correlation.acceptThrottle(), correlation.acceptStreamId());
        }
        if (socksCommandRequestFW.command() != 0x01)
        {
            correlation.nextAcceptSignal(this::attemptFailedConnectionResponse);
            correlation.nextAcceptSignal().accept(true);
        }
        else
        {
            final String dstAddrPort = socksCommandRequestFW.getDstAddrPort();
            final RouteFW connectRoute = resolveTarget(
                correlation.acceptSourceRef(),
                correlation.acceptSourceName(),
                dstAddrPort);
            final String connectTargetName = connectRoute.target().asString();
            final MessageConsumer connectEndpoint = context.router.supplyTarget(connectTargetName);
            final long connectTargetRef = connectRoute.targetRef();
            final long connectStreamId = context.supplyStreamId.getAsLong();
            final long connectCorrelationId = context.supplyCorrelationId.getAsLong();
            correlation.connectRoute(connectRoute);
            correlation.connectEndpoint(connectEndpoint);
            correlation.connectTargetRef(connectTargetRef);
            correlation.connectTargetName(connectTargetName);
            correlation.connectStreamId(connectStreamId);
            correlation.connectCorrelationId(connectCorrelationId);
            context.correlations.put(connectCorrelationId, correlation); // Use this map on the CONNECT STREAM
            context.router.setThrottle(
                connectTargetName,
                connectStreamId,
                this::handleConnectThrottle);
            doBegin(connectEndpoint, connectStreamId, connectTargetRef, connectCorrelationId);
            correlation.nextAcceptSignal(this::noop);
            this.streamState = this::afterTargetConnectBegin;
        }
    }

    private void handleConnectRequestData(
        DataFW data)
    {
        handleFragmentedData(
            data,
            context.socksConnectionRequestRO,
            this::handleConnectRequestFlyweight,
            correlation.acceptThrottle(),
            correlation.acceptStreamId());
    }

    @State
    private void afterTargetConnectBegin(
        int msgTypeId,
        DirectBuffer buffer,
        int index,
        int length)
    {
        doReset(correlation.acceptThrottle(), correlation.acceptStreamId());
    }

    @Override
    public void transitionToConnectionReady(Optional<TcpBeginExFW> connectionInfo)
    {
        TcpBeginExFW tcpBeginEx = connectionInfo.get(); // Only reading so should be safe
        socksPort = tcpBeginEx.localPort();
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
        attemptConnectionResponse(false);
    }


    @Signal
    private void attemptFailedConnectionResponse(
        boolean isReadyState)
    {
        if (!isReadyState)
        {
            correlation.nextAcceptSignal(this::attemptFailedConnectionResponse);
        }
        SocksCommandResponseFW socksConnectResponseFW = context.socksConnectionResponseRW
            .wrap(context.writeBuffer, DataFW.FIELD_OFFSET_PAYLOAD, context.writeBuffer.capacity())
            .version((byte) 0x05)
            .reply((byte) 0x07) // COMMAND NOT SUPPORTED
            .bind((byte)0x01, new byte[]{0x00, 0x00, 0x00, 0x00}, 0x00)
            .build();
        DataFW dataConnectionResponseFW = context.dataRW.wrap(context.writeBuffer, 0, context.writeBuffer.capacity())
            .streamId(correlation.acceptReplyStreamId())
            .payload(p -> p.set(
                socksConnectResponseFW.buffer(),
                socksConnectResponseFW.offset(),
                socksConnectResponseFW.sizeof()))
            .extension(e -> e.reset())
            .build();
        if (acceptReplyWindowBytes > dataConnectionResponseFW.sizeof() &&
            acceptReplyWindowFrames > 0)
        {
            // System.out.println("ACCEPT/attemptFailedConnectionResponse: \n\tSending dataConnectionResponseFW: " +
            //    dataConnectionResponseFW);
            this.streamState = this::afterFailedHandshake;
            correlation.nextAcceptSignal(this::noop);
            correlation.acceptReplyEndpoint().accept(
                dataConnectionResponseFW.typeId(),
                dataConnectionResponseFW.buffer(),
                dataConnectionResponseFW.offset(),
                dataConnectionResponseFW.sizeof());
            acceptReplyWindowBytes -= dataConnectionResponseFW.payload().sizeof();
            acceptReplyWindowFrames--;
        }
    }

    @Signal
    private void attemptConnectionResponse(
        boolean isReadyState)
    {
        if (!isReadyState)
        {
            correlation.nextAcceptSignal(this::attemptConnectionResponse);
        }
        SocksCommandResponseFW socksConnectResponseFW = context.socksConnectionResponseRW
            .wrap(context.writeBuffer, DataFW.FIELD_OFFSET_PAYLOAD, context.writeBuffer.capacity())
            .version((byte) 0x05)
            .reply((byte) 0x00) // CONNECT
            .bind(socksAtyp, socksAddr, socksPort)
            .build();
        DataFW dataConnectionResponseFW = context.dataRW.wrap(context.writeBuffer, 0, context.writeBuffer.capacity())
            .streamId(correlation.acceptReplyStreamId())
            .payload(p -> p.set(
                socksConnectResponseFW.buffer(),
                socksConnectResponseFW.offset(),
                socksConnectResponseFW.sizeof()))
            .extension(e -> e.reset())
            .build();
        if (acceptReplyWindowBytes > dataConnectionResponseFW.sizeof() &&
            acceptReplyWindowFrames > 0)
        {
            // System.out.println("ACCEPT/attemptConnectionResponse: \n\t" +
            //    "Sending dataConnectionResponseFW: " +
            //    dataConnectionResponseFW);
            // Optimistic case, the frames can be forwarded back and forth
            this.streamState = this::afterSourceConnect;
            this.acceptReplyThrottleState = this::handleAcceptReplyThrottleAfterHandshake;
            this.connectThrottleState = this::handleConnectThrottleAfterHandshake;

            // Send the data frame with the connection response
            correlation.acceptReplyEndpoint().accept(
                dataConnectionResponseFW.typeId(),
                dataConnectionResponseFW.buffer(),
                dataConnectionResponseFW.offset(),
                dataConnectionResponseFW.sizeof());

            acceptReplyWindowBytes -= dataConnectionResponseFW.sizeof();
            acceptReplyWindowFrames--;

            // Forward the remaining acceptReplyWindow to the CONNECT-REPLY throttle
            // System.out.println("ACCEPT/attemptConnectionResponse -> initialize CONNECT-REPLY throttle");
            doWindow(
                correlation.connectReplyThrottle(),
                correlation.connectReplyStreamId(),
                acceptReplyWindowBytes,
                acceptReplyWindowFrames
            );
            // If ACCEPT still has a window larger than what the connect has offer must buffer until synchronization
            // System.out.println("ACCEPT/attemptConnectionResponse connectWindowBytes: " +
            //    connectWindowBytes + " connectWindowFrames: " + connectWindowFrames);
            if (isConnectWindowGreaterThanAcceptWindow())
            {
                // Forward to the ACCEPT throttle the available CONNECT window
                // Subtract what ACCEPT has already sent during handshake
                // System.out.println("ACCEPT/attemptConnectionResponse");
                doWindow(
                    correlation.acceptThrottle(),
                    correlation.acceptStreamId(),
                    connectWindowBytes - receivedBytes,
                    connectWindowFrames - receivedFrames);
            }
            else
            {
                this.connectThrottleState = this::handleConnectThrottleBufferUnwind;
                this.streamState = this::afterSourceConnectBufferUnwind;
            }
        }
    }

    @State
    private void afterSourceConnectBufferUnwind(
        int msgTypeId,
        DirectBuffer buffer,
        int index,
        int length)
    {
        switch (msgTypeId)
        {
        case DataFW.TYPE_ID:
            final DataFW data = context.dataRO.wrap(buffer, index, index + length);
            receivedBytes += data.payload().sizeof();
            receivedFrames++;
            handleHighLevelDataBufferUnwind(data);
            break;
        case EndFW.TYPE_ID:
        case AbortFW.TYPE_ID:
            doAbort(correlation.acceptReplyEndpoint(), correlation.acceptReplyStreamId());
            break;
        default:
            doReset(correlation.acceptThrottle(), correlation.acceptStreamId());
        }
    }

    private void handleHighLevelDataBufferUnwind(DataFW data)
    {
        OctetsFW payload = data.payload();
        if (this.slotWriteOffset != 0)
        {
            MutableDirectBuffer acceptBuffer = context.bufferPool.buffer(this.slotIndex, this.slotWriteOffset);
            acceptBuffer.putBytes(
                slotWriteOffset,
                payload.buffer(),
                payload.offset(),
                payload.sizeof()
             );
            slotWriteOffset += payload.sizeof();
            return;
        }
        if (connectWindowBytes > payload.sizeof() &&
            connectWindowFrames > 0)
        {
            DataFW dataForwardFW = context.dataRW.wrap(context.writeBuffer, 0, context.writeBuffer.capacity())
                .streamId(correlation.connectStreamId())
                .payload(p -> p.set(
                    payload.buffer(),
                    payload.offset(),
                    payload.sizeof()))
                .extension(e -> e.reset())
                .build();
            // System.out.println("ACCEPT/handleHighLevelDataBufferUnwind: Forwarding data frame: \n\t" + data +
            //    "\n\t" + dataForwardFW);
            correlation.connectEndpoint()
                .accept(
                    dataForwardFW.typeId(),
                    dataForwardFW.buffer(),
                    dataForwardFW.offset(),
                    dataForwardFW.sizeof());
            connectWindowBytes -= payload.sizeof();
            connectWindowFrames--;
            // System.out.println("ACCEPT/handleHighLevelDataBufferUnwind connectWindowBytes: " +
            //    connectWindowBytes + " connectWindowFrames: " + connectWindowFrames);
            if (connectWindowBytes == 0)
            {
                // System.out.println("ACCEPT/handleHighLevelDataBufferUnwind changing state, connect window emptied");
                this.connectThrottleState = this::handleConnectThrottleAfterHandshake;
                this.streamState = this::afterSourceConnect;
            }
        }
        else
        {
            int bufferedSizeBytes = connectWindowBytes;
            if (connectWindowFrames > 0)
            {
                DataFW dataForwardFW = context.dataRW.wrap(context.writeBuffer, 0, context.writeBuffer.capacity())
                    .streamId(correlation.connectStreamId())
                    .payload(p -> p.set(
                        payload.buffer(),
                        payload.offset(),
                        connectWindowBytes))
                    .extension(e -> e.reset())
                    .build();
                // System.out.println("ACCEPT/handleHighLevelDataBufferUnwind: Forwarding data frame: \n\t" + data +
                //    "\n\t" + dataForwardFW);
                correlation.connectEndpoint()
                    .accept(
                        dataForwardFW.typeId(),
                        dataForwardFW.buffer(),
                        dataForwardFW.offset(),
                        dataForwardFW.sizeof());
                connectWindowBytes = 0;
                connectWindowFrames--;
            }
            else
            {
                bufferedSizeBytes = payload.sizeof();
            }
            if (NO_SLOT == (slotIndex = context.bufferPool.acquire(correlation.connectStreamId())))
            {
                doReset(correlation.acceptThrottle(), correlation.acceptStreamId());
                return;
            }
            MutableDirectBuffer acceptBuffer = context.bufferPool.buffer(this.slotIndex);
            acceptBuffer.putBytes(
                0,
                payload.buffer(),
                payload.offset() + bufferedSizeBytes,
                payload.sizeof() - bufferedSizeBytes
            );
            slotWriteOffset = payload.sizeof() - bufferedSizeBytes;
            slotReadOffset = 0;
        }
    }

    @State
    private void afterSourceConnect(
        int msgTypeId,
        DirectBuffer buffer,
        int index,
        int length)
    {
        switch (msgTypeId)
        {
        case DataFW.TYPE_ID:
            final DataFW data = context.dataRO.wrap(buffer, index, index + length);
            handleHighLevelData(data);
            break;
        case EndFW.TYPE_ID:
            doEnd(correlation.acceptReplyEndpoint(), correlation.acceptReplyStreamId());
            break;
        case AbortFW.TYPE_ID:
            doAbort(correlation.acceptReplyEndpoint(), correlation.acceptReplyStreamId());
            break;
        default:
            doReset(correlation.acceptThrottle(), correlation.acceptStreamId());
        }
    }

    private void handleHighLevelData(DataFW data)
    {
        OctetsFW payload = data.payload();
        DataFW dataForwardFW = context.dataRW.wrap(context.writeBuffer, 0, context.writeBuffer.capacity())
            .streamId(correlation.connectStreamId())
            .payload(p -> p.set(
                payload.buffer(),
                payload.offset(),
                payload.sizeof()))
            .extension(e -> e.reset())
            .build();
        // System.out.println("ACCEPT/handleHighLevelData: Forwarding data frame: \n\t" + data + "\n\t" + dataForwardFW);
        // System.out.println("\tconnectWindowBytes: " + connectWindowBytes + " connectWindowFrames: " + connectWindowFrames);
        correlation.connectEndpoint()
            .accept(
                dataForwardFW.typeId(),
                dataForwardFW.buffer(),
                dataForwardFW.offset(),
                dataForwardFW.sizeof());
    }

    private void handleAcceptReplyThrottleBeforeHandshake(
        int msgTypeId,
        DirectBuffer buffer,
        int index,
        int length)
    {
        switch (msgTypeId)
        {
        case WindowFW.TYPE_ID:
            final WindowFW window = context.windowRO.wrap(buffer, index, index + length);
            acceptReplyWindowBytes += window.update();
            acceptReplyWindowFrames += window.frames();
            // System.out.println("acceptReplyWindowBytes: " + acceptReplyWindowBytes +
            //    " acceptReplyWindowFrames: " + acceptReplyWindowFrames);
            correlation.nextAcceptSignal().accept(true);
            break;
        case ResetFW.TYPE_ID:
            final ResetFW reset = context.resetRO.wrap(buffer, index, index + length);
            doReset(correlation.acceptThrottle(), correlation.acceptStreamId());
            break;
        default:
            // ignore
            break;
        }
    }

    private void handleAcceptReplyThrottleAfterHandshake(
        int msgTypeId,
        DirectBuffer buffer,
        int index,
        int length)
    {
        switch (msgTypeId)
        {
        case WindowFW.TYPE_ID:
            final WindowFW window = context.windowRO.wrap(buffer, index, index + length);
            // System.out.println("ACCEPT/handleAcceptReplyThrottleAfterHandshake forwarding window frame");
            // System.out.println("\tReceived window: " + window);
            doWindow(
                correlation.connectReplyThrottle(),
                correlation.connectReplyStreamId(),
                window.update(),
                window.frames()
            );
            break;
        case ResetFW.TYPE_ID:
            final ResetFW reset = context.resetRO.wrap(buffer, index, index + length);
            doReset(correlation.acceptThrottle(), correlation.acceptStreamId());
            doAbort(correlation.connectEndpoint(), correlation.connectStreamId());
            doReset(correlation.connectReplyThrottle(), correlation.connectReplyStreamId());
            break;
        default:
            // ignore
            break;
        }
    }

    private void handleConnectThrottleBeforeHandshake(
        int msgTypeId,
        DirectBuffer buffer,
        int index,
        int length)
    {
        switch (msgTypeId)
        {
        case WindowFW.TYPE_ID:
            final WindowFW window = context.windowRO.wrap(buffer, index, index + length);
            connectWindowBytes += window.update();
            connectWindowFrames += window.frames();
            // System.out.println("ACCEPT: \n\t" + window);
            // System.out.println("\tconnectWindowBytes: " + connectWindowBytes + " connectWindowFrames: " + connectWindowFrames);
            break;
        case ResetFW.TYPE_ID:
            final ResetFW reset = context.resetRO.wrap(buffer, index, index + length);
            doReset(correlation.acceptThrottle(), correlation.acceptStreamId());
            break;
        default:
            // ignore
            break;
        }
    }

    private void handleConnectThrottleBufferUnwind(
        int msgTypeId,
        DirectBuffer buffer,
        int index,
        int length)
    {
        switch (msgTypeId)
        {
        case WindowFW.TYPE_ID:
            final WindowFW window = context.windowRO.wrap(buffer, index, index + length);
            connectWindowBytes += window.update();
            connectWindowFrames += window.frames();
            // System.out.println("ACCEPT/handleConnectThrottleBufferUnwind: \n\t" + window);
            // System.out.println("\tconnectWindowBytes: " + connectWindowBytes + " connectWindowFrames: " + connectWindowFrames);
            if (connectWindowFrames == 0)
            {
                return;
            }
            if (this.slotWriteOffset != 0)
            {
                MutableDirectBuffer acceptBuffer = context.bufferPool.buffer(this.slotIndex, slotReadOffset);
                final int payloadSize = slotWriteOffset - slotReadOffset;
                if (connectWindowBytes < payloadSize)
                {
                    // System.out.println("\treturning");
                    return; // not covering an empty data frame
                }
                // System.out.println("\tComputed payload size: " + payloadSize + "bytes.");
                DataFW dataForwardFW = context.dataRW.wrap(context.writeBuffer, 0, context.writeBuffer.capacity())
                    .streamId(correlation.connectStreamId())
                    .payload(p -> p.set(
                        acceptBuffer,
                        0,
                        payloadSize))
                    .extension(e -> e.reset())
                    .build();
                // System.out.println("\tForwarding buffered frame (based on window): \n\t" + window + "\n\t" + dataForwardFW);
                correlation.connectEndpoint()
                    .accept(
                        dataForwardFW.typeId(),
                        dataForwardFW.buffer(),
                        dataForwardFW.offset(),
                        dataForwardFW.sizeof());
                connectWindowBytes -= dataForwardFW.sizeof();
                connectWindowFrames--;
                slotReadOffset += payloadSize;
                if (slotReadOffset == slotWriteOffset)
                {
                    // all buffer could be sent, must switch Throttling states
                    this.connectThrottleState = this::handleConnectThrottleAfterHandshake;
                    this.streamState = this::afterSourceConnect;
                    // Release the buffer
                    context.bufferPool.release(this.slotIndex);
                    this.slotWriteOffset = 0;
                    this.slotReadOffset = 0;
                    this.slotIndex = NO_SLOT;
                }
            }
            else
            {
                // System.out.println("\tNO Data frame received from accept yet!");
                // System.out.println("\treceivedBytes=" + receivedBytes + " receivedFrames=" + receivedFrames);
                // System.out.println("\tconnectWindowBytes=" + connectWindowBytes +
                //    " connectWindowFrames=" + connectWindowFrames);
                if (isConnectWindowGreaterThanAcceptWindow())
                {
                    this.connectThrottleState = this::handleConnectThrottleAfterHandshake;
                    this.streamState = this::afterSourceConnect;
                    // System.out.println("ACCEPT/handleConnectThrottleBufferUnwind changing state and restoring acceptThrottle");
                    doWindow(
                        correlation.acceptThrottle(),
                        correlation.acceptStreamId(),
                        receivedBytes,
                        receivedFrames
                    );
                }
            }
            break;
        case ResetFW.TYPE_ID:
            final ResetFW reset = context.resetRO.wrap(buffer, index, index + length);
            doReset(correlation.acceptThrottle(), correlation.acceptStreamId());
            doAbort(correlation.acceptReplyEndpoint(), correlation.acceptReplyStreamId());
            doReset(correlation.connectReplyThrottle(), correlation.connectReplyStreamId());
            break;
        default:
            // ignore
            break;
        }
    }

    private void handleConnectThrottleAfterHandshake(
        int msgTypeId,
        DirectBuffer buffer,
        int index,
        int length)
    {
        switch (msgTypeId)
        {
        case WindowFW.TYPE_ID:
            final WindowFW window = context.windowRO.wrap(buffer, index, index + length);
            // System.out.println("ACCEPT/handleConnectThrottleAfterHandshake");
            // System.out.println("\twindow: " + window);
            doWindow(
                correlation.acceptThrottle(),
                correlation.acceptStreamId(),
                window.update(),
                window.frames()
            );
            break;
        case ResetFW.TYPE_ID:
            final ResetFW reset = context.resetRO.wrap(buffer, index, index + length);
            doReset(correlation.acceptThrottle(), correlation.acceptStreamId());
            doReset(correlation.acceptThrottle(), correlation.acceptStreamId());
            doAbort(correlation.acceptReplyEndpoint(), correlation.acceptReplyStreamId());
            doReset(correlation.connectReplyThrottle(), correlation.connectReplyStreamId());
            break;
        default:
            // ignore
            break;
        }
    }

    @Override
    public void transitionToAborted()
    {
        // TODO implement me
    }

    RouteFW resolveTarget(
        long sourceRef,
        String sourceName,
        String dstAddrPort)
    {
        MessagePredicate filter = (t, b, o, l) ->
        {
            RouteFW route = context.routeRO.wrap(b, o, l);
            final SocksRouteExFW routeEx = route.extension()
                .get(context.routeExRO::wrap);
            return sourceRef == route.sourceRef() &&
                sourceName.equals(route.source().asString()) &&
                dstAddrPort.equals(routeEx.destAddrPort().asString());
        };
        return context.router.resolve(filter, this::wrapRoute);
    }

    private RouteFW wrapRoute(
        int msgTypeId,
        DirectBuffer buffer,
        int index,
        int length)
    {
        return context.routeRO.wrap(buffer, index, index + length);
    }

    public boolean isConnectWindowGreaterThanAcceptWindow()
    {
        return
            MAX_WRITABLE_BYTES - receivedBytes <= connectWindowBytes &&
            MAX_WRITABLE_FRAMES - receivedFrames <= connectWindowFrames;
    }
}
