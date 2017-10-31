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
package org.reaktivity.nukleus.socks.internal.stream.client;

import java.util.Optional;

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.reaktivity.nukleus.function.MessageConsumer;
import org.reaktivity.nukleus.socks.internal.metadata.State;
import org.reaktivity.nukleus.socks.internal.stream.AbstractStreamProcessor;
import org.reaktivity.nukleus.socks.internal.stream.Context;
import org.reaktivity.nukleus.socks.internal.stream.Correlation;
import org.reaktivity.nukleus.socks.internal.stream.types.FragmentedFlyweight;
import org.reaktivity.nukleus.socks.internal.stream.types.SocksCommandResponseFW;
import org.reaktivity.nukleus.socks.internal.stream.types.SocksNegotiationResponseFW;
import org.reaktivity.nukleus.socks.internal.types.OctetsFW;
import org.reaktivity.nukleus.socks.internal.types.stream.AbortFW;
import org.reaktivity.nukleus.socks.internal.types.stream.BeginFW;
import org.reaktivity.nukleus.socks.internal.types.stream.DataFW;
import org.reaktivity.nukleus.socks.internal.types.stream.EndFW;
import org.reaktivity.nukleus.socks.internal.types.stream.ResetFW;
import org.reaktivity.nukleus.socks.internal.types.stream.WindowFW;

final class ConnectReplyStreamProcessor extends AbstractStreamProcessor
{

    private final int socksInitialWindow;

    private MessageConsumer streamState;
    private MessageConsumer acceptReplyThrottleState;

    Correlation correlation;
    private final MessageConsumer connectReplyThrottle;
    private final long connectReplyStreamId;

    private int acceptReplyCredit;
    private int acceptReplyPadding;

    private int receivedConnectReplyBytes;

    ConnectReplyStreamProcessor(
        Context context,
        MessageConsumer connectReplyThrottle,
        long connectReplyId)
    {
        super(context);
        this.socksInitialWindow = context.socksConfiguration.socksInitialWindow();
        this.streamState = this::beforeBegin;
        this.connectReplyThrottle = connectReplyThrottle;
        this.connectReplyStreamId = connectReplyId;
        this.acceptReplyThrottleState = this::handleAcceptReplyThrottleBeforeHandshake;
    }

    @Override
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
            doReset(connectReplyThrottle, connectReplyStreamId);
        }
    }

    private void handleBegin(BeginFW begin)
    {
        final long connectRef = begin.sourceRef();
        final long correlationId = begin.correlationId();
        correlation = context.correlations.remove(correlationId);
        if (connectRef == 0L && correlation != null)
        {
            correlation.connectReplyThrottle(connectReplyThrottle);
            correlation.connectReplyStreamId(connectReplyStreamId);
            MessageConsumer acceptReplyEndpoint = context.router.supplyTarget(correlation.acceptSourceName());
            correlation.acceptReplyEndpoint(acceptReplyEndpoint);
            context.router.setThrottle(
                correlation.acceptSourceName(),
                correlation.acceptReplyStreamId(),
                this::handleAcceptReplyThrottle);
            doBegin(correlation.acceptReplyEndpoint(),
                correlation.acceptReplyStreamId(),
                0L,
                correlation.acceptCorrelationId());
            streamState = this::beforeNegotiationResponse;
            doWindow(
                connectReplyThrottle,
                connectReplyStreamId,
                socksInitialWindow,
                0);
        }
        else
        {
            doReset(connectReplyThrottle, connectReplyStreamId);
        }
    }

    @State
    private void beforeNegotiationResponse(
        int msgTypeId,
        DirectBuffer buffer,
        int index,
        int length)
    {
        switch (msgTypeId)
        {
        case DataFW.TYPE_ID:
            final DataFW data = context.dataRO.wrap(buffer, index, index + length);
            receivedConnectReplyBytes += data.payload().sizeof();
            handleNegotiationResponse(data);
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
            doReset(connectReplyThrottle, connectReplyStreamId);
            break;
        }
    }

    private void handleNegotiationResponse(DataFW data)
    {
        handleFragmentedData(
            data,
            context.socksNegotiationResponseRO,
            this::handleFullNegotiationFlyweight,
            correlation.acceptThrottle(),
            correlation.acceptStreamId());
    }

    private void handleFullNegotiationFlyweight(
        FragmentedFlyweight<SocksNegotiationResponseFW> flyweight,
        DirectBuffer buffer,
        int offset,
        int limit)
    {
        final SocksNegotiationResponseFW socksNegotiationResponse = flyweight.wrap(buffer, offset, limit);
        if (socksNegotiationResponse.version() != 0x05 ||
            socksNegotiationResponse.method() != 0x00)
        {
            doReset(connectReplyThrottle, connectReplyStreamId); // TODO diagnostic
        }
        streamState = this::beforeConnectionResponse;
        correlation.nextAcceptSignal().accept(true);
    }

    @State
    private void beforeConnectionResponse(
        int msgTypeId,
        DirectBuffer buffer,
        int index,
        int length)
    {
        switch (msgTypeId)
        {
        case DataFW.TYPE_ID:
            final DataFW data = context.dataRO.wrap(buffer, index, index + length);
            receivedConnectReplyBytes += data.payload().sizeof();
            handleConnectionResponse(data);
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
            doReset(connectReplyThrottle, connectReplyStreamId);
            break;
        }
    }

    private void handleConnectionResponse(DataFW data)
    {
        handleFragmentedData(
            data,
            context.socksConnectionResponseRO,
            this::handleFullConnectionFlyweight,
            correlation.acceptThrottle(),
            correlation.acceptStreamId());
    }

    private void handleFullConnectionFlyweight(
        FragmentedFlyweight<SocksCommandResponseFW> flyweight,
        DirectBuffer buffer,
        int offset,
        int limit)
    {
        final SocksCommandResponseFW socksConnectionResponse = flyweight.wrap(buffer, offset, limit);
        if (socksConnectionResponse.version() != 0x05 ||
            socksConnectionResponse.reply() != 0x00)
        {
            doReset(connectReplyThrottle, connectReplyStreamId); // TODO diagnostic
        }
        streamState = this::afterConnectionResponse;
        acceptReplyThrottleState = this::handleAcceptReplyThrottleAfterHandshake;
        correlation.acceptTransitionListener().transitionToConnectionReady(Optional.empty());
        if (isAcceptReplyWindowGreaterThanConnectReplyWindow())
        {
            doWindow(
                correlation.connectReplyThrottle(),
                correlation.connectReplyStreamId(),
                acceptReplyCredit - receivedConnectReplyBytes - socksInitialWindow,
                acceptReplyPadding);
        }
        else
        {
            this.acceptReplyThrottleState = this::handleAcceptReplyThrottleBufferUnwind;
            this.streamState = this::afterConnectionResponseBufferUnwind;
        }
    }

    @State
    private void afterConnectionResponseBufferUnwind(
        int msgTypeId,
        DirectBuffer buffer,
        int index,
        int length)
    {
        switch (msgTypeId)
        {
        case DataFW.TYPE_ID:
            final DataFW data = context.dataRO.wrap(buffer, index, index + length);
            receivedConnectReplyBytes += data.payload().sizeof();
            handleHighLevelDataBufferUnwind(
                data,
                getCurrentTargetCredit(),
                correlation.acceptReplyStreamId(),
                connectReplyThrottle,
                connectReplyStreamId,
                this::updateSentPartialData,
                this::updateSentCompleteData);
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
            doReset(connectReplyThrottle, connectReplyStreamId);
            break;
        }
    }

    private void updateSentPartialData(
        OctetsFW payload,
        int payloadLength)
    {
        doForwardData(
            payload,
            payloadLength,
            correlation.acceptReplyStreamId(),
            correlation.acceptReplyEndpoint());
        acceptReplyCredit = 0;
    }

    private void updateSentCompleteData(OctetsFW payload)
    {
        doForwardData(
            payload,
            correlation.acceptReplyStreamId(),
            correlation.acceptReplyEndpoint());
        acceptReplyCredit -= payload.sizeof() + acceptReplyPadding;
        if (acceptReplyCredit == 0)
        {
            this.acceptReplyThrottleState = this::handleAcceptReplyThrottleAfterHandshake;
            this.streamState = this::afterConnectionResponse;
        }
    }

    @State
    private void afterConnectionResponse(
        int msgTypeId,
        DirectBuffer buffer,
        int index,
        int length)
    {
        switch (msgTypeId)
        {
        case DataFW.TYPE_ID:
            final DataFW data = context.dataRO.wrap(buffer, index, index + length);
            receivedConnectReplyBytes += data.payload().sizeof();
            handleHighLevelData(data);
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
            doReset(connectReplyThrottle, connectReplyStreamId);
            break;
        }
    }

    private void handleHighLevelData(DataFW data)
    {
        OctetsFW payload = data.payload();
        doForwardData(payload,
            correlation.acceptReplyStreamId(),
            correlation.acceptReplyEndpoint());
    }

    private void handleEnd(EndFW end)
    {
        EndFW endForwardFW = context.endRW
            .wrap(context.writeBuffer, 0, context.writeBuffer.capacity())
            .streamId(correlation.acceptReplyStreamId())
            .build();
        correlation.acceptReplyEndpoint()
            .accept(
                endForwardFW.typeId(),
                endForwardFW.buffer(),
                endForwardFW.offset(),
                endForwardFW.sizeof());
    }

    private void handleAbort(AbortFW abort)
    {
        doAbort(correlation.acceptReplyEndpoint(), correlation.acceptReplyStreamId());
    }

    protected void handleAcceptReplyThrottle(
        int msgTypeId,
        DirectBuffer buffer,
        int index,
        int length)
    {
        acceptReplyThrottleState.accept(msgTypeId, buffer, index, length);
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
            acceptReplyCredit += window.credit();
            acceptReplyPadding = window.padding();
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

    private void handleAcceptReplyThrottleBufferUnwind(
        int msgTypeId,
        DirectBuffer buffer,
        int index,
        int length)
    {
        switch (msgTypeId)
        {
        case WindowFW.TYPE_ID:
            final WindowFW window = context.windowRO.wrap(buffer, index, index + length);
            acceptReplyCredit += window.credit();
            acceptReplyPadding = window.padding();
            handleThrottlingAndDataBufferUnwind(
                getCurrentTargetCredit(),
                this::updateForwardBufferedData,
                this::updateForwardBufferEmpty
            );
            break;
        case ResetFW.TYPE_ID:
            final ResetFW reset = context.resetRO.wrap(buffer, index, index + length);
            doReset(correlation.connectReplyThrottle(), correlation.connectReplyStreamId());
            break;
        default:
            // ignore
            break;
        }
    }

    private void updateForwardBufferedData(
        MutableDirectBuffer connectReplyBuffer,
        int payloadSize)
    {
        doForwardData(
            connectReplyBuffer,
            0,
            payloadSize,
            correlation.acceptReplyStreamId(),
            correlation.acceptReplyEndpoint());
        acceptReplyCredit -= payloadSize + acceptReplyPadding;
    }

    private void updateForwardBufferEmpty(boolean shouldForwardRemainingWindow)
    {
        if (isAcceptReplyWindowGreaterThanConnectReplyWindow())
        {
            this.acceptReplyThrottleState = this::handleAcceptReplyThrottleAfterHandshake;
            this.streamState = this::afterConnectionResponse;
            if (shouldForwardRemainingWindow)
            {
                doWindow(
                    correlation.connectReplyThrottle(),
                    correlation.connectReplyStreamId(),
                    receivedConnectReplyBytes,
                    acceptReplyPadding);
            }
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
            doWindow(
                correlation.connectReplyThrottle(),
                correlation.connectReplyStreamId(),
                window.credit(),
                window.padding());
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

    private void handleReset(ResetFW reset)
    {
        doReset(connectReplyThrottle, connectReplyStreamId);
    }

    public boolean isAcceptReplyWindowGreaterThanConnectReplyWindow()
    {
        return acceptReplyCredit - acceptReplyPadding >= socksInitialWindow - receivedConnectReplyBytes;
    }

    private int getCurrentTargetCredit()
    {
        return acceptReplyCredit - acceptReplyPadding;
    }
}
