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

import java.util.function.Consumer;

import org.agrona.DirectBuffer;
import org.reaktivity.nukleus.function.MessageConsumer;
import org.reaktivity.nukleus.socks.internal.stream.Context;
import org.reaktivity.nukleus.socks.internal.stream.Correlation;
import org.reaktivity.nukleus.socks.internal.stream.DefaultStreamHandler;
import org.reaktivity.nukleus.socks.internal.types.stream.AbortFW;
import org.reaktivity.nukleus.socks.internal.types.stream.BeginFW;
import org.reaktivity.nukleus.socks.internal.types.stream.DataFW;
import org.reaktivity.nukleus.socks.internal.types.stream.EndFW;
import org.reaktivity.nukleus.socks.internal.types.stream.ResetFW;
import org.reaktivity.nukleus.socks.internal.types.stream.WindowFW;

final class ConnectReplyStreamHandler extends DefaultStreamHandler
{
    private final MessageConsumer connectReplyThrottle;
    private final long connectReplyId;

    private MessageConsumer acceptReply;
    private long acceptReplyId;

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

    void handleStream(
        int msgTypeId,
        DirectBuffer buffer,
        int index,
        int length)
    {
        System.out.println("ServerConnectReplyStream doStream");
        streamState.accept(msgTypeId, buffer, index, length);
    }

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
            final String acceptReplyName = correlation.acceptName();

            final MessageConsumer newAcceptReply = context.router.supplyTarget(acceptReplyName);
            final long newAcceptReplyId = context.supplyStreamId.getAsLong();
            final long newCorrelationId = correlation.correlationId();


            // TODO the Socks Server has responded with a Begin Frame
            // TODO negotiate the version and authentication methods
            // FIXME how to deal with the correlation (the response back on the accept reply)


            context.router.setThrottle(acceptReplyName, newAcceptReplyId, this::handleThrottle);

            this.acceptReply = newAcceptReply;
            this.acceptReplyId = newAcceptReplyId;

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
