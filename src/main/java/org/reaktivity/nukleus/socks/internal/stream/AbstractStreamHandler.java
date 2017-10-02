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
package org.reaktivity.nukleus.socks.internal.stream;

import org.agrona.DirectBuffer;
import org.reaktivity.nukleus.function.MessageConsumer;
import org.reaktivity.nukleus.socks.internal.types.stream.AbortFW;
import org.reaktivity.nukleus.socks.internal.types.stream.ResetFW;
import org.reaktivity.nukleus.socks.internal.types.stream.WindowFW;

public abstract class AbstractStreamHandler
{
    public static final String NUKLEUS_SOCKS_NAME = "socks";

    protected final Context context;

    public AbstractStreamHandler(Context context)
    {
        this.context = context;
    }

    protected abstract void handleStream(
        int msgTypeId,
        DirectBuffer buffer,
        int index,
        int length);

    protected void doAbort(
        MessageConsumer stream,
        long streamId)
    {
        final AbortFW abort =
            context.abortRW
                .wrap(context.writeBuffer, 0, context.writeBuffer.capacity())
                .streamId(streamId)
                .extension(e -> e.reset())
                .build();
        stream.accept(abort.typeId(), abort.buffer(), abort.offset(), abort.sizeof());
    }

    protected void doWindow(
        final MessageConsumer throttle,
        final long throttleId,
        final int writableBytes,
        final int writableFrames)
    {
        final WindowFW window = context.windowRW
            .wrap(context.writeBuffer, 0, context.writeBuffer.capacity())
            .streamId(throttleId)
            .update(writableBytes)
            .frames(writableFrames)
            .build();
        throttle.accept(window.typeId(), window.buffer(), window.offset(), window.sizeof());
    }

    protected void doZeroWindow(
        final MessageConsumer throttle,
        final long throttleId)
    {
        final WindowFW window =
            context.windowRW
                .wrap(context.writeBuffer, 0, context.writeBuffer.capacity())
                .streamId(throttleId)
                .update(0)
                .frames(0)
                .build();
        throttle.accept(window.typeId(), window.buffer(), window.offset(), window.sizeof());
    }

    protected void doReset(
        final MessageConsumer throttle,
        final long throttleId)
    {
        final ResetFW reset =
            context.resetRW
                .wrap(context.writeBuffer, 0, context.writeBuffer.capacity())
                .streamId(throttleId)
                .build();
        throttle.accept(reset.typeId(), reset.buffer(), reset.offset(), reset.sizeof());
        // TODO change state
    }
}