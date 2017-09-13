/*
 *
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
package org.reaktivity.nukleus.socks.internal.stream.protocol;

import org.agrona.BitUtil;
import org.agrona.DirectBuffer;
import org.reaktivity.nukleus.socks.internal.types.Flyweight;

public class SocksNegotiationRequestFW extends Flyweight
{

    private static final int FIELD_OFFSET_VERSION = 0;
    private static final int FIELD_SIZEBY_VERSION = BitUtil.SIZE_OF_BYTE;

    private static final int FIELD_OFFSET_METHODS = FIELD_OFFSET_VERSION + FIELD_SIZEBY_VERSION;
    private static final int FIELD_SIZEBY_METHODS = BitUtil.SIZE_OF_BYTE;

    @Override
    public int limit()
    {
        return limit(buffer(), offset());
    }

    private int limit(
        DirectBuffer buffer,
        int offset)
    {
        final int currentFieldOffsetMethods = offset + FIELD_OFFSET_METHODS;
        return currentFieldOffsetMethods + FIELD_SIZEBY_METHODS + buffer.getByte(currentFieldOffsetMethods);
    }

    public boolean canWrap(
        DirectBuffer buffer,
        int offset,
        int maxLimit)
    {
        final int maxLength = maxLimit - offset;
        if (maxLength < 2)
        {
            return false;
        }

        return limit(buffer, offset) <= maxLimit;
    }

    @Override
    public SocksNegotiationRequestFW wrap(DirectBuffer buffer, int offset, int maxLimit)
    {
        super.wrap(buffer, offset, maxLimit);
        checkLimit(limit(), maxLimit);
        return this;
    }

    public byte version()
    {
        return (buffer().getByte(offset() + FIELD_OFFSET_VERSION));
    }

    public byte nmethods()
    {
        return (buffer().getByte(offset() + FIELD_OFFSET_METHODS));
    }

    public byte[] methods()
    {
        byte[] methods = new byte[sizeof()];
        buffer().getBytes(offset() + FIELD_OFFSET_METHODS + FIELD_SIZEBY_METHODS, methods);
        return methods;
    }
}
