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
package org.reaktivity.nukleus.socks.internal.stream.types;

import java.nio.charset.StandardCharsets;

import org.agrona.BitUtil;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.reaktivity.nukleus.socks.internal.types.Flyweight;
import org.reaktivity.nukleus.socks.internal.types.OctetsFW;
import org.reaktivity.nukleus.socks.internal.types.SocksAddressFW;
import org.reaktivity.nukleus.socks.internal.types.StringFW;
import org.reaktivity.nukleus.socks.internal.types.control.SocksRouteExFW;

public class SocksCommandRequestFW extends FragmentedFlyweight<SocksCommandRequestFW>
{

    /*
     * From https://tools.ietf.org/html/rfc1928
     *      The SOCKS request is formed as follows:
            +----+-----+-------+------+----------+----------+
            |VER | CMD |  RSV  | ATYP | DST.ADDR | DST.PORT |
            +----+-----+-------+------+----------+----------+
            | 1  |  1  | X'00' |  1   | Variable |    2     |
            +----+-----+-------+------+----------+----------+

             o  ATYP   address type of following address
                 o  IP V4 address: X'01'
                 o  DOMAINNAME: X'03'
                 o  IP V6 address: X'04'
     *
     */
    private static final int FIELD_OFFSET_VERSION = 0;
    private static final int FIELD_SIZEBY_VERSION = BitUtil.SIZE_OF_BYTE;

    private static final int FIELD_OFFSET_COMMAND = FIELD_OFFSET_VERSION + FIELD_SIZEBY_VERSION;
    private static final int FIELD_SIZEBY_COMMAND = BitUtil.SIZE_OF_BYTE;

    private static final int FIELD_OFFSET_RSV = FIELD_OFFSET_COMMAND + FIELD_SIZEBY_COMMAND;
    private static final int FIELD_SIZEBY_RSV = BitUtil.SIZE_OF_BYTE;

    private static final int FIELD_OFFSET_ADDRTYP = FIELD_OFFSET_RSV + FIELD_SIZEBY_RSV;
    private static final int FIELD_SIZEBY_ADDRTYP = BitUtil.SIZE_OF_BYTE;

    // DST.ADDR offset is computed for each request

    private static final int FIELD_SIZEBY_DSTPORT = BitUtil.SIZE_OF_SHORT; // Offset 0, relative to end of ADDRDST field

    // Temporary data used for decoding
    private final StringFW domainFW = new StringFW();
    private final OctetsFW addressFW = new OctetsFW();


    private byte[] ipv4 = new byte[4];
    private byte[] ipv6 = new byte[16];

    private BuildState buildState = BuildState.INITIAL;

    @Override
    public int limit()
    {
        return decodeLimit(buffer(), offset());
    }

    @Override
    public ReadState canWrap(
        DirectBuffer buffer,
        int offset,
        int maxLimit)
    {
        final int maxLength = maxLimit - offset;
        if (maxLength < FIELD_OFFSET_ADDRTYP + FIELD_SIZEBY_ADDRTYP)
        {
            return ReadState.INCOMPLETE;
        }
        final int limit = decodeLimit(buffer, offset);
        if (limit < 0)
        {
            return ReadState.BROKEN;
        }
        if (limit > maxLimit)
        {
            return ReadState.INCOMPLETE;
        }
        return ReadState.FULL;
    }

    @Override
    public BuildState getBuildState()
    {
        return buildState;
    }

    private int decodeLimit(
        DirectBuffer buffer,
        int offset)
    {
        final int addrTypOffset = offset + FIELD_OFFSET_ADDRTYP;
        final byte addrLength;
        byte addrVariableSize = 0;
        switch (buffer.getByte(addrTypOffset))
        {
        case 0x01:
            addrLength = 4;
            break;
        case 0x03:
            addrLength = buffer.getByte(addrTypOffset + FIELD_SIZEBY_ADDRTYP);
            addrVariableSize = 1;
            break;
        case 0x04:
            addrLength = 16;
            break;
        default:
            return -1;
        }
        return addrTypOffset + FIELD_SIZEBY_ADDRTYP + addrVariableSize + addrLength + FIELD_SIZEBY_DSTPORT;
    }

    public byte version()
    {
        return (buffer().getByte(offset() + FIELD_OFFSET_VERSION));
    }

    public byte command()
    {
        return (buffer().getByte(offset() + FIELD_OFFSET_COMMAND));
    }

    public byte atype()
    {
        return (buffer().getByte(offset() + FIELD_OFFSET_ADDRTYP));
    }

    public String domain()
    {
        if (atype() == 0x03)
        {
            return domainFW.wrap(buffer(), offset() + FIELD_OFFSET_ADDRTYP + FIELD_SIZEBY_ADDRTYP, maxLimit())
                .asString();
        }
        return null;
    }

    public Flyweight socksAddress()
    {
        if (atype() == 0x01)
        {
            return addressFW.wrap(buffer(), offset()  + FIELD_OFFSET_ADDRTYP + FIELD_SIZEBY_ADDRTYP, 4);
        }
        else if (atype() == 0x04)
        {
            return addressFW.wrap(buffer(), offset()  + FIELD_OFFSET_ADDRTYP + FIELD_SIZEBY_ADDRTYP, 16);
        }
        else if (atype() == 0x03)
        {
            return domainFW.wrap(buffer(), offset() + FIELD_OFFSET_ADDRTYP + FIELD_SIZEBY_ADDRTYP, maxLimit());
        }
        return null;
    }

    public int port()
    {
        int portOffset = decodeLimit(buffer(), offset()) - FIELD_SIZEBY_DSTPORT;
        return ((buffer().getByte(portOffset) & 0xff) << 8) | (buffer().getByte(portOffset + 1) & 0xff);
    }

    public static final class Builder extends Flyweight.Builder<SocksCommandRequestFW>
    {
        public Builder()
        {
            super(new SocksCommandRequestFW());
        }

        @Override
        public SocksCommandRequestFW build()
        {
            final SocksCommandRequestFW socksCommandRequestFW = super.build();
            if (socksCommandRequestFW.buildState == BuildState.INITIAL)
            {
                socksCommandRequestFW.buildState = BuildState.FINAL;
            }
            return socksCommandRequestFW;
        }

        @Override
        public Builder wrap(
            MutableDirectBuffer buffer,
            int offset,
            int maxLimit)
        {
            flyweight().buildState = BuildState.INITIAL;
            super.wrap(buffer, offset, maxLimit);
            int newLimit = limit() + BitUtil.SIZE_OF_BYTE;
            checkLimit(newLimit, maxLimit());
            buffer().putByte(offset() + FIELD_OFFSET_RSV, (byte) 0x00);
            limit(newLimit);
            return this;
        }

        public Builder version(byte version)
        {
            int newLimit = limit() + BitUtil.SIZE_OF_BYTE;
            checkLimit(newLimit, maxLimit());
            buffer().putByte(offset() + FIELD_OFFSET_VERSION, version);
            limit(newLimit);
            return this;
        }

        public Builder command(byte command)
        {
            int newLimit = limit() + BitUtil.SIZE_OF_BYTE;
            checkLimit(newLimit, maxLimit());
            buffer().putByte(offset() + FIELD_OFFSET_COMMAND, command);
            limit(newLimit);
            return this;
        }

        public Builder destination(SocksRouteExFW socksRouteExFW)
        {
            int port = socksRouteExFW.socksPort();
            byte[] a = null;

            SocksAddressFW fw = socksRouteExFW.socksAddress();
            if (fw.kind() == 1)
            {
                a = new byte[4];
                fw.ipv4Address().buffer().getBytes(fw.ipv4Address().offset(), a);
            }
            else if (fw.kind() == 3)
            {
                a = fw.domainName().asString().getBytes(StandardCharsets.UTF_8);
            }
            else if (fw.kind() == 4)
            {
                a = new byte[16];
                fw.ipv6Address().buffer().getBytes(fw.ipv6Address().offset(), a);
            }
            if (a != null)
            {
                return this.destination((byte) fw.kind(), a, port);
            }
            return this;
        }


        public Builder destination(
            byte atyp,
            byte[] addr,
            int port)
        {
            int newLimit = limit() + BitUtil.SIZE_OF_BYTE;
            checkLimit(newLimit, maxLimit());
            buffer().putByte(offset() + FIELD_OFFSET_ADDRTYP, atyp);

            int addrOffset = offset() + FIELD_OFFSET_ADDRTYP + FIELD_SIZEBY_ADDRTYP;
            if (atyp == 0x03)
            {
                checkLimit(++newLimit, maxLimit());
                buffer().putByte(addrOffset++, (byte) addr.length);
            }
            newLimit += addr.length + 2;
            checkLimit(newLimit, maxLimit());
            buffer().putBytes(addrOffset, addr);
            buffer().putByte(addrOffset + addr.length, (byte) ((port >> 8) & 0xFF));
            buffer().putByte(addrOffset + addr.length + 1, (byte) (port & 0xFF));
            limit(newLimit);
            return this;
        }
    }
}
