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

import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;

import org.agrona.BitUtil;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.reaktivity.nukleus.socks.internal.types.Flyweight;
import org.reaktivity.nukleus.socks.internal.types.StringFW;

public class SocksCommandRequestFW extends Flyweight implements Fragmented
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
    private byte[] ipv4 = new byte[4];
    private byte[] ipv6 = new byte[16];

    @Override
    public int limit()
    {
        return decodeLimit(buffer(), offset());
    }

    @Override
    public boolean canWrap(
        DirectBuffer buffer,
        int offset,
        int maxLimit)
    {
        final int maxLength = maxLimit - offset;
        if (maxLength < FIELD_OFFSET_ADDRTYP + FIELD_SIZEBY_ADDRTYP)
        {
            return false;
        }

        return decodeLimit(buffer, offset) <= maxLimit;
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
            throw new IllegalStateException("Unable to decode Socks destination address type");

        }

        return addrTypOffset + FIELD_SIZEBY_ADDRTYP + addrVariableSize + addrLength + FIELD_SIZEBY_DSTPORT;
    }

    @Override
    public SocksCommandRequestFW wrap(
        DirectBuffer buffer,
        int offset,
        int maxLimit)
    {
        super.wrap(buffer, offset, maxLimit);
        checkLimit(limit(), maxLimit);
        return this;
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

    public InetAddress ip() throws UnknownHostException
    {
        if (atype() == 0x01)
        {
            buffer().getBytes(offset() + FIELD_OFFSET_ADDRTYP + FIELD_SIZEBY_ADDRTYP, ipv4);
            return Inet4Address.getByAddress(ipv4);
        }
        else if (atype() == 0x04)
        {
            buffer().getBytes(offset() + FIELD_OFFSET_ADDRTYP + FIELD_SIZEBY_ADDRTYP, ipv6);
            return Inet6Address.getByAddress(ipv6);
        }
        return null;
    }

    public int port()
    {
        int portOffset = decodeLimit(buffer(), offset()) - FIELD_SIZEBY_DSTPORT;
        return ((buffer().getByte(portOffset) & 0xff) << 8) | (buffer().getByte(portOffset + 1) & 0xff);
    }

    public String validateAndGetDstAddrPort()
    {
        if (this.version() != 0x05)
        {
            throw new IllegalStateException(
                String.format("Unsupported SOCKS protocol version (expected 0x05, received 0x%02x",
                    this.version()));
        }

        if (this.command() != 0x01)
        {
            throw new IllegalStateException(
                String.format("Unsupported SOCKS command (expected 0x01 - CONNECT, received 0x%02x",
                    this.version()));
        }

        StringBuilder dstAddrPortBuilder = new StringBuilder();
        byte atype = this.atype();
        if (atype == 0x03)
        {
            dstAddrPortBuilder.append(this.domain());
        }
        else if (atype == 0x01 || atype == 0x04)
        {
            try
            {
                dstAddrPortBuilder.append(this.ip().getHostAddress());
            }
            catch (UnknownHostException e)
            {
                throw new IllegalStateException("Unsupported SOCKS connection address", e);
            }
        }
        else
        {
            throw new IllegalStateException(
                String.format("Unsupported SOCKS address type (expected 0x01/0x03/0x04, received 0x%02x", atype));
        }
        dstAddrPortBuilder.append(":").append(this.port());
        return dstAddrPortBuilder.toString();
    }

    public static final class Builder extends Flyweight.Builder<SocksCommandRequestFW>
    {
        public Builder()
        {
            super(new SocksCommandRequestFW());
        }

        @Override
        public Builder wrap(
            MutableDirectBuffer buffer,
            int offset,
            int maxLimit)
        {
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

        public Builder destination(String destinationAddress)
        {
            String[] tokens = destinationAddress.split(":");
            int port = Integer.parseInt(tokens[1]);
            // Remove digits, ., : and [ ], and check if there is anything remaining
            if (tokens[0].replaceAll("[0-9\\.\\]\\[:]", "").length() == 0)
            {
                try
                {
                    InetAddress inetAddress = InetAddress.getByName(tokens[0]);
                    return this.destination(inetAddress instanceof Inet4Address ? (byte) 0x01 : (byte) 0x04,
                        inetAddress.getAddress(), port);
                }
                catch (UnknownHostException e)
                {
                    throw new IllegalStateException("Unable to encode destinationAddress: " + destinationAddress, e);
                }
            }

            return this.destination((byte) 0x03, tokens[0].getBytes(StandardCharsets.UTF_8), port);
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
