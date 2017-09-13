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
package org.reaktivity.nukleus.socks.internal;

import org.agrona.DirectBuffer;
import org.jboss.byteman.rule.helper.Helper;

public final class SocksBufferHelper
{
    public static class SocksNegotiationFWCanWrap extends Helper
    {
        protected SocksNegotiationFWCanWrap(org.jboss.byteman.rule.Rule rule)
        {
            super(rule);
        }

        public void write(
            DirectBuffer buffer,
            int offset,
            int maxLimit)
        {
            SocksBufferHelper.write("Checking if buffer can be wrapped on SocksNegotiationRequestFW: [", buffer, offset,
                maxLimit);
        }
    }

    public static class Flyweight extends Helper
    {
        protected Flyweight(org.jboss.byteman.rule.Rule rule)
        {
            super(rule);
        }

        public void write(
            Object flyweight,
            DirectBuffer buffer,
            int offset,
            int limit)
        {
            SocksBufferHelper.write("Wrapping on Flyweight[" + flyweight.getClass()+"]: [", buffer, offset, limit);
        }
    }

    private SocksBufferHelper()
    {

    }

    private static void write(
        String message,
        DirectBuffer buffer,
        int offset,
        int maxLimit)
    {
        System.out.print(message);
        for (int i = offset; i < maxLimit; i++)
        {
            System.out.print(String.format("%02X ", buffer.getByte(i)));
        }
        System.out.println("]");
    }
}
