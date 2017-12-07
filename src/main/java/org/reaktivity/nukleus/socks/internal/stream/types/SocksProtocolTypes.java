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

public interface SocksProtocolTypes
{
    byte SOCKS_VERSION_5 = 0x05;
    byte COMMAND_CONNECT = 0x01;
    byte AUTH_METHOD_NONE = 0x00;
    byte AUTH_NO_ACCEPTABLE_METHODS = (byte) 0xFF;
    byte REPLY_SUCCEEDED = 0x00;
    byte REPLY_COMMAND_NOT_SUPPORTED = 0x07;

}
