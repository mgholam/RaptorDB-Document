/**
 * 
 * Modifications by Simon Hewitt
 *  - change constructors/methods to return byte[]
 *  - append original source size at the end of the destination buffer
 *  - add support for MemoryStream internal buffer usage
 * 
 * 
 * ManagedLZO.MiniLZO
 * 
 * Minimalistic reimplementation of minilzo in C#
 * 
 * @author Shane Eric Bryldt, Copyright (C) 2006, All Rights Reserved
 * @note Uses unsafe/fixed pointer contexts internally
 * @liscence Bound by same liscence as minilzo as below, see file COPYING
 */

/* Based on minilzo.c -- mini subset of the LZO real-time data compression library

   This file is part of the LZO real-time data compression library.

   Copyright (C) 2005 Markus Franz Xaver Johannes Oberhumer
   Copyright (C) 2004 Markus Franz Xaver Johannes Oberhumer
   Copyright (C) 2003 Markus Franz Xaver Johannes Oberhumer
   Copyright (C) 2002 Markus Franz Xaver Johannes Oberhumer
   Copyright (C) 2001 Markus Franz Xaver Johannes Oberhumer
   Copyright (C) 2000 Markus Franz Xaver Johannes Oberhumer
   Copyright (C) 1999 Markus Franz Xaver Johannes Oberhumer
   Copyright (C) 1998 Markus Franz Xaver Johannes Oberhumer
   Copyright (C) 1997 Markus Franz Xaver Johannes Oberhumer
   Copyright (C) 1996 Markus Franz Xaver Johannes Oberhumer
   All Rights Reserved.

   The LZO library is free software; you can redistribute it and/or
   modify it under the terms of the GNU General Public License,
   version 2, as published by the Free Software Foundation.

   The LZO library is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with the LZO library; see the file COPYING.
   If not, write to the Free Software Foundation, Inc.,
   51 Franklin Street, Fifth Floor, Boston, MA 02110-1301, USA.

   Markus F.X.J. Oberhumer
   <markus@oberhumer.com>
   http://www.oberhumer.com/opensource/lzo/
 */

/*
 * NOTE:
 *   the full LZO package can be found at
 *   http://www.oberhumer.com/opensource/lzo/
 */

using System;
using System.IO;

namespace RaptorDB
{
    public class MiniLZO
    {
        private const uint M2_MAX_LEN = 8;
        private const uint M4_MAX_LEN = 9;
        private const byte M3_MARKER = 32;
        private const byte M4_MARKER = 16;
        private const uint M2_MAX_OFFSET = 0x0800;
        private const uint M3_MAX_OFFSET = 0x4000;
        private const uint M4_MAX_OFFSET = 0xbfff;
        private const byte BITS = 14;
        private const uint D_MASK = (1 << BITS) - 1;
        private static uint DICT_SIZE = 65536 + 3;

        static MiniLZO()
        {
            if (IntPtr.Size == 8)
                DICT_SIZE = (65536 + 3) * 2;
        }
        public static byte[] Compress(byte[] src) { return Compress(src, 0, src.Length); }
        public static byte[] Compress(byte[] src, int srcCount) { return Compress(src, 0, srcCount); }
        public static byte[] Compress(byte[] src, int srcStart, int srcLength)
        {
            byte[] workMem = new byte[DICT_SIZE];
            uint dstlen = (uint)(srcLength + (srcLength / 16) + 64 + 3 + 4);
            byte[] dst = new byte[dstlen];

            uint compressedSize = Compress(src, (uint)srcStart, (uint)srcLength, dst, 0, dstlen, workMem, 0);

            if (dst.Length != compressedSize)
            {
                byte[] final = new byte[compressedSize];
                Buffer.BlockCopy(dst, 0, final, 0, (int)compressedSize);
                dst = final;
            }

            return dst;

        }

        public static byte[] Compress(MemoryStream source)
        {
            byte[] destinationBuffer;
            byte[] workspaceBuffer;
            uint sourceOffset;
            uint workspaceOffset;
            uint sourceLength;
            uint destinationLength;

            byte[] sourceBuffer = source.GetBuffer();
            uint sourceCapacity = (uint)source.Capacity;
            sourceLength = (uint)source.Length;
            destinationLength = sourceLength + (sourceLength / 16) + 64 + 3 + 4;

            uint unusedSpace = sourceCapacity - sourceLength;
            uint inplaceOverhead = Math.Min(sourceLength, M4_MAX_OFFSET) + sourceLength / 64 + 16 + 3 + 4;

            if (unusedSpace < inplaceOverhead)
            {
                sourceOffset = 0;
                destinationBuffer = new byte[destinationLength];
            }
            else
            {
                sourceOffset = inplaceOverhead;
                source.SetLength(sourceLength + inplaceOverhead);
                destinationBuffer = sourceBuffer;
                Buffer.BlockCopy(destinationBuffer, 0, destinationBuffer, (int)inplaceOverhead, (int)sourceLength);
                unusedSpace -= inplaceOverhead;
            }

            if (unusedSpace < DICT_SIZE)
            {
                workspaceBuffer = new byte[DICT_SIZE];
                workspaceOffset = 0;
            }
            else
            {
                workspaceBuffer = sourceBuffer;
                workspaceOffset = sourceCapacity - DICT_SIZE;
            }

            uint compressedSize = Compress(sourceBuffer, sourceOffset, sourceLength, destinationBuffer, 0, destinationLength, workspaceBuffer, workspaceOffset);

            if (destinationBuffer == sourceBuffer)
            {
                source.SetLength(compressedSize);
                source.Capacity = (int)compressedSize;
                return source.GetBuffer();
            }
            else
            {
                byte[] final = new byte[compressedSize];
                Buffer.BlockCopy(destinationBuffer, 0, final, 0, (int)compressedSize);
                return final;
            }

        }

        private static unsafe uint Compress(byte[] src, uint srcstart, uint srcLength, byte[] dst, uint dststart, uint dstlen, byte[] workmem, uint workmemstart)
        {
            uint tmp;
            if (srcLength <= M2_MAX_LEN + 5)
            {
                tmp = (uint)srcLength;
                dstlen = 0;
            }
            else
            {
                fixed (byte* work = &workmem[workmemstart], input = &src[srcstart], output = &dst[dststart])
                {
                    byte** dict = (byte**)work;
                    byte* in_end = input + srcLength;
                    byte* ip_end = input + srcLength - M2_MAX_LEN - 5;
                    byte* ii = input;
                    byte* ip = input + 4;
                    byte* op = output;
                    bool literal = false;
                    bool match = false;
                    uint offset;
                    uint length;
                    uint index;
                    byte* pos;

                    for (; ; )
                    {
                        offset = 0;
                        index = D_INDEX1(ip);
                        pos = ip - (ip - dict[index]);
                        if (pos < input || (offset = (uint)(ip - pos)) <= 0 || offset > M4_MAX_OFFSET)
                            literal = true;
                        else if (offset <= M2_MAX_OFFSET || pos[3] == ip[3]) { }
                        else
                        {
                            index = D_INDEX2(index);
                            pos = ip - (ip - dict[index]);
                            if (pos < input || (offset = (uint)(ip - pos)) <= 0 || offset > M4_MAX_OFFSET)
                                literal = true;
                            else if (offset <= M2_MAX_OFFSET || pos[3] == ip[3]) { }
                            else
                                literal = true;
                        }

                        if (!literal)
                        {
                            if (*((ushort*)pos) == *((ushort*)ip) && pos[2] == ip[2])
                                match = true;
                        }

                        literal = false;
                        if (!match)
                        {
                            dict[index] = ip;
                            ++ip;
                            if (ip >= ip_end)
                                break;
                            continue;
                        }
                        match = false;
                        dict[index] = ip;
                        if (ip - ii > 0)
                        {
                            uint t = (uint)(ip - ii);
                            if (t <= 3)
                            {
                                //Debug.Assert(op - 2 > output);
                                op[-2] |= (byte)(t);
                            }
                            else if (t <= 18)
                                *op++ = (byte)(t - 3);
                            else
                            {
                                uint tt = t - 18;
                                *op++ = 0;
                                while (tt > 255)
                                {
                                    tt -= 255;
                                    *op++ = 0;
                                }
                                //Debug.Assert(tt > 0);
                                *op++ = (byte)(tt);
                            }
                            do
                            {
                                *op++ = *ii++;
                            } while (--t > 0);
                        }
                        //Debug.Assert(ii == ip);
                        ip += 3;
                        if (pos[3] != *ip++ || pos[4] != *ip++ || pos[5] != *ip++
                           || pos[6] != *ip++ || pos[7] != *ip++ || pos[8] != *ip++)
                        {
                            --ip;
                            length = (uint)(ip - ii);
                            //Debug.Assert(length >= 3);
                            //Debug.Assert(length <= M2_MAX_LEN);
                            if (offset <= M2_MAX_OFFSET)
                            {
                                --offset;
                                *op++ = (byte)(((length - 1) << 5) | ((offset & 7) << 2));
                                *op++ = (byte)(offset >> 3);
                            }
                            else if (offset <= M3_MAX_OFFSET)
                            {
                                --offset;
                                *op++ = (byte)(M3_MARKER | (length - 2));
                                *op++ = (byte)((offset & 63) << 2);
                                *op++ = (byte)(offset >> 6);
                            }
                            else
                            {
                                offset -= 0x4000;
                                //Debug.Assert(offset > 0);
                                //Debug.Assert(offset <= 0x7FFF);
                                *op++ = (byte)(M4_MARKER | ((offset & 0x4000) >> 11) | (length - 2));
                                *op++ = (byte)((offset & 63) << 2);
                                *op++ = (byte)(offset >> 6);
                            }
                        }
                        else
                        {
                            byte* m = pos + M2_MAX_LEN + 1;
                            while (ip < in_end && *m == *ip)
                            {
                                ++m;
                                ++ip;
                            }
                            length = (uint)(ip - ii);
                            //Debug.Assert(length > M2_MAX_LEN);
                            if (offset <= M3_MAX_OFFSET)
                            {
                                --offset;
                                if (length <= 33)
                                    *op++ = (byte)(M3_MARKER | (length - 2));
                                else
                                {
                                    length -= 33;
                                    *op++ = M3_MARKER | 0;
                                    while (length > 255)
                                    {
                                        length -= 255;
                                        *op++ = 0;
                                    }
                                    //Debug.Assert(length > 0);
                                    *op++ = (byte)(length);
                                }
                            }
                            else
                            {
                                offset -= 0x4000;
                                //Debug.Assert(offset > 0);
                                //Debug.Assert(offset <= 0x7FFF);
                                if (length <= M4_MAX_LEN)
                                    *op++ = (byte)(M4_MARKER | ((offset & 0x4000) >> 11) | (length - 2));
                                else
                                {
                                    length -= M4_MAX_LEN;
                                    *op++ = (byte)(M4_MARKER | ((offset & 0x4000) >> 11));
                                    while (length > 255)
                                    {
                                        length -= 255;
                                        *op++ = 0;
                                    }
                                    //Debug.Assert(length > 0);
                                    *op++ = (byte)(length);
                                }
                            }
                            *op++ = (byte)((offset & 63) << 2);
                            *op++ = (byte)(offset >> 6);
                        }
                        ii = ip;
                        if (ip >= ip_end)
                            break;
                    }
                    dstlen = (uint)(op - output);
                    tmp = (uint)(in_end - ii);
                }
            }
            if (tmp > 0)
            {
                uint ii = (uint)srcLength - tmp + srcstart;
                if (dstlen == 0 && tmp <= 238)
                {
                    dst[dstlen++] = (byte)(17 + tmp);
                }
                else if (tmp <= 3)
                {
                    dst[dstlen - 2] |= (byte)(tmp);
                }
                else if (tmp <= 18)
                {
                    dst[dstlen++] = (byte)(tmp - 3);
                }
                else
                {
                    uint tt = tmp - 18;
                    dst[dstlen++] = 0;
                    while (tt > 255)
                    {
                        tt -= 255;
                        dst[dstlen++] = 0;
                    }
                    //Debug.Assert(tt > 0);
                    dst[dstlen++] = (byte)(tt);
                }
                do
                {
                    dst[dstlen++] = src[ii++];
                } while (--tmp > 0);
            }
            dst[dstlen++] = M4_MARKER | 1;
            dst[dstlen++] = 0;
            dst[dstlen++] = 0;

            // Append the source count
            dst[dstlen++] = (byte)srcLength;
            dst[dstlen++] = (byte)(srcLength >> 8);
            dst[dstlen++] = (byte)(srcLength >> 16);
            dst[dstlen++] = (byte)(srcLength >> 24);

            return dstlen;
        }

        public static unsafe byte[] Decompress(byte[] src)
        {
            byte[] dst = new byte[(src[src.Length - 4] | (src[src.Length - 3] << 8) | (src[src.Length - 2] << 16 | src[src.Length - 1] << 24))];

            uint t = 0;
            fixed (byte* input = src, output = dst)
            {
                byte* pos = null;
                byte* ip_end = input + src.Length - 4;
                byte* op_end = output + dst.Length;
                byte* ip = input;
                byte* op = output;
                bool match = false;
                bool match_next = false;
                bool match_done = false;
                bool copy_match = false;
                bool first_literal_run = false;
                bool eof_found = false;

                if (*ip > 17)
                {
                    t = (uint)(*ip++ - 17);
                    if (t < 4)
                        match_next = true;
                    else
                    {
                        //Debug.Assert(t > 0);
                        if ((op_end - op) < t)
                            throw new OverflowException("Output Overrun");
                        if ((ip_end - ip) < t + 1)
                            throw new OverflowException("Input Overrun");
                        do
                        {
                            *op++ = *ip++;
                        } while (--t > 0);
                        first_literal_run = true;
                    }
                }
                while (!eof_found && ip < ip_end)
                {
                    if (!match_next && !first_literal_run)
                    {
                        t = *ip++;
                        if (t >= 16)
                            match = true;
                        else
                        {
                            if (t == 0)
                            {
                                if ((ip_end - ip) < 1)
                                    throw new OverflowException("Input Overrun");
                                while (*ip == 0)
                                {
                                    t += 255;
                                    ++ip;
                                    if ((ip_end - ip) < 1)
                                        throw new OverflowException("Input Overrun");
                                }
                                t += (uint)(15 + *ip++);
                            }
                            //Debug.Assert(t > 0);
                            if ((op_end - op) < t + 3)
                                throw new OverflowException("Output Overrun");
                            if ((ip_end - ip) < t + 4)
                                throw new OverflowException("Input Overrun");
                            for (int x = 0; x < 4; ++x, ++op, ++ip)
                                *op = *ip;
                            if (--t > 0)
                            {
                                if (t >= 4)
                                {
                                    do
                                    {
                                        for (int x = 0; x < 4; ++x, ++op, ++ip)
                                            *op = *ip;
                                        t -= 4;
                                    } while (t >= 4);
                                    if (t > 0)
                                    {
                                        do
                                        {
                                            *op++ = *ip++;
                                        } while (--t > 0);
                                    }
                                }
                                else
                                {
                                    do
                                    {
                                        *op++ = *ip++;
                                    } while (--t > 0);
                                }
                            }
                        }
                    }
                    if (!match && !match_next)
                    {
                        first_literal_run = false;

                        t = *ip++;
                        if (t >= 16)
                            match = true;
                        else
                        {
                            pos = op - (1 + M2_MAX_OFFSET);
                            pos -= t >> 2;
                            pos -= *ip++ << 2;
                            if (pos < output || pos >= op)
                                throw new OverflowException("Lookbehind Overrun");
                            if ((op_end - op) < 3)
                                throw new OverflowException("Output Overrun");
                            *op++ = *pos++;
                            *op++ = *pos++;
                            *op++ = *pos++;
                            match_done = true;
                        }
                    }
                    match = false;
                    do
                    {
                        if (t >= 64)
                        {
                            pos = op - 1;
                            pos -= (t >> 2) & 7;
                            pos -= *ip++ << 3;
                            t = (t >> 5) - 1;
                            if (pos < output || pos >= op)
                                throw new OverflowException("Lookbehind Overrun");
                            if ((op_end - op) < t + 2)
                                throw new OverflowException("Output Overrun");
                            copy_match = true;
                        }
                        else if (t >= 32)
                        {
                            t &= 31;
                            if (t == 0)
                            {
                                if ((ip_end - ip) < 1)
                                    throw new OverflowException("Input Overrun");
                                while (*ip == 0)
                                {
                                    t += 255;
                                    ++ip;
                                    if ((ip_end - ip) < 1)
                                        throw new OverflowException("Input Overrun");
                                }
                                t += (uint)(31 + *ip++);
                            }
                            pos = op - 1;
                            pos -= (*(ushort*)ip) >> 2;
                            ip += 2;
                        }
                        else if (t >= 16)
                        {
                            pos = op;
                            pos -= (t & 8) << 11;

                            t &= 7;
                            if (t == 0)
                            {
                                if ((ip_end - ip) < 1)
                                    throw new OverflowException("Input Overrun");
                                while (*ip == 0)
                                {
                                    t += 255;
                                    ++ip;
                                    if ((ip_end - ip) < 1)
                                        throw new OverflowException("Input Overrun");
                                }
                                t += (uint)(7 + *ip++);
                            }
                            pos -= (*(ushort*)ip) >> 2;
                            ip += 2;
                            if (pos == op)
                                eof_found = true;
                            else
                                pos -= 0x4000;
                        }
                        else
                        {
                            pos = op - 1;
                            pos -= t >> 2;
                            pos -= *ip++ << 2;
                            if (pos < output || pos >= op)
                                throw new OverflowException("Lookbehind Overrun");
                            if ((op_end - op) < 2)
                                throw new OverflowException("Output Overrun");
                            *op++ = *pos++;
                            *op++ = *pos++;
                            match_done = true;
                        }
                        if (!eof_found && !match_done && !copy_match)
                        {
                            if (pos < output || pos >= op)
                                throw new OverflowException("Lookbehind Overrun");
                            //Debug.Assert(t > 0);
                            if ((op_end - op) < t + 2)
                                throw new OverflowException("Output Overrun");
                        }
                        if (!eof_found && t >= 2 * 4 - 2 && (op - pos) >= 4 && !match_done && !copy_match)
                        {
                            for (int x = 0; x < 4; ++x, ++op, ++pos)
                                *op = *pos;
                            t -= 2;
                            do
                            {
                                for (int x = 0; x < 4; ++x, ++op, ++pos)
                                    *op = *pos;
                                t -= 4;
                            } while (t >= 4);
                            if (t > 0)
                            {
                                do
                                {
                                    *op++ = *pos++;
                                } while (--t > 0);
                            }
                        }
                        else if (!eof_found && !match_done)
                        {
                            copy_match = false;

                            *op++ = *pos++;
                            *op++ = *pos++;
                            do
                            {
                                *op++ = *pos++;
                            } while (--t > 0);
                        }

                        if (!eof_found && !match_next)
                        {
                            match_done = false;

                            t = (uint)(ip[-2] & 3);
                            if (t == 0)
                                break;
                        }
                        if (!eof_found)
                        {
                            match_next = false;
                            //Debug.Assert(t > 0);
                            //Debug.Assert(t < 4);
                            if ((op_end - op) < t)
                                throw new OverflowException("Output Overrun");
                            if ((ip_end - ip) < t + 1)
                                throw new OverflowException("Input Overrun");
                            *op++ = *ip++;
                            if (t > 1)
                            {
                                *op++ = *ip++;
                                if (t > 2)
                                    *op++ = *ip++;
                            }
                            t = *ip++;
                        }
                    } while (!eof_found && ip < ip_end);
                }
                if (!eof_found)
                    throw new OverflowException("EOF Marker Not Found");
                else
                {
                    //Debug.Assert(t == 1);
                    if (ip > ip_end)
                        throw new OverflowException("Input Overrun");
                    else if (ip < ip_end)
                        throw new OverflowException("Input Not Consumed");
                }
            }

            return dst;
        }

        private unsafe static uint D_INDEX1(byte* input)
        {
            return D_MS(D_MUL(0x21, D_X3(input, 5, 5, 6)) >> 5, 0);
        }

        private static uint D_INDEX2(uint idx)
        {
            return (idx & (D_MASK & 0x7FF)) ^ (((D_MASK >> 1) + 1) | 0x1F);
        }

        private static uint D_MS(uint v, byte s)
        {
            return (v & (D_MASK >> s)) << s;
        }

        private static uint D_MUL(uint a, uint b)
        {
            return a * b;
        }

        private unsafe static uint D_X2(byte* input, byte s1, byte s2)
        {
            return (uint)((((input[2] << s2) ^ input[1]) << s1) ^ input[0]);
        }

        private unsafe static uint D_X3(byte* input, byte s1, byte s2, byte s3)
        {
            return (D_X2(input + 1, s2, s3) << s1) ^ input[0];
        }
    }
}