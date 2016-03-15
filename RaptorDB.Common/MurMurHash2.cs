using System;

namespace RaptorDB.Common
{
    //internal static class murmur3
    //{
    //    private static uint seed = 7878;

    //    public static uint MurmurHash3(byte[] data)
    //    {
    //        const uint c1 = 0xcc9e2d51;
    //        const uint c2 = 0x1b873593;

    //        int curLength = data.Length;    /* Current position in byte array */
    //        int length = curLength;   /* the const length we need to fix tail */
    //        uint h1 = seed;
    //        uint k1 = 0;

    //        /* body, eat stream a 32-bit int at a time */
    //        int currentIndex = 0;
    //        while (curLength >= 4)
    //        {
    //            /* Get four bytes from the input into an UInt32 */
    //            k1 = (uint)(data[currentIndex++]
    //              | data[currentIndex++] << 8
    //              | data[currentIndex++] << 16
    //              | data[currentIndex++] << 24);

    //            /* bitmagic hash */
    //            k1 *= c1;
    //            k1 = rotl32(k1, 15);
    //            k1 *= c2;

    //            h1 ^= k1;
    //            h1 = rotl32(h1, 13);
    //            h1 = h1 * 5 + 0xe6546b64;
    //            curLength -= 4;
    //        }

    //        /* tail, the reminder bytes that did not make it to a full int */
    //        /* (this switch is slightly more ugly than the C++ implementation
    //         * because we can't fall through) */
    //        switch (curLength)
    //        {
    //            case 3:
    //                k1 = (UInt32)(data[currentIndex++]
    //                  | data[currentIndex++] << 8
    //                  | data[currentIndex++] << 16);
    //                k1 *= c1;
    //                k1 = rotl32(k1, 15);
    //                k1 *= c2;
    //                h1 ^= k1;
    //                break;
    //            case 2:
    //                k1 = (UInt32)(data[currentIndex++]
    //                  | data[currentIndex++] << 8);
    //                k1 *= c1;
    //                k1 = rotl32(k1, 15);
    //                k1 *= c2;
    //                h1 ^= k1;
    //                break;
    //            case 1:
    //                k1 = (UInt32)(data[currentIndex++]);
    //                k1 *= c1;
    //                k1 = rotl32(k1, 15);
    //                k1 *= c2;
    //                h1 ^= k1;
    //                break;
    //        };

    //        // finalization, magic chants to wrap it all up
    //        h1 ^= (uint)length;
    //        h1 = fmix(h1);

    //        unchecked
    //        {
    //            return (uint)h1;
    //        }
    //    }
    //    private static uint rotl32(uint x, byte r)
    //    {
    //        return (x << r) | (x >> (32 - r));
    //    }

    //    private static uint fmix(uint h)
    //    {
    //        h ^= h >> 16;
    //        h *= 0x85ebca6b;
    //        h ^= h >> 13;
    //        h *= 0xc2b2ae35;
    //        h ^= h >> 16;
    //        return h;
    //    }
    //}

    public class MurmurHash2Unsafe
    {
        public UInt32 Hash(Byte[] data)
        {
            return Hash(data, 0xc58f1a7b);
        }
        const UInt32 m = 0x5bd1e995;
        const Int32 r = 24;

        public unsafe UInt32 Hash(Byte[] data, UInt32 seed)
        {
            Int32 length = data.Length;
            if (length == 0)
                return 0;
            UInt32 h = seed ^ (UInt32)length;
            Int32 remainingBytes = length & 3; // mod 4
            Int32 numberOfLoops = length >> 2; // div 4
            fixed (byte* firstByte = &(data[0]))
            {
                UInt32* realData = (UInt32*)firstByte;
                while (numberOfLoops != 0)
                {
                    UInt32 k = *realData;
                    k *= m;
                    k ^= k >> r;
                    k *= m;

                    h *= m;
                    h ^= k;
                    numberOfLoops--;
                    realData++;
                }
                switch (remainingBytes)
                {
                    case 3:
                        h ^= (UInt16)(*realData);
                        h ^= ((UInt32)(*(((Byte*)(realData)) + 2))) << 16;
                        h *= m;
                        break;
                    case 2:
                        h ^= (UInt16)(*realData);
                        h *= m;
                        break;
                    case 1:
                        h ^= *((Byte*)realData);
                        h *= m;
                        break;
                    default:
                        break;
                }
            }

            // Do a few final mixes of the hash to ensure the last few
            // bytes are well-incorporated.

            h ^= h >> 13;
            h *= m;
            h ^= h >> 15;

            return h;
        }
    }
}
