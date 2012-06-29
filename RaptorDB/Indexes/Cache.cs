using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace RaptorDB
{
    internal class Cache
    {
        public enum OPERATION
        {
            AND,
            OR,
            ANDNOT
        }

        public Cache()
        {
        }

        public bool isLoaded = false;
        public bool isDirty = true;
        public long FileOffset = -1;
        public int LastBitSaveLength = 0;
        private WAHBitArray _bits;

        public void SetBit(int index, bool val)
        {
            if (_bits != null)
                _bits.Set(index, val);
            else
            {
                _bits = new WAHBitArray();
                _bits.Set(index, val);
            }
            isDirty = true;
        }

        public uint[] GetCompressedBits()
        {
            if (_bits != null)
                return _bits.GetCompressed();
            else
                return null;
        }

        public void FreeMemory(bool unload)
        {
            if (_bits != null)
                _bits.FreeMemory();

            if (unload)
            {
                _bits = null;
                isLoaded = false;
            }
        }

        public void SetCompressedBits(uint[] bits)
        {
            _bits = new WAHBitArray(WAHBitArray.TYPE.Compressed_WAH, bits);
            LastBitSaveLength = bits.Length;
            isLoaded = true;
            isDirty = false;
        }

        public WAHBitArray Op(WAHBitArray bits, OPERATION op)
        {
            if (_bits == null)
            {
                // should not be here
            }

            if (op == OPERATION.AND)
                return _bits.And(bits);
            else if (op == OPERATION.OR)
                return _bits.Or(bits);
            else
                return bits.And(_bits.Not());
        }

        public WAHBitArray GetBitmap()
        {
            return _bits;
        }
    }
}
