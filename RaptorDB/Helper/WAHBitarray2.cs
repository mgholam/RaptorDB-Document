using System;
using System.Collections.Generic;
using System.Text;
using System.Collections;

namespace RaptorDB
{
    internal class WAHBitArray
    {
        public enum TYPE
        {
            Compressed_WAH,
            Uncompressed_WAH,
            Indexes
        }

        public WAHBitArray()
        {
            _usingIndexes = true;
        }

        public WAHBitArray(TYPE type, uint[] ints)
        {
            _usingIndexes = false;
            switch (type)
            {
                case TYPE.Compressed_WAH:
                    _compressed = ints;
                    break;
                case TYPE.Uncompressed_WAH:
                    _uncompressed = ints;
                    break;
                case TYPE.Indexes:
                    _offsets = new List<uint>(ints);
                    _usingIndexes = true;
                    break;
            }
        }

        private uint[] _compressed;
        private uint[] _uncompressed;
        private List<uint> _offsets = new List<uint>();
        private bool _usingIndexes = true;
        private uint _curMax = 0;
        public bool isDirty = false;

        public bool UsingIndexes { get { return _usingIndexes; } }

        public bool Get(int index)
        {
            if (_usingIndexes)
            {
                var f = _offsets.Find(delegate(uint i) { return i == (uint)index; });
                if (f > 0)
                    return true;
                else
                    return false;
            }
            CheckBitArray();

            Resize(index);

            return internalGet(index);
        }

        private object _lock = new object();
        public void Set(int index, bool val)
        {
            lock (_lock)
            {
                if (_usingIndexes)
                {
                    isDirty = true;
                    _offsets.RemoveAll(delegate(uint i) { return i == index; });
                    if (val == true)
                    {
                        _offsets.Add((uint)index);
                        // set max
                        if (index > _curMax)
                            _curMax = (uint)index;
                    }

                    ChangeTypeIfNeeded();
                    return;
                }
                CheckBitArray();

                Resize(index);

                internalSet(index, val);
            }
        }

        public int Length
        {
            set
            {
                if (_usingIndexes)
                {
                    // ignore
                    return;
                }
                CheckBitArray();
                int c = value >> 5;
                c++;
                if (c > _uncompressed.Length)
                {
                    uint[] ar = new uint[c];
                    _uncompressed.CopyTo(ar, 0);
                    _uncompressed = ar;
                }
            }
            get
            {
                if (_usingIndexes)
                {
                    _offsets.Sort();
                    uint l = _offsets[_offsets.Count - 1];
                    return (int)l;
                }
                CheckBitArray();
                return _uncompressed.Length << 5;
            }
        }

        public WAHBitArray And(WAHBitArray op)
        {
            lock (_lock)
            {
                uint[] left;
                uint[] right;
                prelogic(op, out left, out right);

                for (int i = 0; i < left.Length; i++)
                    left[i] &= right[i];

                return new WAHBitArray(TYPE.Uncompressed_WAH, left);
            }
        }

        public WAHBitArray AndNot(WAHBitArray op)
        {
            lock (_lock)
            {
                uint[] left;
                uint[] right;
                prelogic(op, out left, out right);

                for (int i = 0; i < left.Length; i++)
                    left[i] &= ~right[i];

                return new WAHBitArray(TYPE.Uncompressed_WAH, left);
            }
        }

        public WAHBitArray Or(WAHBitArray op)
        {
            lock (_lock)
            {
                uint[] left;
                uint[] right;
                prelogic(op, out left, out right);

                for (int i = 0; i < left.Length; i++)
                    left[i] |= right[i];

                return new WAHBitArray(TYPE.Uncompressed_WAH, left);
            }
        }

        public WAHBitArray Not()
        {
            lock (_lock)
            {
                this.CheckBitArray();

                uint[] left = this.GetUncompressed();

                for (int i = 0; i < left.Length; i++)
                    left[i] = ~left[i];

                return new WAHBitArray(TYPE.Uncompressed_WAH, left);
            }
        }

        public WAHBitArray Xor(WAHBitArray op)
        {
            lock (_lock)
            {
                uint[] left;
                uint[] right;
                prelogic(op, out left, out right);

                for (int i = 0; i < left.Length; i++)
                    left[i] ^= right[i];

                return new WAHBitArray(TYPE.Uncompressed_WAH, left);
            }
        }

        public long CountOnes()
        {
            if (_usingIndexes)
            {
                return _offsets.Count;
            }

            long c = 0;
            CheckBitArray();

            foreach (uint i in _uncompressed)
            {
                if (i != 0)
                {
                    uint m = 1;
                    for (int j = 0; j < 32; j++)
                    {
                        uint o = i & m;
                        if (o != 0)
                            c++;
                        m <<= 1;
                    }
                }
            }

            //int count = _uncompressed.Length << 5;

            //for (int i = 0; i < count; i++)
            //{
            //    if (internalGet(i))
            //        c++;
            //}
            return c;
        }

        public long CountZeros()
        {
            if (_usingIndexes)
            {
                long ones = _offsets.Count;
                _offsets.Sort();
                long l = _offsets[_offsets.Count - 1];
                return l - ones;
            }

            //long c = 0;
            CheckBitArray();
            int count = _uncompressed.Length << 5;
            long cc = CountOnes();

            //for (int i = 0; i < count; i++)
            //{
            //    if (internalGet(i) == false)
            //        c++;
            //}
            return count - cc;
        }

        public void FreeMemory()
        {
            if (_uncompressed != null)
                Compress();
            _uncompressed = null;
        }

        public uint[] GetCompressed()
        {
            if (_usingIndexes)
            {
                return _offsets.ToArray();
            }

            if (_uncompressed == null)
                return new uint[] { 0 };

            Compress();
            return _compressed;
        }

        public IEnumerable<int> GetBitIndexes(bool ones)
        {
            if (_usingIndexes)
            {
                foreach (uint i in _offsets)
                    yield return (int)i;
            }
            else
            {
                CheckBitArray();
                int count = _uncompressed.Length;//<< 5;

                for (int i = 0; i < count; i++)
                {
                    if (_uncompressed[i] > 0 && ones == true)
                    {
                        for (int j = 0; j < 32; j++)
                        {
                            bool b = internalGet((i << 5) + j);
                            if (b == ones)
                                yield return (i << 5) + j;
                        }
                    }
                }
            }
        }

        #region [  P R I V A T E  ]

        private void prelogic(WAHBitArray op, out uint[] left, out uint[] right)
        {
            this.CheckBitArray();

            left = this.GetUncompressed();
            right = op.GetUncompressed();
            int ic = left.Length;
            int uc = right.Length;
            if (ic > uc)
            {
                uint[] ar = new uint[ic];
                right.CopyTo(ar, 0);
                right = ar;
            }
            else if (ic < uc)
            {
                uint[] ar = new uint[uc];
                left.CopyTo(ar, 0);
                left = ar;
            }

            //FixLengths(ints, uncomp);
        }

        //private void FixLengths(uint[] ar1, uint[] ar2)
        //{
        //    int ic = ar1.Length;
        //    int uc = ar2.Length;
        //    if (ic > uc)
        //    {
        //        uint[] ar = new uint[ic];
        //        ar2.CopyTo(ar, 0);
        //        ar2 = ar;
        //    }
        //    else if (ic < uc)
        //    {
        //        uint[] ar = new uint[uc];
        //        ar1.CopyTo(ar, 0);
        //        ar1 = ar;
        //    }
        //}

        protected uint[] GetUncompressed()
        {
            if (_usingIndexes)
            {
                // return bitmap uints 
                uint max = 0;
                foreach (uint i in _offsets)
                    if (i > max)
                        max = i;

                uint[] ints = new uint[(max >> 5) + 1];

                foreach (uint index in _offsets)
                {
                    int pointer = ((int)index) >> 5;
                    uint mask = (uint)1 << (31 - // high order bit set
                        ((int)index % 32));

                    ints[pointer] |= mask;
                }

                return ints;
            }

            this.CheckBitArray();
            uint[] ui = new uint[_uncompressed.Length];
            _uncompressed.CopyTo(ui, 0);

            return ui;
        }

        private void ChangeTypeIfNeeded()
        {
            if (_usingIndexes == false)
                return;

            uint T = (_curMax >> 5) + 1;
            int c = _offsets.Count;
            if (c > T && c > Global.BitmapOffsetSwitchOverCount)
            {
                // change type to WAH
                _usingIndexes = false;

                // create bitmap
                foreach (var i in _offsets)
                    Set((int)i, true);
                // clear list
                _offsets = new List<uint>();
            }
        }

        private void Resize(int index)
        {
            int c = index >> 5;
            c++;
            if (c > _uncompressed.Length)
            {
                //int j = ((c >> 11)+1 )<< 11;
                uint[] ar = new uint[c]; // j
                _uncompressed.CopyTo(ar, 0);
                _uncompressed = ar;
            }
        }

        private void ResizeAsNeeded(List<uint> list, int index)
        {
            int count = index >> 5;

            while (list.Count < count)
                list.Add(0);
        }

        private void internalSet(int index, bool val)
        {
            isDirty = true;
            int pointer = index >> 5;
            uint mask = (uint)1 << (31 - // high order bit set
                (index % 32));

            if (val)
                _uncompressed[pointer] |= mask;
            else
                _uncompressed[pointer] &= ~mask;
        }

        private bool internalGet(int index)
        {
            int pointer = index >> 5;
            uint mask = (uint)1 << (31 - // high order bit get
                (index % 32));

            if (pointer < _uncompressed.Length)
                return (_uncompressed[pointer] & mask) != 0;
            else
                return false;
        }

        private void CheckBitArray()
        {
            if (_usingIndexes)
                return;

            if (_compressed == null && _uncompressed == null)
            {
                _uncompressed = new uint[0];
                return;
            }
            if (_compressed == null)
                return;
            if (_uncompressed == null)
                Uncompress();
        }

        private uint Take31Bits(int index)
        {
            long l1 = 0;
            long l2 = 0;
            long l = 0;
            long ret = 0;
            int off = (index % 32);
            int pointer = index >> 5;

            l1 = _uncompressed[pointer];
            pointer++;
            if (pointer < _uncompressed.Length)
                l2 = _uncompressed[pointer];

            l = (l1 << 32) + l2;
            ret = (l >> (33 - off)) & 0x7fffffff;

            return (uint)ret;
        }

        private void Compress()
        {
            List<uint> compressed = new List<uint>();
            uint zeros = 0;
            uint ones = 0;
            int count = _uncompressed.Length << 5;
            for (int i = 0; i < count; )
            {
                uint num = Take31Bits(i);
                i += 31;
                if (num == 0)
                {
                    zeros += 31;
                    FlushOnes(compressed, ref ones);
                }
                else if (num == 0x7fffffff)
                {
                    ones += 31;
                    FlushZeros(compressed, ref zeros);
                }
                else
                {
                    FlushOnes(compressed, ref ones);
                    FlushZeros(compressed, ref zeros);
                    compressed.Add(num);
                }
            }
            FlushOnes(compressed, ref ones);
            FlushZeros(compressed, ref zeros);
            _compressed = compressed.ToArray();
        }

        private void FlushOnes(List<uint> compressed, ref uint ones)
        {
            if (ones > 0)
            {
                uint n = 0xc0000000 + ones;
                ones = 0;
                compressed.Add(n);
            }
        }

        private void FlushZeros(List<uint> compressed, ref uint zeros)
        {
            if (zeros > 0)
            {
                uint n = 0x80000000 + zeros;
                zeros = 0;
                compressed.Add(n);
            }
        }

        private void Write31Bits(List<uint> list, int index, uint val)
        {
            this.ResizeAsNeeded(list, index + 32);

            int off = (index % 32);
            int pointer = index >> 5;
            if (off > 0)
            {
                list[pointer] |= val >> (off - 1);
                if (pointer >= list.Count - 1)
                    list.Add(0);
                list[pointer + 1] |= val << (33 - off);
            }
            else
                list[pointer] |= val << 1;
        }

        private void WriteBits(List<uint> list, int index, uint count)
        {
            this.ResizeAsNeeded(list, index);

            int bit = index % 32;
            int pointer = index >> 5;
            int cc = (int)count;

            if (pointer >= list.Count)
                list.Add(0);

            if (bit > 0)
            {
                list[pointer] |= ~(uint)((0xffffffff << (32 - bit)));
                cc -= (32 - bit);
            }
            else
            {
                if (cc >= 32)
                {
                    list[pointer] = 0xffffffff;
                    cc -= 32;
                }
                else
                {
                    list[pointer] |= 0xffffffff << (32 - cc);
                    cc = 0;
                }
            }
            while (cc >= 32)//full ints
            {
                list.Add(0xffffffff);
                cc -= 32;
            }
            if (cc > 0) //remaining
                list.Add((0xffffffff << (32 - cc)));
        }

        private void Uncompress()
        {
            int index = 0;
            List<uint> list = new List<uint>();
            if (_compressed == null)
                return;

            foreach (uint ci in _compressed)
            {
                if ((ci & 0x80000000) == 0) // literal
                {
                    this.Write31Bits(list, index, ci & 0x7fffffff);
                    index += 31;
                }
                else
                {
                    uint c = ci & 0x3fffffff;
                    if ((ci & 0x40000000) > 0) // ones count
                        this.WriteBits(list, index, c);

                    index += (int)c;
                }
            }
            this.ResizeAsNeeded(list, index);
            _uncompressed = list.ToArray();
        }
        #endregion
    }
}
