using RaptorDB.Common;
using System;
using System.Collections.Generic;

namespace RaptorDB
{
    public class MGRB
    {
        public MGRB()
        { }

        internal MGRB(SafeSortedList<int, Container> containers) : this(containers, -1)
        { }

        internal MGRB(SafeSortedList<int, Container> containers, long size)
        {
            _containers = containers;
            var k = _containers.Keys();
            _size = size;
            if (size <= 0)//== -1)
            {
                _size = 0;
                var l = k.Length - 1;
                if (l >= 0)
                    _size = (k[l] << 16) + _containers.GetValue(l).Size;
            }
        }

        private SafeSortedList<int, Container> _containers = new SafeSortedList<int, Container>();
        private long _size;
        private ushort _MASK = 0xffff;
        private object _lock = new object();
        public bool isDirty = false;

        public long Length { get { return _size; } }

        public void Set(long position, bool val)
        {
            lock (_lock)
            {
                isDirty = true;
                if (_size < position && val == true)
                    _size = position;

                var idx = (int)(position >> 16);
                Container c = null;
                if (_containers.TryGetValue(idx, out c) == false)
                {
                    //if (Global.useSortedList)
                    //    c = new OffsetContainerSL();
                    //else
                    c = new OffsetContainer();
                    // add container
                    _containers.Add(idx, c);
                }
                c.Set(position & _MASK, val);

                //if (c.ChangeRequired())
                //    _containers[idx] = c.Change();
            }
        }

        public bool Get(long position)
        {
            lock (_lock)
            {
                var idx = (int)(position >> 16);
                if (_containers.TryGetValue(idx, out Container c))
                    return c.Get(position & _MASK);

                return false;
            }
        }

        public MGRB And(MGRB B)
        {
            var v = new SafeSortedList<int, Container>();
            var len = _size;
            if (B.Length < len)
                len = B.Length;
            var a = LastContainerIdx();
            var b = B.LastContainerIdx();
            var min = a;
            if (b < min)
                min = b;
            min++;

            for (int i = 0; i < min; i++)
            {
                Container ca = null;
                Container cb = null;

                _containers.TryGetValue(i, out ca);
                B._containers.TryGetValue(i, out cb);

                if (ca != null && cb != null)
                    v.Add(i, containerAND(ca, cb));
            }

            return new MGRB(v, len);
        }

        public MGRB Or(MGRB B)
        {
            var v = new SafeSortedList<int, Container>();
            var len = _size;
            if (B.Length > len)
                len = B.Length;
            var a = LastContainerIdx();
            var b = B.LastContainerIdx();
            var max = a;
            if (b > max)
                max = b;
            max++;

            for (int i = 0; i < max; i++)
            {
                Container ca = null;
                Container cb = null;

                _containers.TryGetValue(i, out ca);
                B._containers.TryGetValue(i, out cb);

                if (ca == null && cb != null)
                    v.Add(i, cb.Copy());
                else if (cb == null && ca != null)
                    v.Add(i, ca.Copy());
                else if (ca != null && cb != null)
                    v.Add(i, containerOR(ca, cb));
            }

            return new MGRB(v, len);
        }

        public MGRB AndNot(MGRB b)
        {
            long c = _size;
            if (b._size > c)
                c = b._size;

            return And(b.Not(c));
        }

        public MGRB Not()
        {
            var con = new SafeSortedList<int, Container>();
            foreach (var c in _containers)
            {
                con.Add(c.Key, c.Value.Not());
            }

            return new MGRB(con, _size);
        }

        public MGRB Not(long count)
        {
            var con = new SafeSortedList<int, Container>();
            var c = count >> 16;
            for (int i = 0; i <= c; i++)
            {
                Container a = null;
                _containers.TryGetValue(i, out a);
                if (a == null)
                    con.Add(i, new BitmapContainer(true));
                else
                    con.Add(i, a.Not());
            }

            return new MGRB(con, count);
        }

        public static MGRB Fill(long count)
        {
            if (count == 0)
                return new MGRB();

            var con = new SafeSortedList<int, Container>();
            int i = 0;
            long c = count;
            while (count > 0)
            {
                if (count > Container.BSize)
                    con.Add(i, new BitmapContainer(true));
                else
                    con.Add(i, new BitmapContainer((int)count));
                count -= Container.BSize;
                i++;
            }

            return new MGRB(con, c);
        }

        public long CountOnes()
        {
            long c = 0;

            if (_size > 0)
                foreach (var i in _containers)
                    c += i.Value.CountOnes();

            return c;
        }

        public long CountZeros()
        {
            var c = CountOnes();

            return _size - c;
        }

        public IEnumerable<int> GetBitIndexes()
        {
            foreach (var c in _containers)
            {
                int i = c.Key << 16;
                foreach (var j in c.Value.GetBitIndexes())
                    yield return i + j;
            }
        }

        public MGRB Optimize()
        {
            lock (_lock)
            {
                var keys = _containers.Keys();
                var remove = new List<int>();

                for (int i = 0; i < keys.Length; i++)
                {
                    var k = keys[i];
                    var c = _containers[k];

                    if (c.CountOnes() == 0)
                        remove.Add(k);

                    //else if (c.CountZeros() < Container.CHGOVER)
                    //{
                    //    _containers[k] = new ZeroContainer();
                    //}

                    else if (c.ChangeRequired())
                        _containers[k] = c.Change();
                }

                foreach (var k in remove)
                    _containers.Remove(k);

                return this;
            }
        }

        public MGRBData Serialize()
        {
            var d = new MGRBData();

            foreach (var c in _containers)
            {
                var cd = new CData();
                {
                    cd.i = (ushort)c.Key;
                    if (c.Value is BitmapContainer)
                    {
                        var bm = c.Value as BitmapContainer;
                        cd.t = CTYPE.BITMAP;
                        if (bm.ALLONE)
                            cd.t = CTYPE.ALLONES;
                        else
                        {
                            // get data
                            cd.d = ToByteArray(bm.Values());
                        }
                    }
                    else if (c.Value is OffsetContainer)
                    {
                        var of = c.Value as OffsetContainer;
                        cd.t = CTYPE.OFFSET;
                        cd.d = ToByteArray(of.Values());
                    }
                    else if (c.Value is InvertedContainer)
                    {
                        var inv = c.Value as InvertedContainer;
                        cd.t = CTYPE.INV;
                        cd.d = ToByteArray(inv.Values());
                    }
                    //else
                    //{
                    //    var of = c.Value as OffsetContainerSL;
                    //    cd.t = CTYPE.OFFSETSL;
                    //    var b = new byte[cd.d.Length];
                    //    int k = 0;
                    //    foreach (var i in of._values)
                    //    {
                    //        Buffer.BlockCopy(GetBytes(i.Key, false), 0, b, k, 2);
                    //        k += 2;
                    //    }
                    //    cd.d = b;
                    //}

                    d.c.Add(cd);
                }
            }
            return d;
        }

        public void Deserialize(MGRBData input)
        {
            foreach (var c in input.c)
            {
                Container con = null;
                if (c.t == CTYPE.ALLONES)
                {
                    con = new BitmapContainer(true);
                }
                else if (c.t == CTYPE.BITMAP)
                {
                    List<ulong> list = new List<ulong>();
                    var dataLen = c.d.Length;
                    for (int i = 0; i < dataLen; i += 8)
                    {
                        list.Add(ToULong(c.d, i));
                    }
                    con = new BitmapContainer(list.ToArray());
                }
                else if (c.t == CTYPE.OFFSET)
                {
                    List<ushort> list = new List<ushort>();
                    var dataLen = c.d.Length;
                    for (int i = 0; i < dataLen; i += 2)
                    {
                        list.Add(ToUShort(c.d, i));
                    }
                    con = new OffsetContainer(list);
                }
                else if (c.t == CTYPE.INV)
                {
                    List<ushort> list = new List<ushort>();
                    var dataLen = c.d.Length;
                    for (int i = 0; i < dataLen; i += 2)
                    {
                        list.Add(ToUShort(c.d, i));
                    }
                    con = new InvertedContainer(list);
                }
                //else
                //{
                //    List<ushort> list = new List<ushort>();
                //    var dataLen = c.d.Length;
                //    for (int i = 0; i < dataLen; i += 2)
                //    {
                //        list.Add(ToUShort(c.d, i));
                //    }
                //    con = new OffsetContainerSL(list);
                //}
                _containers.Add(c.i, con);
            }
            var k = _containers.Keys();
            var l = k.Length - 1;
            if (l >= 0)
                _size = (k[l] << 16) + _containers.GetValue(l).Size;
        }

        public MGRB Copy()
        {
            if (_containers.Count() > 0)
            {
                var o = Serialize();
                var m = new MGRB();
                m.Deserialize(o);
                return m;
            }
            return new MGRB();
        }

        public int GetFirst()
        {
            int j = 0;
            foreach (var i in GetBitIndexes())
            {
                j = i;
                break;
            }
            return j;
        }

        private int LastContainerIdx()
        {
            if (_containers.Count() > 0)
                return _containers.Keys()[_containers.Count() - 1];
            else
                return 0;
        }

        private static Container containerAND(Container ca, Container cb)
        {
            BitmapContainer a = null;
            BitmapContainer b = null;

            if (ca is BitmapContainer)
                a = (BitmapContainer)ca;
            else if (ca is OffsetContainer)
                a = (BitmapContainer)ca.ToBitmap();
            else
                a = (BitmapContainer)ca.ToBitmap();

            if (cb is BitmapContainer)
                b = (BitmapContainer)cb;
            else if (cb is OffsetContainer)
                b = (BitmapContainer)cb.ToBitmap();
            else
                b = (BitmapContainer)cb.ToBitmap();

            var av = a.Values();
            var bv = b.Values();
            var la = av != null ? av.Length : 1024;
            var lb = bv != null ? bv.Length : 1024;
            var min = la;
            if (lb < min)
                min = lb;

            List<ulong> vals = new List<ulong>();
            for (int i = 0; i < min; i++)
            {
                ulong ua = ulong.MaxValue;
                ulong ub = ulong.MaxValue;
                if (av != null)
                    ua = av[i];
                if (bv != null)
                    ub = bv[i];

                vals.Add(ua & ub);
            }

            return new BitmapContainer(vals.ToArray());
        }

        private static Container containerOR(Container ca, Container cb)
        {
            BitmapContainer a = null;
            BitmapContainer b = null;

            if (ca is BitmapContainer)
                a = (BitmapContainer)ca;
            else if (ca is OffsetContainer)
                a = (BitmapContainer)ca.ToBitmap();
            else
                a = (BitmapContainer)ca.ToBitmap();

            if (cb is BitmapContainer)
                b = (BitmapContainer)cb;
            else if (cb is OffsetContainer)
                b = (BitmapContainer)cb.ToBitmap();
            else
                b = (BitmapContainer)cb.ToBitmap();

            var av = a.Values();
            var bv = b.Values();
            var la = av != null ? av.Length : 1024;
            var lb = bv != null ? bv.Length : 1024;
            var max = la;
            if (lb > max)
                max = lb;

            List<ulong> vals = new List<ulong>();

            for (int i = 0; i < max; i++)
            {
                ulong ua = 0;
                ulong ub = 0;
                if (av != null && i < la)
                    ua = av[i];
                if (bv != null && i < lb)
                    ub = bv[i];

                vals.Add(ua | ub);
            }

            return new BitmapContainer(vals.ToArray());
        }

        private static unsafe byte[] GetBytes(ushort num, bool reverse)
        {
            byte[] buffer = new byte[2];
            fixed (byte* numRef = buffer)
            {
                *((ushort*)numRef) = num;
            }
            if (reverse)
                Array.Reverse(buffer);
            return buffer;
        }

        private static unsafe ulong ToULong(byte[] value, int startIndex)
        {
            fixed (byte* numRef = &(value[startIndex]))
            {
                return *(((ulong*)numRef));
            }
        }

        private static unsafe ushort ToUShort(byte[] value, int startIndex)
        {
            fixed (byte* numRef = &(value[startIndex]))
            {
                return *(((ushort*)numRef));
            }
        }

        private static unsafe byte[] ToByteArray(ulong[] data)
        {
            int arrayLength = data.Length;
            byte[] byteArray = new byte[8 * arrayLength];
            fixed (ulong* pointer = data)
            {
                fixed (byte* bytePointer = byteArray)
                {
                    ulong* read = pointer;
                    ulong* write = (ulong*)bytePointer;

                    for (int i = 0; i < arrayLength; i++)
                    {
                        *write++ = *read++;
                    }
                }
                // below not working
                //System.Runtime.InteropServices.Marshal.Copy(new IntPtr(pointer), byteArray, 0, arrayLength);
            }

            // not working
            //fixed (ulong* src = data)
            //{
            //    System.Runtime.InteropServices.Marshal.Copy(new IntPtr(src), byteArray, 0, arrayLength);
            //}

            // not working
            //Buffer.BlockCopy(data, 0, byteArray, 0, arrayLength);
            return byteArray;
        }

        private static unsafe byte[] ToByteArray(ushort[] data)
        {
            int arrayLength = data.Length;
            byte[] byteArray = new byte[2 * arrayLength];
            fixed (ushort* pointer = data)
            {
                fixed (byte* bytePointer = byteArray)
                {
                    ushort* read = pointer;
                    ushort* write = (ushort*)bytePointer;
                    for (int i = 0; i < arrayLength; i++)
                    {
                        *write++ = *read++;
                    }
                }
            }

            // not working
            //fixed (ushort* src = data)
            //{
            //    System.Runtime.InteropServices.Marshal.Copy(new IntPtr(src), byteArray, 0, arrayLength);
            //}

            // not working
            //Buffer.BlockCopy(data, 0, byteArray, 0, arrayLength);

            return byteArray;
        }
    }
}
