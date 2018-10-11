using System.Collections.Generic;

namespace RaptorDB
{
    //----------------------------------------------------------------------------------------------------------------
    //----------------------------------------------------------------------------------------------------------------
    //----------------------------------------------------------------------------------------------------------------
    class BitmapContainer : Container
    {
        public BitmapContainer()
        {
        }

        public BitmapContainer(int OneCount)
        {
            if (OneCount > Container.BSize)
                throw new System.Exception("OneCount > 65536");

            else if (OneCount == Container.BSize)
                ALLONE = true;

            else
            {
                List<ulong> l = new List<ulong>();
                while(OneCount>0)
                {
                    if (OneCount > 64)
                        l.Add(ulong.MaxValue);
                    else
                        l.Add(ulong.MaxValue<<OneCount);
                    OneCount -= 64;
                }
                _values = l.ToArray();
                Size = _values.Length * 64;
            }
        }

        public BitmapContainer(bool allones)
        {
            ALLONE = allones;
        }

        public BitmapContainer(ulong[] vals)
        {
            _values = vals;
            Size = _values.Length * 64;
        }

        public bool ALLONE = false;
        ulong[] _values = null;
        long _onecount = -1;
        object _lock = new object();

        public ulong[] Values()
        {
            lock (_lock)
            {
                if (_values != null)
                    return (ulong[])_values.Clone();
                else
                    return null;
            }
        }

        public override long CountOnes()
        {
            if (ALLONE)
                return BSize;

            if (_onecount > 0)
                return _onecount;

            lock (_lock)
            {
                long c = 0;

                foreach (var l in _values)
                    c += BitCount(l);

                _onecount = c;

                return c;
            }
        }

        public override long CountZeros()
        {
            if (ALLONE)
                return 0;

            return BSize - CountOnes();
        }

        public override IEnumerable<ushort> GetBitIndexes()
        {
            lock (_lock)
            {
                if (ALLONE)
                {
                    for (int i = 0; i < BSize; i++)
                        yield return (ushort)i;
                }

                ushort c = 0;
                foreach (var l in _values)
                {
                    for (int i = 0; i < 64; i++)
                    {
                        ulong mask = (ulong)1 << (63 - i); // high order bit get
                        if ((l & mask) != 0)
                            yield return (ushort)(c + i);
                    }
                    c += 64;
                }
            }
        }

        public override bool Get(long offset)
        {
            lock (_lock)
            {
                if (ALLONE)
                    return true;

                int pos = (ushort)offset >> 6;
                int off = (int)(offset % 64);

                if (pos >= _values.Length) // out of range
                {
                    return false;
                }

                ulong mask = (ulong)1 << (63 - off); // high order bit get

                return (_values[pos] & mask) != 0;
            }
        }

        public override void Set(long offset, bool val)
        {
            lock (_lock)
            {
                if (ALLONE)
                {
                    if (val == true)
                        return;
                    // change to bits
                    ALLONE = false;
                    _values = new ulong[1024];
                    for (int i = 0; i < 1024; i++)
                        _values[i] = ulong.MaxValue;
                }
                int pos = (ushort)offset >> 6;
                int off = (int)(offset % 64);
                _onecount = -1;

                if (_values == null)
                {
                    _values = new ulong[0];
                }
                if (pos >= _values.Length) // out of range
                {
                    // resize
                    var a = new ulong[pos + 1];
                    _values.CopyTo(a, 0);
                    _values = a;
                }
                ulong mask = (ulong)1 << (63 - off); // high order bit get

                if (val)
                    _values[pos] |= mask;
                else
                    _values[pos] &= ~mask;

                Size = _values.Length * 64;
            }
        }

        public override bool ChangeRequired()
        {
            if (ALLONE)
                return false;

            if (CountOnes() == BSize) // -> all ones container
                return true;

            if (CountZeros() < CHGOVER) // -> inverted container
                return true;

            var offbytes = CountOnes() << 1; //*2
            var bytes = _values.Length << 3; //*8
            if (bytes > offbytes)
                return true; // -> offset container

            return false;
        }

        public override Container ToBitmap()
        {
            return Copy();
        }

        public override Container Change()
        {
            if (ALLONE)
                return new BitmapContainer(true);

            if (CountOnes() == BSize)
                return new BitmapContainer(true);

            // create inverted
            if (CountZeros() < CHGOVER)
                return new InvertedContainer(Not().GetBitIndexes());

            Container c = null;
            //if (Global.useSortedList)
            //    c = new OffsetContainerSL();
            //else
            c = new OffsetContainer();

            foreach (var i in GetBitIndexes())
                c.Set(i, true);

            return c;
        }

        public override Container Copy()
        {
            if (ALLONE)
                return new BitmapContainer(true);

            if (_values != null && _values.Length > 0)
                return new BitmapContainer(Values());
            else
                return new BitmapContainer();
        }

        public override Container Not()
        {
            lock (_lock)
            {
                if (ALLONE)
                    return new BitmapContainer();

                var vals = new ulong[1024]; // TODO : upto Size ??
                for (int i = 0; i < 1024; i++)
                    vals[i] = ulong.MaxValue;

                for (int i = 0; i < _values.Length; i++)
                    vals[i] = ~_values[i];

                return new BitmapContainer(vals);
            }
        }
    }

    //----------------------------------------------------------------------------------------------------------------
    //----------------------------------------------------------------------------------------------------------------
    //----------------------------------------------------------------------------------------------------------------
    class OffsetContainer : Container
    {
        public OffsetContainer()
        {

        }
        public OffsetContainer(IEnumerable<ushort> vals)
        {
            _values = new List<ushort>(vals);
            Size = _values[_values.Count - 1];
        }

        List<ushort> _values = new List<ushort>();
        private object _lock = new object();

        public ushort[] Values()
        {
            lock (_lock)
                return _values.ToArray();
        }

        public override Container ToBitmap()
        {
            lock (_lock)
            {
                var c = new BitmapContainer();
                foreach (var i in _values)
                    c.Set(i, true);
                return c;
            }
        }

        public override Container Change()
        {
            if (CountOnes() == Container.BSize)
                return new BitmapContainer(true);

            if (CountZeros() < CHGOVER)
                return new InvertedContainer(ToBitmap().Not().GetBitIndexes());

            return ToBitmap();
        }

        public override bool ChangeRequired()
        {
            if (_values.Count > CHGOVER)
                return true;

            if (CountZeros() < CHGOVER)
                return true;

            return false;
        }

        public override Container Copy()
        {
            lock (_lock)
            {
                if (_values != null && _values.Count > 0)
                    return new OffsetContainer(_values.ToArray());
                else
                    return new OffsetContainer();
            }
        }

        public override long CountOnes()
        {
            return _values.Count;
        }

        public override long CountZeros()
        {
            return BSize - CountOnes();
        }

        public override bool Get(long offset)
        {
            lock (_lock)
            {
                var i = _values.BinarySearch((ushort)offset);
                if (i >= 0)
                    return true;
                return false;
            }
        }

        public override IEnumerable<ushort> GetBitIndexes()
        {
            lock (_lock)
                foreach (var i in _values)
                    yield return i;
        }

        public override void Set(long offset, bool val)
        {
            lock (_lock)
            {
                var i = _values.BinarySearch((ushort)offset);

                if (val == true)
                {
                    if (i < 0) // not in array -> add
                    {
                        var c = ~i;
                        if (c < _values.Count)
                            _values.Insert(c, (ushort)offset);
                        else
                            _values.Add((ushort)offset);
                    }
                }
                else if (i >= 0)
                {
                    // remove from array
                    _values.RemoveAt(i);
                }
                if (_values.Count > 0)
                    Size = _values[_values.Count - 1];
                else
                    Size = -1;
            }
        }

        public override Container Not()
        {
            return ToBitmap().Not();
        }
    }

    //----------------------------------------------------------------------------------------------------------------
    //----------------------------------------------------------------------------------------------------------------
    //----------------------------------------------------------------------------------------------------------------
    class InvertedContainer : Container
    {
        public InvertedContainer()
        {

        }

        // list of zeros
        public InvertedContainer(IEnumerable<ushort> vals)
        {
            _values = new List<ushort>(vals);
            if (_values.Count > 0)
                Size = _values[_values.Count - 1];
            else
                Size = Container.BSize;
        }

        List<ushort> _values = new List<ushort>();
        private object _lock = new object();

        public override Container ToBitmap()
        {
            lock (_lock)
            {
                var b = new BitmapContainer();

                foreach (var i in GetBitIndexes())
                {
                    b.Set(i, true);
                }
                return b;
            }
        }

        public override Container Change()
        {
            if (CountZeros() > CHGOVER)
                return ToBitmap();

            return Copy();
        }

        public override bool ChangeRequired()
        {
            if (CountZeros() > CHGOVER)
                return true;

            return false;
        }

        public override Container Copy()
        {
            lock (_lock)
            {
                if (_values != null && _values.Count > 0)
                    return new InvertedContainer(_values.ToArray());
                else
                    return new InvertedContainer();
            }
        }

        public override long CountOnes()
        {
            return BSize - CountZeros();
        }

        public override long CountZeros()
        {
            return _values.Count;
        }

        public override bool Get(long offset)
        {
            lock (_lock)
            {
                var i = _values.BinarySearch((ushort)offset);
                if (i >= 0) // in the list of zeros
                    return false;
                return true;
            }
        }

        public override IEnumerable<ushort> GetBitIndexes()
        {
            lock (_lock)
            {
                for (int i = 0; i < BSize; i++)
                {
                    var j = _values.BinarySearch((ushort)i);
                    if (j < 0) // not in the list of zeros
                        yield return (ushort)i;
                }
            }
        }

        public override Container Not()
        {
            return ToBitmap().Not();
        }

        public override void Set(long offset, bool val)
        {
            lock (_lock)
            {
                var i = _values.BinarySearch((ushort)offset);

                if (val == false)
                {
                    if (i < 0) // not in array -> add
                    {
                        var c = ~i;
                        if (c < _values.Count)
                            _values.Insert(c, (ushort)offset);
                        else
                            _values.Add((ushort)offset);
                    }
                }
                else if (i >= 0)
                {
                    // remove from array
                    _values.RemoveAt(i);
                }

                // fix : return correct size
                if (_values.Count > 0)
                    Size = _values[_values.Count - 1];
                else
                    Size = -1;
            }
        }

        public ushort[] Values()
        {
            lock (_lock)
                return _values.ToArray();
        }
    }

    //----------------------------------------------------------------------------------------------------------------
    //----------------------------------------------------------------------------------------------------------------
    //----------------------------------------------------------------------------------------------------------------
    public abstract class Container
    {
        internal const int BSize = 65536;
        internal const int CHGOVER = 4096;

        public abstract void Set(long offset, bool val);
        public abstract bool Get(long offset);
        public abstract long CountOnes();
        public abstract long CountZeros();
        public abstract IEnumerable<ushort> GetBitIndexes();
        public abstract bool ChangeRequired();
        public abstract Container Change();
        public abstract Container ToBitmap();
        public abstract Container Copy();
        public abstract Container Not();

        public int Size = -1;

        //[MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int BitCount(ulong x)
        {
            x -= (x >> 1) & 0x5555555555555555UL; //put count of each 2 bits into those 2 bits
            x = (x & 0x3333333333333333UL) + ((x >> 2) & 0x3333333333333333UL); //put count of each 4 bits into those 4 bits 
            x = (x + (x >> 4)) & 0x0F0F0F0F0F0F0F0FUL; //put count of each 8 bits into those 8 bits 
            return (int)((x * 0x0101010101010101UL) >> 56); //returns left 8 bits of x + (x<<8) + (x<<16) + (x<<24) + ... 
        }
    }

    public enum CTYPE
    {
        ALLONES
        , BITMAP
        , OFFSET
        , INV
        //,OFFSETSL
    }
    public class CData
    {
        public ushort i;
        public CTYPE t;
        public byte[] d;
    }

    public class MGRBData
    {
        public List<CData> c = new List<CData>();
    }
}
