using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;
using RaptorDB.Common;

namespace RaptorDB
{
    #region [  TypeIndexes  ]
    internal class TypeIndexes<T> : MGIndex<T>, IIndex where T : IComparable<T>
    {
        public TypeIndexes(string path, string filename, byte keysize)
            : base(path, filename + ".mgidx", keysize, Global.PageItemCount, true)
        {

        }

        public void Set(object key, int recnum)
        {
            if (key == null) return; // FEATURE : index null values ??

            base.Set((T)key, recnum);
        }

        public WAHBitArray Query(RDBExpression ex, object from, int maxsize)
        {
            T f = default(T);
            if (typeof(T).Equals(from.GetType()) == false)
                f = (T)Convert.ChangeType(from, typeof(T));
            else
                f = (T)from;

            return base.Query(ex, f, maxsize);
        }

        void IIndex.FreeMemory()
        {
            base.FreeMemory();
        }

        void IIndex.Shutdown()
        {
            base.SaveIndex();
            base.Shutdown();
        }

        public WAHBitArray Query(object fromkey, object tokey, int maxsize)
        {
            T f = default(T);
            if (typeof(T).Equals(fromkey.GetType()) == false)
                f = (T)Convert.ChangeType(fromkey, typeof(T));
            else
                f = (T)fromkey;

            T t = default(T);
            if (typeof(T).Equals(tokey.GetType()) == false)
                t = (T)Convert.ChangeType(tokey, typeof(T));
            else
                t = (T)tokey;

            return base.Query(f, t, maxsize);
        }
    }
    #endregion

    #region [  BoolIndex  ]
    internal class BoolIndex : IIndex
    {
        public BoolIndex(string path, string filename)
        {
            // create file
            _filename = filename;
            if (_filename.Contains(".") == false) _filename += ".idx";
            _path = path;
            if (_path.EndsWith(Path.DirectorySeparatorChar.ToString()) == false) 
                _path += Path.DirectorySeparatorChar.ToString();

            if (File.Exists(_path + _filename))
                ReadFile();
        }

        private WAHBitArray _bits = new WAHBitArray();
        private string _filename;
        private string _path;
        private object _lock = new object();
        private bool _inMemory = false;

        public WAHBitArray GetBits()
        {
            return _bits.Copy();
        }

        public void Set(object key, int recnum)
        {
            _bits.Set(recnum, (bool)key);
        }

        public WAHBitArray Query(RDBExpression ex, object from, int maxsize)
        {
            bool b = (bool)from;
            if (b)
                return _bits;
            else
                return _bits.Not(maxsize);
        }

        public void FreeMemory()
        {
            // free memory
            _bits.FreeMemory();
        }

        public void Shutdown()
        {
            // shutdown
            if (_inMemory == false)
                WriteFile();
        }

        public void SaveIndex()
        {
            if (_inMemory == false)
                WriteFile();
        }

        public void InPlaceOR(WAHBitArray left)
        {
            _bits = _bits.Or(left);
        }

        private void WriteFile()
        {
            WAHBitArray.TYPE t;
            uint[] ints = _bits.GetCompressed(out t);
            MemoryStream ms = new MemoryStream();
            BinaryWriter bw = new BinaryWriter(ms);
            bw.Write((byte)t);// write new format with the data type byte
            foreach (var i in ints)
            {
                bw.Write(i);
            }
            File.WriteAllBytes(_path + _filename, ms.ToArray());
        }

        private void ReadFile()
        {
            byte[] b = File.ReadAllBytes(_path + _filename);
            WAHBitArray.TYPE t = WAHBitArray.TYPE.WAH;
            int j = 0;
            if (b.Length % 4 > 0) // new format with the data type byte
            {
                t = (WAHBitArray.TYPE)Enum.ToObject(typeof(WAHBitArray.TYPE), b[0]);
                j = 1;
            }
            List<uint> ints = new List<uint>();
            for (int i = 0; i < b.Length / 4; i++)
            {
                ints.Add((uint)Helper.ToInt32(b, (i * 4)+j));
            }
            _bits = new WAHBitArray(t, ints.ToArray());
        }

        //internal WAHBitArray Not()
        //{
        //    return _bits.Not();
        //}


        public WAHBitArray Query(object fromkey, object tokey, int maxsize)
        {
            return Query(RDBExpression.Greater, fromkey, maxsize);
        }

        internal void FixSize(int size)
        {
            _bits.Length = size;
        }
    }
    #endregion

    #region [  FullTextIndex  ]
    internal class FullTextIndex : Hoot, IIndex
    {
        public FullTextIndex(string IndexPath, string FileName)
            : base(IndexPath, FileName)
        {

        }

        public void Set(object key, int recnum)
        {
            base.Index(recnum, (string)key);
        }

        public WAHBitArray Query(RDBExpression ex, object from, int maxsize)
        {
            return base.Query("" + from, maxsize);
        }

        public void FreeMemory()
        {
            base.FreeMemory(true);
        }

        public void SaveIndex()
        {
            base.Save();
        }


        public WAHBitArray Query(object fromkey, object tokey, int maxsize)
        {
            return base.Query("" + fromkey, maxsize); 
        }
    }
    #endregion
}
