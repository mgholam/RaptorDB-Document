using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;

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
            base.Set((T)key, recnum);
        }

        public WAHBitArray Query(RDBExpression ex, object from)
        {
            T f = default(T);
            if (typeof(T).Equals(from.GetType()) == false)
            {
                f = (T)Convert.ChangeType(from, typeof(T));
            }
            else
                f = (T)from;
            return base.Query(ex, f);
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
    }
    #endregion

    #region [  BoolIndex  ]
    internal class BoolIndex : IIndex
    {
        public BoolIndex(string path, string filename)
        {
            // create file
            _filename = filename;
            _path = path;
            if (_path.EndsWith("\\") == false) _path += "\\";

            if (File.Exists(_path + _filename))
                ReadFile();
        }

        private WAHBitArray _bits = new WAHBitArray();
        private string _filename;
        private string _path;
        private object _lock = new object();

        public WAHBitArray GetBits()
        {
            return _bits.Copy();
        }

        public void Set(object key, int recnum)
        {
            _bits.Set(recnum, (bool)key);
        }

        public WAHBitArray Query(RDBExpression ex, object from)
        {
            bool b = (bool)from;
            if (b)
                return _bits;
            else
                return _bits.Not();
        }

        public void FreeMemory()
        {
            // free memory
            _bits.FreeMemory();
        }

        public void Shutdown()
        {
            // shutdown
            WriteFile();
        }

        public void SaveIndex()
        {
            WriteFile();
        }

        public void InPlaceOR(WAHBitArray left)
        {
            _bits = _bits.Or(left);
        }

        private void WriteFile()
        {
            uint[] ints = _bits.GetCompressed();
            MemoryStream ms = new MemoryStream();
            BinaryWriter bw = new BinaryWriter(ms);
            foreach (var i in ints)
            {
                bw.Write(i);
            }
            File.WriteAllBytes(_path + _filename, ms.ToArray());
        }

        private void ReadFile()
        {
            byte[] b = File.ReadAllBytes(_path + _filename);
        }

        internal WAHBitArray Not()
        {
            return _bits.Not();
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

        public WAHBitArray Query(RDBExpression ex, object from)
        {
            return base.Query("" + from);
        }

        public void FreeMemory()
        {
            base.FreeMemory(true);
        }

        public void SaveIndex()
        {
            base.Save();
        }
    }
    #endregion
}
