using System;
using System.Collections.Generic;
using System.IO;

namespace RaptorDB
{
    #region [  TypeIndexes  ]
    internal class TypeIndexes<T> : MGIndex<T>, IIndex where T : IComparable<T>
    {
        public TypeIndexes(string path, string filename, byte keysize)
            : base(path, filename + ".mgidx", keysize, true)
        {

        }

        public void Set(object key, int recnum)
        {
            if (key == null) return; // FEATURE : index null values ??

            base.Set((T)key, recnum);
        }

        public MGRB Query(RDBExpression ex, object from, int maxsize)
        {
            T f = default(T);
            if (typeof(T).Equals(from.GetType()) == false)
                f = Converter(from);
            else
                f = (T)from;

            return base.Query(ex, f, maxsize);
        }

        private T Converter(object from)
        {
            if (typeof(T) == typeof(Guid))
            {
                object o = new Guid(from.ToString());
                return (T)o;
            }
            else
                return (T)Convert.ChangeType(from, typeof(T));
        }

        void IIndex.FreeMemory()
        {
            base.SaveIndex();
            base.FreeMemory();
        }

        void IIndex.Shutdown()
        {
            base.Shutdown();
        }

        object[] IIndex.GetKeys()
        {
            return base.GetKeys();
        }

        public MGRB Query(object fromkey, object tokey, int maxsize)
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
        public BoolIndex(string path, string filename, string extension)
        {
            // create file
            _filename = filename + extension;
            _path = path;
            if (_path.EndsWith(Path.DirectorySeparatorChar.ToString()) == false)
                _path += Path.DirectorySeparatorChar.ToString();

            if (File.Exists(_path + _filename))
                ReadFile();
        }

        private MGRB _bits = new MGRB();
        private string _filename;
        private string _path;
        private object _lock = new object();

        public MGRB GetBits()
        {
            return _bits.Copy();
        }

        public void Set(object key, int recnum)
        {
            lock (_lock)
                if (key != null)
                    _bits.Set(recnum, (bool)key);
        }

        public MGRB Query(RDBExpression ex, object from, int maxsize)
        {
            lock (_lock)
            {
                bool b = (bool)from;
                if (b)
                    return _bits;
                else
                    return _bits.Not(maxsize);
            }
        }

        public void FreeMemory()
        {
            lock (_lock)
            {
                _bits.Optimize();
                SaveIndex();
            }
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

        public void InPlaceOR(MGRB left)
        {
            lock (_lock)
                _bits = _bits.Or(left);
        }

        private void WriteFile()
        {
            lock (_lock)
            {
                _bits.Optimize();
                var o = _bits.Serialize();
                var b = fastBinaryJSON.BJSON.ToBJSON(o, new fastBinaryJSON.BJSONParameters { UseExtensions = false });
                File.WriteAllBytes(_path + _filename, b);
            }
        }

        private void ReadFile()
        {
            byte[] b = File.ReadAllBytes(_path + _filename);
            var o = fastBinaryJSON.BJSON.ToObject<MGRBData>(b);
            _bits = new MGRB();
            _bits.Deserialize(o);
        }

        public MGRB Query(object fromkey, object tokey, int maxsize)
        {
            return Query(RDBExpression.Greater, fromkey, maxsize); // range doesn't make sense here just do from 
        }

        public object[] GetKeys()
        {
            return new object[] { true, false };
        }
    }
    #endregion

    #region [  FullTextIndex  ]
    internal class FullTextIndex : Hoot, IIndex
    {
        public FullTextIndex(string IndexPath, string FileName, bool docmode, bool sortable, ITokenizer tokenizer)
            : base(IndexPath, FileName, docmode, tokenizer)
        {
            if (sortable)
            {
                _idx = new TypeIndexes<string>(IndexPath, FileName, Global.DefaultStringKeySize);
                _sortable = true;
            }
        }
        private bool _sortable = false;
        private IIndex _idx;

        public void Set(object key, int recnum)
        {
            base.Index(recnum, (string)key);
            if (_sortable)
                _idx.Set(key, recnum);
        }

        public MGRB Query(RDBExpression ex, object from, int maxsize)
        {
            return base.Query("" + from, maxsize);
        }

        public void SaveIndex()
        {
            base.Save();
            if (_sortable)
                _idx.SaveIndex();
        }

        public MGRB Query(object fromkey, object tokey, int maxsize)
        {
            return base.Query("" + fromkey, maxsize); // range doesn't make sense here just do from  
        }

        public object[] GetKeys()
        {
            if (_sortable)
                return _idx.GetKeys(); // support get keys 
            else
                return new object[] { };
        }
        void IIndex.FreeMemory()
        {
            base.FreeMemory();

            this.SaveIndex();
        }

        void IIndex.Shutdown()
        {
            this.SaveIndex();
            base.Shutdown();
            if (_sortable)
                _idx.Shutdown();
        }

    }
    #endregion

    #region [  EnumIndex  ]
    internal class EnumIndex<T> : MGIndex<string>, IIndex //where T : IComparable<T>
    {
        public EnumIndex(string path, string filename)
            : base(path, filename + ".mgidx", 30, /*Global.PageItemCount,*/ true)
        {

        }

        public void Set(object key, int recnum)
        {
            if (key == null) return; // FEATURE : index null values ??

            base.Set(key.ToString(), recnum);
        }

        public MGRB Query(RDBExpression ex, object from, int maxsize)
        {
            T f = default(T);
            if (typeof(T).Equals(from.GetType()) == false)
                f = Converter(from);
            else
                f = (T)from;

            return base.Query(ex, f.ToString(), maxsize);
        }

        private T Converter(object from)
        {
            if (typeof(T) == typeof(Guid))
            {
                object o = new Guid(from.ToString());
                return (T)o;
            }
            else
                return (T)Convert.ChangeType(from, typeof(T));
        }

        void IIndex.FreeMemory()
        {
            base.SaveIndex();
            base.FreeMemory();
        }

        void IIndex.Shutdown()
        {
            base.SaveIndex();
            base.Shutdown();
        }

        public MGRB Query(object fromkey, object tokey, int maxsize)
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

            return base.Query(f.ToString(), t.ToString(), maxsize);
        }

        object[] IIndex.GetKeys()
        {
            return base.GetKeys();
        }
    }
    #endregion

    #region [  NoIndex  ]
    internal class NoIndex : IIndex
    {
        public void Set(object key, int recnum)
        {
            // ignore set
        }

        public MGRB Query(RDBExpression ex, object from, int maxsize)
        {
            // always return everything
            return MGRB.Fill(maxsize);
        }

        public void FreeMemory()
        {

        }

        public void Shutdown()
        {

        }

        public void SaveIndex()
        {

        }

        public object[] GetKeys()
        {
            return new object[] { };
        }

        public MGRB Query(object fromkey, object tokey, int maxsize)
        {
            return MGRB.Fill(maxsize); // TODO : all or none??
        }
    }
    #endregion
}
