using System;
using System.Collections.Generic;
using System.Text;
using System.Collections;
using System.IO;
using System.Xml.Serialization;
using System.Threading;
using System.Text.RegularExpressions;

namespace RaptorDB
{
    internal class Hoot 
    {
        private string _bmpext = ".mgbmp";

        public Hoot(string IndexPath, string FileName)
        {
            _Path = IndexPath;
            _FileName = FileName;
            if (_Path.EndsWith("\\") == false) _Path += "\\";
            Directory.CreateDirectory(IndexPath);
            //_log.Debug("\r\n\r\n");
            _log.Debug("Starting hOOt....");
            _log.Debug("Storage Folder = " + _Path);

            // read words
            LoadWords();
            // open bitmap index
            _bitmapFile = new FileStream(_Path + _FileName + _bmpext, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.ReadWrite);
            _lastBitmapOffset = _bitmapFile.Seek(0L, SeekOrigin.End);
        }

        private ILog _log = LogManager.GetLogger(typeof(Hoot));
        private string _FileName = "words";
        private string _Path = "";
        private SafeDictionary<string, Cache> _index = new SafeDictionary<string, Cache>();
        // FEATURE : remove _internalOP & sleeps
        private bool _internalOP = false;
        private object _lock = new object();
        private FileStream _bitmapFile;
        private long _lastBitmapOffset = 0;


        public void FreeMemory(bool freecache)
        {
            lock (_lock)
            {
                _internalOP = true;
                _log.Debug("freeing memory");

                // free bitmap memory
                foreach (var v in _index)
                {
                    if (freecache)
                    {
                        long off = SaveBitmap(v.Value.GetCompressedBits());
                        v.Value.isDirty = false;
                        v.Value.FileOffset = off;
                        v.Value.FreeMemory(true);
                    }
                    else
                        v.Value.FreeMemory(false);
                }
                _internalOP = false;
            }
        }

        public void Save()
        {
            lock (_lock)
            {
                _internalOP = true;
                InternalSave();
                _internalOP = false;
            }
        }

        public void Index(int recordnumber, string text)
        {
            while (_internalOP) Thread.Sleep(50);

            AddtoIndex(recordnumber, text);
        }

        public WAHBitArray Query(string filter)
        {
            while (_internalOP) Thread.Sleep(50);

            return ExecutionPlan(filter);
        }

        public IEnumerable<int> FindRows(string filter)
        {
            // enumerate records
            return Query(filter).GetBitIndexes();
        }

        public void OptimizeIndex()
        {
            lock (_lock)
            {
                _internalOP = true;
                InternalSave();
                _log.Debug("optimizing index..");
                DateTime dt = FastDateTime.Now;
                _lastBitmapOffset = 0;
                _bitmapFile.Flush();
                _bitmapFile.Close();
                // compact bitmap index file to new file
                _bitmapFile = new FileStream(_Path + _FileName + _bmpext + "$", FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.ReadWrite);
                MemoryStream ms = new MemoryStream();
                BinaryWriter bw = new BinaryWriter(ms, Encoding.UTF8);
                // save words and bitmaps
                using (FileStream words = new FileStream(_Path + _FileName + ".words", FileMode.Create))
                {
                    foreach (KeyValuePair<string, Cache> kv in _index)
                    {
                        bw.Write(kv.Key);
                        uint[] ar = LoadBitmap(kv.Value.FileOffset);
                        long offset = SaveBitmap(ar);
                        kv.Value.FileOffset = offset;
                        bw.Write(kv.Value.FileOffset);
                    }
                    // save words
                    byte[] b = ms.ToArray();
                    words.Write(b, 0, b.Length);
                    words.Flush();
                    words.Close();
                }
                // rename files
                _bitmapFile.Flush();
                _bitmapFile.Close();
                File.Delete(_Path + _FileName + _bmpext);
                File.Move(_Path + _FileName + _bmpext +"$", _Path + _FileName + _bmpext);
                // reload everything
                _bitmapFile = new FileStream(_Path + _FileName + _bmpext, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.ReadWrite);
                _lastBitmapOffset = _bitmapFile.Seek(0L, SeekOrigin.End);
                _log.Debug("optimizing index done = " + DateTime.Now.Subtract(dt).TotalSeconds + " sec");
                _internalOP = false;
            }
        }

        #region [  P R I V A T E   M E T H O D S  ]

        private WAHBitArray ExecutionPlan(string filter)
        {
            _log.Debug("query : " + filter);
            DateTime dt = FastDateTime.Now;
            // query indexes
            string[] words = filter.Split(' ');

            WAHBitArray bits = null;

            foreach (string s in words)
            {
                Cache c;
                string word = s;
                if (s == "") continue;

                Cache.OPERATION op = Cache.OPERATION.OR;

                if (s.StartsWith("+"))
                {
                    op = Cache.OPERATION.AND;
                    word = s.Replace("+","");
                }

                if (s.StartsWith("-"))
                {
                    op = Cache.OPERATION.ANDNOT;
                    word = s.Replace("-","");
                }

                if (s.Contains("*") || s.Contains("?"))
                {
                    WAHBitArray wildbits = null;
                    // do wildcard search
                    Regex reg = new Regex(s.Replace("*", ".*").Replace("?", "."), RegexOptions.IgnoreCase);
                    foreach (var key in _index)
                    {
                        if (reg.IsMatch(key.Key))
                        {
                            c = _index[key.Key];
                            if (c.isLoaded == false)
                                LoadCache(c);

                            wildbits = DoBitOperation(wildbits, c, Cache.OPERATION.OR);
                        }
                    }
                    if (bits == null)
                        bits = wildbits;
                    else
                    {
                        if (op == Cache.OPERATION.AND)
                            bits = bits.And(wildbits);
                        else
                            bits = bits.Or(wildbits);
                    }
                }
                else if (_index.TryGetValue(word.ToLowerInvariant(), out c))
                {
                    // bits logic
                    if (c.isLoaded == false)
                        LoadCache(c);
                    bits = DoBitOperation(bits, c, op);
                }
            }
            if (bits == null)
                return new WAHBitArray();

            //// remove deleted docs
            //if (bits.Length > _deleted.Length)
            //    _deleted.Length = bits.Length;
            //else if (bits.Length < _deleted.Length)
            //    bits.Length = _deleted.Length;

            //WAHBitArray nd = _deleted.Not();

            WAHBitArray ret = bits;//.And(nd);
            _log.Debug("query time (ms) = " + FastDateTime.Now.Subtract(dt).TotalMilliseconds);
            return ret;
        }

        private static WAHBitArray DoBitOperation(WAHBitArray bits, Cache c, Cache.OPERATION op)
        {
            if (bits != null)
                bits = c.Op(bits, op);
            else
                bits = c.GetBitmap();
            return bits;
        }

        private void LoadCache(Cache c)
        {
            if (c.FileOffset != -1)
            {
                uint[] bits = LoadBitmap(c.FileOffset);
                c.SetCompressedBits(bits);
            }
            else
            {
                c.SetCompressedBits(new uint[] { 0 });
            }
        }

        private void InternalSave()
        {
            _log.Debug("saving index...");
            DateTime dt = FastDateTime.Now;

            MemoryStream ms = new MemoryStream();
            BinaryWriter bw = new BinaryWriter(ms, Encoding.UTF8);

            // save words and bitmaps
            using (FileStream words = new FileStream(_Path + _FileName + ".words", FileMode.Create))
            {
                foreach (KeyValuePair<string, Cache> kv in _index)
                {
                    bw.Write(kv.Key);
                    if (kv.Value.isDirty)
                    {
                        // write bit index
                        uint[] ar = kv.Value.GetCompressedBits();
                        if (ar != null)
                        {
                            // save bitmap data to disk
                            long off = SaveBitmap(ar);
                            // set the saved info in cache
                            kv.Value.FileOffset = off;
                            kv.Value.LastBitSaveLength = ar.Length;
                            // set the word bitmap offset
                            bw.Write(kv.Value.FileOffset);
                        }
                        else
                            bw.Write(kv.Value.FileOffset);
                    }
                    else
                        bw.Write(kv.Value.FileOffset);

                    kv.Value.isDirty = false;
                }
                byte[] b = ms.ToArray();
                words.Write(b, 0, b.Length);
                words.Flush();
                words.Close();
            }
            _log.Debug("save time (ms) = " + FastDateTime.Now.Subtract(dt).TotalMilliseconds);
        }

        private void LoadWords()
        {
            if (File.Exists(_Path + _FileName + ".words") == false)
                return;
            // load words
            byte[] b = File.ReadAllBytes(_Path + _FileName + ".words");
            MemoryStream ms = new MemoryStream(b);
            BinaryReader br = new BinaryReader(ms, Encoding.UTF8);
            string s = br.ReadString();
            while (s != "")
            {
                long off = br.ReadInt64();
                Cache c = new Cache();
                c.isLoaded = false;
                c.isDirty = false;
                c.FileOffset = off;
                _index.Add(s, c);
                try
                {
                    s = br.ReadString();
                }
                catch { s = ""; }
            }
            _log.Debug("Word Count = " + _index.Count);
        }

        //-----------------------------------------------------------------
        // BITMAP FILE FORMAT
        //    0  'B','M'
        //    2  uint count = 4 bytes
        //    6  '0'
        //    7  uint data
        //-----------------------------------------------------------------
        private long SaveBitmap(uint[] bits)
        {
            long off = _lastBitmapOffset;

            byte[] b = new byte[bits.Length * 4 + 7];
            // write header data
            b[0] = ((byte)'B');
            b[1] = ((byte)'M');
            Buffer.BlockCopy(Helper.GetBytes(bits.Length, false), 0, b, 2, 4);
            b[6] = (0);

            for (int i = 0; i < bits.Length; i++)
            {
                byte[] u = Helper.GetBytes((int)bits[i], false);
                Buffer.BlockCopy(u, 0, b, i * 4 + 7, 4);
            }
            _bitmapFile.Write(b, 0, b.Length);
            _lastBitmapOffset += b.Length;
            _bitmapFile.Flush();
            return off;
        }

        private uint[] LoadBitmap(long offset)
        {
            if (offset == -1)
                return null;

            List<uint> ar = new List<uint>();

            using (FileStream bmp = new FileStream(_Path + _FileName + _bmpext, FileMode.Open,
                                                   FileAccess.ReadWrite, FileShare.ReadWrite))
            {
                bmp.Seek(offset, SeekOrigin.Begin);

                byte[] b = new byte[7];
                bmp.Read(b, 0, 7);
                if (b[0] == (byte)'B' && b[1] == (byte)'M' && b[6] == 0)
                {
                    int c = Helper.ToInt32(b, 2);
                    for (int i = 0; i < c; i++)
                    {
                        bmp.Read(b, 0, 4);
                        ar.Add((uint)Helper.ToInt32(b, 0));
                    }
                }

                bmp.Flush();
                bmp.Close();
            }
            return ar.ToArray();
        }

        private void AddtoIndex(int recnum, string text)
        {
            if (text == null)
                return;

            foreach (string key in text.Split(' '))
            {
                if (key == "")
                    continue;
                Cache cache;
                if (_index.TryGetValue(key.ToLower(), out cache))
                {
                    cache.SetBit(recnum, true);
                }
                else
                {
                    cache = new Cache();
                    cache.isLoaded = true;
                    cache.SetBit(recnum, true);
                    _index.Add(key.ToLower(), cache);
                }
            }
        }

        #endregion

        public void Shutdown()
        {
            Save();
            _bitmapFile.Flush();
            _bitmapFile.Close();
        }
    }
}
