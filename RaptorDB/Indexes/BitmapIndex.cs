using System;
using System.Collections.Generic;
using System.Text;
using System.IO;
using RaptorDB.Common;
using System.Threading;
using System.Collections;

namespace RaptorDB
{
    internal class BitmapIndex
    {
        public BitmapIndex(string path, string filename)
        {
            _FileName = Path.GetFileNameWithoutExtension(filename);
            _Path = path;
            if (_Path.EndsWith(Path.DirectorySeparatorChar.ToString()) == false)
                _Path += Path.DirectorySeparatorChar.ToString();

            Initialize();
        }

        class L : IDisposable
        {
            BitmapIndex _sc;
            public L(BitmapIndex sc)
            {
                _sc = sc;
                _sc.CheckInternalOP();
            }
            void IDisposable.Dispose()
            {
                _sc.Done();
            }
        }
        private string _recExt = ".mgbmr";
        private string _bmpExt = ".mgbmp";
        private string _FileName = "";
        private string _Path = "";
        private FileStream _bitmapFileWriteOrg;
        private BufferedStream _bitmapFileWrite;
        private FileStream _bitmapFileRead;
        private FileStream _recordFileRead;
        private FileStream _recordFileWriteOrg;
        private BufferedStream _recordFileWrite;
        private long _lastBitmapOffset = 0;
        private int _lastRecordNumber = 0;
        private SafeDictionary<int, WAHBitArray> _cache = new SafeDictionary<int, WAHBitArray>();
        private SafeDictionary<int, long> _offsetCache = new SafeDictionary<int, long>();
        private ILog log = LogManager.GetLogger(typeof(BitmapIndex));
        private bool _optimizing = false;
        private bool _shutdownDone = false;
        private Queue _que = new Queue();

        #region [  P U B L I C  ]
        public void Shutdown()
        {
            using (new L(this))
            {
                log.Debug("Shutdown BitmapIndex");

                InternalShutdown();
            }
        }

        public int GetFreeRecordNumber()
        {
            using (new L(this))
            {
                int i = _lastRecordNumber++;

                _cache.Add(i, new WAHBitArray());
                return i;
            }
        }

        public void Commit(bool freeMemory)
        {
            using (new L(this))
            {
                int[] keys = _cache.Keys();
                Array.Sort(keys);

                foreach (int k in keys)
                {
                    var bmp = _cache[k];
                    if (bmp.isDirty)
                    {
                        SaveBitmap(k, bmp);
                        bmp.FreeMemory();
                        bmp.isDirty = false;
                    }
                }
                Flush();
                if (freeMemory)
                {
                    _cache = new SafeDictionary<int, WAHBitArray>();
                }
            }
        }

        public void SetDuplicate(int bitmaprecno, int record)
        {
            using (new L(this))
            {
                WAHBitArray ba = null;

                ba = GetBitmap(bitmaprecno);

                ba.Set(record, true);
            }
        }

        public WAHBitArray GetBitmap(int recno)
        {
            using (new L(this))
            {
                return internalGetBitmap(recno);
            }
        }

        private object _oplock = new object();
        public void Optimize()
        {
            lock (_oplock)
                lock (_readlock)
                    lock (_writelock)
                    {
                        _optimizing = true;
                        while (_que.Count > 0) Thread.SpinWait(1);
                        Flush();

                        if (File.Exists(_Path + _FileName + "$" + _bmpExt))
                            File.Delete(_Path + _FileName + "$" + _bmpExt);

                        if (File.Exists(_Path + _FileName + "$" + _recExt))
                            File.Delete(_Path + _FileName + "$" + _recExt);

                        FileStream _newrec = new FileStream(_Path + _FileName + "$" + _recExt, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.ReadWrite);
                        FileStream _newbmp = new FileStream(_Path + _FileName + "$" + _bmpExt, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.ReadWrite);

                        long newoffset = 0;
                        int c = (int)(_recordFileRead.Length / 8);
                        for (int i = 0; i < c; i++)
                        {
                            long offset = ReadRecordOffset(i);

                            byte[] b = ReadBMPData(offset);
                            if (b == null)
                            {
                                _optimizing = false;
                                throw new Exception("bitmap index file is corrupted");
                            }

                            _newrec.Write(Helper.GetBytes(newoffset, false), 0, 8); 
                            newoffset += b.Length;
                            _newbmp.Write(b, 0, b.Length);

                        }
                        _newbmp.Flush();
                        _newbmp.Close();
                        _newrec.Flush();
                        _newrec.Close();

                        InternalShutdown();

                        File.Delete(_Path + _FileName + _bmpExt);
                        File.Delete(_Path + _FileName + _recExt);
                        File.Move(_Path + _FileName + "$" + _bmpExt, _Path + _FileName + _bmpExt);
                        File.Move(_Path + _FileName + "$" + _recExt, _Path + _FileName + _recExt);

                        Initialize();
                        _optimizing = false;
                    }
        }
        #endregion


        #region [  P R I V A T E  ]
        private byte[] ReadBMPData(long offset)
        {
            _bitmapFileRead.Seek(offset, SeekOrigin.Begin);

            byte[] b = new byte[8];

            _bitmapFileRead.Read(b, 0, 8);
            if (b[0] == (byte)'B' && b[1] == (byte)'M' && b[7] == 0)
            {
                int c = Helper.ToInt32(b, 2) * 4 + 8;
                byte[] data = new byte[c];
                _bitmapFileRead.Seek(offset, SeekOrigin.Begin);
                _bitmapFileRead.Read(data, 0, c);
                return data;
            }
            return null;
        }

        private long ReadRecordOffset(int recnum)
        {
            byte[] b = new byte[8];
            long off = ((long)recnum) * 8;
            _recordFileRead.Seek(off, SeekOrigin.Begin);
            _recordFileRead.Read(b, 0, 8);
            return Helper.ToInt64(b, 0);
        }

        private void Initialize()
        {
            _recordFileRead = new FileStream(_Path + _FileName + _recExt, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.ReadWrite);
            _recordFileWriteOrg = new FileStream(_Path + _FileName + _recExt, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.ReadWrite);
            _recordFileWrite = new BufferedStream(_recordFileWriteOrg);

            _bitmapFileRead = new FileStream(_Path + _FileName + _bmpExt, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.ReadWrite);
            _bitmapFileWriteOrg = new FileStream(_Path + _FileName + _bmpExt, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.ReadWrite);
            _bitmapFileWrite = new BufferedStream(_bitmapFileWriteOrg);

            _bitmapFileWrite.Seek(0L, SeekOrigin.End);
            _lastBitmapOffset = _bitmapFileWrite.Length;
            _lastRecordNumber = (int)(_recordFileRead.Length / 8);
            _shutdownDone = false;
        }

        private void InternalShutdown()
        {
            bool d1 = false;
            bool d2 = false;
            Flush();
            if (_shutdownDone == false)
            {
                if (_recordFileWrite.Length == 0) d1 = true;
                if (_bitmapFileWrite.Length == 0) d2 = true;
                _recordFileRead.Close();
                _bitmapFileRead.Close();
                _bitmapFileWriteOrg.Close();
                _recordFileWriteOrg.Close();
                _recordFileWrite.Close();
                _bitmapFileWrite.Close();
                if (d1)
                    File.Delete(_Path + _FileName + _recExt);
                if (d2)
                    File.Delete(_Path + _FileName + _bmpExt);
                _shutdownDone = true;
            }
        }

        private void Flush()
        {
            if (_recordFileWrite != null)
                _recordFileWrite.Flush();
            if (_bitmapFileWrite != null)
                _bitmapFileWrite.Flush();
            if (_recordFileRead != null)
                _recordFileRead.Flush();
            if (_bitmapFileRead != null)
                _bitmapFileRead.Flush();
            if (_bitmapFileWriteOrg != null)
                _bitmapFileWriteOrg.Flush();
            if (_recordFileWriteOrg != null)
                _recordFileWriteOrg.Flush();
        }

        private object _readlock = new object();
        private WAHBitArray internalGetBitmap(int recno)
        {
            lock (_readlock)
            {
                WAHBitArray ba = new WAHBitArray();
                if (recno == -1)
                    return ba;

                if (_cache.TryGetValue(recno, out ba))
                {
                    return ba;
                }
                else
                {
                    long offset = 0;
                    if (_offsetCache.TryGetValue(recno, out offset) == false)
                    {
                        offset = ReadRecordOffset(recno);
                        _offsetCache.Add(recno, offset);
                    }
                    ba = LoadBitmap(offset);

                    _cache.Add(recno, ba);

                    return ba;
                }
            }
        }

        private object _writelock = new object();
        private void SaveBitmap(int recno, WAHBitArray bmp)
        {
            lock (_writelock)
            {
                long offset = SaveBitmapToFile(bmp);
                long v;
                if (_offsetCache.TryGetValue(recno, out v))
                    _offsetCache[recno] = offset;
                else
                    _offsetCache.Add(recno, offset);

                long pointer = ((long)recno) * 8;
                _recordFileWrite.Seek(pointer, SeekOrigin.Begin);
                byte[] b = new byte[8];
                b = Helper.GetBytes(offset, false);
                _recordFileWrite.Write(b, 0, 8);
            }
        }

        //-----------------------------------------------------------------
        // BITMAP FILE FORMAT
        //    0  'B','M'
        //    2  uint count = 4 bytes
        //    6  Bitmap type :
        //                0 = int record list   
        //                1 = uint bitmap
        //                2 = rec# indexes
        //    7  '0'
        //    8  uint data
        //-----------------------------------------------------------------
        private long SaveBitmapToFile(WAHBitArray bmp)
        {
            long off = _lastBitmapOffset;
            WAHBitArray.TYPE t;
            uint[] bits = bmp.GetCompressed(out t);

            byte[] b = new byte[bits.Length * 4 + 8];
            // write header data
            b[0] = ((byte)'B');
            b[1] = ((byte)'M');
            Buffer.BlockCopy(Helper.GetBytes(bits.Length, false), 0, b, 2, 4);

            b[6] = (byte)t;
            b[7] = (byte)(0);

            for (int i = 0; i < bits.Length; i++)
            {
                byte[] u = Helper.GetBytes((int)bits[i], false);
                Buffer.BlockCopy(u, 0, b, i * 4 + 8, 4);
            }
            _bitmapFileWrite.Write(b, 0, b.Length);
            _lastBitmapOffset += b.Length;
            return off;
        }

        private WAHBitArray LoadBitmap(long offset)
        {
            WAHBitArray bc = new WAHBitArray();
            if (offset == -1)
                return bc;

            List<uint> ar = new List<uint>();
            WAHBitArray.TYPE type = WAHBitArray.TYPE.WAH;
            FileStream bmp = _bitmapFileRead;
            {
                bmp.Seek(offset, SeekOrigin.Begin);

                byte[] b = new byte[8];

                bmp.Read(b, 0, 8);
                if (b[0] == (byte)'B' && b[1] == (byte)'M' && b[7] == 0)
                {
                    type = (WAHBitArray.TYPE)Enum.ToObject(typeof(WAHBitArray.TYPE), b[6]);
                    int c = Helper.ToInt32(b, 2);
                    byte[] buf = new byte[c * 4];
                    bmp.Read(buf, 0, c * 4);
                    for (int i = 0; i < c; i++)
                    {
                        ar.Add((uint)Helper.ToInt32(buf, i * 4));
                    }
                }
            }
            bc = new WAHBitArray(type, ar.ToArray());

            return bc;
        }

        private void CheckInternalOP()
        {
            if (_optimizing)
                lock (_oplock) ;
            _que.Enqueue(1);
        }

        private void Done()
        {
            if (_que.Count > 0)
                _que.Dequeue();
        }
        #endregion

        internal void FreeMemory()
        {
            try
            {
                List<int> free = new List<int>();
                foreach (var b in _cache)
                {
                    if (b.Value.isDirty == false)
                        free.Add(b.Key);
                }
                log.Debug("releasing bmp count = " + free.Count + " out of " + _cache.Count);
                foreach (int i in free)
                    _cache.Remove(i);
            }
            catch { }
        }
    }
}
