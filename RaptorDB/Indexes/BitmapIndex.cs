using RaptorDB.Common;
using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;

namespace RaptorDB
{
    internal class BitmapIndex
    {
        public BitmapIndex(string path, string filename)
        {
            if (Global.UseLessMemoryStructures)
                _cache = new SafeSortedList<int, MGRB>();
            else
                _cache = new SafeDictionary<int, MGRB>();

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
        //private SafeDictionary<int, MGRB> _cache = new SafeDictionary<int, MGRB>();
        private IKV<int, MGRB> _cache = null;// new SafeSortedList<int, MGRB>();
        private ILog log = LogManager.GetLogger(typeof(BitmapIndex));
        private bool _stopOperations = false;
        private bool _shutdownDone = false;
        private int _workingCount = 0;
        private bool _isDirty = false;

        #region
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

                _cache.Add(i, new MGRB());
                return i;
            }
        }

        public void Commit(bool freeMemory)
        {
            if (_isDirty == false)
                return;
            using (new L(this))
            {
                log.Debug("writing " + _FileName);
                int[] keys = _cache.Keys();
                Array.Sort(keys);

                foreach (int k in keys)
                {
                    MGRB bmp = null;
                    if (_cache.TryGetValue(k, out bmp) && bmp.isDirty)
                    {
                        bmp.Optimize();
                        SaveBitmap(k, bmp);
                        bmp.isDirty = false;
                    }
                }
                Flush();
                if (freeMemory)
                {
                    if (Global.UseLessMemoryStructures)
                        _cache = new SafeSortedList<int, MGRB>();
                    else
                        _cache = new SafeDictionary<int, MGRB>();
                    log.Debug("  freeing cache");
                }
                _isDirty = false;
            }
        }

        public void SetDuplicate(int bitmaprecno, int record)
        {
            using (new L(this))
            {
                MGRB ba = null;

                ba = internalGetBitmap(bitmaprecno); //GetBitmap(bitmaprecno);

                ba.Set(record, true);
                _isDirty = true;
            }
        }

        public MGRB GetBitmap(int recno)
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
                        _stopOperations = true;
                        while (_workingCount > 0) Thread.SpinWait(1);
                        Flush();

                        if (File.Exists(_Path + _FileName + "$" + _bmpExt))
                            File.Delete(_Path + _FileName + "$" + _bmpExt);

                        if (File.Exists(_Path + _FileName + "$" + _recExt))
                            File.Delete(_Path + _FileName + "$" + _recExt);

                        Stream _newrec = new FileStream(_Path + _FileName + "$" + _recExt, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.ReadWrite);
                        Stream _newbmp = new FileStream(_Path + _FileName + "$" + _bmpExt, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.ReadWrite);

                        long newoffset = 0;
                        int c = (int)(_recordFileRead.Length / 8);
                        for (int i = 0; i < c; i++)
                        {
                            long offset = ReadRecordOffset(i);

                            byte[] b = ReadBMPDataForOptimize(offset);
                            if (b == null)
                            {
                                _stopOperations = false;
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
                        _stopOperations = false;
                    }
        }

        internal void FreeMemory()
        {
            try
            {
                List<int> free = new List<int>();
                foreach (var k in _cache.Keys())
                {
                    var val = _cache.GetValue(k);
                    if (val.isDirty == false)
                        free.Add(k);
                }
                log.Info("releasing bmp count = " + free.Count + " out of " + _cache.Count());
                foreach (int i in free)
                    _cache.Remove(i);
            }
            catch (Exception ex)
            {
                log.Error(ex);
            }
        }
        #endregion

        #region [  P R I V A T E  ]
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

            if (_shutdownDone == false)
            {
                Flush();
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
                _recordFileWrite = null;
                _recordFileRead = null;
                _bitmapFileRead = null;
                _bitmapFileWrite = null;
                _recordFileRead = null;
                _recordFileWrite = null;
                _shutdownDone = true;
            }
        }

        private void Flush()
        {
            if (_shutdownDone)
                return;

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
        private MGRB internalGetBitmap(int recno)
        {
            lock (_readlock)
            {
                MGRB ba = new MGRB();
                if (recno == -1)
                    return ba;

                if (_cache.TryGetValue(recno, out ba))
                {
                    return ba;
                }
                else
                {
                    long offset = 0;
                    //if (_offsetCache.TryGetValue(recno, out offset) == false)
                    {
                        offset = ReadRecordOffset(recno);
                        // _offsetCache.Add(recno, offset);
                    }
                    ba = LoadBitmap(offset);

                    _cache.Add(recno, ba);

                    return ba;
                }
            }
        }

        private object _writelock = new object();
        private void SaveBitmap(int recno, MGRB bmp)
        {
            lock (_writelock)
            {
                long offset = SaveBitmapToFile(bmp);
                //long v;
                //if (_offsetCache.TryGetValue(recno, out v))
                //    _offsetCache[recno] = offset;
                //else
                //    _offsetCache.Add(recno, offset);

                long pointer = ((long)recno) * 8;
                _recordFileWrite.Seek(pointer, SeekOrigin.Begin);
                byte[] b = new byte[8];
                b = Helper.GetBytes(offset, false);
                _recordFileWrite.Write(b, 0, 8);
            }
        }

        //-----------------------------------------------------------------
        //  new format 
        //  0 : b 
        //  1 : m
        //  2 : type 0 = uncompressed, 1 = compressed
        //  3 : data size (int)
        //  8 : data bytes
        private byte _hdrlen = 2 + 4 + 1;
        private long SaveBitmapToFile(MGRB bmp)
        {
            long off = _lastBitmapOffset;
            var dat = bmp.Serialize();
            var hdr = new byte[_hdrlen];
            var b = fastBinaryJSON.BJSON.ToBJSON(dat, new fastBinaryJSON.BJSONParameters { UseExtensions = false });
            hdr[0] = (byte)'b';
            hdr[1] = (byte)'m';
            hdr[2] = 0; // uncompressed

            if (Global.CompressBitmapBytes)
            {
                hdr[2] = 1;
                b = MiniLZO.Compress(b);
            }

            var s = Helper.GetBytes(b.Length, false);
            Buffer.BlockCopy(s, 0, hdr, 3, 4);

            _bitmapFileWrite.Write(hdr, 0, hdr.Length);
            _lastBitmapOffset += hdr.Length;

            _bitmapFileWrite.Write(b, 0, b.Length);
            _lastBitmapOffset += b.Length;

            return off;
        }

        private byte[] ReadBMPDataForOptimize(long offset)
        {
            // return data + header
            _bitmapFileRead.Seek(offset, SeekOrigin.Begin);

            byte[] hdr = new byte[_hdrlen];

            _bitmapFileRead.Read(hdr, 0, _hdrlen);
            if (hdr[0] == (byte)'b' && hdr[1] == (byte)'m')
            {
                int c = Helper.ToInt32(hdr, 3);
                var data = new byte[c + _hdrlen];
                Buffer.BlockCopy(hdr, 0, data, 0, _hdrlen);
                _bitmapFileRead.Read(data, _hdrlen, c);
                return data;
            }
            return null;
        }

        private MGRB LoadBitmap(long offset)
        {
            MGRB bc = new MGRB();
            if (offset == -1)
                return bc;
            FileStream bmp = _bitmapFileRead;
            bmp.Seek(offset, SeekOrigin.Begin);
            var hdr = new byte[_hdrlen];
            bmp.Read(hdr, 0, hdr.Length);
            if (hdr[0] == (byte)'b' && hdr[1] == (byte)'m')
            {
                int c = Helper.ToInt32(hdr, 3);
                var b = new byte[c];
                bmp.Read(b, 0, c);
                if (hdr[2] == 1)
                    b = MiniLZO.Decompress(b);
                bc.Deserialize(fastBinaryJSON.BJSON.ToObject<MGRBData>(b));
            }
            else
                log.Error("bitmap not recognized");

            return bc;
        }

#pragma warning disable 642
        private void CheckInternalOP()
        {
            if (_stopOperations)
                lock (_oplock) { } // yes! this is good
            Interlocked.Increment(ref _workingCount);
        }
#pragma warning restore 642

        private void Done()
        {
            Interlocked.Decrement(ref _workingCount);
        }
        #endregion
    }
}
