using System;
using System.Diagnostics;
using System.Collections;
using System.Runtime.InteropServices;
using System.IO;
using System.Text;
using System.Collections.Generic;
using RaptorDB.Common;

namespace RaptorDB
{
    internal class StorageData
    {
        public byte[] Key;
        public byte[] Data;
        public bool isDeleted;
    }

    internal class StorageFile<T>
    {
        FileStream _datawrite;
        FileStream _recfilewrite;
        FileStream _recfileread = null;
        FileStream _dataread = null;

        private string _filename = "";
        private string _recfilename = "";
        private int _lastRecordNum = 0;
        private long _lastWriteOffset = 6;
        private object _lock = new object();
        private bool _dirty = false;
        IGetBytes<T> _T = null;

        public static byte[] _fileheader = { (byte)'M', (byte)'G', (byte)'D', (byte)'B',
                                              0, // 4 -- not used,
                                              0  // 5 -- not used
                                           };

        public static byte[] _rowheader = { (byte)'M', (byte)'G', (byte)'R' ,
                                           0,               // 3     [keylen]
                                           0,0,0,0,0,0,0,0, // 4-11  [datetime] 8 bytes = insert time
                                           0,0,0,0,         // 12-15 [data length] 4 bytes
                                           0,               // 16 -- [flags] = 1 : isDeletd:1
                                                            //                 2 : isCompressed:1
                                                            //                 
                                                            //                 
                                           0                // 17 -- [crc] = header crc check
                                       };
        private enum HDR_POS
        {
            KeyLen = 3,
            DateTime = 4,
            DataLength = 12,
            Flags = 16,
            CRC = 17
        }

        public bool SkipDateTime = false;

        public StorageFile(string filename)
        {
            Initialize(filename, false);
        }

        public StorageFile(string filename, bool SkipChecking)
        {
            Initialize(filename, SkipChecking);
        }

        private void Initialize(string filename, bool SkipChecking)
        {
            _T = RDBDataType<T>.ByteHandler();
            _filename = filename;
            if (File.Exists(filename) == false)
                _datawrite = new FileStream(filename, FileMode.CreateNew, FileAccess.ReadWrite, FileShare.ReadWrite);
            else
                _datawrite = new FileStream(filename, FileMode.Open, FileAccess.ReadWrite, FileShare.ReadWrite);

            _dataread = new FileStream(_filename, FileMode.Open, FileAccess.Read, FileShare.ReadWrite);

            if (SkipChecking == false)
            {
                // load rec pointers
                _recfilename = filename.Substring(0, filename.LastIndexOf('.')) + ".mgrec";
                if (File.Exists(_recfilename) == false)
                    _recfilewrite = new FileStream(_recfilename, FileMode.CreateNew, FileAccess.Write, FileShare.ReadWrite);
                else
                    _recfilewrite = new FileStream(_recfilename, FileMode.Open, FileAccess.Write, FileShare.ReadWrite);

                _recfileread = new FileStream(_recfilename, FileMode.Open, FileAccess.Read, FileShare.ReadWrite);

                if (_datawrite.Length == 0)
                {
                    // new file
                    _datawrite.Write(_fileheader, 0, _fileheader.Length);
                    _datawrite.Flush();
                    _lastWriteOffset = _fileheader.Length;
                }
                else
                {
                    _lastWriteOffset = _datawrite.Seek(0L, SeekOrigin.End);
                }
                _lastRecordNum = (int)(_recfilewrite.Length / 8);
                _recfilewrite.Seek(0L, SeekOrigin.End);
            }
        }

        public int Count()
        {
            return (int)(_recfilewrite.Length >> 3);
        }

        public int WriteData(T key, byte[] data, bool deleted)
        {
            lock (_lock)
            {
                _dirty = true;
                byte[] k = _T.GetBytes(key);
                int kl = k.Length;

                // seek end of file
                long offset = _lastWriteOffset;
                byte[] hdr = CreateRowHeader(kl, (data == null ? 0 : data.Length));
                if (deleted)
                    hdr[(int)HDR_POS.Flags] = (byte)1;
                // write header info
                _datawrite.Write(hdr, 0, hdr.Length);
                // write key
                _datawrite.Write(k, 0, kl);
                if (data != null)
                {
                    // write data block
                    _datawrite.Write(data, 0, data.Length);
                    _lastWriteOffset += data.Length;
                }
                // update pointer
                _lastWriteOffset += hdr.Length;
                _lastWriteOffset += kl;
                // return starting offset -> recno
                int recno = _lastRecordNum++;
                _recfilewrite.Write(Helper.GetBytes(offset, false), 0, 8);
                if (Global.FlushStorageFileImmetiatley)
                {
                    _datawrite.Flush();
                    _recfilewrite.Flush();
                }
                return recno;
            }
        }

        public byte[] ReadData(int recnum)
        {
            bool isdel = false;
            return ReadData(recnum, out isdel);
        }

        public void Shutdown()
        {
            FlushClose(_dataread);
            FlushClose(_recfileread);
            FlushClose(_recfilewrite);
            FlushClose(_datawrite);

            _dataread = null;
            _recfileread = null;
            _recfilewrite = null;
            _datawrite = null;
        }

        public static StorageFile<int> ReadForward(string filename)
        {
            StorageFile<int> sf = new StorageFile<int>(filename, true);

            return sf;
        }

        #region [ private / internal  ]

        private byte[] CreateRowHeader(int keylen, int datalen)
        {
            byte[] rh = new byte[_rowheader.Length];
            Buffer.BlockCopy(_rowheader, 0, rh, 0, rh.Length);
            rh[3] = (byte)keylen;
            if (SkipDateTime == false)
                Buffer.BlockCopy(Helper.GetBytes(FastDateTime.Now.Ticks, false), 0, rh, 4, 4);
            Buffer.BlockCopy(Helper.GetBytes(datalen, false), 0, rh, 12, 4);

            return rh;
        }

        internal byte[] ReadData(int recnum, out bool isdeleted)
        {
            isdeleted = false;
            if (recnum >= _lastRecordNum)
                return null;

            lock (_lock)
            {
                long off = ComputeOffset(recnum);
                byte[] key;
                return internalReadData(off, out key, out isdeleted);
            }
        }

        private long ComputeOffset(int recnum)
        {
            if (_dirty)
            {
                _datawrite.Flush();
                _recfilewrite.Flush();
            }
            long off = recnum * 8L;
            byte[] b = new byte[8];

            _recfileread.Seek(off, SeekOrigin.Begin);
            _recfileread.Read(b, 0, 8);
            off = Helper.ToInt64(b, 0);
            if (off == 0)// kludge
                off = 6;
            return off;
        }

        private byte[] internalReadData(long offset, out byte[] key, out bool isdeleted)
        {
            // seek offset in file
            byte[] hdr = new byte[_rowheader.Length];
            _dataread.Seek(offset, System.IO.SeekOrigin.Begin);
            // read header
            _dataread.Read(hdr, 0, _rowheader.Length);
            // check header
            if (CheckHeader(hdr))
            {
                //isdeleted = false;
                key = null;
                byte[] data = null;
                isdeleted = isDeleted(hdr);
                //if (isDeleted(hdr) == false)
                //    isdeleted = false;
                int kl = hdr[(int)HDR_POS.KeyLen];
                if (kl > 0)
                {
                    key = new byte[kl];
                    _dataread.Read(key, 0, key.Length);
                }
                int dl = Helper.ToInt32(hdr, (int)HDR_POS.DataLength);
                if (dl > 0)
                {
                    data = new byte[dl];
                    // read data block
                    _dataread.Read(data, 0, dl);
                }
                return data;
            }
            else
                throw new Exception("data header error at offset : " + offset + " data file size = " + _dataread.Length);

        }

        private bool CheckHeader(byte[] hdr)
        {
            if (hdr[0] == (byte)'M' && hdr[1] == (byte)'G' && hdr[2] == (byte)'R' && hdr[(int)HDR_POS.CRC] == (byte)0)
                return true;
            return false;
        }

        private void FlushClose(FileStream st)
        {
            if (st != null)
            {
                st.Flush(true);
                st.Close();
            }
        }

        internal T GetKey(int recnum, out bool deleted)
        {
            lock (_lock)
            {
                deleted = false;
                long off = recnum * 8L;
                byte[] b = new byte[8];

                _recfileread.Seek(off, SeekOrigin.Begin);
                _recfileread.Read(b, 0, 8);
                off = Helper.ToInt64(b, 0);

                // seek offset in file
                byte[] hdr = new byte[_rowheader.Length];
                _dataread.Seek(off, System.IO.SeekOrigin.Begin);
                // read header
                _dataread.Read(hdr, 0, _rowheader.Length);

                if (CheckHeader(hdr))
                {
                    deleted = isDeleted(hdr);
                    byte kl = hdr[3];
                    byte[] kbyte = new byte[kl];

                    _dataread.Read(kbyte, 0, kl);
                    return _T.GetObject(kbyte, 0, kl);
                }

                return default(T);
            }
        }

        private bool isDeleted(byte[] hdr)
        {
            return (hdr[(int)HDR_POS.Flags] & (byte)1) > 0;
        }

        internal int CopyTo(StorageFile<int> storageFile, int start)
        {
            // copy data here
            lock (_lock)
            {
                long off = ComputeOffset(start);
                _dataread.Seek(off, SeekOrigin.Begin);
                Pump(_dataread, storageFile._datawrite);

                return _lastRecordNum;
            }
        }

        private static void Pump(Stream input, Stream output)
        {
            byte[] bytes = new byte[4096 * 2];
            int n;
            while ((n = input.Read(bytes, 0, bytes.Length)) != 0)
                output.Write(bytes, 0, n);
        }

        internal IEnumerable<StorageData> Enumerate()
        {
            long offset = 6;
            long size = _dataread.Length;
            while (offset < size)
            {
                // skip header
                _dataread.Seek(offset, SeekOrigin.Begin);
                byte[] hdr = new byte[_rowheader.Length];
                // read header
                _dataread.Read(hdr, 0, _rowheader.Length);
                offset += hdr.Length;
                if (CheckHeader(hdr))
                {
                    StorageData sd = new StorageData();
                    sd.isDeleted = isDeleted(hdr);
                    byte kl = hdr[3];
                    byte[] kbyte = new byte[kl];
                    offset += kl;
                    _dataread.Read(kbyte, 0, kl);
                    sd.Key = kbyte;
                    int dl = Helper.ToInt32(hdr, (int)HDR_POS.DataLength);
                    byte[] data = new byte[dl];
                    // read data block
                    _dataread.Read(data, 0, dl);
                    sd.Data = data;
                    offset += dl;
                    yield return sd;
                }
                else
                {
                    throw new Exception("Data read failed");
                }
            }
        }
        #endregion
    }
}
