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
    internal class StorageData<T>
    {
        public StorageItem<T> meta;
        public byte[] data;
    }

    public class StorageItem<T>
    {
        public T key;
        public string typename;
        public DateTime date = FastDateTime.Now;
        public bool isDeleted;
        public bool isReplicated;
        public int dataLength;
    }

    public interface IDocStorage<T>
    {
        int RecordCount();

        byte[] GetBytes(int rowid, out StorageItem<T> meta);
        object GetObject(int rowid, out StorageItem<T> meta);
        StorageItem<T> GetMeta(int rowid);

        bool GetObject(T key, out object doc);
    }

    public enum SF_FORMAT
    {
        BSON,
        JSON
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
        private long _lastWriteOffset = _fileheader.Length;
        private object _readlock = new object();
        private bool _dirty = false;
        IGetBytes<T> _T = null;
        ILog _log = LogManager.GetLogger(typeof(StorageFile<T>));
        private SF_FORMAT _saveFormat = SF_FORMAT.BSON;

        // **** change this if storage format changed ****
        internal static int _CurrentVersion = 2;

        public static byte[] _fileheader = { (byte)'M', (byte)'G', (byte)'D', (byte)'B',
                                              0, // 4 -- storage file version number,
                                              0  // 5 -- not used
                                           };

        // record format :
        //    1 type (0 = raw no meta data, 1 = bson meta, 2 = json meta)  
        //    4 byte meta/data length, 
        //    n byte meta serialized data if exists 
        //    m byte data (if meta exists then m is in meta.dataLength)

        public StorageFile(string filename, SF_FORMAT format, bool readmode)
        {
            _saveFormat = format;
            // add version number
            _fileheader[5] = (byte)_CurrentVersion;
            Initialize(filename, readmode);
        }

        public StorageFile(string filename, bool ReadMode)
        {
            Initialize(filename, ReadMode);
        }

        private void Initialize(string filename, bool ReadMode)
        {
            _T = RDBDataType<T>.ByteHandler();
            _filename = filename;
            if (File.Exists(filename) == false)
                _datawrite = new FileStream(filename, FileMode.CreateNew, FileAccess.ReadWrite, FileShare.ReadWrite);
            else
                _datawrite = new FileStream(filename, FileMode.Open, FileAccess.ReadWrite, FileShare.ReadWrite);

            _dataread = new FileStream(_filename, FileMode.Open, FileAccess.Read, FileShare.ReadWrite);

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
            if (ReadMode == false)
            {
                // load rec pointers
                _recfilename = filename.Substring(0, filename.LastIndexOf('.')) + ".mgrec";
                if (File.Exists(_recfilename) == false)
                    _recfilewrite = new FileStream(_recfilename, FileMode.CreateNew, FileAccess.Write, FileShare.ReadWrite);
                else
                    _recfilewrite = new FileStream(_recfilename, FileMode.Open, FileAccess.Write, FileShare.ReadWrite);

                _recfileread = new FileStream(_recfilename, FileMode.Open, FileAccess.Read, FileShare.ReadWrite);

                _lastRecordNum = (int)(_recfilewrite.Length / 8);
                _recfilewrite.Seek(0L, SeekOrigin.End);
            }
        }

        public static int GetStorageFileHeaderVersion(string filename)
        {
            if (File.Exists(filename))
            {
                var fs = new FileStream(filename, FileMode.Open, FileAccess.ReadWrite, FileShare.ReadWrite);
                fs.Seek(0L, SeekOrigin.Begin);
                byte[] b = new byte[_fileheader.Length];
                fs.Read(b, 0, _fileheader.Length);
                fs.Close();
                return b[5];
            }
            return _CurrentVersion;
        }

        public int Count()
        {
            return (int)(_recfilewrite.Length >> 3);
        }

        public int WriteRawData(byte[] b)
        {
            return internalWriteData(null, b, true);
        }

        public int Delete(T key)
        {
            StorageItem<T> meta = new StorageItem<T>();
            meta.key = key;
            meta.isDeleted = true;

            return internalWriteData(meta, null, false);
        }

        public int DeleteReplicated(T key)
        {
            StorageItem<T> meta = new StorageItem<T>();
            meta.key = key;
            meta.isReplicated = true;
            meta.isDeleted = true;

            return internalWriteData(meta, null, false);
        }

        public int WriteObject(T key, object obj)
        {
            StorageItem<T> meta = new StorageItem<T>();
            meta.key = key;
            meta.typename = fastJSON.Reflection.Instance.GetTypeAssemblyName(obj.GetType());
            byte[] data;
            if (_saveFormat == SF_FORMAT.BSON)
                data = fastBinaryJSON.BJSON.Instance.ToBJSON(obj);
            else
                data = Helper.GetBytes(fastJSON.JSON.Instance.ToJSON(obj));

            return internalWriteData(meta, data, false);
        }

        public int WriteReplicationObject(T key, object obj)
        {
            StorageItem<T> meta = new StorageItem<T>();
            meta.key = key;
            meta.isReplicated = true;
            meta.typename = fastJSON.Reflection.Instance.GetTypeAssemblyName(obj.GetType());
            byte[] data;
            if (_saveFormat == SF_FORMAT.BSON)
                data = fastBinaryJSON.BJSON.Instance.ToBJSON(obj);
            else
                data = Helper.GetBytes(fastJSON.JSON.Instance.ToJSON(obj));

            return internalWriteData(meta, data, false);
        }

        public int WriteData(T key, byte[] data)
        {
            StorageItem<T> meta = new StorageItem<T>();
            meta.key = key;

            return internalWriteData(meta, data, false);
        }

        public byte[] ReadData(int recnum)
        {
            StorageItem<T> meta;
            return ReadData(recnum, out meta);
        }

        public object ReadObject(int recnum)
        {
            StorageItem<T> meta = null;
            return ReadObject(recnum, out meta);
        }

        public object ReadObject(int recnum, out StorageItem<T> meta)
        {
            byte[] b = ReadData(recnum, out meta);
            if (b == null)
                return null;
            if (b[0] < 32)
                return fastBinaryJSON.BJSON.Instance.ToObject(b);
            else
                return fastJSON.JSON.Instance.ToObject(Encoding.ASCII.GetString(b));
        }

        /// <summary>
        /// used for views only
        /// </summary>
        /// <param name="recnum"></param>
        /// <returns></returns>
        public byte[] ReadRawData(int recnum)
        {
            if (recnum >= _lastRecordNum)
                return null;

            lock (_readlock)
            {
                long offset = ComputeOffset(recnum);
                _dataread.Seek(offset, System.IO.SeekOrigin.Begin);
                byte[] hdr = new byte[5];
                // read header
                _dataread.Read(hdr, 0, 5); // meta length
                int len = Helper.ToInt32(hdr, 1);

                int type = hdr[0];
                if (type == 0)
                {
                    byte[] data = new byte[len];
                    _dataread.Read(data, 0, len);
                    return data;
                }
                return null;
            }
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

        public static StorageFile<Guid> ReadForward(string filename)
        {
            StorageFile<Guid> sf = new StorageFile<Guid>(filename, true);

            return sf;
        }

        public StorageItem<T> ReadMeta(int rowid)
        {
            if (rowid >= _lastRecordNum)
                return null;
            lock (_readlock)
            {
                int metalen = 0;
                long off = ComputeOffset(rowid);
                _dataread.Seek(off, SeekOrigin.Begin);
                StorageItem<T> meta = ReadMetaData(out metalen);
                return meta;
            }
        }

        #region [ private / internal  ]

        private int internalWriteData(StorageItem<T> meta, byte[] data, bool raw)
        {
            lock (_readlock)
            {
                _dirty = true;
                // seek end of file
                long offset = _lastWriteOffset;

                if (raw == false)
                {
                    if (data != null)
                        meta.dataLength = data.Length;
                    byte[] metabytes = fastBinaryJSON.BJSON.Instance.ToBJSON(meta, new fastBinaryJSON.BJSONParameters { UseExtensions = false });

                    // write header info
                    _datawrite.Write(new byte[] { 1 }, 0, 1); // TODO : add json here, write bson for now
                    _datawrite.Write(Helper.GetBytes(metabytes.Length, false), 0, 4);
                    _datawrite.Write(metabytes, 0, metabytes.Length);
                    // update pointer
                    _lastWriteOffset += metabytes.Length + 5;
                }
                else
                {
                    // write header info
                    _datawrite.Write(new byte[] { 0 }, 0, 1); // write raw
                    _datawrite.Write(Helper.GetBytes(data.Length, false), 0, 4);
                    // update pointer
                    _lastWriteOffset += 5;
                }

                if (data != null)
                {
                    // write data block
                    _datawrite.Write(data, 0, data.Length);
                    _lastWriteOffset += data.Length;
                }
                // return starting offset -> recno
                int recno = _lastRecordNum++;
                if (_recfilewrite != null)
                    _recfilewrite.Write(Helper.GetBytes(offset, false), 0, 8);
                if (Global.FlushStorageFileImmediately)
                {
                    _datawrite.Flush();
                    if (_recfilewrite != null)
                        _recfilewrite.Flush();
                }
                return recno;
            }
        }

        internal byte[] ReadData(int recnum, out StorageItem<T> meta)
        {
            meta = null;
            if (recnum >= _lastRecordNum)
                return null;
            lock (_readlock)
            {
                long off = ComputeOffset(recnum);
                byte[] data = internalReadData(off, out meta);
                return data;
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

        private byte[] internalReadData(long offset, out StorageItem<T> meta)
        {
            // seek offset in file
            _dataread.Seek(offset, System.IO.SeekOrigin.Begin);
            int metalen = 0;
            meta = ReadMetaData(out metalen);
            if (meta != null)
            {
                if (meta.isDeleted == false)
                {
                    byte[] data = new byte[meta.dataLength];
                    _dataread.Read(data, 0, meta.dataLength);
                    return data;
                }
            }
            else
            {
                byte[] data = new byte[metalen];
                _dataread.Read(data, 0, metalen);
                return data;
            }
            return null;
        }

        private StorageItem<T> ReadMetaData(out int metasize)
        {
            byte[] hdr = new byte[5];
            // read header
            _dataread.Read(hdr, 0, 5); // meta length
            int len = Helper.ToInt32(hdr, 1);
            int type = hdr[0];
            if (type > 0)
            {
                metasize = len + 5;
                hdr = new byte[len];
                _dataread.Read(hdr, 0, len);
                StorageItem<T> meta;
                if (type == 1)
                    meta = fastBinaryJSON.BJSON.Instance.ToObject<StorageItem<T>>(hdr);
                else
                {
                    string str = Helper.GetString(hdr, 0, (short)hdr.Length);
                    meta = fastJSON.JSON.Instance.ToObject<StorageItem<T>>(str);
                }
                return meta;
            }
            else
            {
                metasize = len;
                return null;
            }
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
            lock (_readlock)
            {
                deleted = false;
                long off = ComputeOffset(recnum);
                _dataread.Seek(off, SeekOrigin.Begin);
                //long off = recnum * 8L;
                //byte[] b = new byte[8];

                //_recfileread.Seek(off, SeekOrigin.Begin);

                int metalen = 0;
                StorageItem<T> meta = ReadMetaData(out metalen);
                deleted = meta.isDeleted;
                return meta.key;
            }
        }

        internal int CopyTo(StorageFile<T> storageFile, int start)
        {
            // copy data here
            lock (_readlock)
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

        //internal IEnumerable<StorageData<T>> Enumerate()
        //{
        //    return Enumerate(6); // skip header
        //}

        internal IEnumerable<StorageData<T>> Enumerate()//long start)
        {
            lock (_readlock)
            {
                long offset = _fileheader.Length;// start; // skip header
                long size = _dataread.Length;
                while (offset < size)
                {
                    _dataread.Seek(offset, SeekOrigin.Begin);
                    int metalen = 0;
                    StorageItem<T> meta = ReadMetaData(out metalen);
                    offset += metalen;
                    StorageData<T> sd = new StorageData<T>();
                    sd.meta = meta;
                    if (meta.dataLength > 0)
                    {
                        byte[] data = new byte[meta.dataLength];
                        _dataread.Read(data, 0, meta.dataLength);
                        sd.data = data;
                    }
                    offset += meta.dataLength;
                    yield return sd;
                }
            }
        }
        #endregion
    }
}
