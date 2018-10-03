using System;
using System.IO;
using System.Collections.Generic;
using RaptorDB.Common;
using System.Threading;

namespace RaptorDB
{
    // high frequency storage file with overwrite old values
    internal class StorageFileHF
    {
        FileStream _datawrite;
        MGRB _freeList;
        //Action<MGRB> _savefreeList = null;
        //Func<MGRB> _readfreeList = null;

        private string _filename = "";
        private object _readlock = new object();
        ILog _log = LogManager.GetLogger(typeof(StorageFileHF));

        // **** change this if storage format changed ****
        internal static int _CurrentVersion = 1;
        int _lastBlockNumber = -1;
        private ushort _BLOCKSIZE = 4096;
        private string _Path = "";
        private string _S = Path.DirectorySeparatorChar.ToString();

        public static byte[] _fileheader = { (byte)'M', (byte)'G', (byte)'H', (byte)'F',
                                              0,   // 4 -- storage file version number,
                                              0,2, // 5,6 -- block size ushort low, hi
                                              1    // 7 -- key type 0 = guid, 1 = string
                                           };

        //private SafeDictionary<string, int> _masterblock = new SafeDictionary<string, int>();

        public StorageFileHF(string filename, ushort blocksize) //: this(filename, blocksize, null, null)
        {
            _Path = Path.GetDirectoryName(filename);
            if (_Path.EndsWith(_S) == false) _Path += _S;
            _filename = Path.GetFileNameWithoutExtension(filename);

            Initialize(filename, blocksize);
        }

        // used for bitmapindexhf
        //public StorageFileHF(string filename, ushort blocksize, Func<MGRB> readfreelist, Action<MGRB> savefreelist)
        //{
        //    _savefreeList = savefreelist;
        //    _readfreeList = readfreelist;
        //    _Path = Path.GetDirectoryName(filename);
        //    if (_Path.EndsWith(_S) == false) _Path += _S;
        //    _filename = Path.GetFileNameWithoutExtension(filename);

        //    Initialize(filename, blocksize);
        //}

        public void Shutdown()
        {
            // write free list 
            //if (_savefreeList != null)
            //    _savefreeList(_freeList);
            //else
                WriteFreeListBMPFile(_Path + _filename + ".free");
            FlushClose(_datawrite);
            _datawrite = null;
        }

        public ushort GetBlockSize()
        {
            return _BLOCKSIZE;
        }

        internal void FreeBlocks(List<int> list)
        {
            list.ForEach(x => _freeList.Set(x, true));
        }

        internal byte[] ReadBlock(int blocknumber)
        {
            SeekBlock(blocknumber);
            byte[] data = new byte[_BLOCKSIZE];
            _datawrite.Read(data, 0, _BLOCKSIZE);

            return data;
        }

        internal byte[] ReadBlockBytes(int blocknumber, int bytes)
        {
            SeekBlock(blocknumber);
            byte[] data = new byte[bytes];
            _datawrite.Read(data, 0, bytes);

            return data;
        }

        internal int GetFreeBlockNumber()
        {
            // get the first free block or append to the end
            if (_freeList.CountOnes() > 0)
            {
                int i = _freeList.GetFirst();
                _freeList.Set(i, false);
                return i;
            }
            else
                return Interlocked.Increment(ref _lastBlockNumber);//++;
        }

        internal void Initialize()
        {
            if (_lastBlockNumber < 0)
            {
                // write master block
                _datawrite.Write(new byte[_BLOCKSIZE], 0, _BLOCKSIZE);
                _lastBlockNumber = 1;
            }
            //if (_readfreeList != null)
            //    _freeList = _readfreeList();
            //else
            {
                _freeList = new MGRB();
                if (File.Exists(_Path + _filename + ".free"))
                {
                    ReadFreeListBMPFile(_Path + _filename + ".free");
                    // delete file so if failure no big deal on restart
                    File.Delete(_Path + _filename + ".free");
                }
            }
        }

        internal void SeekBlock(int blocknumber)
        {
            long offset = (long)_fileheader.Length + (long)blocknumber * _BLOCKSIZE;
            _datawrite.Seek(offset, SeekOrigin.Begin);// wiil seek past the end of file on fs.Write will zero the difference
        }

        internal void WriteBlockBytes(byte[] data, int start, int len)
        {
            _datawrite.Write(data, start, len);
        }

        #region [ private / internal  ]

        private void WriteFreeListBMPFile(string filename)
        {
            if (_freeList != null)
            {
                _freeList.Optimize();
                var o = _freeList.Serialize();
                var b = fastBinaryJSON.BJSON.ToBJSON(o, new fastBinaryJSON.BJSONParameters { UseExtensions = false });
                File.WriteAllBytes(filename, b);
            }
        }

        private void ReadFreeListBMPFile(string filename)
        {
            byte[] b = File.ReadAllBytes(filename);
            var o = fastBinaryJSON.BJSON.ToObject<MGRBData>(b);
            _freeList = new MGRB();
            _freeList.Deserialize(o);
        }

        private void Initialize(string filename, ushort blocksize)
        {
            if (File.Exists(filename) == false)
                _datawrite = new FileStream(filename, FileMode.CreateNew, FileAccess.ReadWrite, FileShare.ReadWrite);
            else
                _datawrite = new FileStream(filename, FileMode.Open, FileAccess.ReadWrite, FileShare.ReadWrite);

            if (_datawrite.Length == 0)
            {
                CreateFileHeader(blocksize);
                // new file
                _datawrite.Write(_fileheader, 0, _fileheader.Length);
                _datawrite.Flush();
            }
            else
            {
                int filever = ReadFileHeader();
                if(filever<_CurrentVersion)
                {
                    // fixx : upgrade storage file here
                }
                _lastBlockNumber = (int)((_datawrite.Length - _fileheader.Length) / _BLOCKSIZE);
                _lastBlockNumber++;
            }

            Initialize();
        }

        private int ReadFileHeader()
        {
            // set _blockize
            _datawrite.Seek(0L, SeekOrigin.Begin);
            byte[] hdr = new byte[_fileheader.Length];
            _datawrite.Read(hdr, 0, _fileheader.Length);

            _BLOCKSIZE = 0;
            _BLOCKSIZE = (ushort)((int)hdr[5] + ((int)hdr[6]) << 8);

            return hdr[4];
        }

        private void CreateFileHeader(int blocksize)
        {
            // add version number
            _fileheader[4] = (byte)_CurrentVersion;
            // block size
            _fileheader[5] = (byte)(blocksize & 0xff);
            _fileheader[6] = (byte)(blocksize >> 8);
            _BLOCKSIZE = (ushort)blocksize;
        }

        private void FlushClose(FileStream st)
        {
            if (st != null)
            {
                st.Flush(true);
                st.Close();
            }
        }
        #endregion

        internal int NumberofBlocks()
        {
            return (int)((_datawrite.Length / (int)_BLOCKSIZE)) + 1;
        }

        internal void FreeBlock(int i)
        {
            _freeList.Set(i, true);
        }
    }
}
