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
        WAHBitArray _freeList;

        private string _filename = "";
        private object _readlock = new object();
        ILog _log = LogManager.GetLogger(typeof(StorageFileHF));

        // **** change this if storage format changed ****
        internal static int _CurrentVersion = 1;
        int _lastBlockNumber = 0;
        private ushort _BLOCKSIZE = 4096;
        private string _Path = "";
        private string _S = Path.DirectorySeparatorChar.ToString();

        public static byte[] _fileheader = { (byte)'M', (byte)'G', (byte)'H', (byte)'F',
                                              0,   // 4 -- storage file version number,
                                              0,2, // 5,6 -- block size ushort low, hi
                                              1    // 7 -- key type 0 = guid, 1 = string
                                           };

        public StorageFileHF(string filename, ushort blocksize)
        {
            _Path = Path.GetDirectoryName(filename);
            if (_Path.EndsWith(_S) == false) _Path += _S;
            _filename = Path.GetFileNameWithoutExtension(filename);

            Initialize(filename, blocksize);
        }

        public void Shutdown()
        {
            FlushClose(_datawrite);
            // write free list 
            WriteFreeListBMPFile(_Path + _filename + ".free");

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
            WAHBitArray.TYPE t;
            uint[] ints = _freeList.GetCompressed(out t);
            MemoryStream ms = new MemoryStream();
            BinaryWriter bw = new BinaryWriter(ms);
            bw.Write((byte)t);// write new format with the data type byte
            foreach (var i in ints)
            {
                bw.Write(i);
            }
            File.WriteAllBytes(filename, ms.ToArray());
        }

        private void ReadFreeListBMPFile(string filename)
        {
            byte[] b = File.ReadAllBytes(filename);
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
                ints.Add((uint)Helper.ToInt32(b, (i * 4) + j));
            }
            _freeList = new WAHBitArray(t, ints.ToArray());
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
                ReadFileHeader();
                _lastBlockNumber = (int)((_datawrite.Length - _fileheader.Length) / _BLOCKSIZE);
                _lastBlockNumber++;
            }
            _freeList = new WAHBitArray();
            if (File.Exists(_Path + _filename + ".free"))
            {
                ReadFreeListBMPFile(_Path + _filename + ".free");
                // delete file so if failure no big deal on restart
                File.Delete(_Path + _filename + ".free");
            }
        }

        private void ReadFileHeader()
        {
            // set _blockize
            _datawrite.Seek(0L, SeekOrigin.Begin);
            byte[] hdr = new byte[_fileheader.Length];
            _datawrite.Read(hdr, 0, _fileheader.Length);

            _BLOCKSIZE = 0;
            _BLOCKSIZE = (ushort)((int)hdr[5] + ((int)hdr[6]) << 8);
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
            return (int)((_datawrite.Length / (int)_BLOCKSIZE) + 1);
        }

        internal void FreeBlock(int i)
        {
            _freeList.Set(i, true);
        }
    }
}
