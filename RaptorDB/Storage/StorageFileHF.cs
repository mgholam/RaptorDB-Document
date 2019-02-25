using RaptorDB.Common;
using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;

namespace RaptorDB
{
    // high frequency storage file with overwrite old values
    internal class StorageFileHF
    {
        private FileStream _datawriteorg;
        private BufferedStream _datawrite;
        private MGRB _freeList = new MGRB();
        private string _filename = "";
        private object _readlock = new object();
        ILog _log = LogManager.GetLogger(typeof(StorageFileHF));

        // **** change this if storage format changed ****
        private static int _CurrentVersion = 1;
        private int _lastBlockNumber = -1;
        private ushort _BLOCKSIZE = 4096;
        private string _Path = "";
        private string _S = Path.DirectorySeparatorChar.ToString();

        public static byte[] _fileheader = { (byte)'M', (byte)'G', (byte)'H', (byte)'F',
                                              0,   // 4 -- storage file version number,
                                              0,2, // 5,6 -- block size ushort low, hi
                                              1    // 7 -- key type 0 = guid, 1 = string
                                           };

        //private SafeDictionary<string, int> _masterblock = new SafeDictionary<string, int>();

        public StorageFileHF(string filename, ushort blocksize)
        {
            _Path = Path.GetDirectoryName(filename);
            if (_Path.EndsWith(_S) == false) _Path += _S;
            _filename = Path.GetFileNameWithoutExtension(filename);

            Initialize(filename, blocksize);
        }

        public void Shutdown()
        {
            WriteFreeListBMPFile();
            _datawrite.Flush();
            FlushClose(_datawriteorg);
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
                return Interlocked.Increment(ref _lastBlockNumber);
        }

        private void InitializeFreeList()
        {
            if (_lastBlockNumber < 0)
            {
                // write master block
                _datawrite.Write(new byte[_BLOCKSIZE], 0, _BLOCKSIZE);
                _lastBlockNumber = 1;
                //_masterblock.Add("freelist", -1);
            }
            else
            {
                _freeList = new MGRB();
                // read master block data
                var b = ReadBlock(0);
                if (b[0] == (byte)'F' && b[1] == (byte)'L')
                {
                    // get free block num and size
                    int block = Helper.ToInt32(b, 2);
                    int len = Helper.ToInt32(b, 2 + 4);
                    int freeblock = block;
                    b = new byte[len];
                    var offset = 0;
                    bool failed = false;
                    // read blocks upto size from block num
                    SeekBlock(block);
                    while (len > 0)
                    {
                        // check header 
                        var bb = ReadBlock(block++);
                        if (bb[0] != (byte)'F' || bb[1] != (byte)'L')
                        {
                            // throw exception??
                            _log.Error("Free list header does not match : " + _filename);
                            failed = true;
                            break;
                        }
                        int c = len > _BLOCKSIZE ? _BLOCKSIZE - 2 : len;
                        Buffer.BlockCopy(bb, 2, b, offset, c);
                        len -= c;
                        offset += c;
                    }
                    if (failed == false)
                    {
                        // read freelist from master block from end of file
                        var o = fastBinaryJSON.BJSON.ToObject<MGRBData>(b);
                        _freeList.Deserialize(o);
                        // truncate end of file freelist blocks if lastblock < file size
                        if (_datawrite.Length > _lastBlockNumber * _BLOCKSIZE)
                            _datawrite.SetLength(_lastBlockNumber * _BLOCKSIZE);
                    }
                    _lastBlockNumber = freeblock;
                }
            }
        }

        internal void SeekBlock(int blocknumber)
        {
            long offset = _fileheader.Length + (long)blocknumber * _BLOCKSIZE;
            // wiil seek past the end of file on fs.Write will zero the difference
            _datawrite.Seek(offset, SeekOrigin.Begin);
        }

        internal void WriteBlockBytes(byte[] data, int start, int len)
        {
            _datawrite.Write(data, start, len);
        }

        #region [ private / internal  ]

        private void WriteFreeListBMPFile()
        {
            // write freelist to end of blocks and update master block
            if (_freeList != null)
            {
                _freeList.Optimize();
                var o = _freeList.Serialize();
                var b = fastBinaryJSON.BJSON.ToBJSON(o, new fastBinaryJSON.BJSONParameters { UseExtensions = false });

                var len = b.Length;
                var offset = 0;
                // write master block 
                SeekBlock(0);
                _lastBlockNumber++;
                WriteBlockBytes(new byte[] { (byte)'F', (byte)'L' }, 0, 2);
                WriteBlockBytes(Helper.GetBytes(_lastBlockNumber, false), 0, 4);
                WriteBlockBytes(Helper.GetBytes(len, false), 0, 4);
                // seek to end of file
                SeekBlock(_lastBlockNumber);
                while (len > 0)
                {
                    WriteBlockBytes(new byte[] { (byte)'F', (byte)'L' }, 0, 2);
                    WriteBlockBytes(b, offset, len > _BLOCKSIZE ? _BLOCKSIZE - 2 : len);
                    len -= (_BLOCKSIZE - 2);
                }
            }
        }

        private void Initialize(string filename, ushort blocksize)
        {
            if (File.Exists(filename) == false)
                _datawriteorg = new FileStream(filename, FileMode.CreateNew, FileAccess.ReadWrite, FileShare.ReadWrite);
            else
                _datawriteorg = new FileStream(filename, FileMode.Open, FileAccess.ReadWrite, FileShare.ReadWrite);

            _datawrite = new BufferedStream(_datawriteorg);

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
                if (filever < _CurrentVersion)
                {
                    // fixx : upgrade storage file here
                }
                _lastBlockNumber = (int)((_datawrite.Length - _fileheader.Length) / _BLOCKSIZE);
                _lastBlockNumber++;
            }

            InitializeFreeList();
        }

        private int ReadFileHeader()
        {
            // set _blockize
            _datawrite.Seek(0L, SeekOrigin.Begin);
            byte[] hdr = new byte[_fileheader.Length];
            _datawrite.Read(hdr, 0, _fileheader.Length);

            _BLOCKSIZE = 0;
            _BLOCKSIZE = (ushort)(hdr[5] + hdr[6] << 8);

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
            return (int)((_datawrite.Length / _BLOCKSIZE)) + 1;
        }

        internal void FreeBlock(int i)
        {
            _freeList.Set(i, true);
        }
    }
}
