using RaptorDB.Common;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;

namespace RaptorDB
{
    // high frequency string value store
    public class StringHF //: IKeyStoreHF
    {
        internal class AllocationBlock
        {
            public string key;
            public byte keylen;
            public int datalength;
            public bool isCompressed;
            public bool isBinaryJSON;
            public bool deleteKey;
            public List<int> Blocks = new List<int>();
            public int blocknumber;
        }

        //MGIndex<string> _keys;
        StorageFileHF _datastore;
        object _lock = new object();
        ushort _BlockSize = 2048;
        //private const int _KILOBYTE = 1024;
        ILog _log = LogManager.GetLogger(typeof(KeyStoreHF));

        byte[] _blockheader = new byte[]{
            0,0,0,0,    // 0  block # (used for validate block reads and rebuild)
            0,0,0,0,    // 4  next block # 
            0,          // 8  flags bits 0:iscompressed  1:isbinary  2:deletekey
            0,0,0,0,    // 9  data length (compute alloc blocks needed)
            0,          // 13 key length 
            0,          // 14 key type 0=guid 1=string
        };
        private string _Path = "";
        private string _S = Path.DirectorySeparatorChar.ToString();
        //private bool _isDirty = false;
        //private string _dirtyFilename = "temp.$";

        // mgindex special storage for strings ctor -> no idx file
        //    use SaveData() GetData()
        public StringHF(string folder, string filename)
        {
            _Path = folder;
            Directory.CreateDirectory(_Path);
            if (_Path.EndsWith(_S) == false) _Path += _S;

            _datastore = new StorageFileHF(_Path + filename, Global.HighFrequencyKVDiskBlockSize);
            //_datastore.Initialize();
            _BlockSize = _datastore.GetBlockSize();
        }

        public void Shutdown()
        {
            _datastore.Shutdown();
            //if (_keys != null)
            //    _keys.Shutdown();

            //if (File.Exists(_Path + _dirtyFilename))
            //    File.Delete(_Path + _dirtyFilename);
        }

        internal void FreeMemory()
        {
            //if (_keys != null)
            //    _keys.FreeMemory();
        }

        #region [  private methods  ]
        private byte[] readblockdata(AllocationBlock ab)
        {
            byte[] data = new byte[ab.datalength];
            long offset = 0;
            int len = ab.datalength;
            int dbsize = _BlockSize - _blockheader.Length - ab.keylen;
            ab.Blocks.ForEach(x =>
            {
                byte[] b = _datastore.ReadBlock(x);
                int c = len;
                if (c > dbsize) c = dbsize;
                Buffer.BlockCopy(b, _blockheader.Length + ab.keylen, data, (int)offset, c);
                offset += c;
                len -= c;
            });
            if (ab.isCompressed)
                data = MiniLZO.Decompress(data);
            return data;
        }

        private int internalSave(byte[] keybytes, byte[] data, AllocationBlock ab)
        {
            ab.Blocks = new List<int>();
            int firstblock = _datastore.GetFreeBlockNumber();
            ab.Blocks.Add(firstblock);
            int blocknum = firstblock;
            byte[] header = CreateAllocHeader(ab, keybytes);
            int dblocksize = _BlockSize - header.Length;
            int offset = 0;
            // compute data block count
            int datablockcount = (data.Length / dblocksize) + 1;
            // save data blocks
            int counter = 0;
            int len = data.Length;
            while (datablockcount > 0)
            {
                datablockcount--;
                int next = 0;
                if (datablockcount > 0)
                    next = _datastore.GetFreeBlockNumber();
                Buffer.BlockCopy(Helper.GetBytes(counter, false), 0, header, 0, 4);    // set block number
                Buffer.BlockCopy(Helper.GetBytes(next, false), 0, header, 4, 4); // set next pointer

                _datastore.SeekBlock(blocknum);
                _datastore.WriteBlockBytes(header, 0, header.Length);
                int c = len;
                if (c > dblocksize)
                    c = dblocksize;
                _datastore.WriteBlockBytes(data, offset, c);

                if (next > 0)
                {
                    blocknum = next;
                    ab.Blocks.Add(next);
                }
                offset += c;
                len -= c;
                counter++;
            }
            return firstblock;
        }

        private byte[] CreateAllocHeader(AllocationBlock ab, byte[] keybytes)
        {
            byte[] alloc = new byte[_blockheader.Length + keybytes.Length];

            if (ab.isCompressed)
                alloc[8] = 1;
            if (ab.isBinaryJSON)
                alloc[8] += 2;
            if (ab.deleteKey)
                alloc[8] += 4;

            Buffer.BlockCopy(Helper.GetBytes(ab.datalength, false), 0, alloc, 9, 4);
            alloc[13] = ab.keylen;
            alloc[14] = 1; // string keys for now
            Buffer.BlockCopy(keybytes, 0, alloc, _blockheader.Length, ab.keylen);

            return alloc;
        }

        private AllocationBlock FillAllocationBlock(int blocknumber)
        {
            AllocationBlock ab = new AllocationBlock();

            ab.blocknumber = blocknumber;
            ab.Blocks.Add(blocknumber);

            byte[] b = _datastore.ReadBlockBytes(blocknumber, _blockheader.Length + 255);

            int blocknumexpected = 0;

            int next = ParseBlockHeader(ab, b, blocknumexpected);

            blocknumexpected++;

            while (next > 0)
            {
                ab.Blocks.Add(next);
                b = _datastore.ReadBlockBytes(next, _blockheader.Length + ab.keylen);
                next = ParseBlockHeader(ab, b, blocknumexpected);
                blocknumexpected++;
            }

            return ab;
        }

        private int ParseBlockHeader(AllocationBlock ab, byte[] b, int blocknumberexpected)
        {
            int bnum = Helper.ToInt32(b, 0);
            if (bnum != blocknumberexpected)
            {
                _log.Error("Block numbers does not match, looking for : " + blocknumberexpected);
                //throw new Exception("Block numbers does not match, looking for : " + blocknumberexpected);
                return -1;
            }
            if (b[14] != 1)
            {
                _log.Error("Expecting string keys only, got : " + b[14]);
                //throw new Exception("Expecting string keys only, got : " + b[11]);
                return -1;
            }

            int next = Helper.ToInt32(b, 4);

            if (ab.keylen == 0)
            {
                byte flags = b[8];

                if ((flags & 0x01) > 0)
                    ab.isCompressed = true;
                if ((flags & 0x02) > 0)
                    ab.isBinaryJSON = true;
                if ((flags & 0x04) > 0)
                    ab.deleteKey = true;

                ab.datalength = Helper.ToInt32(b, 9);
                byte keylen = b[13];
                ab.keylen = keylen;
                ab.key = Helper.GetString(b, _blockheader.Length, keylen);
            }
            return next;
        }

        #endregion

        internal void FreeBlocks(List<int> list)
        {
            lock (_lock)
                _datastore.FreeBlocks(list);
        }


        // for .string files
        internal int SaveData(string key, byte[] data, out List<int> blocks)
        {
            lock (_lock)
            {
                byte[] kb = Helper.GetBytes(key);
                AllocationBlock ab = new AllocationBlock();
                ab.key = key;
                ab.keylen = (byte)kb.Length;
                ab.isCompressed = false;
                ab.isBinaryJSON = true;
                ab.datalength = data.Length;

                int firstblock = internalSave(kb, data, ab);
                blocks = ab.Blocks;
                return firstblock;
            }
        }

        // for .string files
        internal byte[] GetData(int blocknumber, out List<int> usedblocks)
        {
            lock (_lock)
            {
                AllocationBlock ab = FillAllocationBlock(blocknumber);
                usedblocks = ab.Blocks;
                byte[] data = readblockdata(ab);

                return data;
            }
        }
    }
}
