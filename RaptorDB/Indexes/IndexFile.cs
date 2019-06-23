using System;
using System.Collections.Generic;
using System.IO;
using RaptorDB.Common;
using System.Threading;
using fastBinaryJSON;

namespace RaptorDB
{
    internal class IndexFile<T>
    {
        FileStream _file = null;
        private byte[] _FileHeader = new byte[] {
            (byte)'M', (byte)'G', (byte)'I',
            0,               // 3 = [keysize]   max 255
            0,0,             // 4 = [node size] max 65536
            0,0,0,0,         // 6 = [root page num]
            0,               // 10 = Index file type : 0=mgindex 1=mgindex+strings (key = firstallocblock)
            0,0,0,0          // 11 = last record number indexed 
            };

        private byte[] _BlockHeader = new byte[] {
            (byte)'P',(byte)'A',(byte)'G',(byte)'E',
            0,               // 4 = [Flag] = 0=page 1=page list   
            0,0,             // 5 = [item count] 
            0,0,0,0,         // 7 = reserved               
            0,0,0,0          // 11 = [right page number] / [next page number]
        };

        internal byte _maxKeySize;
        internal ushort _PageNodeCount = 5000;
        private int _LastPageNumber = 1; // 0 = page list
        private int _PageLength;
        private int _rowSize;
        private bool _allowDups = true;
        ILog log = LogManager.GetLogger(typeof(IndexFile<T>));
        private BitmapIndex _bitmap;
        IGetBytes<T> _T = null;
        private object _fileLock = new object();

        private StringHF _strings;
        private bool _externalStrings = false;
        //private List<int> _pagelistalllocblock = null;
        private string _FileName = "";

        public IndexFile(string filename, byte maxKeySize)//, ushort pageNodeCount)
        {
            _T = RDBDataType<T>.ByteHandler();
            if (typeof(T) == typeof(string) && Global.EnableOptimizedStringIndex)
            {
                _externalStrings = true;
                _maxKeySize = 4;// blocknum:int
            }
            else
                _maxKeySize = maxKeySize;

            _PageNodeCount = Global.PageItemCount;// pageNodeCount;
            _rowSize = (_maxKeySize + 1 + 4 + 4);

            _FileName = filename.Substring(0, filename.LastIndexOf('.'));
            string path = Path.GetDirectoryName(filename);

            Directory.CreateDirectory(path);
            if (File.Exists(filename))
            {
                // if file exists open and read header
                _file = File.Open(filename, FileMode.Open, FileAccess.ReadWrite, FileShare.ReadWrite);
                ReadFileHeader();
                if (_externalStrings == false)// if the file says different
                {
                    _rowSize = (_maxKeySize + 1 + 4 + 4);
                }
                // compute last page number from file length 
                _PageLength = (_BlockHeader.Length + _rowSize * (_PageNodeCount));
                _LastPageNumber = (int)((_file.Length - _FileHeader.Length) / _PageLength);
            }
            else
            {
                // else create new file
                _file = File.Open(filename, FileMode.Create, FileAccess.ReadWrite, FileShare.ReadWrite);

                _PageLength = (_BlockHeader.Length + _rowSize * (_PageNodeCount));

                CreateFileHeader(0);

                _LastPageNumber = (int)((_file.Length - _FileHeader.Length) / _PageLength);
            }
            if (_externalStrings)
            {
                _strings = new StringHF(path, Path.GetFileNameWithoutExtension(filename) + ".strings");
            }
            if (_LastPageNumber == 0)
                _LastPageNumber = 1;
            // bitmap duplicates 
            if (_allowDups)
                _bitmap = new BitmapIndex(Path.GetDirectoryName(filename), Path.GetFileNameWithoutExtension(filename));
        }

        #region [  C o m m o n  ]
        public void SetBitmapDuplicate(int bitmaprec, int rec)
        {
            _bitmap.SetDuplicate(bitmaprec, rec);
        }

        public int GetBitmapDuplaicateFreeRecordNumber()
        {
            return _bitmap.GetFreeRecordNumber();
        }

        public IEnumerable<int> GetDuplicatesRecordNumbers(int recno)
        {
            return GetDuplicateBitmap(recno).GetBitIndexes();
        }

        public MGRB GetDuplicateBitmap(int recno)
        {
            return _bitmap.GetBitmap(recno);
        }

        private byte[] CreateBlockHeader(byte type, ushort itemcount, int rightpagenumber)
        {
            byte[] block = new byte[_BlockHeader.Length];
            Array.Copy(_BlockHeader, block, block.Length);
            block[4] = type;
            byte[] b = Helper.GetBytes(itemcount, false);
            Buffer.BlockCopy(b, 0, block, 5, 2);
            b = Helper.GetBytes(rightpagenumber, false);
            Buffer.BlockCopy(b, 0, block, 11, 4);
            return block;
        }

        private void CreateFileHeader(int rowsindexed)
        {
            lock (_fileLock)
            {
                // max key size
                byte[] b = Helper.GetBytes(_maxKeySize, false);
                Buffer.BlockCopy(b, 0, _FileHeader, 3, 1);
                // page node count
                b = Helper.GetBytes(_PageNodeCount, false);
                Buffer.BlockCopy(b, 0, _FileHeader, 4, 2);
                b = Helper.GetBytes(rowsindexed, false);
                Buffer.BlockCopy(b, 0, _FileHeader, 11, 4);

                if (_externalStrings)
                    _FileHeader[10] = 1;

                _file.Seek(0L, SeekOrigin.Begin);
                _file.Write(_FileHeader, 0, _FileHeader.Length);
                if (rowsindexed == 0)
                {
                    byte[] pagezero = new byte[_PageLength];
                    byte[] block = CreateBlockHeader(1, 0, -1);
                    Buffer.BlockCopy(block, 0, pagezero, 0, block.Length);
                    _file.Write(pagezero, 0, _PageLength);
                }
                _file.Flush();
            }
        }

        private bool ReadFileHeader()
        {
            _file.Seek(0L, SeekOrigin.Begin);
            byte[] b = new byte[_FileHeader.Length];
            _file.Read(b, 0, _FileHeader.Length);

            if (b[0] == _FileHeader[0] && b[1] == _FileHeader[1] && b[2] == _FileHeader[2]) // header
            {
                byte maxks = b[3];
                ushort nodes = (ushort)Helper.ToInt16(b, 4);
                int root = Helper.ToInt32(b, 6);
                _maxKeySize = maxks;
                _PageNodeCount = nodes;
                _FileHeader = b;
                if (b[10] == 0)
                    _externalStrings = false;
            }

            return false;
        }

        public int GetNewPageNumber()
        {
            return Interlocked.Increment(ref _LastPageNumber); //_LastPageNumber++;
        }

        private void SeekPage(int pnum)
        {
            long offset = _FileHeader.Length;
            offset += (long)pnum * _PageLength;
            if (offset > _file.Length)
                CreateBlankPages(pnum);

            _file.Seek(offset, SeekOrigin.Begin);
        }

        private void CreateBlankPages(int pnum)
        {
            // create space
            byte[] b = new byte[_PageLength];
            _file.Seek(0L, SeekOrigin.Current);
            for (int i = pnum; i < _LastPageNumber; i++)
                _file.Write(b, 0, b.Length);

            _file.Flush();
        }

        public void FreeMemory()
        {
            if (_allowDups)
                _bitmap.FreeMemory();
        }

        public void Shutdown()
        {
            log.Debug("Shutdown IndexFile");
            if (_externalStrings)
                _strings.Shutdown();

            if (_file != null)
            {
                _file.Flush();
                _file.Close();
            }
            _file = null;
            if (_allowDups)
            {
                _bitmap.Commit(Global.FreeBitmapMemoryOnSave);
                _bitmap.Shutdown();
            }
        }

        #endregion

        #region [  P a g e s ]

        public void GetPageList(List<int> PageListDiskPages, SafeSortedList<T, PageInfo> PageList, out int lastIndexedRow)
        {
            lastIndexedRow = Helper.ToInt32(_FileHeader, 11);
            // load page list
            PageListDiskPages.Add(0); // first page list
            int nextpage = LoadPageListData(0, PageList);
            while (nextpage != -1)
            {
                nextpage = LoadPageListData(nextpage, PageList);

                if (nextpage != -1)
                    PageListDiskPages.Add(nextpage);
            }
        }

        private int LoadPageListData(int page, SafeSortedList<T, PageInfo> PageList)
        {
            lock (_fileLock)
            {
                // load page list data
                int nextpage = -1;
                SeekPage(page);
                byte[] b = new byte[_PageLength];
                _file.Read(b, 0, _PageLength);

                if (b[0] == _BlockHeader[0] && b[1] == _BlockHeader[1] && b[2] == _BlockHeader[2] && b[3] == _BlockHeader[3])
                {
                    short count = Helper.ToInt16(b, 5);
                    if (count > _PageNodeCount)
                        throw new Exception("Count > node size");
                    nextpage = Helper.ToInt32(b, 11);
                    int index = _BlockHeader.Length;
                    object[] keys = null;
                    // TODO : needed??
                    //if (File.Exists(_FileName + ".pagelist"))
                    //{
                    //    var bn = File.ReadAllBytes(_FileName + ".pagelist");
                    //    int blknum = Helper.ToInt32(bn, 0);
                    //    byte[] bb = _strings.GetData(blknum, out _pagelistalllocblock);
                    //    keys = (object[])BJSON.ToObject(bb);
                    //}
                    for (int i = 0; i < count; i++)
                    {
                        int idx = index + _rowSize * i;
                        byte ks = b[idx];
                        T key;
                        if (_externalStrings == false)
                            key = _T.GetObject(b, idx + 1, ks);
                        else
                        {
                            if (keys == null)
                                key = _T.GetObject(b, idx + 1, ks); // do old way until better way
                            else
                                key = (T)keys[i];
                        }
                        int pagenum = Helper.ToInt32(b, idx + 1 + _maxKeySize);
                        // add counts
                        int unique = Helper.ToInt32(b, idx + 1 + _maxKeySize + 4);
                        // FEATURE : add dup count
                        PageList.Add(key, new PageInfo(pagenum, unique, 0));
                    }
                }
                else
                    throw new Exception("Page List header is invalid");

                return nextpage;
            }
        }

        internal void SavePage(Page<T> page)
        {
            lock (_fileLock)
            {
                int pnum = page.DiskPageNumber;
                if (pnum > _LastPageNumber)
                    throw new Exception("should not be here: page out of bounds");

                SeekPage(pnum);
                byte[] pagebytes = new byte[_PageLength];
                byte[] blockheader = CreateBlockHeader(0, (ushort)page.tree.Count(), page.RightPageNumber);
                Buffer.BlockCopy(blockheader, 0, pagebytes, 0, blockheader.Length);

                int index = blockheader.Length;
                int i = 0;
                byte[] b = null;
                T[] keys = page.tree.Keys();
                Array.Sort(keys); // sort keys on save for read performance
                int blocknum = 0;
                if (_externalStrings)
                {
                    // free old blocks
                    if (page.allocblocks != null)
                        _strings.FreeBlocks(page.allocblocks);
                    List<int> blocks = new List<int>();
                    blocknum = _strings.SaveData(page.DiskPageNumber.ToString(), BJSON.ToBJSON(keys,
                        new BJSONParameters { UseUnicodeStrings = false, UseTypedArrays = false }), out blocks);
                    page.allocblocks = blocks;
                }
                // node children
                foreach (var kp in keys)
                {
                    var val = page.tree[kp];
                    int idx = index + _rowSize * i;
                    // key bytes
                    byte[] kk;
                    byte size;
                    if (_externalStrings == false)
                    {
                        kk = _T.GetBytes(kp);
                        size = (byte)kk.Length;
                        if (size > _maxKeySize)
                            size = _maxKeySize;
                    }
                    else
                    {
                        kk = new byte[4];
                        Buffer.BlockCopy(Helper.GetBytes(blocknum, false), 0, kk, 0, 4);
                        size = 4;
                    }
                    // key size = 1 byte
                    pagebytes[idx] = size;
                    Buffer.BlockCopy(kk, 0, pagebytes, idx + 1, pagebytes[idx]);
                    // offset = 4 bytes
                    b = Helper.GetBytes(val.RecordNumber, false);
                    Buffer.BlockCopy(b, 0, pagebytes, idx + 1 + _maxKeySize, b.Length);
                    // duplicatepage = 4 bytes
                    b = Helper.GetBytes(val.DuplicateBitmapNumber, false);
                    Buffer.BlockCopy(b, 0, pagebytes, idx + 1 + _maxKeySize + 4, b.Length);
                    i++;
                }
                _file.Write(pagebytes, 0, pagebytes.Length);
            }
        }

        public Page<T> LoadPageFromPageNumber(int number)
        {
            lock (_fileLock)
            {
                SeekPage(number);
                byte[] b = new byte[_PageLength];
                _file.Read(b, 0, _PageLength);

                if (b[0] == _BlockHeader[0] && b[1] == _BlockHeader[1] && b[2] == _BlockHeader[2] && b[3] == _BlockHeader[3])
                {
                    // create node here
                    Page<T> page = new Page<T>();

                    short count = Helper.ToInt16(b, 5);
                    if (count > _PageNodeCount)
                        throw new Exception("Count > node size");
                    page.DiskPageNumber = number;
                    page.RightPageNumber = Helper.ToInt32(b, 11);
                    int index = _BlockHeader.Length;
                    object[] keys = null;

                    for (int i = 0; i < count; i++)
                    {
                        int idx = index + _rowSize * i;
                        byte ks = b[idx];
                        T key = default(T);
                        if (_externalStrings == false)
                            key = _T.GetObject(b, idx + 1, ks);
                        else
                        {
                            if (keys == null)
                            {
                                int blknum = Helper.ToInt32(b, idx + 1, false);
                                List<int> ablocks = new List<int>();
                                byte[] bb = _strings.GetData(blknum, out ablocks);
                                page.allocblocks = ablocks;
                                keys = (object[])BJSON.ToObject(bb);
                            }
                            key = (T)keys[i];
                        }
                        int offset = Helper.ToInt32(b, idx + 1 + _maxKeySize);
                        int duppage = Helper.ToInt32(b, idx + 1 + _maxKeySize + 4);
                        page.tree.Add(key, new KeyInfo(offset, duppage));
                    }
                    return page;
                }
                else
                    throw new Exception("Page read error header invalid, number = " + number);
            }
        }
        #endregion

        internal void SavePageList(SafeSortedList<T, PageInfo> _pages, List<int> diskpages)
        {
            lock (_fileLock)
            {
                T[] keys = _pages.Keys();
                int blocknum = 0;
                // TODO : needed??
                //if (_externalStrings)
                //{
                //    if (_pagelistalllocblock != null)
                //        _strings.FreeBlocks(_pagelistalllocblock);
                //    blocknum = _strings.SaveData("pagelist", BJSON.ToBJSON(keys,
                //        new BJSONParameters { UseUnicodeStrings = false, UseTypedArrays = false }));
                //    File.WriteAllBytes(_FileName + ".pagelist", Helper.GetBytes(blocknum, false));
                //}
                // save page list
                int c = (_pages.Count() / Global.PageItemCount) + 1;
                // allocate pages needed 
                while (c > diskpages.Count)
                    diskpages.Add(GetNewPageNumber());

                byte[] page = new byte[_PageLength];

                for (int i = 0; i < (diskpages.Count - 1); i++)
                {
                    byte[] block = CreateBlockHeader(1, Global.PageItemCount, diskpages[i + 1]);
                    Buffer.BlockCopy(block, 0, page, 0, block.Length);

                    for (int j = 0; j < Global.PageItemCount; j++)
                        CreatePageListData(_pages, i * Global.PageItemCount, block.Length, j, page, blocknum);

                    SeekPage(diskpages[i]);
                    _file.Write(page, 0, page.Length);
                }

                c = _pages.Count() % Global.PageItemCount;
                byte[] lastblock = CreateBlockHeader(1, (ushort)c, -1);
                Buffer.BlockCopy(lastblock, 0, page, 0, lastblock.Length);
                int lastoffset = (_pages.Count() / Global.PageItemCount) * Global.PageItemCount;

                for (int j = 0; j < c; j++)
                    CreatePageListData(_pages, lastoffset, lastblock.Length, j, page, blocknum);

                SeekPage(diskpages[diskpages.Count - 1]);
                _file.Write(page, 0, page.Length);
            }
        }

        private void CreatePageListData(SafeSortedList<T, PageInfo> _pages, int offset, int rowindex, int counter, byte[] page, int blocknum)
        {
            int idx = rowindex + _rowSize * counter;
            // key bytes
            byte[] kk;
            byte size;
            if (_externalStrings == false)
            {
                kk = _T.GetBytes(_pages.GetKey(counter + offset));
                size = (byte)kk.Length;
                if (size > _maxKeySize)
                    size = _maxKeySize;
            }
            else
            {
                kk = new byte[4];
                Buffer.BlockCopy(Helper.GetBytes(counter + offset, false), 0, kk, 0, 4);
                size = 4;
            }
            // key size = 1 byte
            page[idx] = size;
            Buffer.BlockCopy(kk, 0, page, idx + 1, page[idx]);
            // offset = 4 bytes
            byte[] b = Helper.GetBytes(_pages.GetValue(offset + counter).PageNumber, false);
            Buffer.BlockCopy(b, 0, page, idx + 1 + _maxKeySize, b.Length);
            // add counts 
            b = Helper.GetBytes(_pages.GetValue(offset + counter).UniqueCount, false);
            Buffer.BlockCopy(b, 0, page, idx + 1 + _maxKeySize + 4, b.Length);
            // FEATURE : add dup counts
        }

        internal void SaveLastRecordNumber(int recnum)
        {
            // save the last record number indexed to the header
            CreateFileHeader(recnum);
        }

        internal void BitmapFlush()
        {
            if (_allowDups)
                _bitmap.Commit(Global.FreeBitmapMemoryOnSave);
        }
    }
}