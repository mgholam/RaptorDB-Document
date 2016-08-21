using System;
using System.Collections.Generic;
using System.IO;
using RaptorDB.Common;

namespace RaptorDB
{
    #region [ internal classes ]

    internal struct PageInfo  // FEATURE : change back to class for count access for query caching
    {
        public PageInfo(int pagenum, int uniquecount, int duplicatecount)
        {
            PageNumber = pagenum;
            UniqueCount = uniquecount;
        }
        public int PageNumber;
        public int UniqueCount;
    }

    internal struct KeyInfo
    {
        public KeyInfo(int recnum)
        {
            RecordNumber = recnum;
            DuplicateBitmapNumber = -1;
        }
        public KeyInfo(int recnum, int bitmaprec)
        {
            RecordNumber = recnum;
            DuplicateBitmapNumber = bitmaprec;
        }
        public int RecordNumber;
        public int DuplicateBitmapNumber;
    }

    internal class Page<T>
    {
        public Page() // kludge so the compiler doesn't complain
        {
            DiskPageNumber = -1;
            RightPageNumber = -1;
            tree = new SafeDictionary<T, KeyInfo>(Global.PageItemCount);
            isDirty = false;
            FirstKey = default(T);
        }
        public int DiskPageNumber;
        public int RightPageNumber;
        public T FirstKey;
        public bool isDirty;
        public SafeDictionary<T, KeyInfo> tree;
        public List<int> allocblocks = null; // for string keys in HF key store
    }

    #endregion

    internal class MGIndex<T> where T : IComparable<T>
    {
        ILog _log = LogManager.GetLogger(typeof(MGIndex<T>));
        private SafeSortedList<T, PageInfo> _pageList = new SafeSortedList<T, PageInfo>();
        //private SafeDictionary<int, Page<T>> _cache = new SafeDictionary<int, Page<T>>();
        private SafeSortedList<int, Page<T>> _cache = new SafeSortedList<int, Page<T>>();
        private List<int> _pageListDiskPages = new List<int>();
        private IndexFile<T> _index;
        private bool _AllowDuplicates = true;
        private int _LastIndexedRecordNumber = 0;
        //private int _maxPageItems = 0;

        public MGIndex(string path, string filename, byte keysize, /*ushort maxcount,*/ bool allowdups)
        {
            _AllowDuplicates = allowdups;
            _index = new IndexFile<T>(path + Path.DirectorySeparatorChar + filename, keysize);//, maxcount);
            //_maxPageItems = maxcount;
            // load page list
            _index.GetPageList(_pageListDiskPages, _pageList, out _LastIndexedRecordNumber);
            if (_pageList.Count == 0)
            {
                Page<T> page = new Page<T>();
                page.FirstKey = (T)RDBDataType<T>.GetEmpty();
                page.DiskPageNumber = _index.GetNewPageNumber();
                page.isDirty = true;
                _pageList.Add(page.FirstKey, new PageInfo(page.DiskPageNumber, 0, 0));
                _cache.Add(page.DiskPageNumber, page);
            }
        }

        public int GetLastIndexedRecordNumber()
        {
            return _LastIndexedRecordNumber;
        }

        public WAHBitArray Query(T from, T to, int maxsize)
        {
            WAHBitArray bits = new WAHBitArray();
            T temp = default(T);
            if (from.CompareTo(to) > 0) // check values order
            {
                temp = from;
                from = to;
                to = temp;
            }
            // find first page and do > than
            bool found = false;
            int startpos = FindPageOrLowerPosition(from, ref found);
            // find last page and do < than
            int endpos = FindPageOrLowerPosition(to, ref found);
            bool samepage = startpos == endpos;

            // from key page
            Page<T> page = LoadPage(_pageList.GetValue(startpos).PageNumber);
            T[] keys = page.tree.Keys();
            Array.Sort(keys);

            // find better start position rather than 0
            int pos = Array.BinarySearch<T>(keys, from); // FEATURE : rewrite??
            if (pos < 0) pos = ~pos;

            for (int i = pos; i < keys.Length; i++)
            {
                T k = keys[i];
                int bn = page.tree[k].DuplicateBitmapNumber;

                if (samepage)
                {
                    if (k.CompareTo(from) >= 0 && k.CompareTo(to) <= 0) // if from,to same page
                        bits = bits.Or(_index.GetDuplicateBitmap(bn));
                }
                else
                {
                    if (k.CompareTo(from) >= 0)
                        bits = bits.Or(_index.GetDuplicateBitmap(bn));
                }
            }
            if (!samepage)
            {
                // to key page
                page = LoadPage(_pageList.GetValue(endpos).PageNumber);
                keys = page.tree.Keys();
                Array.Sort(keys);
                // find better end position rather than last key
                pos = Array.BinarySearch<T>(keys, to);
                if (pos < 0) pos = ~pos;

                for (int i = 0; i <= pos; i++)
                {
                    T k = keys[i];
                    int bn = page.tree[k].DuplicateBitmapNumber;

                    if (k.CompareTo(to) <= 0)
                        bits = bits.Or(_index.GetDuplicateBitmap(bn));
                }
                // do all pages in between
                for (int i = startpos + 1; i < endpos; i++)
                {
                    doPageOperation(ref bits, i);
                }
            }
            return bits;
        }

        public WAHBitArray Query(RDBExpression exp, T from, int maxsize)
        {
            T key = from;
            if (exp == RDBExpression.Equal || exp == RDBExpression.NotEqual)
                return doEqualOp(exp, key, maxsize);

            // FEATURE : optimize complement search if page count less for the complement pages

            if (exp == RDBExpression.Less || exp == RDBExpression.LessEqual)
            {
                return doLessOp(exp, key);
            }
            else if (exp == RDBExpression.Greater || exp == RDBExpression.GreaterEqual)
            {
                return doMoreOp(exp, key);
            }

            return new WAHBitArray(); // blank results 
        }

        private object _setlock = new object();
        public void Set(T key, int val)
        {
            lock (_setlock)
            {
                PageInfo pi;
                Page<T> page = LoadPage(key, out pi);

                KeyInfo ki;
                if (page.tree.TryGetValue(key, out ki))
                {
                    // item exists
                    if (_AllowDuplicates)
                    {
                        SaveDuplicate(key, ref ki);
                        // set current record in the bitmap also
                        _index.SetBitmapDuplicate(ki.DuplicateBitmapNumber, val);
                    }
                    ki.RecordNumber = val;
                    page.tree[key] = ki; // structs need resetting
                }
                else
                {
                    // new item 
                    ki = new KeyInfo(val);
                    if (_AllowDuplicates)
                        SaveDuplicate(key, ref ki);
                    pi.UniqueCount++;
                    page.tree.Add(key, ki);
                }

                if (page.tree.Count > Global.PageItemCount)
                    SplitPage(page);

                _LastIndexedRecordNumber = val;
                page.isDirty = true;
            }
        }

        public bool Get(T key, out int val)
        {
            val = -1;
            PageInfo pi;
            Page<T> page = LoadPage(key, out pi);
            KeyInfo ki;
            bool ret = page.tree.TryGetValue(key, out ki);
            if (ret)
                val = ki.RecordNumber;
            return ret;
        }

        public void SaveIndex()
        {
            //_log.Debug("Total split time (s) = " + _totalsplits);
            //_log.Debug("Total pages = " + _pageList.Count);
            int[] keys = _cache.Keys();
            Array.Sort(keys);
            // save index to disk
            foreach (var i in keys)
            {
                var p = _cache[i];
                if (p.isDirty)
                {
                    _index.SavePage(p);
                    p.isDirty = false;
                }
            }
            _index.SavePageList(_pageList, _pageListDiskPages);
            _index.BitmapFlush();
        }

        public void Shutdown()
        {
            SaveIndex();
            // save page list
            //_index.SavePageList(_pageList, _pageListDiskPages);
            // shutdown
            _index.Shutdown();
        }

        public void FreeMemory()
        {
            _index.FreeMemory();
            try
            {
                List<int> free = new List<int>();
                foreach (var k in _cache.Keys())
                {
                    var val = _cache[k];
                    if (val.isDirty == false)
                        free.Add(k);
                }
                _log.Info("releasing page count = " + free.Count + " out of " + _cache.Count);
                foreach (var i in free)
                    _cache.Remove(i);
            }
            catch { }
        }


        public IEnumerable<int> GetDuplicates(T key)
        {
            PageInfo pi;
            Page<T> page = LoadPage(key, out pi);
            KeyInfo ki;
            bool ret = page.tree.TryGetValue(key, out ki);
            if (ret)
                // get duplicates
                if (ki.DuplicateBitmapNumber != -1)
                    return _index.GetDuplicatesRecordNumbers(ki.DuplicateBitmapNumber);

            return new List<int>();
        }

        public void SaveLastRecordNumber(int recnum)
        {
            _index.SaveLastRecordNumber(recnum);
        }

        public bool RemoveKey(T key)
        {
            PageInfo pi;
            Page<T> page = LoadPage(key, out pi);
            bool b = page.tree.Remove(key);
            // TODO : reset the first key for page ??
            if (b)
            {
                pi.UniqueCount--;
                // FEATURE : decrease dup count
            }
            page.isDirty = true;
            return b;
        }

        #region [  P R I V A T E  ]
        private WAHBitArray doMoreOp(RDBExpression exp, T key)
        {
            bool found = false;
            int pos = FindPageOrLowerPosition(key, ref found);
            WAHBitArray result = new WAHBitArray();
            if (pos < _pageList.Count)
            {
                // all the pages after
                for (int i = pos + 1; i < _pageList.Count; i++)
                    doPageOperation(ref result, i);
            }
            // key page
            Page<T> page = LoadPage(_pageList.GetValue(pos).PageNumber);
            T[] keys = page.tree.Keys();
            Array.Sort(keys);

            // find better start position rather than 0
            pos = Array.BinarySearch<T>(keys, key);
            if (pos < 0) pos = ~pos;

            for (int i = pos; i < keys.Length; i++)
            {
                T k = keys[i];
                int bn = page.tree[k].DuplicateBitmapNumber;

                if (k.CompareTo(key) > 0)
                    result = result.Or(_index.GetDuplicateBitmap(bn));

                if (exp == RDBExpression.GreaterEqual && k.CompareTo(key) == 0)
                    result = result.Or(_index.GetDuplicateBitmap(bn));
            }
            return result;
        }

        private WAHBitArray doLessOp(RDBExpression exp, T key)
        {
            bool found = false;
            int pos = FindPageOrLowerPosition(key, ref found);
            WAHBitArray result = new WAHBitArray();
            if (pos > 0)
            {
                // all the pages before
                for (int i = 0; i < pos - 1; i++)
                    doPageOperation(ref result, i);
            }
            // key page
            Page<T> page = LoadPage(_pageList.GetValue(pos).PageNumber);
            T[] keys = page.tree.Keys();
            Array.Sort(keys);
            // find better end position rather than last key
            pos = Array.BinarySearch<T>(keys, key);
            if (pos < 0) pos = ~pos;
            for (int i = 0; i <= pos; i++)
            {
                T k = keys[i];
                if (k.CompareTo(key) > 0)
                    break;
                int bn = page.tree[k].DuplicateBitmapNumber;

                if (k.CompareTo(key) < 0)
                    result = result.Or(_index.GetDuplicateBitmap(bn));

                if (exp == RDBExpression.LessEqual && k.CompareTo(key) == 0)
                    result = result.Or(_index.GetDuplicateBitmap(bn));
            }
            return result;
        }

        private WAHBitArray doEqualOp(RDBExpression exp, T key, int maxsize)
        {
            PageInfo pi;
            Page<T> page = LoadPage(key, out pi);
            KeyInfo k;
            if (page.tree.TryGetValue(key, out k))
            {
                int bn = k.DuplicateBitmapNumber;

                if (exp == RDBExpression.Equal)
                    return _index.GetDuplicateBitmap(bn);
                else
                    return _index.GetDuplicateBitmap(bn).Not(maxsize);
            }
            else
            {
                if (exp == RDBExpression.NotEqual)
                    return new WAHBitArray().Not(maxsize);
                else
                    return new WAHBitArray();
            }
        }

        private void doPageOperation(ref WAHBitArray res, int pageidx)
        {
            Page<T> page = LoadPage(_pageList.GetValue(pageidx).PageNumber);
            T[] keys = page.tree.Keys(); // avoid sync issues
            foreach (var k in keys)
            {
                int bn = page.tree[k].DuplicateBitmapNumber;

                res = res.Or(_index.GetDuplicateBitmap(bn));
            }
        }

        private double _totalsplits = 0;
        private void SplitPage(Page<T> page)
        {
            // split the page
            DateTime dt = FastDateTime.Now;

            Page<T> newpage = new Page<T>();
            newpage.DiskPageNumber = _index.GetNewPageNumber();
            newpage.RightPageNumber = page.RightPageNumber;
            newpage.isDirty = true;
            page.RightPageNumber = newpage.DiskPageNumber;
            // get and sort keys
            T[] keys = page.tree.Keys();
            Array.Sort<T>(keys);
            // copy data to new 
            for (int i = keys.Length / 2; i < keys.Length; i++)
            {
                newpage.tree.Add(keys[i], page.tree[keys[i]]);
                // remove from old page
                page.tree.Remove(keys[i]);
            }
            // set the first key
            newpage.FirstKey = keys[keys.Length / 2];
            // set the first key refs
            _pageList.Remove(page.FirstKey);
            _pageList.Remove(keys[0]);
            // dup counts
            _pageList.Add(keys[0], new PageInfo(page.DiskPageNumber, page.tree.Count, 0));
            page.FirstKey = keys[0];
            // FEATURE : dup counts
            _pageList.Add(newpage.FirstKey, new PageInfo(newpage.DiskPageNumber, newpage.tree.Count, 0));
            _cache.Add(newpage.DiskPageNumber, newpage);

            _totalsplits += FastDateTime.Now.Subtract(dt).TotalSeconds;
        }

        private Page<T> LoadPage(T key, out PageInfo pageinfo)
        {
            int pagenum = -1;
            // find page in list of pages

            bool found = false;
            int pos = 0;
            if (key != null)
                pos = FindPageOrLowerPosition(key, ref found);
            pageinfo = _pageList.GetValue(pos);
            pagenum = pageinfo.PageNumber;

            Page<T> page;
            if (_cache.TryGetValue(pagenum, out page) == false)
            {
                //load page from disk
                page = _index.LoadPageFromPageNumber(pagenum);
                _cache.Add(pagenum, page);
            }
            return page;
        }

        private Page<T> LoadPage(int pagenum)
        {
            Page<T> page;
            if (_cache.TryGetValue(pagenum, out page) == false)
            {
                //load page from disk
                page = _index.LoadPageFromPageNumber(pagenum);
                _cache.Add(pagenum, page);
            }
            return page;
        }

        private void SaveDuplicate(T key, ref KeyInfo ki)
        {
            if (ki.DuplicateBitmapNumber == -1)
                ki.DuplicateBitmapNumber = _index.GetBitmapDuplaicateFreeRecordNumber();

            _index.SetBitmapDuplicate(ki.DuplicateBitmapNumber, ki.RecordNumber);
        }

        private int FindPageOrLowerPosition(T key, ref bool found)
        {
            if (_pageList.Count == 0)
                return 0;
            // binary search
            int lastlower = 0;
            int first = 0;
            int last = _pageList.Count - 1;
            int mid = 0;
            while (first <= last)
            {
                mid = (first + last) >> 1;
                T k = _pageList.GetKey(mid);
                int compare = k.CompareTo(key);
                if (compare < 0)
                {
                    lastlower = mid;
                    first = mid + 1;
                }
                if (compare == 0)
                {
                    found = true;
                    return mid;
                }
                if (compare > 0)
                {
                    last = mid - 1;
                }
            }

            return lastlower;
        }
        #endregion

        internal object[] GetKeys()
        {
            List<object> keys = new List<object>();
            for (int i = 0; i < _pageList.Count; i++)
            {
                Page<T> page = LoadPage(_pageList.GetValue(i).PageNumber);
                foreach (var k in page.tree.Keys())
                    keys.Add(k);
            }
            return keys.ToArray();
        }

        internal int Count()
        {
            int count = 0;
            for (int i = 0; i < _pageList.Count; i++)
            {
                Page<T> page = LoadPage(_pageList.GetValue(i).PageNumber);
                //foreach (var k in page.tree.Keys())
                //    count++;
                count += page.tree.Count;
            }
            return count;
        }
    }
}
