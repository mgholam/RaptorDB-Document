using System;
using System.Collections.Generic;
using System.Text;
using System.Collections;
using System.IO;
using System.Threading;
using System.Text.RegularExpressions;
using RaptorDB.Common;

namespace RaptorDB
{
    internal class Hoot
    {
        public Hoot(string IndexPath, string FileName, bool DocMode)
        {
            _Path = IndexPath;
            _FileName = FileName;
            _docMode = DocMode;
            if (_Path.EndsWith(Path.DirectorySeparatorChar.ToString()) == false) _Path += Path.DirectorySeparatorChar;
            Directory.CreateDirectory(IndexPath);

            _log.Debug("Starting hOOt....");
            _log.Debug("Storage Folder = " + _Path);

            if (DocMode)
            {
                _docs = new KeyStoreString(_Path + "files.docs", false);
                // read deleted
                _deleted = new BoolIndex(_Path, "_deleted" , ".hoot");
                _lastDocNum = (int)_docs.Count();
            }
            _bitmaps = new BitmapIndex(_Path, _FileName + "_hoot.bmp");
            // read words
            LoadWords();
        }

        private SafeDictionary<string, int> _words = new SafeDictionary<string, int>();
        private BitmapIndex _bitmaps;
        private BoolIndex _deleted;
        private ILog _log = LogManager.GetLogger(typeof(Hoot));
        private int _lastDocNum = 0;
        private string _FileName = "words";
        private string _Path = "";
        private KeyStoreString _docs;
        private bool _docMode = false;

        public int WordCount
        {
            get { return _words.Count; }
        }

        public int DocumentCount
        {
            get { return _lastDocNum - (int)_deleted.GetBits().CountOnes(); }
        }

        public void Save()
        {
            lock (_lock)
                InternalSave();
        }

        public void Index(int recordnumber, string text)
        {
            AddtoIndex(recordnumber, text);
        }

        public WAHBitArray Query(string filter, int maxsize)
        {
            return ExecutionPlan(filter, maxsize);
        }

        public int Index(Document doc, bool deleteold)
        {
            _log.Debug("indexing doc : " + doc.FileName);
            DateTime dt = FastDateTime.Now;

            if (deleteold && doc.DocNumber > -1)
                _deleted.Set(true, doc.DocNumber);

            if (deleteold == true || doc.DocNumber == -1)
                doc.DocNumber = _lastDocNum++;

            // save doc to disk
            string dstr = fastJSON.JSON.ToJSON(doc, new fastJSON.JSONParameters { UseExtensions = false });
            _docs.Set(doc.FileName.ToLower(), Encoding.Unicode.GetBytes(dstr));

            _log.Debug("writing doc to disk (ms) = " + FastDateTime.Now.Subtract(dt).TotalMilliseconds);

            dt = FastDateTime.Now;
            // index doc
            AddtoIndex(doc.DocNumber, doc.Text);
            _log.Debug("indexing time (ms) = " + FastDateTime.Now.Subtract(dt).TotalMilliseconds);

            return _lastDocNum;
        }

        public IEnumerable<int> FindRows(string filter)
        {
            WAHBitArray bits = ExecutionPlan(filter, _docs.RecordCount());
            // enumerate records
            return bits.GetBitIndexes();
        }

        public IEnumerable<Document> FindDocuments(string filter)
        {
            WAHBitArray bits = ExecutionPlan(filter, _docs.RecordCount());
            // enumerate documents
            foreach (int i in bits.GetBitIndexes())
            {
                if (i > _lastDocNum - 1)
                    break;
                string b = _docs.ReadData(i);
                Document d = fastJSON.JSON.ToObject<Document>(b);

                yield return d;
            }
        }

        public IEnumerable<string> FindDocumentFileNames(string filter)
        {
            WAHBitArray bits = ExecutionPlan(filter, _docs.RecordCount());
            // enumerate documents
            foreach (int i in bits.GetBitIndexes())
            {
                if (i > _lastDocNum - 1)
                    break;
                string b = _docs.ReadData(i);
                var d = (Dictionary<string, object>)fastJSON.JSON.Parse(b);

                yield return d["FileName"].ToString();
            }
        }

        public void RemoveDocument(int number)
        {
            // add number to deleted bitmap
            _deleted.Set(true, number);
        }

        public bool RemoveDocument(string filename)
        {
            // remove doc based on filename
            byte[] b;
            if (_docs.Get(filename.ToLower(), out b))
            {
                Document d = fastJSON.JSON.ToObject<Document>(Encoding.Unicode.GetString(b));
                RemoveDocument(d.DocNumber);
                return true;
            }
            return false;
        }

        public bool IsIndexed(string filename)
        {
            byte[] b;
            return _docs.Get(filename.ToLower(), out b);
        }

        public void OptimizeIndex()
        {
            _bitmaps.Commit(false);
            _bitmaps.Optimize();
        }

        #region [  P R I V A T E   M E T H O D S  ]

        private WAHBitArray ExecutionPlan(string filter, int maxsize)
        {
            //_log.Debug("query : " + filter);
            DateTime dt = FastDateTime.Now;
            // query indexes
            string[] words = filter.Split(' ');
            //bool defaulttoand = true;
            //if (filter.IndexOfAny(new char[] { '+', '-' }, 0) > 0)
            //    defaulttoand = false;

            WAHBitArray found = null;// WAHBitArray.Fill(maxsize);            

            foreach (string s in words)
            {
                int c;
                bool not = false;
                string word = s;
                if (s == "") continue;

                OPERATION op = OPERATION.AND;
                //if (defaulttoand)
                //    op = OPERATION.AND;

                if (word.StartsWith("+"))
                {
                    op = OPERATION.OR;
                    word = s.Replace("+", "");
                }

                if (word.StartsWith("-"))
                {
                    op = OPERATION.ANDNOT;
                    word = s.Replace("-", "");
                    not = true;
                    if (found == null) // leading with - -> "-oak hill"
                    {
                        found = WAHBitArray.Fill(maxsize);
                    }
                }

                if (word.Contains("*") || word.Contains("?"))
                {
                    WAHBitArray wildbits = new WAHBitArray();

                    // do wildcard search
                    Regex reg = new Regex("^" + word.Replace("*", ".*").Replace("?", ".") + "$", RegexOptions.IgnoreCase);
                    foreach (string key in _words.Keys())
                    {
                        if (reg.IsMatch(key))
                        {
                            _words.TryGetValue(key, out c);
                            WAHBitArray ba = _bitmaps.GetBitmap(c);

                            wildbits = DoBitOperation(wildbits, ba, OPERATION.OR, maxsize);
                        }
                    }

                    if (found == null)
                        found = wildbits;
                    else
                    {
                        if (not) // "-oak -*l"
                            found = found.AndNot(wildbits);
                        else if (op == OPERATION.AND)
                            found = found.And(wildbits);
                        else
                            found = found.Or(wildbits);
                    }
                }
                else if (_words.TryGetValue(word.ToLowerInvariant(), out c))
                {
                    // bits logic
                    WAHBitArray ba = _bitmaps.GetBitmap(c);
                    found = DoBitOperation(found, ba, op, maxsize);
                }
                else if (op == OPERATION.AND)
                    found = null;
            }
            if (found == null)
                return new WAHBitArray();

            // remove deleted docs
            WAHBitArray ret;
            if (_docMode)
                ret = found.AndNot(_deleted.GetBits());
            else
                ret = found;
            //_log.Debug("query time (ms) = " + FastDateTime.Now.Subtract(dt).TotalMilliseconds);
            return ret;
        }

        private static WAHBitArray DoBitOperation(WAHBitArray bits, WAHBitArray c, OPERATION op, int maxsize)
        {
            if (bits != null)
            {
                switch (op)
                {
                    case OPERATION.AND:
                        bits = bits.And(c);
                        break;
                    case OPERATION.OR:
                        bits = bits.Or(c);
                        break;
                    case OPERATION.ANDNOT:
                        bits = bits.And(c.Not(maxsize));
                        break;
                }
            }
            else
                bits = c;
            return bits;
        }

        private object _lock = new object();
        private void InternalSave()
        {
            _log.Debug("saving index...");
            DateTime dt = FastDateTime.Now;
            // save deleted
            if (_deleted != null)
                _deleted.SaveIndex();

            // save docs 
            if (_docMode)
                _docs.SaveIndex();
            _bitmaps.Commit(false);

            MemoryStream ms = new MemoryStream();
            BinaryWriter bw = new BinaryWriter(ms, Encoding.UTF8);

            // save words and bitmaps
            using (FileStream words = new FileStream(_Path + _FileName + ".words", FileMode.Create))
            {
                foreach (string key in _words.Keys())
                {
                    bw.Write(key);
                    bw.Write(_words[key]);
                }
                byte[] b = ms.ToArray();
                words.Write(b, 0, b.Length);
                words.Flush();
                words.Close();
            }
            _log.Debug("save time (ms) = " + FastDateTime.Now.Subtract(dt).TotalMilliseconds);
        }

        private void LoadWords()
        {
            if (File.Exists(_Path + _FileName + ".words") == false)
                return;
            // load words
            byte[] b = File.ReadAllBytes(_Path + _FileName + ".words");
            if (b.Length == 0)
                return;
            MemoryStream ms = new MemoryStream(b);
            BinaryReader br = new BinaryReader(ms, Encoding.UTF8);
            string s = br.ReadString();
            while (s != "")
            {
                int off = br.ReadInt32();
                _words.Add(s, off);
                try
                {
                    s = br.ReadString();
                }
                catch { s = ""; }
            }
            _log.Debug("Word Count = " + _words.Count);
        }

        private void AddtoIndex(int recnum, string text)
        {
            if (text == "" || text == null)
                return;
            text = text.ToLowerInvariant(); // lowercase index 
            string[] keys;
            if (_docMode)
            {
                //_log.Debug("text size = " + text.Length);
                Dictionary<string, int> wordfreq = GenerateWordFreq(text);
                //_log.Debug("word count = " + wordfreq.Count);
                var kk = wordfreq.Keys;
                keys = new string[kk.Count];
                kk.CopyTo(keys, 0);
            }
            else
            {
                keys = text.Split(' ');
            }

            foreach (string key in keys)
            {
                if (key == "")
                    continue;

                int bmp;
                if (_words.TryGetValue(key, out bmp))
                {
                    _bitmaps.GetBitmap(bmp).Set(recnum, true);
                }
                else
                {
                    bmp = _bitmaps.GetFreeRecordNumber();
                    _bitmaps.SetDuplicate(bmp, recnum);
                    _words.Add(key, bmp);
                }
            }
        }

        private Dictionary<string, int> GenerateWordFreq(string text)
        {
            Dictionary<string, int> dic = new Dictionary<string, int>(500);

            char[] chars = text.ToCharArray();
            int index = 0;
            int run = -1;
            int count = chars.Length;
            while (index < count)
            {
                char c = chars[index++];
                if (!(char.IsLetterOrDigit(c) || char.IsPunctuation(c) )) // rdb specific
                {
                    if (run != -1)
                    {
                        ParseString(dic, chars, index, run);
                        run = -1;
                    }
                }
                else
                    if (run == -1)
                        run = index - 1;
            }

            if (run != -1)
            {
                ParseString(dic, chars, index, run);
                run = -1;
            }

            return dic;
        }

        private void ParseString(Dictionary<string, int> dic, char[] chars, int end, int start)
        {
            // check if upper lower case mix -> extract words
            int uppers = 0;
            bool found = false;
            for (int i = start; i < end; i++)
            {
                if (char.IsUpper(chars[i]))
                    uppers++;
            }
            // not all uppercase
            if (uppers != end - start - 1)
            {
                int lastUpper = start;

                string word = "";
                for (int i = start + 1; i < end; i++)
                {
                    char c = chars[i];
                    if (char.IsUpper(c))
                    {
                        found = true;
                        word = new string(chars, lastUpper, i - lastUpper).ToLowerInvariant().Trim();
                        AddDictionary(dic, word);
                        lastUpper = i;
                    }
                }
                if (lastUpper > start)
                {
                    string last = new string(chars, lastUpper, end - lastUpper).ToLowerInvariant().Trim();
                    if (word != last)
                        AddDictionary(dic, last);
                }
            }
            if (found == false)
            {
                string s = new string(chars, start, end - start).ToLowerInvariant().Trim();
                AddDictionary(dic, s);
            }
        }

        private void AddDictionary(Dictionary<string, int> dic, string word)
        {
            int l = word.Length;
            if (l > Global.DefaultStringKeySize)
                return;
            if (l < 2)
                return;
            if (char.IsLetterOrDigit(word[l - 1]) == false) // rdb specific
                word = new string(word.ToCharArray(), 0, l - 1);
            if (word.Length < 2)
                return;
            int cc = 0;
            if (dic.TryGetValue(word, out cc))
                dic[word] = ++cc;
            else
                dic.Add(word, 1);
        }
        #endregion

        public void Shutdown()
        {
            lock (_lock)
            {
                InternalSave();

                if (_docMode)
                {
                    _docs.Shutdown();
                    _deleted.Shutdown();
                }
            }
        }

        public void FreeMemory()
        {
            if (_bitmaps != null)
                _bitmaps.FreeMemory();
            if (_docs != null)
                _docs.FreeMemory();
        }
    }
}