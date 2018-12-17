using System;
using System.Collections.Generic;
using System.Text;
using System.IO;
using System.Text.RegularExpressions;
using RaptorDB.Common;

namespace RaptorDB
{
    public class Hoot
    {
        public Hoot(string IndexPath, string FileName, bool DocMode) : this(IndexPath, FileName, DocMode, new tokenizer())
        {
        }
        public Hoot(string IndexPath, string FileName, bool DocMode, ITokenizer tokenizer)
        {
            if (tokenizer != null)
                _tokenizer = tokenizer;
            else
                _tokenizer = new tokenizer();
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
                _deleted = new BoolIndex(_Path, "_deleted", ".hoot");
                _lastDocNum = (int)_docs.Count();
            }
            _bitmaps = new BitmapIndex(_Path, _FileName + "_hoot.bmp");
            // read words
            LoadWords();
        }
        private ITokenizer _tokenizer;
        private SafeDictionary<string, int> _words = new SafeDictionary<string, int>();
        //private SafeSortedList<string, int> _words = new SafeSortedList<string, int>();
        private BitmapIndex _bitmaps;
        private BoolIndex _deleted;
        private ILog _log = LogManager.GetLogger(typeof(Hoot));
        private int _lastDocNum = 0;
        private string _FileName = "words";
        private string _Path = "";
        private KeyStoreString _docs;
        private bool _docMode = false;
        private bool _wordschanged = true;
        private bool _shutdowndone = false;
        private object _lock = new object();

        public string[] Words
        {
            get { checkloaded(); return _words.Keys(); }
        }

        public int WordCount
        {
            get { checkloaded(); return _words.Count(); }
        }

        public int DocumentCount
        {
            get { checkloaded(); return _lastDocNum - (int)_deleted.GetBits().CountOnes(); }
        }

        public string IndexPath { get { return _Path; } }

        public void Save()
        {
            lock (_lock)
                InternalSave();
        }

        public void Index(int recordnumber, string text)
        {
            checkloaded();
            AddtoIndex(recordnumber, text);
        }

        public MGRB Query(string filter, int maxsize)
        {
            checkloaded();
            return ExecutionPlan(filter, maxsize);
        }

        public int Index(Document doc, bool deleteold)
        {
            checkloaded();
            _log.Info("indexing doc : " + doc.FileName);
            DateTime dt = FastDateTime.Now;

            if (deleteold && doc.DocNumber > -1)
                _deleted.Set(true, doc.DocNumber);

            if (deleteold == true || doc.DocNumber == -1)
                doc.DocNumber = _lastDocNum++;

            // save doc to disk
            string dstr = fastJSON.JSON.ToJSON(doc, new fastJSON.JSONParameters { UseExtensions = false });
            _docs.Set(doc.FileName.ToLower(), fastJSON.Reflection.UnicodeGetBytes(dstr));

            _log.Info("writing doc to disk (ms) = " + FastDateTime.Now.Subtract(dt).TotalMilliseconds);

            dt = FastDateTime.Now;
            // index doc
            AddtoIndex(doc.DocNumber, doc.Text);
            _log.Info("indexing time (ms) = " + FastDateTime.Now.Subtract(dt).TotalMilliseconds);

            return _lastDocNum;
        }

        public IEnumerable<int> FindRows(string filter)
        {
            checkloaded();
            MGRB bits = ExecutionPlan(filter, _docs.RecordCount());
            // enumerate records
            return bits.GetBitIndexes();
        }

        public IEnumerable<T> FindDocuments<T>(string filter)
        {
            checkloaded();
            MGRB bits = ExecutionPlan(filter, _docs.RecordCount());
            // enumerate documents
            foreach (int i in bits.GetBitIndexes())
            {
                if (i > _lastDocNum - 1)
                    break;
                string b = _docs.ReadData(i);
                T d = fastJSON.JSON.ToObject<T>(b, new fastJSON.JSONParameters { ParametricConstructorOverride = true });

                yield return d;
            }
        }

        public IEnumerable<string> FindDocumentFileNames(string filter)
        {
            checkloaded();
            MGRB bits = ExecutionPlan(filter, _docs.RecordCount());
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
                Document d = fastJSON.JSON.ToObject<Document>(fastJSON.Reflection.UnicodeGetString(b));
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
            lock (_lock)
            {
                InternalSave();
                //_bitmaps.Commit(false);
                _bitmaps.Optimize();
            }
        }

        #region [  P R I V A T E   M E T H O D S  ]

        private void checkloaded()
        {
            if (_wordschanged == false)
            {
                LoadWords();
            }
        }

        private MGRB ExecutionPlan(string filter, int maxsize)
        {
            //_log.Debug("query : " + filter);
            DateTime dt = FastDateTime.Now;
            // query indexes
            string[] words = filter.Split(' ');
            //bool defaulttoand = true;
            //if (filter.IndexOfAny(new char[] { '+', '-' }, 0) > 0)
            //    defaulttoand = false;

            MGRB found = null;// MGRB.Fill(maxsize);            

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
                        found = MGRB.Fill(maxsize);
                    }
                }

                if (word.Contains("*") || word.Contains("?"))
                {
                    MGRB wildbits = new MGRB();

                    // do wildcard search
                    Regex reg = new Regex("^" + word.Replace("*", ".*").Replace("?", ".") + "$", RegexOptions.IgnoreCase);
                    foreach (string key in _words.Keys())
                    {
                        if (reg.IsMatch(key))
                        {
                            _words.TryGetValue(key, out c);
                            MGRB ba = _bitmaps.GetBitmap(c);

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
                    MGRB ba = _bitmaps.GetBitmap(c);
                    found = DoBitOperation(found, ba, op, maxsize);
                }
                else if (op == OPERATION.AND)
                    found = new MGRB();
            }
            if (found == null)
                return new MGRB();

            // remove deleted docs
            MGRB ret;
            if (_docMode)
                ret = found.AndNot(_deleted.GetBits());
            else
                ret = found;
            //_log.Debug("query time (ms) = " + FastDateTime.Now.Subtract(dt).TotalMilliseconds);
            return ret;
        }

        private static MGRB DoBitOperation(MGRB bits, MGRB c, OPERATION op, int maxsize)
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

        private void InternalSave()
        {
            _log.Info("saving index...");
            DateTime dt = FastDateTime.Now;
            // save deleted
            if (_deleted != null)
                _deleted.SaveIndex();

            // save docs 
            if (_docMode)
                _docs.SaveIndex();

            if (_bitmaps != null)
                _bitmaps.Commit(true);

            if (_words != null && _wordschanged == true)
            {
                // save words and bitmaps
                using (FileStream words = new FileStream(_Path + _FileName + ".words", FileMode.Create))
                {
                    using (BinaryWriter bw = new BinaryWriter(words, Encoding.UTF8))
                    {
                        foreach (string key in _words.Keys())
                        {
                            bw.Write(key);
                            bw.Write(_words[key]);
                        }
                    }
                }
                _wordschanged = false;
            }
            _log.Info("save time (ms) = " + FastDateTime.Now.Subtract(dt).TotalMilliseconds);
        }

        private void LoadWords()
        {
            lock (_lock)
            {
                if (_words == null)
                    _words = new SafeDictionary<string, int>();
                             //  new SafeSortedList<string, int>();
                if (File.Exists(_Path + _FileName + ".words") == false)
                    return;
                // load words
                using (FileStream words = new FileStream(_Path + _FileName + ".words", FileMode.Open))
                {
                    if (words.Length == 0)
                        return;
                    using (BinaryReader br = new BinaryReader(words, Encoding.UTF8))
                    {
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
                    }
                }
                //byte[] b = File.ReadAllBytes(_Path + _FileName + ".words");
                //if (b.Length == 0)
                //    return;
                //MemoryStream ms = new MemoryStream(b);
                //BinaryReader br = new BinaryReader(ms, Encoding.UTF8);
                //string s = br.ReadString();
                //while (s != "")
                //{
                //    int off = br.ReadInt32();
                //    _words.Add(s, off);
                //    try
                //    {
                //        s = br.ReadString();
                //    }
                //    catch { s = ""; }
                //}
                _log.Debug("Word Count = " + _words.Count());
                _wordschanged = true;
            }
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
                Dictionary<string, int> wordfreq = _tokenizer.GenerateWordFreq(text);
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
            _wordschanged = true;
        }

        
        #endregion

        public void Shutdown()
        {
            lock (_lock)
            {
                if (_shutdowndone == true)
                    return;

                InternalSave();
                if (_deleted != null)
                {
                    _deleted.SaveIndex();
                    _deleted.Shutdown();
                    _deleted = null;
                }

                if (_bitmaps != null)
                {
                    _bitmaps.Commit(Global.FreeBitmapMemoryOnSave);
                    _bitmaps.Shutdown();
                    _bitmaps = null;
                }

                if (_docMode)
                    _docs.Shutdown();

                _shutdowndone = true;
            }
        }

        public void FreeMemory()
        {
            lock (_lock)
            {
                InternalSave();

                if (_deleted != null)
                    _deleted.FreeMemory();

                if (_bitmaps != null)
                    _bitmaps.FreeMemory();

                if (_docs != null)
                    _docs.FreeMemory();

                //_words = null;// new SafeSortedList<string, int>();
                //_loaded = false;
            }
        }

        public T Fetch<T>(int docnum)
        {
            string b = _docs.ReadData(docnum);
            return fastJSON.JSON.ToObject<T>(b);
        }
    }
}
