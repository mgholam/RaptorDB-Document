using System;
using System.Collections.Generic;
using System.Text;
using System.IO;
using System.Threading;
using System.Collections;
using RaptorDB.Views;
using System.Linq.Expressions;
using System.Threading.Tasks;
using System.Reflection;
using RaptorDB.Common;
using System.IO.Compression;
using System.CodeDom.Compiler;
using System.Text.RegularExpressions;
using System.ComponentModel;

namespace RaptorDB
{
    public class RaptorDB : IRaptorDB
    {
        private RaptorDB(string FolderPath)
        {
            // speed settings
            fastJSON.JSON.Parameters.ParametricConstructorOverride = true;
            fastBinaryJSON.BJSON.Parameters.ParametricConstructorOverride = true;
            fastJSON.JSON.Parameters.UseEscapedUnicode = false;

            if (_S == "/")
                FolderPath = FolderPath.Replace("\\", "/");
            // create folders 
            Directory.CreateDirectory(FolderPath);
            string foldername = Path.GetFullPath(FolderPath);
            if (foldername.EndsWith(_S) == false)
                foldername += _S;
            _Path = foldername;

            // if configs !exists create template config files
            CreateTemplateConfigFiles();

            Initialize();
        }

        private void CreateTemplateConfigFiles()
        {
            if (File.Exists(_Path + "RaptorDB.config") == false)
                File.WriteAllText(_Path + "-RaptorDB.config", fastJSON.JSON.ToNiceJSON(new Global(), new fastJSON.JSONParameters { UseExtensions = false }));

            if (File.Exists(_Path + "RaptorDB-Branch.config") == false)
                File.WriteAllText(_Path + "-RaptorDB-Branch.config", fastJSON.JSON.ToNiceJSON(new Replication.ClientConfiguration(), new fastJSON.JSONParameters { UseExtensions = false }));

            if (File.Exists(_Path + "RaptorDB-Replication.config") == false)
            {
                Replication.ServerConfiguration s = new Replication.ServerConfiguration();
                s.What.Add(new Replication.WhatItem { Name = "default", PackageItemLimit = 10000, Version = 1, B2HQtypes = new List<string> { "*" }, HQ2Btypes = new List<string> { "*" } });
                s.What.Add(new Replication.WhatItem { Name = "b2", PackageItemLimit = 10000, Version = 1, B2HQtypes = new List<string> { "*" }, HQ2Btypes = new List<string> { "config.*" } });
                s.Where.Add(new Replication.WhereItem { BranchName = "b1", Password = "123", When = "*/5 * * * *", What = "default" });
                s.Where.Add(new Replication.WhereItem { BranchName = "b2", Password = "321", When = "*/20 * * * *", What = "b2" });
                File.WriteAllText(_Path + "-RaptorDB-Replication.config", fastJSON.JSON.ToNiceJSON(s, new fastJSON.JSONParameters { UseExtensions = false }));
            }
        }

        public static RaptorDB Open(string FolderPath)
        {
            return new RaptorDB(FolderPath);
        }

        private string _S = Path.DirectorySeparatorChar.ToString();
        private ILog _log = LogManager.GetLogger(typeof(RaptorDB));
        private Views.ViewManager _viewManager;
        private KeyStore<Guid> _objStore;
        private KeyStore<Guid> _fileStore;
        private KeyStoreHF _objHF;
        private string _Path = "";
        private int _LastRecordNumberProcessed = -1; // used by background saver
        private int _LastFulltextIndexed = -1; // used by the fulltext indexer
        private int _LastBackupRecordNumber = -1;
        private int _CurrentRecordNumber = -1;
        private System.Timers.Timer _saveTimer;
        private System.Timers.Timer _fulltextTimer;
        private System.Timers.Timer _freeMemTimer;
        private System.Timers.Timer _processinboxTimer;
        private bool _shuttingdown = false;
        private bool _pauseindexer = false;
        private MethodInfo otherviews = null;
        private MethodInfo save = null;
        private MethodInfo saverep = null;
        private SafeDictionary<Type, MethodInfo> _savecache = new SafeDictionary<Type, MethodInfo>();
        private SafeDictionary<Type, MethodInfo> _saverepcache = new SafeDictionary<Type, MethodInfo>();
        private FullTextIndex _fulltextindex;
        private CronDaemon _cron;
        private Replication.ReplicationServer _repserver;
        private Replication.ReplicationClient _repclient;
        //private bool _disposed = false;
        //private bool _clientReplicationEnabled;

        //public bool SyncNow(string server, int port, string username, string password)
        //{

        //    return false;
        //}

        #region [   p u b l i c    i n t e r f a c e   ]
        /// <summary>
        /// Save files to RaptorDB
        /// </summary>
        /// <param name="docID"></param>
        /// <param name="bytes"></param>
        public bool SaveBytes(Guid docID, byte[] bytes)
        {
            // save files in storage
            _fileStore.SetBytes(docID, bytes);
            return true;
        }
        /// <summary>
        /// Delete a document (note data is not lost just flagged as deleted)
        /// </summary>
        /// <param name="docid"></param>
        /// <returns></returns>
        public bool Delete(Guid docid)
        {
            bool b = _objStore.Delete(docid);
            _viewManager.Delete(docid);
            return b;
        }

        /// <summary>
        /// Delete a file (note data is not lost just flagged as deleted)
        /// </summary>
        /// <param name="bytesid"></param>
        /// <returns></returns>
        public bool DeleteBytes(Guid bytesid)
        {
            return _fileStore.Delete(bytesid);
        }

        /// <summary>
        /// Save a document
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="docid"></param>
        /// <param name="data"></param>
        /// <returns></returns>
        public bool Save<T>(Guid docid, T data)
        {
            string viewname = _viewManager.GetPrimaryViewForType(data.GetType());
            if (viewname == "" && Global.RequirePrimaryView == true)
            {
                _log.Debug("Primary View not defined for object : " + data.GetType());
                return false;
            }
            _pauseindexer = true;
            if (viewname != "" && _viewManager.isTransaction(viewname))
            {
                _log.Debug("TRANSACTION started for docid : " + docid);
                // add code here
                _viewManager.StartTransaction();

                bool b = SaveInPrimaryViewTransaction(viewname, docid, data);
                if (b == true)
                {
                    b = SaveToConsistentViewsTransaction(docid, data);
                    if (b == true)
                    {
                        b = SaveInOtherViewsTransaction(docid, data);
                        if (b == true)
                        {
                            _viewManager.Commit(Thread.CurrentThread.ManagedThreadId);
                            int recnum = _objStore.SetObject(docid, data);
                            _CurrentRecordNumber = recnum;
                            _pauseindexer = false;
                            return true;
                        }
                    }
                }
                _viewManager.Rollback(Thread.CurrentThread.ManagedThreadId);
                _pauseindexer = false;
                return false;
            }
            else
            {
                int recnum = _objStore.SetObject(docid, data);
                _CurrentRecordNumber = recnum;

                if (viewname != "")
                {
                    SaveInPrimaryView(viewname, docid, data);

                    SaveToConsistentViews(docid, data);

                    if (Global.BackgroundSaveToOtherViews == false)
                    {
                        SaveInOtherViews(docid, data);
                        _LastRecordNumberProcessed = recnum;
                    }
                }
                _pauseindexer = false;
                return true;
            }
        }

        /// <summary>
        /// Query any view -> get all rows
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="viewname"></param>
        /// <returns></returns>
        public Result<object> Query(string viewname)
        {
            return _viewManager.Query(viewname, 0, -1);
        }

        /// <summary>
        /// Query a view using a string filter
        /// </summary>
        /// <param name="viewname"></param>
        /// <param name="filter"></param>
        /// <returns></returns>
        public Result<object> Query(string viewname, string filter)
        {
            if (filter == "")
                return _viewManager.Query(viewname, 0, -1);

            return _viewManager.Query(viewname, filter, 0, -1);
        }

        /// <summary>
        /// Fetch a document by it's ID
        /// </summary>
        /// <param name="docID"></param>
        /// <returns></returns>
        public object Fetch(Guid docID)
        {
            object b = null;
            _objStore.GetObject(docID, out b);
            return b;
        }

        /// <summary>
        /// Fetch file data by it's ID
        /// </summary>
        /// <param name="fileID"></param>
        /// <returns></returns>
        public byte[] FetchBytes(Guid fileID)
        {
            byte[] b = null;
            if (_fileStore.GetBytes(fileID, out b))
                return b;
            else
                return null;
        }

        /// <summary>
        /// Register a view
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="view"></param>
        public void RegisterView<T>(View<T> view)
        {
            _viewManager.RegisterView(view);
        }

        /// <summary>
        /// Shutdown the database engine and flush memory to disk
        /// </summary>
        public void Shutdown()
        {
            if (_shuttingdown == true)
                return;

            _shuttingdown = true;

            _processinboxTimer.Enabled = false;
            _saveTimer.Enabled = false;
            _freeMemTimer.Enabled = false;
            _fulltextTimer.Enabled = false;

            if (_repserver != null)
                _repserver.Shutdown();

            if (_repclient != null)
                _repclient.Shutdown();

            // TODO : write global or something else?
            //if (File.Exists(_Path + "RaptorDB.config") == false)
            File.WriteAllText(_Path + "RaptorDB.config", fastJSON.JSON.ToNiceJSON(new Global(), new fastJSON.JSONParameters { UseExtensions = false }));
            if (_cron != null)
                _cron.Stop();
            _fulltextindex.Shutdown();

            _log.Debug("Shutting down");
            _saveTimer.Stop();
            _fulltextTimer.Stop();
            _viewManager.ShutDown();
            _objStore.Shutdown();
            _fileStore.Shutdown();
            _objHF.Shutdown();

            // save records 
            _log.Debug("last full text record = " + _LastFulltextIndexed);
            File.WriteAllBytes(_Path + "Data" + _S + "Fulltext" + _S + "_fulltext.rec", Helper.GetBytes(_LastFulltextIndexed, false));
            _log.Debug("last record = " + _LastRecordNumberProcessed);
            File.WriteAllBytes(_Path + "Data" + _S + "_lastrecord.rec", Helper.GetBytes(_LastRecordNumberProcessed, false));
            _log.Debug("last backup record = " + _LastBackupRecordNumber);
            File.WriteAllBytes(_Path + "Backup" + _S + "LastBackupRecord.rec", Helper.GetBytes(_LastBackupRecordNumber, false));

            _log.Debug("Shutting down log.");
            _log.Debug("RaptorDB done.");
            LogManager.Shutdown();
        }


        #region [   BACKUP/RESTORE and REPLICATION   ]
        private object _backuplock = new object();
        /// <summary>
        /// Backup the document storage file incrementally to "Backup" folder
        /// </summary>
        /// <returns>True = done</returns>
        public bool Backup()
        {
            if (_LastBackupRecordNumber >= _CurrentRecordNumber)
                return false;
            lock (_backuplock)
            {
                _log.Debug("Backup Started...");
                string tempp = _Path + "Temp" + _S + DateTime.Now.ToString("yyyy-MM-dd-HH-mm");
                Directory.CreateDirectory(tempp);
                StorageFile<Guid> backup = new StorageFile<Guid>(tempp + _S + "backup.mgdat", SF_FORMAT.BSON, true);
                _log.Debug("Copying data to backup");
                if (_LastBackupRecordNumber == -1)
                    _LastBackupRecordNumber = 0;
                int rec = _objStore.CopyTo(backup, _LastBackupRecordNumber);
                backup.Shutdown();

                _log.Debug("Last backup rec# = " + rec);

                // compress the file
                using (FileStream read = File.OpenRead(tempp + _S + "backup.mgdat"))
                using (FileStream outp = File.Create(tempp + _S + "backup.mgdat.gz"))
                    CompressForBackup(read, outp);

                _log.Debug("Backup compressed and done");
                File.Move(tempp + _S + "backup.mgdat.gz", _Path + "Backup" + _S + DateTime.Now.ToString("yyyy-MM-dd-HH-mm") + ".mgdat.gz");

                _log.Debug("last backup record = " + _LastBackupRecordNumber);
                File.WriteAllBytes(_Path + "Backup" + _S + "LastBackupRecord.rec", Helper.GetBytes(_LastBackupRecordNumber, false));
                // cleanup temp folder
                Directory.Delete(tempp, true);
                _log.Debug("Backup done.");
                _LastBackupRecordNumber = rec;
                return true;
            }
        }

        private DateTime _lastFailedTime = DateTime.Now;
        private object _replock = new object();
        private void ProcessReplicationInbox(string inboxfolder)
        {
            lock (_replock)
            {
                if (Directory.Exists(inboxfolder) == false)
                    return;

                string[] files = Directory.GetFiles(inboxfolder, "*.counter");

                // check if ".counter" file exists
                if (files.Length > 0)
                {
                    // FEATURE: if lastfailedtime < 15 -> wait 15 min and retry (avoid extra cpu burning)
                    // recovery mode
                    string fn = files[0];
                    int start = -1;
                    if (int.TryParse(File.ReadAllText(fn).Trim(), out start))
                    {
                        if (DoRepProcessing(fn.Replace(".counter", ".mgdat"), start) == false)
                            return;
                    }
                    else
                    {
                        _log.Error("Unable to parse counter value in : " + fn);
                        return;
                    }
                }

                files = Directory.GetFiles(inboxfolder, "*.gz");

                Array.Sort(files);
                foreach (var filename in files)
                {

                    string tmp = filename.Replace(".gz", "");// FEATURE : to temp folder ??
                    if (File.Exists(tmp))
                        File.Delete(tmp);
                    using (FileStream read = File.OpenRead(filename))
                    using (FileStream outp = File.Create(tmp))
                        DecompressForRestore(read, outp);
                    _log.Debug("Uncompress done : " + Path.GetFileName(tmp));
                    if (DoRepProcessing(tmp, 0) == false)
                        return;
                    if (_shuttingdown)
                        return;
                }
            }
        }

        private bool DoRepProcessing(string filename, int start)
        {
            string fn = Path.GetFileNameWithoutExtension(filename);
            string path = Path.GetDirectoryName(filename);
            StorageFile<Guid> sf = StorageFile<Guid>.ReadForward(filename);
            int counter = 0;
            if (start > 0)
                _log.Debug("skipping replication items : " + start);
            foreach (var i in sf.ReadOnlyEnumerate())
            {
                if (start > 0) // skip already done
                {
                    start--;
                    counter++;
                }
                else
                {
                    if (i.meta.isDeleted)
                        DeleteReplicate(i.meta.key);
                    else
                    {
                        try
                        {
                            object obj = CreateObject(i.data);
                            var m = GetSaveReplicate(obj.GetType());
                            m.Invoke(this, new object[] { i.meta.key, obj });
                        }
                        catch (Exception ex)
                        {
                            _log.Error(ex);
                            sf.Shutdown();
                            string err = Properties.Resources.msg.Replace("%js%", fastJSON.JSON.Beautify(Helper.GetString(i.data, 0, (short)i.data.Length)))
                                .Replace("%ex%", "" + ex)
                                .Replace("%c%", path + _S + fn + ".counter");

                            File.WriteAllText(path + _S + fn + ".error.txt", err);
                            _lastFailedTime = DateTime.Now;
                            return false;
                        }
                    }
                    counter++;
                    File.WriteAllText(path + _S + fn + ".counter", "" + counter);
                    if (_shuttingdown)
                    {
                        _log.Debug("shutting down before replicate data completed...");
                        sf.Shutdown();
                        return false;
                    }
                }
            }
            sf.Shutdown();
            _log.Debug("File replicate complete : " + Path.GetFileName(filename));
            foreach (var f in Directory.GetFiles(path, fn + ".*"))
                File.Delete(f);
            return true;
        }

        private void DeleteReplicate(Guid docid)
        {
            bool b = _objStore.DeleteReplicated(docid);
            _viewManager.Delete(docid);
        }

        private object _restoreLock = new object();
        /// <summary>
        /// Start background restore of backups in the "Restore" folder
        /// </summary>
        public void Restore()
        {
            lock (_restoreLock)
            {
                try
                {
                    string[] files = Directory.GetFiles(_Path + "Restore", "*.counter");
                    // check if ".counter" file exists
                    if (files.Length > 0)
                    {
                        // resume mode
                        string fn = files[0];
                        int start = -1;
                        if (int.TryParse(File.ReadAllText(fn).Trim(), out start))
                        {
                            if (DoRestoreProcessinng(fn.Replace(".counter", ".mgdat"), start) == false)
                                return;
                        }
                        else
                        {
                            _log.Error("Unable to parse counter value in : " + fn);
                            return;
                        }
                    }
                    // do restore 
                    files = Directory.GetFiles(_Path + "Restore", "*.gz");
                    Array.Sort(files);
                    _log.Debug("Restoring file count = " + files.Length);

                    foreach (string file in files)
                    {
                        string tmp = file.Replace(".gz", "");// FEATURE : to temp folder ??
                        if (File.Exists(tmp))
                            File.Delete(tmp);
                        using (FileStream read = File.OpenRead(file))
                        using (FileStream outp = File.Create(tmp))
                            DecompressForRestore(read, outp);
                        _log.Debug("Uncompress done : " + Path.GetFileName(tmp));

                        if (DoRestoreProcessinng(tmp, 0))
                            File.Move(file, _Path + "Restore" + _S + "Done" + _S + Path.GetFileName(file));
                    }
                }
                catch (Exception ex)
                {
                    _log.Error(ex);
                }
            }
        }

        private bool DoRestoreProcessinng(string filename, int start)
        {
            string fn = Path.GetFileNameWithoutExtension(filename);
            string path = Path.GetDirectoryName(filename);
            int counter = 0;
            StorageFile<Guid> sf = StorageFile<Guid>.ReadForward(filename);
            foreach (var i in sf.ReadOnlyEnumerate())
            {
                if (start > 0)
                {
                    start--;
                    counter++;
                }
                else
                {
                    if (i.meta.isDeleted)
                        Delete(i.meta.key);
                    else
                    {
                        object obj = CreateObject(i.data);
                        var m = GetSave(obj.GetType());
                        m.Invoke(this, new object[] { i.meta.key, obj });
                    }
                    counter++;
                    File.WriteAllText(path + _S + fn + ".counter", "" + counter);
                    if (_shuttingdown)
                    {
                        _log.Debug("shutting down before restore completed...");
                        sf.Shutdown();
                        return false;
                    }
                }
            }
            sf.Shutdown();
            _log.Debug("File restore complete : " + Path.GetFileName(filename));
            foreach (var f in Directory.GetFiles(path, fn + ".*"))
                File.Delete(f);

            return true;
        }

        private bool SaveReplicationObject<T>(Guid docid, T data)
        {
            string viewname = _viewManager.GetPrimaryViewForType(data.GetType());
            if (viewname == "")
            {
                _log.Debug("Primary View not defined for object : " + data.GetType());
                return false;
            }
            _pauseindexer = true;
            int recnum = _objStore.SetReplicationObject(docid, data);
            _CurrentRecordNumber = recnum;

            SaveInPrimaryView(viewname, docid, data);

            SaveToConsistentViews(docid, data);

            if (Global.BackgroundSaveToOtherViews == false)
            {
                SaveInOtherViews(docid, data);
                _LastRecordNumberProcessed = recnum;
            }
            _pauseindexer = false;
            return true;
        }
        #endregion

        /// <summary>
        /// Add a user (only supported in server mode)
        /// </summary>
        /// <param name="username"></param>
        /// <param name="oldpassword"></param>
        /// <param name="newpassword"></param>
        /// <returns></returns>
        public bool AddUser(string username, string oldpassword, string newpassword)
        {
            return false;
        }

        /// <summary>
        /// Execute a server side string filter query
        /// </summary>
        /// <param name="func"></param>
        /// <param name="filter"></param>
        /// <returns></returns>
        public object[] ServerSide(ServerSideFunc func, string filter)
        {
            return func(this, filter).ToArray();
        }

        /// <summary>
        /// Execute a server side LINQ query
        /// </summary>
        /// <typeparam name="TRowSchema"></typeparam>
        /// <param name="func"></param>
        /// <param name="filter"></param>
        /// <returns></returns>
        public object[] ServerSide<TRowSchema>(ServerSideFunc func, Expression<Predicate<TRowSchema>> filter)
        {
            LINQString ls = new LINQString();
            ls.Visit(filter);
            return func(this, ls.sb.ToString()).ToArray();
        }

        /// <summary>
        /// Full text search the entire original document
        /// </summary>
        /// <param name="filter"></param>
        /// <returns></returns>
        public int[] FullTextSearch(string filter)
        {
            var wbmp = _fulltextindex.Query(filter, _objStore.RecordCount());
            List<int> a = new List<int>();
            a.AddRange(wbmp.GetBitIndexes());

            return a.ToArray();
        }

        /// <summary>
        /// Query a view
        /// </summary>
        /// <typeparam name="TRowSchema"></typeparam>
        /// <param name="filter"></param>
        /// <returns></returns>
        public Result<TRowSchema> Query<TRowSchema>(Expression<Predicate<TRowSchema>> filter)
        {
            return _viewManager.Query<TRowSchema>(filter, 0, -1);
        }

        /// <summary>
        /// Query a view with paging
        /// </summary>
        /// <typeparam name="TRowSchema"></typeparam>
        /// <param name="filter"></param>
        /// <param name="start"></param>
        /// <param name="count"></param>
        /// <returns></returns>
        public Result<TRowSchema> Query<TRowSchema>(Expression<Predicate<TRowSchema>> filter, int start, int count)
        {
            return _viewManager.Query<TRowSchema>(filter, start, count, "");
        }

        /// <summary>
        /// Query a view with paging and order by
        /// </summary>
        /// <typeparam name="TRowSchema"></typeparam>
        /// <param name="filter"></param>
        /// <param name="start"></param>
        /// <param name="count"></param>
        /// <param name="orderby"></param>
        /// <returns></returns>
        public Result<TRowSchema> Query<TRowSchema>(Expression<Predicate<TRowSchema>> filter, int start, int count, string orderby)
        {
            return _viewManager.Query<TRowSchema>(filter, start, count, orderby);
        }

        /// <summary>
        /// Query a view 
        /// </summary>
        /// <typeparam name="TRowSchema"></typeparam>
        /// <param name="filter"></param>
        /// <returns></returns>
        public Result<TRowSchema> Query<TRowSchema>(string filter)
        {
            return _viewManager.Query<TRowSchema>(filter, 0, -1);
        }

        /// <summary>
        /// Query a view with paging
        /// </summary>
        /// <typeparam name="TRowSchema"></typeparam>
        /// <param name="filter"></param>
        /// <param name="start"></param>
        /// <param name="count"></param>
        /// <returns></returns>
        public Result<TRowSchema> Query<TRowSchema>(string filter, int start, int count)
        {
            return _viewManager.Query<TRowSchema>(filter, start, count);
        }

        /// <summary>
        /// Count with filter
        /// </summary>
        /// <typeparam name="TRowSchema"></typeparam>
        /// <param name="filter"></param>
        /// <returns></returns>
        public int Count<TRowSchema>(Expression<Predicate<TRowSchema>> filter)
        {
            return _viewManager.Count<TRowSchema>(filter);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="viewname"></param>
        /// <param name="start"></param>
        /// <param name="count"></param>
        /// <returns></returns>
        public Result<object> Query(string viewname, int start, int count)
        {
            return _viewManager.Query(viewname, start, count);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="viewname"></param>
        /// <param name="filter"></param>
        /// <param name="start"></param>
        /// <param name="count"></param>
        /// <returns></returns>
        public Result<object> Query(string viewname, string filter, int start, int count)
        {
            return _viewManager.Query(viewname, filter, start, count);
        }

        /// <summary>
        /// Count all data associated with View name
        /// </summary>
        /// <param name="viewname"></param>
        /// <returns></returns>
        public int Count(string viewname)
        {
            return _viewManager.Count(viewname, "");
        }

        /// <summary>
        /// Count all data associated with View name and string filter
        /// </summary>
        /// <param name="viewname"></param>
        /// <param name="filter"></param>
        /// <returns></returns>
        public int Count(string viewname, string filter)
        {
            return _viewManager.Count(viewname, filter);
        }

        /// <summary>
        /// Fetch the change history for a document
        /// </summary>
        /// <param name="docid"></param>
        /// <returns></returns>
        public int[] FetchHistory(Guid docid)
        {
            return _objStore.GetHistory(docid);
        }

        /// <summary>
        /// Fetch a change history for a file
        /// </summary>
        /// <param name="fileid"></param>
        /// <returns></returns>
        public int[] FetchBytesHistory(Guid fileid)
        {
            return _fileStore.GetHistory(fileid);
        }

        /// <summary>
        /// Fetch the specific document version 
        /// </summary>
        /// <param name="versionNumber"></param>
        /// <returns></returns>
        public object FetchVersion(int versionNumber)
        {
            StorageItem<Guid> meta = null;
            return _objStore.GetObject(versionNumber, out meta);
        }

        /// <summary>
        /// Fetch the specific file version
        /// </summary>
        /// <param name="versionNumber"></param>
        /// <returns></returns>
        public byte[] FetchBytesVersion(int versionNumber)
        {
            StorageItem<Guid> meta = null;
            return _fileStore.GetBytes(versionNumber, out meta);
        }
        #endregion

        #region [            P R I V A T E     M E T H O D S              ]

        internal string GetViewName(Type type)
        {
            return _viewManager.GetViewName(type);
        }

        private bool SaveToView<T>(Guid docid, T data, List<string> list)
        {
            if (list != null)
                foreach (string name in list)
                {
                    bool ret = _viewManager.InsertTransaction(name, docid, data);
                    if (ret == false)
                        return false;
                }
            return true;
        }

        private bool SaveInOtherViewsTransaction<T>(Guid docid, T data)
        {
            List<string> list = _viewManager.GetOtherViewsList(data.GetType());
            return SaveToView<T>(docid, data, list);
        }

        private bool SaveToConsistentViewsTransaction<T>(Guid docid, T data)
        {
            List<string> list = _viewManager.GetConsistentViews(data.GetType());
            return SaveToView<T>(docid, data, list);
        }

        private bool SaveInPrimaryViewTransaction<T>(string viewname, Guid docid, T data)
        {
            return _viewManager.InsertTransaction(viewname, docid, data);
        }

        private static void PumpDataForBackup(Stream input, Stream output)
        {
            byte[] bytes = new byte[4096 * 2];
            int n;
            while ((n = input.Read(bytes, 0, bytes.Length)) != 0)
                output.Write(bytes, 0, n);
        }

        private static void CompressForBackup(Stream source, Stream destination)
        {
            using (GZipStream gz = new GZipStream(destination, CompressionMode.Compress))
                PumpDataForBackup(source, gz);
        }

        private static void DecompressForRestore(Stream source, Stream destination)
        {
            using (GZipStream gz = new GZipStream(source, CompressionMode.Decompress))
                PumpDataForBackup(gz, destination);
        }

        private void SaveToConsistentViews<T>(Guid docid, T data)
        {
            List<string> list = _viewManager.GetConsistentViews(data.GetType());
            if (list != null)
                foreach (string name in list)
                {
                    _log.Debug("Saving to consistent view : " + name);
                    _viewManager.Insert(name, docid, data);
                }
        }

        private object CreateObject(byte[] b)
        {
            if (b[0] < 32)
                return fastBinaryJSON.BJSON.ToObject(b);
            else
                return fastJSON.JSON.ToObject(Encoding.ASCII.GetString(b));
        }

        private void SaveInOtherViews<T>(Guid docid, T data)
        {
            List<string> list = _viewManager.GetOtherViewsList(data.GetType());
            if (list != null)
                foreach (string name in list)
                    _viewManager.Insert(name, docid, data);
        }

        private void SaveInPrimaryView<T>(string viewname, Guid docid, T data)
        {
            _viewManager.Insert(viewname, docid, data);
        }

        private void Initialize()
        {
            //AppDomain.CurrentDomain.ProcessExit += new EventHandler(CurrentDomain_ProcessExit);

            // TODO : read/write global or another object?
            // read raptordb.config here (running parameters)
            if (File.Exists(_Path + "RaptorDB.config"))
                fastJSON.JSON.FillObject(new Global(), File.ReadAllText(_Path + "RaptorDB.config"));

            Directory.CreateDirectory(_Path + "Data");
            Directory.CreateDirectory(_Path + "Data" + _S + "Fulltext");
            Directory.CreateDirectory(_Path + "Views");
            Directory.CreateDirectory(_Path + "Logs");
            Directory.CreateDirectory(_Path + "Temp");
            Directory.CreateDirectory(_Path + "Backup");
            Directory.CreateDirectory(_Path + "Restore");
            Directory.CreateDirectory(_Path + "Restore" + _S + "Done");
            // load logger
            LogManager.Configure(_Path + "Logs" + _S + "log.txt", 500, false);

            _log.Debug("\r\n\r\nRaptorDB starting...");
            _log.Debug("RaptorDB data folder = " + _Path);

            // check doc & file storage file version and upgrade if needed here
            int v = StorageFile<Guid>.GetStorageFileHeaderVersion(_Path + "Data" + _S + "data");
            if (v < StorageFile<int>._CurrentVersion)
                UpgradeStorageFile(_Path + "Data" + _S + "data", v);

            v = StorageFile<Guid>.GetStorageFileHeaderVersion(_Path + "Data" + _S + "files");
            if (v < StorageFile<int>._CurrentVersion)
                UpgradeStorageFile(_Path + "Data" + _S + "files", v);

            _objStore = new KeyStore<Guid>(_Path + "Data" + _S + "data", true);
            _fileStore = new KeyStore<Guid>(_Path + "Data" + _S + "files", true);

            _objHF = new KeyStoreHF(_Path + "DataHF");

            _viewManager = new Views.ViewManager(_Path + "Views", _objStore, _objHF);

            // load _LastFulltextIndexed 
            if (File.Exists(_Path + "Data" + _S + "Fulltext" + _S + "_fulltext.rec"))
            {
                byte[] b = File.ReadAllBytes(_Path + "Data" + _S + "Fulltext" + _S + "_fulltext.rec");
                _LastFulltextIndexed = Helper.ToInt32(b, 0, false);
            }
            // load _LastRecordNumberProcessed 
            if (File.Exists(_Path + "Data" + _S + "_lastrecord.rec"))
            {
                byte[] b = File.ReadAllBytes(_Path + "Data" + _S + "_lastrecord.rec");
                _LastRecordNumberProcessed = Helper.ToInt32(b, 0, false);
            }
            // load _LastBackupRecordNumber 
            if (File.Exists(_Path + "Backup" + _S + "LastBackupRecord.rec"))
            {
                byte[] b = File.ReadAllBytes(_Path + "Backup" + _S + "LastBackupRecord.rec");
                _LastBackupRecordNumber = Helper.ToInt32(b, 0, false);
            }
            _CurrentRecordNumber = _objStore.RecordCount();

            otherviews = this.GetType().GetMethod("SaveInOtherViews", BindingFlags.Instance | BindingFlags.NonPublic);
            save = this.GetType().GetMethod("Save", BindingFlags.Instance | BindingFlags.Public);
            saverep = this.GetType().GetMethod("SaveReplicationObject", BindingFlags.Instance | BindingFlags.NonPublic);

            _fulltextindex = new FullTextIndex(_Path + "Data" + _S + "Fulltext", "fulltext", true, false);

            // start backround save to views
            _saveTimer = new System.Timers.Timer(Global.BackgroundSaveViewTimer * 1000);
            _saveTimer.Elapsed += new System.Timers.ElapsedEventHandler(_saveTimer_Elapsed);
            _saveTimer.Enabled = true;
            _saveTimer.AutoReset = true;
            _saveTimer.Start();

            // start full text timer 
            _fulltextTimer = new System.Timers.Timer(Global.FullTextTimerSeconds * 1000);
            _fulltextTimer.Elapsed += new System.Timers.ElapsedEventHandler(_fulltextTimer_Elapsed);
            _fulltextTimer.Enabled = true;
            _fulltextTimer.AutoReset = true;
            _fulltextTimer.Start();

            // start free memory timer 
            _freeMemTimer = new System.Timers.Timer(Global.FreeMemoryTimerSeconds * 1000);
            _freeMemTimer.Elapsed += new System.Timers.ElapsedEventHandler(_freeMemTimer_Elapsed);
            _freeMemTimer.Enabled = true;
            _freeMemTimer.AutoReset = true;
            _freeMemTimer.Start();

            // start inbox procesor timer 
            _processinboxTimer = new System.Timers.Timer(Global.ProcessInboxTimerSeconds * 1000);
            _processinboxTimer.Elapsed += new System.Timers.ElapsedEventHandler(_processinboxTimer_Elapsed);
            _processinboxTimer.Enabled = true;
            _processinboxTimer.AutoReset = true;
            _processinboxTimer.Start();

            // start cron daemon
            _cron = new CronDaemon();
            _cron.AddJob(Global.BackupCronSchedule, () => this.Backup());

            // compile & register view files
            CompileAndRegisterScriptViews(_Path + "Views");


            if (File.Exists(_Path + "RaptorDB-Replication.config"))
            {
                // if replication.config exists -> start replication server
                _repserver = new Replication.ReplicationServer(_Path, File.ReadAllText(_Path + "RaptorDB-Replication.config"), _objStore);
            }
            else if (File.Exists(_Path + "RaptorDB-Branch.config"))
            {
                // if branch.config exists -> start replication client
                _repclient = new Replication.ReplicationClient(_Path, File.ReadAllText(_Path + "RaptorDB-Branch.config"), _objStore);
            }
        }

        object _inboxlock = new object();
        void _processinboxTimer_Elapsed(object sender, System.Timers.ElapsedEventArgs e)
        {
            lock (_inboxlock)
            {
                string d = _Path + "Replication" + _S + "Inbox";
                if (Directory.Exists(d) == false)
                    return;

                // start inbox processing timer
                ProcessReplicationInbox(d);

                foreach (var f in Directory.GetDirectories(d))
                    ProcessReplicationInbox(f);
            }
        }

        private void CompileAndRegisterScriptViews(string viewfolder)
        {
            // compile & register views
            string[] files = Directory.GetFiles(viewfolder, "*.view");
            MethodInfo register = this.GetType().GetMethod("RegisterView", BindingFlags.Instance | BindingFlags.Public);
            foreach (var fn in files)
            {
                Assembly a = CompileScript(fn);
                if (a != null)
                {
                    foreach (var t in a.GetTypes())
                    {
                        foreach (var att in t.GetCustomAttributes(typeof(RegisterViewAttribute), false))
                        {
                            try
                            {
                                object o = Activator.CreateInstance(t);
                                //  handle types when view<T> also
                                Type[] args = t.GetGenericArguments();
                                if (args.Length == 0)
                                    args = t.BaseType.GetGenericArguments();
                                Type tt = args[0];
                                var m = register.MakeGenericMethod(new Type[] { tt });
                                m.Invoke(this, new object[] { o });
                            }
                            catch (Exception ex)
                            {
                                _log.Error(ex);
                            }
                        }
                    }
                }
            }
        }

        private Assembly CompileScript(string file)
        {
            try
            {
                _log.Debug("Compiling script view : " + file);
                CodeDomProvider compiler = CodeDomProvider.CreateProvider("CSharp");

                CompilerParameters compilerparams = new CompilerParameters();
                compilerparams.GenerateInMemory = false;
                compilerparams.GenerateExecutable = false;
                compilerparams.OutputAssembly = file.Replace(".view", ".dll");
                compilerparams.CompilerOptions = "/optimize";

                Regex regex = new Regex(
                    @"\/\/\s*ref\s*\:\s*(?<refs>.*)",
                    System.Text.RegularExpressions.RegexOptions.IgnoreCase);

                compilerparams.ReferencedAssemblies.Add(typeof(View<>).Assembly.Location);
                compilerparams.ReferencedAssemblies.Add(typeof(object).Assembly.Location);
                compilerparams.ReferencedAssemblies.Add(typeof(ICustomTypeDescriptor).Assembly.Location);

                foreach (Match m in regex.Matches(File.ReadAllText(file)))
                {
                    string str = m.Groups["refs"].Value.Trim();
#pragma warning disable 618
                    Assembly a = Assembly.LoadWithPartialName(Path.GetFileNameWithoutExtension(str));//load from GAC if possible
#pragma warning restore 618
                    if (a != null)
                        compilerparams.ReferencedAssemblies.Add(a.Location);
                    else
                    {
                        string assm = Path.GetDirectoryName(this.GetType().Assembly.Location) + _S + str;
                        a = Assembly.LoadFrom(assm);
                        if (a != null)
                            compilerparams.ReferencedAssemblies.Add(a.Location);
                        else
                            _log.Error("unable to find referenced file for view compiling : " + str);
                    }
                }

                CompilerResults results = compiler.CompileAssemblyFromFile(compilerparams, file);

                if (results.Errors.HasErrors == true)
                {
                    _log.Error("Error compiling view definition : " + file);
                    foreach (var e in results.Errors)
                        _log.Error(e.ToString());
                    return null;
                }

                return results.CompiledAssembly;
            }
            catch (Exception ex)
            {
                _log.Error("Error compiling view definition : " + file);
                _log.Error(ex);
                return null;
            }
        }

        void _freeMemTimer_Elapsed(object sender, System.Timers.ElapsedEventArgs e)
        {
            long l = GC.GetTotalMemory(true) / (1024 * 1024);
            _log.Debug("GC.GetTotalMemory() = " + l.ToString("#,0"));
            if (l > Global.MemoryLimit)
            {
                _log.Debug("Freeing memory on " + Global.MemoryLimit.ToString("#,0") + " limit ...");
                _viewManager.FreeMemory();
                _fulltextindex.FreeMemory();
                _objStore.FreeMemory();
                _fileStore.FreeMemory();
                _objHF.FreeMemory();
                GC.Collect(2);
            }
        }

        private void UpgradeStorageFile(string filename, int ver)
        {
            _log.Debug("Upgrading storage file version from " + ver + " to " + StorageFile<int>._CurrentVersion + " on file : " + filename);
            throw new Exception("not implemented yet - contact the author if you need this functionality");
            // FEATURE : upgrade from v0 to v1

            // FEATURE : upgrade from v1 to v2
            // read from one file and write to the other 
        }

        //private void CurrentDomain_ProcessExit(object sender, EventArgs e)
        //{
            
        //    _log.Debug("appdomain closing");
        //    Shutdown();
        //}

        private object _slock = new object();
        private void _saveTimer_Elapsed(object sender, System.Timers.ElapsedEventArgs e)
        {
            if (_shuttingdown)
                return;

            if (Global.BackgroundSaveToOtherViews == false)
                return;

            if (_CurrentRecordNumber == 0)
                return;

            if (_CurrentRecordNumber == _LastRecordNumberProcessed)
                return;

            lock (_slock)
            {
                int batch = Global.BackgroundViewSaveBatchSize;
                while (batch > 0)
                {
                    if (_shuttingdown)
                        return;
                    while (_pauseindexer) Thread.Sleep(0);
                    if (_CurrentRecordNumber == _LastRecordNumberProcessed)
                        return;
                    _LastRecordNumberProcessed++;
                    StorageItem<Guid> meta = null;
                    object obj = _objStore.GetObject(_LastRecordNumberProcessed, out meta);
                    if (meta != null && meta.isDeleted)
                        _viewManager.Delete(meta.key);
                    else
                    {
                        if (obj == null)
                        {
                            _log.Debug("byte[] is null");
                            _log.Debug("curr rec = " + _CurrentRecordNumber);
                            _log.Debug("last rec = " + _LastRecordNumberProcessed);
                            continue;
                        }

                        var m = otherviews.MakeGenericMethod(new Type[] { obj.GetType() });
                        m.Invoke(this, new object[] { meta.key, obj });
                    }

                    batch--;
                }
            }
        }

        private object _flock = new object();
        void _fulltextTimer_Elapsed(object sender, System.Timers.ElapsedEventArgs e)
        {
            if (_shuttingdown)
                return;

            if (_CurrentRecordNumber == 0)
                return;

            if (_CurrentRecordNumber == _LastFulltextIndexed)
                return;

            lock (_flock)
            {
                int batch = Global.BackgroundFullTextIndexBatchSize;
                while (batch > 0)
                {
                    if (_shuttingdown)
                        return;
                    //_log.Debug("batch full text indexing...");
                    while (_pauseindexer) Thread.Sleep(0);
                    if (_CurrentRecordNumber == _LastFulltextIndexed)
                        return;
                    _LastFulltextIndexed++;
                    StorageItem<Guid> meta = null;
                    object obj = _objStore.GetObject(_LastFulltextIndexed, out meta);
                    if (meta != null && meta.isDeleted == false)
                    {
                        if (obj != null)
                        {
                            // normal string and normal guid 
                            string json = fastJSON.JSON.ToJSON(obj, new fastJSON.JSONParameters { UseEscapedUnicode = false, UseFastGuid = false });
                            _fulltextindex.Set(json, _LastFulltextIndexed);
                        }
                    }
                    batch--;
                }

                return;
            }
        }

        private MethodInfo GetSave(Type type)
        {
            MethodInfo m = null;
            if (_savecache.TryGetValue(type, out m))
                return m;

            m = save.MakeGenericMethod(new Type[] { type });
            _savecache.Add(type, m);
            return m;
        }

        private MethodInfo GetSaveReplicate(Type type)
        {
            MethodInfo m = null;
            if (_saverepcache.TryGetValue(type, out m))
                return m;

            m = saverep.MakeGenericMethod(new Type[] { type });
            _saverepcache.Add(type, m);
            return m;
        }
        #endregion

        internal object GetAssemblyForView(string viewname, out string typename)
        {
            return _viewManager.GetAssemblyForView(viewname, out typename);
        }

        /// <summary>
        /// Get the current registered views
        /// </summary>
        /// <returns></returns>
        public List<ViewBase> GetViews()
        {
            return _viewManager.GetViews();
        }

        /// <summary>
        /// Get the schema for a view
        /// </summary>
        /// <param name="view"></param>
        /// <returns></returns>
        public ViewRowDefinition GetSchema(string view)
        {
            return _viewManager.GetSchema(view);
        }

        /// <summary>
        /// Query a view with paging and ordering
        /// </summary>
        /// <param name="viewname"></param>
        /// <param name="filter"></param>
        /// <param name="start"></param>
        /// <param name="count"></param>
        /// <param name="orderby"></param>
        /// <returns></returns>
        public Result<object> Query(string viewname, string filter, int start, int count, string orderby)
        {
            return _viewManager.Query(viewname, filter, start, count, orderby);
        }

        /// <summary>
        /// Query a view with paging and ordering
        /// </summary>
        /// <typeparam name="TRowSchema"></typeparam>
        /// <param name="filter"></param>
        /// <param name="start"></param>
        /// <param name="count"></param>
        /// <param name="orderby"></param>
        /// <returns></returns>
        public Result<TRowSchema> Query<TRowSchema>(string filter, int start, int count, string orderby)
        {
            return _viewManager.Query<TRowSchema>(filter, start, count, orderby);
        }

        /// <summary>
        /// Get the history information for a document
        /// </summary>
        /// <param name="docid"></param>
        /// <returns></returns>
        public HistoryInfo[] FetchHistoryInfo(Guid docid)
        {
            List<HistoryInfo> h = new List<HistoryInfo>();

            foreach (int i in FetchHistory(docid))
            {
                HistoryInfo hi = new HistoryInfo();
                hi.Version = i;
                var o = _objStore.GetMeta(i);
                hi.ChangeDate = o.date;
                if (o.isDeleted == false)
                    h.Add(hi);
            }
            return h.ToArray();
        }

        /// <summary>
        /// Get the history information for a file
        /// </summary>
        /// <param name="docid"></param>
        /// <returns></returns>
        public HistoryInfo[] FetchBytesHistoryInfo(Guid docid)
        {
            List<HistoryInfo> h = new List<HistoryInfo>();

            foreach (int i in FetchBytesHistory(docid))
            {
                HistoryInfo hi = new HistoryInfo();
                hi.Version = i;
                var o = _fileStore.GetMeta(i);
                hi.ChangeDate = o.date;
                if (o.isDeleted == false)
                    h.Add(hi);
            }
            return h.ToArray();
        }

        /// <summary>
        /// Direct delete from a view
        /// </summary>
        /// <typeparam name="TRowSchema"></typeparam>
        /// <param name="filter"></param>
        /// <returns></returns>
        public int ViewDelete<TRowSchema>(Expression<Predicate<TRowSchema>> filter)
        {           
            // do the delete
            int c = _viewManager.ViewDelete(filter);
            if (c > 0)
            {
                // save this filter to docs
                View_delete vd = new View_delete();
                LINQString lq = new LINQString();
                lq.Visit(filter);
                vd.Filter = lq.sb.ToString();
                vd.Viewname = _viewManager.GetViewName(typeof(TRowSchema));
                _objStore.SetObject(vd.ID, vd);
            }
            return c;
        }

        /// <summary>
        /// Direct delete from a view
        /// </summary>
        /// <param name="viewname"></param>
        /// <param name="filter"></param>
        /// <returns></returns>
        public int ViewDelete(string viewname, string filter)
        {          
            // do the delete
            int c = _viewManager.ViewDelete(viewname, filter);
            if (c > 0)
            {
                // save this filter to docs
                View_delete vd = new View_delete();
                vd.Filter = filter;
                vd.Viewname = viewname;
                _objStore.SetObject(vd.ID, vd);
            }
            return c;
        }

        /// <summary>
        /// Direct insert into a view
        /// </summary>
        /// <typeparam name="TRowSchema"></typeparam>
        /// <param name="id"></param>
        /// <param name="row"></param>
        /// <returns></returns>
        public bool ViewInsert<TRowSchema>(Guid id, TRowSchema row)
        {
            string vn = _viewManager.GetViewName(typeof(TRowSchema));
            if (vn != "")
            {
                if (_viewManager.ViewInsert(id, row))
                {
                    View_insert vi = new View_insert();
                    vi.Viewname = vn;
                    vi.RowObject = row;
                    _objStore.SetObject(vi.ID, vi);
                    return true;
                }
            }
            return false;
        }

        /// <summary>
        /// Direct insert into a view
        /// </summary>
        /// <param name="viewname"></param>
        /// <param name="id"></param>
        /// <param name="row"></param>
        /// <returns></returns>
        public bool ViewInsert(string viewname, Guid id, object row)
        {
            if (_viewManager.ViewInsert(viewname, id, row))
            {
                View_insert vi = new View_insert();
                vi.Viewname = viewname;
                vi.RowObject = row;
                _objStore.SetObject(vi.ID, vi);
                return true;
            }
            return false;
        }

        /// <summary>
        /// Total number of documents in the storage file including duplicates
        /// </summary>
        /// <returns></returns>
        public long DocumentCount()
        {
            return _objStore.Count();
        }

        public IKeyStoreHF GetKVHF()
        {
            return _objHF;
        }
    }
}
