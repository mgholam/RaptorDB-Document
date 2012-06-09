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

namespace RaptorDB
{
    public class RaptorDB : IDisposable, IRaptorDB
    {
        private RaptorDB(string FolderPath)
        {
            Initialize(FolderPath);
        }

        public static RaptorDB Open(string FolderPath)
        {
            return new RaptorDB(FolderPath);
        }

        private ILog _log = LogManager.GetLogger(typeof(RaptorDB));
        private Views.ViewManager _viewManager;
        private KeyStoreGuid _objStore;
        private KeyStoreGuid _fileStore;
        private string _Path = "";
        private int _LastRecordNumberProcessed = -1; // used by background saver
        private int _LastBackupRecordNumber = -1;
        private int _CurrentRecordNumber = -1;
        private System.Timers.Timer _saveTimer;
        private bool _shuttingdown = false;
        private bool _pauseindexer = false;
        private MethodInfo otherviews = null;
        private MethodInfo save = null;
        private SafeDictionary<Type, MethodInfo> _savecache = new SafeDictionary<Type, MethodInfo>();

        internal string GetViewName(Type type)
        {
            return _viewManager.GetViewName(type);
        }

        /// <summary>
        /// Save files to RaptorDB
        /// </summary>
        /// <param name="docID"></param>
        /// <param name="bytes"></param>
        public bool SaveBytes(Guid docID, byte[] bytes)
        {
            // save files in storage
            _fileStore.Set(docID, bytes);
            return true;
        }

        public bool Delete(Guid docid)
        {
            bool b = _objStore.Delete(docid);
            _viewManager.Delete(docid);
            return b;
        }

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
            if (viewname == "")
            {
                _log.Debug("Primary View not defined for object : " + data.GetType());
                return false;
            }
            _pauseindexer = true;

            int recnum = SaveData(docid, data);
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

        /// <summary>
        /// Query any view -> get all rows
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="viewname"></param>
        /// <returns></returns>
        public Result Query(string viewname)
        {
            return _viewManager.Query(viewname);
        }

        /// <summary>
        /// Query a primary view -> get all rows
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="view"></param>
        /// <returns></returns>
        public Result Query(Type view)
        {
            return _viewManager.Query(view);
        }

        /// <summary>
        /// Query a view using a string filter
        /// </summary>
        /// <param name="viewname"></param>
        /// <param name="filter"></param>
        /// <returns></returns>
        public Result Query(string viewname, string filter)
        {
            if (filter == "")
                return _viewManager.Query(viewname);

            return _viewManager.Query(viewname, filter);
        }

        // FEATURE : add paging to queries -> start, count
        /// <summary>
        /// Query any view with filters
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="viewname">view name</param>
        /// <param name="filter"></param>
        /// <returns></returns>
        public Result Query<T>(string viewname, Expression<Predicate<T>> filter)
        {
            return _viewManager.Query(viewname, filter);
        }

        /// <summary>
        /// Query a view with filters
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="view">base entity type, or typeof the view </param>
        /// <param name="filter"></param>
        /// <returns></returns>
        public Result Query<T>(Type type, Expression<Predicate<T>> filter)
        {
            return _viewManager.Query(type, filter);
        }

        /// <summary>
        /// Fetch a document by it's ID
        /// </summary>
        /// <param name="docID"></param>
        /// <returns></returns>
        public object Fetch(Guid docID)
        {
            byte[] b = null;
            if (_objStore.Get(docID, out b))
                return CreateObject(b);
            else
                return null;
        }

        /// <summary>
        /// Fetch file data by it's ID
        /// </summary>
        /// <param name="fileID"></param>
        /// <returns></returns>
        public byte[] FetchBytes(Guid fileID)
        {
            byte[] b = null;
            if (_fileStore.Get(fileID, out b))
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

        public void Shutdown()
        {

            if (_shuttingdown == true)
                return;

            _shuttingdown = true;
            // save records 
            _log.Debug("last record = " + _LastRecordNumberProcessed);
            File.WriteAllBytes(_Path + "Data\\_lastrecord.rec", Helper.GetBytes(_LastRecordNumberProcessed, false));
            _log.Debug("last backup record = " + _LastBackupRecordNumber);
            File.WriteAllBytes(_Path + "Backup\\LastBackupRecord.rec", Helper.GetBytes(_LastBackupRecordNumber, false));
            _log.Debug("Shutting down");
            _saveTimer.Stop();
            _viewManager.ShutDown();
            _objStore.Shutdown();
            _fileStore.Shutdown();
            LogManager.Shutdown();
        }

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
                _log.Debug("Backup Started");
                string tempp = _Path + "Temp\\" + DateTime.Now.ToString("yyyy-MM-dd-HH-mm");
                Directory.CreateDirectory(tempp);
                StorageFile<int> backup = new StorageFile<int>(tempp + "\\backup.mgdat");
                _log.Debug("Copying data to backup");
                if (_LastBackupRecordNumber == -1)
                    _LastBackupRecordNumber = 0;
                int rec = _objStore.CopyTo(backup, _LastBackupRecordNumber);
                backup.Shutdown();
                _LastBackupRecordNumber = rec;
                _log.Debug("Last backup rec# = " + rec);

                // compress the file
                using (FileStream read = File.OpenRead(tempp + "\\backup.mgdat"))
                using (FileStream outp = File.Create(tempp + "\\backup.mgdat.gz"))
                    Compress(read, outp);

                _log.Debug("Backup compressed and done");
                File.Move(tempp + "\\backup.mgdat.gz", _Path + "Backup\\" + DateTime.Now.ToString("yyyy-MM-dd-HH-mm") + ".mgdat.gz");

                _log.Debug("last backup record = " + _LastBackupRecordNumber);
                File.WriteAllBytes(_Path + "Backup\\LastBackupRecord.rec", Helper.GetBytes(_LastBackupRecordNumber, false));
                // cleanup temp folder
                Directory.Delete(tempp, true);
                return true;
            }
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
                    // do restore 
                    string[] files = Directory.GetFiles(_Path + "Restore\\", "*.gz");
                    Array.Sort(files);
                    _log.Debug("Restoring file count = " + files.Length);

                    foreach (string file in files)
                    {
                        string tmp = file.Replace(".gz", "");// FEATURE : to temp folder ??
                        if (File.Exists(tmp))
                            File.Delete(tmp);
                        using (FileStream read = File.OpenRead(file))
                        using (FileStream outp = File.Create(tmp))
                            Decompress(read, outp);
                        _log.Debug("Uncompress done : " + Path.GetFileName(tmp));
                        StorageFile<int> sf = StorageFile<int>.ReadForward(tmp);
                        foreach (var i in sf.Enumerate())
                        {
                            int len = Helper.ToInt32(i.Data, 0, false);
                            byte[] key = new byte[len];
                            Buffer.BlockCopy(i.Data, 4, key, 0, len);
                            Guid g = new Guid(key);
                            if (i.isDeleted)
                                Delete(g);
                            else
                            {
                                byte[] d = new byte[i.Data.Length - 4 - len];
                                Buffer.BlockCopy(i.Data, 4 + len, d, 0, i.Data.Length - 4 - len);
                                object obj = CreateObject(d);
                                var m = GetSave(obj.GetType());
                                //var m = save.MakeGenericMethod(new Type[] { obj.GetType() });
                                m.Invoke(this, new object[] { g, obj });
                            }
                        }
                        sf.Shutdown();
                        _log.Debug("File restore complete : " + Path.GetFileName(tmp));
                        File.Delete(tmp);
                        File.Move(file, _Path + "\\Restore\\Done\\" + Path.GetFileName(file));
                    }
                }
                catch (Exception ex)
                {
                    _log.Error(ex);
                }
            }
        }

        public bool AddUser(string username, string oldpassword, string newpassword)
        {
            return false;
        }

        public void Dispose()
        {
            _log.Debug("dispose called");
            Shutdown();
            GC.SuppressFinalize(this);
        }

        #region [            P R I V A T E     M E T H O D S              ]

        private static void Pump(Stream input, Stream output)
        {
            byte[] bytes = new byte[4096 * 2];
            int n;
            while ((n = input.Read(bytes, 0, bytes.Length)) != 0)
                output.Write(bytes, 0, n);
        }

        private static void Compress(Stream source, Stream destination)
        {
            using (GZipStream gz = new GZipStream(destination, CompressionMode.Compress))
                Pump(source, gz);
        }

        private static void Decompress(Stream source, Stream destination)
        {
            using (GZipStream gz = new GZipStream(source, CompressionMode.Decompress))
                Pump(gz, destination);
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
                return fastBinaryJSON.BJSON.Instance.ToObject(b);
            else
                return fastJSON.JSON.Instance.ToObject(Encoding.ASCII.GetString(b));
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

        private int SaveData(Guid docid, object data)
        {
            byte[] b = null;
            if (Global.SaveAsBinaryJSON)
                b = fastBinaryJSON.BJSON.Instance.ToBJSON(data);
            else
            {
                string s = fastJSON.JSON.Instance.ToJSON(data);
                b = Encoding.ASCII.GetBytes(s); // json already ascii encoded
            }
            return _objStore.Set(docid, b);
        }

        private void Initialize(string foldername)
        {
            AppDomain.CurrentDomain.ProcessExit += new EventHandler(CurrentDomain_ProcessExit);

            // create folders 
            Directory.CreateDirectory(foldername);
            foldername = Path.GetFullPath(foldername);
            if (foldername.EndsWith("\\") == false)
                foldername += "\\";
            _Path = foldername;

            Directory.CreateDirectory(_Path + "Data");
            Directory.CreateDirectory(_Path + "Views");
            Directory.CreateDirectory(_Path + "Logs");
            Directory.CreateDirectory(_Path + "Temp");
            Directory.CreateDirectory(_Path + "Backup");
            Directory.CreateDirectory(_Path + "Restore");
            Directory.CreateDirectory(_Path + "Restore\\Done");
            // load logger
            LogManager.Configure(_Path + "Logs\\log.txt", 500, false);

            _log.Debug("RaptorDB starting...");
            _log.Debug("RaptorDB data folder = " + _Path);


            _objStore = new KeyStoreGuid(_Path + "Data\\data");
            _fileStore = new KeyStoreGuid(_Path + "Data\\files");

            _viewManager = new Views.ViewManager(_Path + "Views", _objStore);

            // load _LastRecordNumberProcessed 
            if (File.Exists(_Path + "Data\\_lastrecord.rec"))
            {
                byte[] b = File.ReadAllBytes(_Path + "Data\\_lastrecord.rec");
                _LastRecordNumberProcessed = Helper.ToInt32(b, 0, false);
            }
            // load _LastBackupRecordNumber 
            if (File.Exists(_Path + "Backup\\LastBackupRecord.rec"))
            {
                byte[] b = File.ReadAllBytes(_Path + "Backup\\LastBackupRecord.rec");
                _LastBackupRecordNumber = Helper.ToInt32(b, 0, false);
            }
            _CurrentRecordNumber = _objStore.RecordCount();

            otherviews = this.GetType().GetMethod("SaveInOtherViews", BindingFlags.Instance | BindingFlags.NonPublic);
            save = this.GetType().GetMethod("Save", BindingFlags.Instance | BindingFlags.Public);

            // start backround save to views
            _saveTimer = new System.Timers.Timer(Global.BackgroundSaveViewTimer * 1000);
            _saveTimer.Elapsed += new System.Timers.ElapsedEventHandler(_saveTimer_Elapsed);
            _saveTimer.Enabled = true;
            _saveTimer.AutoReset = true;
            _saveTimer.Start();
        }

        private void CurrentDomain_ProcessExit(object sender, EventArgs e)
        {
            _log.Debug("appdomain closing");
            Shutdown();
            GC.SuppressFinalize(this);
        }

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
                    Guid docid;
                    bool isdeleted = false;
                    byte[] b = _objStore.Get(_LastRecordNumberProcessed, out docid, out isdeleted);
                    if (isdeleted)
                        _viewManager.Delete(docid);
                    else
                    {
                        if (b == null)
                        {
                            _log.Debug("byte[] is null");
                            _log.Debug("curr rec = " + _CurrentRecordNumber);
                            _log.Debug("last rec = " + _LastRecordNumberProcessed);
                            continue;
                        }
                        object obj = CreateObject(b);

                        var m = otherviews.MakeGenericMethod(new Type[] { obj.GetType() });
                        m.Invoke(this, new object[] { docid, obj });
                    }

                    batch--;
                }
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
        #endregion
    }
}
