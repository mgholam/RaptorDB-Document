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

namespace RaptorDB
{
    public class RaptorDB : IDisposable
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
        private int _CurrentRecordNumber = -1;
        private System.Timers.Timer _saveTimer;
        private bool _shuttingdown = false;
        //private bool disposed = false;

        public void SaveBytes(Guid docID, byte[] bytes)
        {
            // save files in storage
            _fileStore.Set(docID, bytes);
        }
        //private object _lock = new object();
        public bool Save<T>(Guid docid, T data)
        {
            //lock (_lock)
            {
                string viewname = _viewManager.GetPrimaryViewForType(data.GetType());
                if (viewname == "")
                {
                    _log.Debug("Primary View not defined for object : " + data.GetType());
                    return false;
                }

                int recnum = SaveData(docid, data);
                _CurrentRecordNumber = recnum;

                SaveInPrimaryView(viewname, docid, data);

                if (Global.BackgroundSaveToOtherViews == false)
                {
                    SaveInOtherViews(docid, data);
                    _LastRecordNumberProcessed = recnum;
                }
                return true;
            }
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

        public Result Query(string viewname, string filter)
        {
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

        public object Fetch(Guid docID)
        {
            byte[] b = null;
            if (_objStore.Get(docID, out b))
            {
                return CreateObject(b);
            }
            else
                return null;
        }

        public byte[] FetchBytes(Guid fileID)
        {
            byte[] b = null;
            if (_fileStore.Get(fileID, out b))
            {
                return b;
            }
            else
                return null;
        }

        public Result RegisterView<T>(View<T> view)
        {
            return _viewManager.RegisterView(view);
        }

        private object _sh = new object();
        public void Shutdown()
        {

            if (_shuttingdown == true)
                return;

            lock (_sh)
            {

                _shuttingdown = true;
                // save _LastRecordNumberProcessed here
                _log.Debug("last record = " + _LastRecordNumberProcessed);
                File.WriteAllBytes(_Path + "Data\\_lastrecord.rec", Helper.GetBytes(_LastRecordNumberProcessed, false));
                _log.Debug("Shutting down");
                _saveTimer.Stop();
                _viewManager.ShutDown();
                Thread.Sleep(2000);
                _objStore.Shutdown();
                _fileStore.Shutdown();
                LogManager.Shutdown();
            }
        }

        public void Dispose()
        {
            Shutdown();
        }

        #region [            P R I V A T E     M E T H O D S              ]

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

            // load logger
            LogManager.Configure(_Path + "Logs\\log.txt", 500, false);

            _log.Debug("RaptorDB starting...");

            _objStore = new KeyStoreGuid(_Path + "Data\\data");
            _fileStore = new KeyStoreGuid(_Path + "Data\\files");

            _viewManager = new Views.ViewManager(_Path + "Views", _objStore);

            // load _LastRecordNumberProcessed 
            if (File.Exists(_Path + "Data\\_lastrecord.rec"))
            {
                byte[] b = File.ReadAllBytes(_Path + "Data\\_lastrecord.rec");
                _LastRecordNumberProcessed = Helper.ToInt32(b, 0, false);
            }
            _CurrentRecordNumber = _objStore.RecordCount();

            otherviews = this.GetType().GetMethod("SaveInOtherViews", BindingFlags.Instance | BindingFlags.NonPublic);

            // start backround save to views
            _saveTimer = new System.Timers.Timer(Global.BackgroundSaveViewTimer * 1000);
            _saveTimer.Elapsed += new System.Timers.ElapsedEventHandler(_saveTimer_Elapsed);
            _saveTimer.Enabled = true;
            _saveTimer.AutoReset = true;
            _saveTimer.Start();
        }

        private void CurrentDomain_ProcessExit(object sender, EventArgs e)
        {
            Shutdown();
        }

        private object _slock = new object();
        MethodInfo otherviews = null;
        private void _saveTimer_Elapsed(object sender, System.Timers.ElapsedEventArgs e)
        {
            if (Global.BackgroundSaveToOtherViews == false)
                return;

            if (_CurrentRecordNumber == 0)
                return;
            //return;
            if (_CurrentRecordNumber == _LastRecordNumberProcessed)
                return;

            lock (_slock)
            {
                int batch = 1000000;
                while (batch > 0)
                {
                    if (_shuttingdown)
                        return;
                    if (_CurrentRecordNumber == _LastRecordNumberProcessed)
                        return;
                    _LastRecordNumberProcessed++;
                    Guid docid;
                    byte[] b = _objStore.Get(_LastRecordNumberProcessed, out docid);
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

                    batch--;
                }
            }
        }
        #endregion
    }
}
