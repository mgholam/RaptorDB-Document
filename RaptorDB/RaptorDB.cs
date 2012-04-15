using System;
using System.Collections.Generic;
using System.Text;
using System.IO;
using System.Threading;
using System.Collections;
using RaptorDB.Views;
using System.Linq.Expressions;
using System.Threading.Tasks;

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
        private int _LastRecordNumberProcessed = -1;

        public void SaveBytes(Guid docID, byte[] bytes)
        {
            // save files in storage
            _fileStore.Set(docID, bytes);
        }

        public bool Save<T>(Guid docid, T data)
        {
            string viewname = _viewManager.GetPrimaryViewForType(data.GetType());
            if (viewname == "")
            {
                _log.Debug("Primary View not defined for object : " + data.GetType());
                return false;
            }

            int recnum = SaveData(docid, data);

            SaveInPrimaryView(viewname, docid, data);

            SaveInOtherViews(docid, data, recnum);

            return true;
        }

        public Result Query<T>(string viewname, Expression<Predicate<T>> filter)//, int start, int count)
        {
            return _viewManager.Query(viewname, filter, 0, 0);
        }

        public Result Query<T>(Type view, Expression<Predicate<T>> filter)//, int start, int count)
        {
            return _viewManager.Query(view, filter, 0, 0);
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

        private object CreateObject(byte[] b)
        {
            if (b[0] < 32)
                return fastBinaryJSON.BJSON.Instance.ToObject(b);
            else
                return fastJSON.JSON.Instance.ToObject(Encoding.ASCII.GetString(b));
        }

        public object Fetch(int recnumber, out Guid docid)
        {
            byte[] b = _objStore.Get(recnumber, out docid);

            return CreateObject(b);
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

        #region [            P R I V A T E     M E T H O D S              ]
        private void SaveInOtherViews(Guid docid, object data, int recordnumber)
        {
            if (Global.SyncSaves)
            {
                List<string> list = _viewManager.GetOtherViewsList(data.GetType());
                if (list != null)
                {
                    foreach (string name in list)
                    {
                        _viewManager.Insert(name, docid, data);
                        _LastRecordNumberProcessed = recordnumber;
                    }
                }
            }
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

            // FEATURE : start backround indexer

        }
        #endregion
        private void Shutdown()
        {
            _log.Debug("Shutting down");
            _objStore.Shutdown();
            _fileStore.Shutdown();
            _viewManager.ShutDown();
            LogManager.Shutdown();
        }

        public void Dispose()
        {
            Shutdown();
        }
    }
}
