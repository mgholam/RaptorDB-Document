using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using RaptorDB.Mapping;
using System.Linq.Expressions;
using System.Threading.Tasks;
using System.Threading;

namespace RaptorDB.Views
{
    internal class ViewManager
    {
        public ViewManager(string viewfolder, KeyStoreGuid objstore)
        {
            _Path = viewfolder;
            _objectStore = objstore;
        }

        private KeyStoreGuid _objectStore;
        private ILog _log = LogManager.GetLogger(typeof(ViewManager));
        private string _Path = "";
        private SafeDictionary<string, ViewHandler> _views = new SafeDictionary<string, ViewHandler>();
        private SafeDictionary<Type, string> _primaryView = new SafeDictionary<Type, string>();
        private SafeDictionary<Type, List<string>> _otherViews = new SafeDictionary<Type, List<string>>();
        private TaskQueue _que = new TaskQueue();

        internal Result Query<T>(Type objtype, Expression<Predicate<T>> filter, int start, int count)
        {
            string viewname = null;
            // find view from name
            if (_primaryView.TryGetValue(objtype, out viewname))
            {
                return Query(viewname, filter, start, count);
            }
            _log.Error("view not found", viewname);
            return null;
        }

        internal Result Query<T>(string viewname, Expression<Predicate<T>> filter, int start, int count)
        {
            ViewHandler view = null;
            // find view from name
            if (_views.TryGetValue(viewname, out view))
            {
                return view.Query(filter, start, count);
            }
            _log.Error("view not found", viewname);
            return null;
        }

        internal void Insert<T>(string viewname, Guid docid, T data)
        {
            ViewHandler vman = null;
            // find view from name
            if (_views.TryGetValue(viewname, out vman))
            {
                if (vman._view.isActive == false)
                {
                    _log.Debug("view is not active, skipping insert : " + viewname);
                    return;
                }
                if (vman._view.BackgroundIndexing) 
                    _que.AddTask(() => vman.Insert(docid, data));
                else
                    vman.Insert(docid, data);
                return;
            }
            _log.Error("view not found", viewname);
        }

        //public DataList CallMapFunction(string viewname, Guid docid, object data)
        //{
        //    return _mapengine.Execute(_Path + viewname + "\\" + viewname + ".dll", docid, data);
        //}

        public object Fetch(Guid guid)
        {
            byte[] b = null;
            if (_objectStore.Get(guid, out b))
            {
                return fastJSON.JSON.Instance.ToObject(Encoding.ASCII.GetString(b));
            }
            return null;
        }

        internal string GetPrimaryViewForType(Type type)
        {
            string vn = "";
            if (type == null)
                return vn;
            // find direct
            if (_primaryView.TryGetValue(type, out vn))
                return vn;
            // recurse basetype
            return GetPrimaryViewForType(type.BaseType);
        }

        internal List<string> GetOtherViewsList(Type type)
        {
            List<string> list = new List<string>();
            _otherViews.TryGetValue(type, out list);
            return list;
        }

        internal Result RegisterView<T>(View<T> view)
        {
            if (view.Verify() == false) return new Result(false);
            // FEATURE : check if view name exists in memory -> replace
            //       serialize to folder

            ViewHandler vh = null;
            if (_views.TryGetValue(view.Name, out vh))
            {
                // FEATURE : already exists -> replace? regen?
                _log.Error("View already added and exists : " + view.Name);
            }
            else
            {
                vh = new ViewHandler(_Path, this);
                vh.SetView(view);
                _views.Add(view.Name, vh);
                if (view.isPrimaryList)
                {
                    foreach (string tn in view.FireOnTypes)
                        _primaryView.Add(Type.GetType(tn), view.Name);
                }
                else
                {
                    foreach (string tn in view.FireOnTypes)
                    {
                        // FIX : add to other views
                        //_otherViews.Add(Type.GetType(tn), view.Name);
                    }
                }
            }

            return new Result(true);
        }

        internal void ShutDown()
        {
            _log.Debug("View Manager shutdown");
            _que.Shutdown();
            // shutdown views
            foreach (var v in _views)
            {
                v.Value.Shutdown();
            }
        }
    }
}
