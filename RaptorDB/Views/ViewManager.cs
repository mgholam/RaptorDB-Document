using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
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
        // list of views
        private SafeDictionary<string, ViewHandler> _views = new SafeDictionary<string, ViewHandler>();
        // primary view list
        private SafeDictionary<Type, string> _primaryView = new SafeDictionary<Type, string>();
        // other views type->list of view names to call
        private SafeDictionary<Type, List<string>> _otherViews = new SafeDictionary<Type, List<string>>();
        private TaskQueue _que = new TaskQueue();

        internal Result Query<T>(Type objtype, Expression<Predicate<T>> filter)
        {
            string viewname = null;
            // find view from name
            if (_primaryView.TryGetValue(objtype, out viewname))
                return Query(viewname, filter);

            // FIX : add search for viewtype here

            
            _log.Error("view not found", viewname);
            return new Result(false, new Exception("view not found : "+ viewname));
        }

        internal Result Query(string viewname, string filter)
        {
            ViewHandler view = null;
            // find view from name
            if (_views.TryGetValue(viewname, out view))
                return view.Query(filter);

            _log.Error("view not found", viewname);
            return new Result(false, new Exception("view not found : " + viewname));
        }

        internal Result Query<T>(string viewname, Expression<Predicate<T>> filter)
        {
            ViewHandler view = null;
            // find view from name
            if (_views.TryGetValue(viewname, out view))
                return view.Query(filter);
            
            _log.Error("view not found", viewname);
            return new Result(false, new Exception("view not found : " + viewname));
        }

        internal Result Query(string viewname)
        {
            ViewHandler view = null;
            // find view from name
            if (_views.TryGetValue(viewname, out view))
                return view.Query();

            _log.Error("view not found", viewname);
            return new Result(false, new Exception("view not found : " + viewname));
        }

        internal Result Query(Type view)
        {
            string viewname = null;
            // find view from name
            if (_primaryView.TryGetValue(view, out viewname))
                return Query(viewname);

            // FIX : add search for viewtype here

            _log.Error("view not found", viewname);
            return new Result(false, new Exception("view not found : " + viewname));
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
                    _que.AddTask(() => vman.Insert<T>(docid, data));
                else
                    vman.Insert<T>(docid, data);

                return;
            }
            _log.Error("view not found", viewname);
        }

        public object Fetch(Guid guid)
        {
            byte[] b = null;
            if (_objectStore.Get(guid, out b))
                return fastJSON.JSON.Instance.ToObject(Encoding.ASCII.GetString(b));
            
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
            Result ret = view.Verify();
            if (ret.OK == false) return ret;

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
                        // add to other views
                        List<string> list = null;
                        Type t = Type.GetType(tn);
                        if (_otherViews.TryGetValue(t, out list))
                            list.Add(view.Name);
                        else
                        {
                            list = new List<string>();
                            list.Add(view.Name);
                            _otherViews.Add(t, list);
                        }
                    }
                }
            }

            // FEATURE : add existing data to this view

            return new Result(true);
        }

        internal void ShutDown()
        {
            _log.Debug("View Manager shutdown");
            _que.Shutdown();
            // shutdown views
            foreach (var v in _views)
            {
                _log.Debug(" shutting down view : " + v.Value._view.Name);
                v.Value.Shutdown();
            }
        }
    }
}
