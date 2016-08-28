using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Threading;
using RaptorDB.Common;

namespace RaptorDB.Views
{
    internal class ViewManager
    {
        public ViewManager(string viewfolder, IDocStorage<Guid> objstore, IKeyStoreHF kvhf)
        {
            _Path = viewfolder;
            _objectStore = objstore;
            _kvhf = kvhf;
        }

        private IKeyStoreHF _kvhf;
        private IDocStorage<Guid> _objectStore;
        private ILog _log = LogManager.GetLogger(typeof(ViewManager));
        private string _Path = "";
        // list of views
        private SafeDictionary<string, ViewHandler> _views = new SafeDictionary<string, ViewHandler>();
        // primary view list
        private SafeDictionary<Type, string> _primaryView = new SafeDictionary<Type, string>();
        // like primary view list 
        private SafeDictionary<Type, string> _otherViewTypes = new SafeDictionary<Type, string>();
        // consistent views
        private SafeDictionary<Type, List<string>> _consistentViews = new SafeDictionary<Type, List<string>>();
        // other views type->list of view names to call
        private SafeDictionary<Type, List<string>> _otherViews = new SafeDictionary<Type, List<string>>();
        private TaskQueue _que = new TaskQueue();
        private SafeDictionary<int, bool> _transactions = new SafeDictionary<int, bool>();

        internal int Count(string viewname, string filter)
        {
            ViewHandler view = null;
            // find view from name
            if (_views.TryGetValue(viewname.ToLower(), out view))
                return view.Count(filter);

            _log.Error("view not found", viewname);
            return 0;
        }

        internal Result<object> Query(string viewname, string filter, int start, int count)
        {
            return Query(viewname, filter, start, count, "");
        }

        internal Result<object> Query(string viewname, int start, int count)
        {
            ViewHandler view = null;
            // find view from name
            if (_views.TryGetValue(viewname.ToLower(), out view))
                return view.Query(start, count);

            _log.Error("view not found", viewname);
            return new Result<object>(false, new Exception("view not found : " + viewname));
        }

        internal void Insert<T>(string viewname, Guid docid, T data)
        {
            ViewHandler vman = null;
            // find view from name
            if (_views.TryGetValue(viewname.ToLower(), out vman))
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

        internal bool InsertTransaction<T>(string viewname, Guid docid, T data)
        {
            ViewHandler vman = null;
            // find view from name
            if (_views.TryGetValue(viewname.ToLower(), out vman))
            {
                if (vman._view.isActive == false)
                {
                    _log.Debug("view is not active, skipping insert : " + viewname);
                    return false;
                }

                return vman.InsertTransaction<T>(docid, data);
            }
            _log.Error("view not found", viewname);
            return false;
        }

        internal object Fetch(Guid guid)
        {
            object b = null;
            _objectStore.GetObject(guid, out b);

            return b;
        }

        internal string GetPrimaryViewForType(Type type)
        {
            string vn = "";
            if (type == null || type == typeof(object)) // reached the end
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

        internal string GetViewName(Type type) // used for queries
        {
            string viewname = null;
            // find view from name

            viewname = GetPrimaryViewForType(type);
            if (viewname != "")
                return viewname;

            // search for viewtype here
            if (_otherViewTypes.TryGetValue(type, out viewname))
                return viewname;

            return "";
        }

        internal void RegisterView<T>(View<T> view)
        {
            view.Verify();

            ViewHandler vh = null;
            if (_views.TryGetValue(view.Name.ToLower(), out vh))
            {
                _log.Error("View already added and exists : " + view.Name);
            }
            else
            {
                vh = new ViewHandler(_Path, this);
                vh.SetView(view, _objectStore);
                _views.Add(view.Name.ToLower(), vh);
                _otherViewTypes.Add(view.GetType(), view.Name.ToLower());

                // add view schema mapping 
                _otherViewTypes.Add(view.Schema, view.Name.ToLower());

                Type basetype = vh.GetFireOnType();
                if (view.isPrimaryList)
                {
                    _primaryView.Add(basetype, view.Name.ToLower());
                }
                else
                {
                    if (view.ConsistentSaveToThisView)
                        AddToViewList(_consistentViews, basetype, view.Name);
                    else
                        AddToViewList(_otherViews, basetype, view.Name);
                }
            }
        }

        internal void ShutDown()
        {
            _log.Debug("View Manager shutdown");
            // shutdown views
            foreach (var v in _views)
            {
                try
                {
                    _log.Debug(" shutting down view : " + v.Value._view.Name);
                    v.Value.Shutdown();
                }
                catch (Exception ex)
                {
                    _log.Error(ex);
                }
            }
            _que.Shutdown();
        }

        internal List<string> GetConsistentViews(Type type)
        {
            List<string> list = new List<string>();
            _consistentViews.TryGetValue(type, out list);
            return list;
        }

        private void AddToViewList(SafeDictionary<Type, List<string>> diclist, Type fireontype, string viewname)
        {
            //foreach (var tn in view.FireOnTypes)
            {
                List<string> list = null;
                Type t = fireontype;// Type.GetType(tn);
                if (diclist.TryGetValue(t, out list))
                    list.Add(viewname);
                else
                {
                    list = new List<string>();
                    list.Add(viewname);
                    diclist.Add(t, list);
                }
            }
        }

        internal void Delete(Guid docid)
        {
            // remove from all views
            foreach (var v in _views)
                v.Value.Delete(docid);
        }

        internal void Rollback(int ID)
        {
            _log.Debug("ROLLBACK");
            // rollback all views with tran id
            foreach (var v in _views)
                v.Value.RollBack(ID);

            _transactions.Remove(ID);
        }

        internal void Commit(int ID)
        {
            _log.Debug("COMMIT");
            // commit all data in vews with tran id
            foreach (var v in _views)
                v.Value.Commit(ID);

            _transactions.Remove(ID);
        }

        internal bool isTransaction(string viewname)
        {
            return _views[viewname.ToLower()]._view.TransactionMode;
        }

        internal bool inTransaction()
        {
            bool b = false;
            return _transactions.TryGetValue(Thread.CurrentThread.ManagedThreadId, out b);
        }

        internal void StartTransaction()
        {
            _transactions.Add(Thread.CurrentThread.ManagedThreadId, false);
        }

        internal Result<T> Query<T>(Expression<Predicate<T>> filter, int start, int count)
        {
            return Query<T>(filter, start, count, "");
        }

        internal Result<T> Query<T>(Expression<Predicate<T>> filter, int start, int count, string orderby)
        {
            string view = GetViewName(typeof(T));

            ViewHandler vman = null;
            // find view from name
            if (_views.TryGetValue(view.ToLower(), out vman))
            {
                return vman.QueryWithTypedResult<T>(filter, start, count, orderby);
            }
            return new Result<T>(false, new Exception("View not found"));
        }

        internal Result<T> Query<T>(string filter, int start, int count)
        {
            return Query<T>(filter, start, count, "");
        }

        internal Result<T> Query<T>(string filter, int start, int count, string orderby)
        {
            string view = GetViewName(typeof(T));

            ViewHandler vman = null;
            // find view from name
            if (_views.TryGetValue(view.ToLower(), out vman))
            {
                return vman.QueryWithTypedResult<T>(filter, start, count, orderby);
            }
            return new Result<T>(false, new Exception("View not found"));
        }

        internal int Count<T>(Expression<Predicate<T>> filter)
        {
            string view = GetViewName(typeof(T));

            ViewHandler vman = null;
            // find view from name
            if (_views.TryGetValue(view.ToLower(), out vman))
            {
                return vman.Count<T>(filter);
            }
            return 0;
        }

        internal void FreeMemory()
        {
            foreach (var v in _views)
                v.Value.FreeMemory();
        }

        internal object GetAssemblyForView(string viewname, out string typename)
        {
            ViewHandler view = null;
            typename = "";
            // find view from name
            if (_views.TryGetValue(viewname.ToLower(), out view))
            {
                return view.GetAssembly(out typename);
            }
            return null;
        }

        internal List<ViewBase> GetViews()
        {
            List<ViewBase> o = new List<ViewBase>();
            foreach (var i in _views)
                o.Add(i.Value._view);
            return o;
        }

        internal ViewRowDefinition GetSchema(string view)
        {
            ViewHandler v = null;
            if (_views.TryGetValue(view.ToLower(), out v))
            {
                return v.GetSchema();
            }
            return null;
        }

        internal Result<object> Query(string viewname, string filter, int start, int count, string orderby)
        {
            ViewHandler view = null;
            // find view from name
            if (_views.TryGetValue(viewname.ToLower(), out view))
                return view.Query(filter, start, count, orderby);

            _log.Error("view not found", viewname);
            return new Result<object>(false, new Exception("view not found : " + viewname));
        }

        internal int ViewDelete<T>(Expression<Predicate<T>> filter)
        {
            string view = GetViewName(typeof(T));

            ViewHandler vman = null;
            // find view from name
            if (_views.TryGetValue(view.ToLower(), out vman))
            {
                return vman.ViewDelete<T>(filter);
            }
            return -1;
        }

        internal int ViewDelete(string viewname, string filter)
        {
            ViewHandler view = null;
            // find view from name
            if (_views.TryGetValue(viewname.ToLower(), out view))
                return view.ViewDelete(filter);
            return -1;
        }

        internal bool ViewInsert<T>(Guid id, T row)
        {
            string view = GetViewName(typeof(T));

            ViewHandler vman = null;
            // find view from name
            if (_views.TryGetValue(view.ToLower(), out vman))
            {
                return vman.ViewInsert(id, row);
            }
            return false;
        }

        internal bool ViewInsert(string viewname, Guid id, object row)
        {
            ViewHandler vman = null;
            // find view from name
            if (_views.TryGetValue(viewname.ToLower(), out vman))
            {
                return vman.ViewInsert(id, row);
            }
            return false;
        }

        internal IKeyStoreHF GetKVHF()
        {
            return _kvhf;
        }
    }
}
