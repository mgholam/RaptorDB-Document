using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Linq.Expressions;
using System.Threading.Tasks;
using System.Threading;
using RaptorDB.Common;

namespace RaptorDB.Views
{
    internal class ViewManager
    {
        public ViewManager(string viewfolder, IDocStorage<Guid> objstore)
        {
            _Path = viewfolder;
            _objectStore = objstore;
        }

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

        internal int Count<T>(Type objtype, Expression<Predicate<T>> filter)
        {
            string viewname = null;
            // find view from name
            if (_primaryView.TryGetValue(objtype, out viewname))
                return Count(viewname, filter);

            // search for viewtype here
            if (_otherViewTypes.TryGetValue(objtype, out viewname))
                return Count(viewname, filter);

            _log.Error("view not found", viewname);
            return 0;
        }
             
        internal int Count(Type objtype, string filter)
        {
            string viewname = null;
            // find view from name
            if (_primaryView.TryGetValue(objtype, out viewname))
                return Count(viewname, filter);

            // search for viewtype here
            if (_otherViewTypes.TryGetValue(objtype, out viewname))
                return Count(viewname, filter);

            _log.Error("view not found", viewname);
            return 0;
        }

        internal int Count<T>(string viewname, Expression<Predicate<T>> filter)
        {
            ViewHandler view = null;
            // find view from name
            if (_views.TryGetValue(viewname.ToLower(), out view))
                return view.Count(filter);

            _log.Error("view not found", viewname);
            return 0;
        }

        internal int Count(string  viewname, string filter)
        {
            ViewHandler view = null;
            // find view from name
            if (_views.TryGetValue(viewname.ToLower(), out view))
                return view.Count(filter);

            _log.Error("view not found", viewname);
            return 0;
        }

        internal Result<object> Query<T>(Type objtype, Expression<Predicate<T>> filter, int start, int count)
        {
            string viewname = null;
            // find view from name
            if (_primaryView.TryGetValue(objtype, out viewname))
                return Query(viewname, filter, start, count);

            // search for viewtype here
            if (_otherViewTypes.TryGetValue(objtype, out viewname))
                return Query(viewname, filter, start, count);

            _log.Error("view not found", viewname);
            return new Result<object>(false, new Exception("view not found : " + viewname));
        }

        internal Result<object> Query(Type objtype, string filter, int start, int count)
        {
            string viewname = GetViewName(objtype);

            // find view from name
            if (viewname!="") //_primaryView.TryGetValue(objtype, out viewname))
                return Query(viewname, filter, start, count);

            //// search for viewtype here
            //if (_otherViewTypes.TryGetValue(objtype, out viewname))
            //    return Query(viewname, filter, start, count);

            //if(_viewAQFNmapping.TryGetValue(objtype.AssemblyQualifiedName, out viewname))
            //    return Query(viewname, filter, start, count);

            _log.Error("view not found for : ", objtype.AssemblyQualifiedName);
            return new Result<object>(false, new Exception("view not found : " + objtype.AssemblyQualifiedName));
        }

        internal Result<object> Query(string viewname, string filter, int start, int count)
        {
            ViewHandler view = null;
            // find view from name
            if (_views.TryGetValue(viewname.ToLower(), out view))
                return view.Query(filter, start, count);

            _log.Error("view not found", viewname);
            return new Result<object>(false, new Exception("view not found : " + viewname));
        }

        internal Result<object> Query<T>(string viewname, Expression<Predicate<T>> filter, int start, int count)
        {
            ViewHandler view = null;
            // find view from name
            if (_views.TryGetValue(viewname.ToLower(), out view))
                return view.Query(filter, start, count);

            _log.Error("view not found", viewname);
            return new Result<object>(false, new Exception("view not found : " + viewname));
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

        internal Result<object> Query(Type view, int start, int count)
        {
            string viewname = GetViewName(view);

            // find view from name
            if (viewname != "") //_primaryView.TryGetValue(objtype, out viewname))
                return Query(viewname, start, count);

            //string viewname = null;
            //// find view from name
            //if (_primaryView.TryGetValue(view, out viewname))
            //    return Query(viewname, start, count);

            //// search for viewtype here
            //if (_otherViewTypes.TryGetValue(view, out viewname))
            //    return Query(viewname, start, count);

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

        internal void RegisterView<T>(View<T> view)
        {
            view.Verify();

            ViewHandler vh = null;
            if (_views.TryGetValue(view.Name.ToLower(), out vh))
            {
                _log.Error("View already added and exists : " + view.Name);
                //vh.RebuildExisting(_objectStore, view);
            }
            else
            {
                vh = new ViewHandler(_Path, this);
                vh.SetView(view , _objectStore);
                _views.Add(view.Name.ToLower(), vh);
                _otherViewTypes.Add(view.GetType(), view.Name.ToLower());

                // add view schema mapping 
                _otherViewTypes.Add(view.Schema, view.Name.ToLower());

                if (view.isPrimaryList)
                {
                    foreach (string tn in view.FireOnTypes)
                        _primaryView.Add(Type.GetType(tn), view.Name.ToLower());
                }
                else
                {
                    if (view.ConsistentSaveToThisView)
                        AddToViewList(_consistentViews, view);
                    else
                        AddToViewList(_otherViews, view);
                }
            }
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

        internal List<string> GetConsistentViews(Type type)
        {
            List<string> list = new List<string>();
            _consistentViews.TryGetValue(type, out list);
            return list;
        }

        private void AddToViewList<T>(SafeDictionary<Type, List<string>> diclist, View<T> view)
        {
            foreach (string tn in view.FireOnTypes)
            {
                List<string> list = null;
                Type t = Type.GetType(tn);
                if (diclist.TryGetValue(t, out list))
                    list.Add(view.Name);
                else
                {
                    list = new List<string>();
                    list.Add(view.Name);
                    diclist.Add(t, list);
                }
            }
        }
        
        //// for when the type full name is not found in server mode
        //internal string GetViewName(string typefullname)
        //{
        //    string viewname = "";
        //    //if (_viewAQFNmapping.TryGetValue(typefullname, out viewname))
        //    //    return viewname;
        //    return "";
        //}

        internal string GetViewName(Type type)
        {
            string viewname = null;
            // find view from name
            if (_primaryView.TryGetValue(type, out viewname))
                return viewname;

            // search for viewtype here
            if (_otherViewTypes.TryGetValue(type, out viewname))
                return viewname;

            //if (_viewAQFNmapping.TryGetValue(type.AssemblyQualifiedName, out viewname))
            //    return viewname;

            return "";
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
            string view = GetViewName(typeof(T));

            ViewHandler vman = null;
            // find view from name
            if (_views.TryGetValue(view.ToLower(), out vman))
            {
                return vman.Query2<T>(filter, start, count);
            }
            return new Result<T>(false, new Exception("View not found"));
        }

        internal Result<T> Query<T>(string filter, int start, int count)
        {
            string view = GetViewName(typeof(T));

            ViewHandler vman = null;
            // find view from name
            if (_views.TryGetValue(view.ToLower(), out vman))
            {
                return vman.Query2<T>(filter, start, count);
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
    }
}
