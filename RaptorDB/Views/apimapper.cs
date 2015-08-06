using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Linq.Expressions;

namespace RaptorDB.Views
{
    internal class apimapper : IMapAPI
    {
        public apimapper(ViewManager man, ViewHandler vhandler)
        {
            _viewmanager = man;
            _viewhandler = vhandler;
        }

        ViewManager _viewmanager;
        ViewHandler _viewhandler;
        private ILog _log = LogManager.GetLogger(typeof(apimapper));
        internal Dictionary<Guid, List<object[]>> emit = new Dictionary<Guid, List<object[]>>();
        internal Dictionary<Guid, List<object>> emitobj = new Dictionary<Guid, List<object>>();
        internal bool _RollBack = false;

        public void Log(string message)
        {
            _log.Debug(message);
        }

        public object Fetch(Guid guid)
        {
            return _viewmanager.Fetch(guid);
        }

        public void Emit(Guid docid, params object[] data)
        {
            if (data == null)
                return;
            List<object[]> d = null;
            if (emit.Count == 0)
            {
                d = new List<object[]>();
                d.Add(data);
                emit.Add(docid, d);
            }
            else
            {
                if (emit.TryGetValue(docid, out d))
                {
                    d.Add(data);
                }
                else
                {
                    d = new List<object[]>();
                    d.Add(data);
                    emit.Add(docid, d);
                }
            }
        }

        public void EmitObject<T>(Guid docid, T doc)
        {
            if (doc == null)
                return;
            List<object> d = null;
            if (emitobj.Count == 0)
            {
                d = new List<object>();
                d.Add(doc);
                emitobj.Add(docid, d);
            }
            else
            {
                if (emitobj.TryGetValue(docid, out d))
                {
                    d.Add(doc);
                }
                else
                {
                    d = new List<object>();
                    d.Add(doc);
                    emitobj.Add(docid, d);
                }
            }
        }

        public void RollBack()
        {
            _RollBack = true;
        }

        public int Count(string viewname)
        {
            return _viewmanager.Count(viewname, "");
        }

        public int Count(string ViewName, string Filter)
        {
            return _viewmanager.Count(ViewName, Filter);
        }

        public Result<T> Query<T>(Expression<Predicate<T>> Filter)
        {
            return _viewmanager.Query<T>(Filter, 0, -1);
        }

        public Result<T> Query<T>(Expression<Predicate<T>> Filter, int start, int count)
        {
            return _viewmanager.Query<T>(Filter, start, count);
        }

        public Result<T> Query<T>(string Filter)
        {
            return _viewmanager.Query<T>(Filter, 0, -1);
        }

        public Result<T> Query<T>(string Filter, int start, int count)
        {
            return _viewmanager.Query<T>(Filter, start, count);
        }

        public int Count<T>(Expression<Predicate<T>> Filter)
        {
            return _viewmanager.Count<T>(Filter);
        }

        public int NextRowNumber()
        {
            return _viewhandler.NextRowNumber();
        }

        public Common.IKeyStoreHF GetKVHF()
        {
            return _viewmanager.GetKVHF();
        }
    }
}
