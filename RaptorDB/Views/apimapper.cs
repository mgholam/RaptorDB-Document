using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Linq.Expressions;

namespace RaptorDB.Views
{
    internal class apimapper : IMapAPI
    {
        public apimapper(ViewManager man)
        {
            _viewmanager = man;
        }

        ViewManager _viewmanager;
        private ILog _log = LogManager.GetLogger(typeof(apimapper));
        internal Dictionary<Guid, List<object[]>> emit = new Dictionary<Guid, List<object[]>>();
        internal Dictionary<Guid, List<object>> emitobj = new Dictionary<Guid, List<object>>();
        internal bool _RollBack = false;

        public void Log(string message)
        {
            _log.Debug(message);
        }

        public Result Query<T>(string ViewName, Expression<Predicate<T>> Filter)
        {
            return _viewmanager.Query(ViewName, Filter, 0, 0);
        }

        public Result Query<T>(Type View, Expression<Predicate<T>> Filter)
        {
            return _viewmanager.Query(View, Filter, 0, 0);
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


        public Result Query<T>(string ViewName, Expression<Predicate<T>> Filter, int start, int count)
        {
            return _viewmanager.Query(ViewName, Filter, start, count);
        }

        public Result Query<T>(Type View, Expression<Predicate<T>> Filter, int start, int count)
        {
            return _viewmanager.Query(View, Filter, start, count);
        }

        public int Count(Type type)
        {
            return _viewmanager.Count(type, "");
        }

        public int Count(string viewname)
        {
            return _viewmanager.Count(viewname, "");
        }

        public int Count<T>(Type type, Expression<Predicate<T>> Filter)
        {
            return _viewmanager.Count(type, Filter);
        }

        public int Count<T>(string ViewName, Expression<Predicate<T>> Filter)
        {
            return _viewmanager.Count(ViewName, Filter);
        }

        public int Count(string ViewName, string Filter)
        {
            return _viewmanager.Count(ViewName, Filter);
        }

        public int Count<T>(Type type, string Filter)
        {
            return _viewmanager.Count(type, Filter);
        }
    }
}
