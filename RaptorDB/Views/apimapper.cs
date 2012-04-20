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

        public void Log(string message)
        {
            _log.Debug(message);
        }

        public Result Query<T>(string ViewName, Expression<Predicate<T>> Filter)//, int start, int count)
        {
            return _viewmanager.Query(ViewName, Filter);//, start, count);
        }

        public Result Query<T>(Type View, Expression<Predicate<T>> Filter)//, int start, int count)
        {
            return _viewmanager.Query(View, Filter);//, start, count);
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
    }
}
