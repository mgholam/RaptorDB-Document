using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using RaptorDB.Views;
using System.Linq.Expressions;

namespace RaptorDB
{
    public interface IMapAPI
    {
        void Log(string message);
        Result Query<T>(string ViewName, Expression<Predicate<T>> Filter);//, int start, int count); // Query primary list
        Result Query<T>(Type View, Expression<Predicate<T>> Filter);//, int start, int count); //Query other views
        object Fetch(Guid guid);
        void Emit(Guid docid, params object[] data);
    }
}
