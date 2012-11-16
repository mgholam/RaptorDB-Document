using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Linq.Expressions;

namespace RaptorDB
{
    public static class RDBExtensions
    {
        /// <summary>
        /// For RaptorDB optimized range queries
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="obj"></param>
        /// <param name="from"></param>
        /// <param name="to"></param>
        /// <returns></returns>
        public static bool Between<T>(this T obj, T from, T to)
        {
            return true;
        }

        /// <summary>
        /// For RaptorDB full text search queries
        /// </summary>
        /// <param name="obj"></param>
        /// <param name="what"></param>
        /// <returns></returns>
        public static bool Contains(this string obj, string what)
        {
            return true;
        }
    }

    /// <summary>
    /// Used for the indexer -> hOOt full text indexing
    /// </summary>
    [AttributeUsage(AttributeTargets.Field | AttributeTargets.Property)]
    public class FullTextAttribute : Attribute
    {
    }

    /// <summary>
    /// Used for declaring view extensions DLL's
    /// </summary>
    [AttributeUsage(AttributeTargets.Class)]
    public class RegisterViewAttribute : Attribute
    {
    }

    public interface IMapAPI
    {
        /// <summary>
        /// Log messages
        /// </summary>
        /// <param name="message"></param>
        void Log(string message);

        /// <summary>
        /// Query a view by it's name with a LINQ filter
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="ViewName"></param>
        /// <param name="Filter"></param>
        /// <returns></returns>
        Result<object> Query<T>(string ViewName, Expression<Predicate<T>> Filter); // Query primary list

        /// <summary>
        /// Query a View with a LINQ filter with paging
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="ViewName"></param>
        /// <param name="Filter"></param>
        /// <param name="start"></param>
        /// <param name="count"></param>
        /// <returns></returns>
        Result<object> Query<T>(string ViewName, Expression<Predicate<T>> Filter, int start, int count); // Query primary list

        /// <summary>
        /// Query a view by it's type with a LINQ filter
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="View"></param>
        /// <param name="Filter"></param>
        /// <returns></returns>
        Result<object> Query<T>(Type View, Expression<Predicate<T>> Filter); //Query other views

        /// <summary>
        /// Query a View Type with a LINQ filter with paging
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="View"></param>
        /// <param name="Filter"></param>
        /// <param name="start"></param>
        /// <param name="count"></param>
        /// <returns></returns>
        Result<object> Query<T>(Type View, Expression<Predicate<T>> Filter, int start, int count); //Query other views

        /// <summary>
        /// Count all data associated with the Documnet Type or the View Type
        /// </summary>
        /// <param name="type"></param>
        /// <returns></returns>
        int Count(Type type);

        /// <summary>
        /// Count all data associated with the Documnet Type or the View Type with a string filter
        /// </summary>
        /// <param name="viewname"></param>
        /// <returns></returns>
        int Count(string viewname);

        /// <summary>
        /// Count all data associated with the Documnet Type or the View Type with a LINQ filter
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="type"></param>
        /// <param name="Filter"></param>
        /// <returns></returns>
        int Count<T>(Type type, Expression<Predicate<T>> Filter);

        /// <summary>
        /// Count all data associated with View name and LINQ filter
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="ViewName"></param>
        /// <param name="Filter"></param>
        /// <returns></returns>
        int Count<T>(string ViewName, Expression<Predicate<T>> Filter);

        /// <summary>
        /// Count all data associated with View name and string filter
        /// </summary>
        /// <param name="ViewName"></param>
        /// <param name="Filter"></param>
        /// <returns></returns>
        int Count(string ViewName, string Filter);

        /// <summary>
        /// Count all data associated with the Documnet Type or the View Type with a string filter
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="type"></param>
        /// <param name="Filter"></param>
        /// <returns></returns>
        int Count<T>(Type type, string Filter);

        /// <summary>
        /// Fetch a document by it's Guid
        /// </summary>
        /// <param name="guid"></param>
        /// <returns></returns>
        object Fetch(Guid guid);

        /// <summary>
        /// Emit values, the ordering must match the view schema
        /// </summary>
        /// <param name="docid"></param>
        /// <param name="data"></param>
        void Emit(Guid docid, params object[] data);

        /// <summary>
        /// Emits the object matching the view schema, you must make sure the object property names match the row schema
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="docid"></param>
        /// <param name="doc"></param>
        void EmitObject<T>(Guid docid, T doc);

        /// <summary>
        /// Roll back the transaction if the primary view is in transaction mode
        /// </summary>
        void RollBack();

        // new query model
        Result<T> Query<T>(Expression<Predicate<T>> Filter);
        Result<T> Query<T>(Expression<Predicate<T>> Filter, int start, int count);
        Result<T> Query<T>(string Filter);
        Result<T> Query<T>(string Filter, int start, int count);
        int Count<T>(Expression<Predicate<T>> Filter);
        //int Count<T>(string Filter);
    }
}
