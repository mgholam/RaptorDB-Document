using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Linq.Expressions;
using RaptorDB.Common;

namespace RaptorDB
{
    //public static class RDBExtensions
    //{
    //    ///// <summary>
    //    ///// For RaptorDB optimized range queries
    //    ///// </summary>
    //    ///// <typeparam name="T"></typeparam>
    //    ///// <param name="obj"></param>
    //    ///// <param name="from"></param>
    //    ///// <param name="to"></param>
    //    ///// <returns></returns>
    //    //public static bool Between<T>(this T obj, T from, T to)
    //    //{
    //    //    return true;
    //    //}

    //    ///// <summary>
    //    ///// For RaptorDB full text search queries
    //    ///// </summary>
    //    ///// <param name="obj"></param>
    //    ///// <param name="what"></param>
    //    ///// <returns></returns>
    //    //public static bool Contains(this string obj, string what)
    //    //{
    //    //    return true;
    //    //}
    //}

    /// <summary>
    /// Used for normal string columns 
    /// </summary>
    [AttributeUsage(AttributeTargets.Field | AttributeTargets.Property)]
    public class CaseInsensitiveAttribute : Attribute
    {
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

    /// <summary>
    /// Index file max string length size in UTF8 (Default = 60)
    /// </summary>
    [AttributeUsage(AttributeTargets.Property | AttributeTargets.Field)]
    public class StringIndexLength : Attribute
    {
        public StringIndexLength()
        {
            Length = 60; // default
        }
        public StringIndexLength(byte length)
        {
            Length = length;
        }
        public byte Length;
    }

    public interface IQueryInterface
    {
        /// <summary>
        /// Log messages
        /// </summary>
        /// <param name="message"></param>
        void Log(string message);

        /// <summary>
        /// Count all data associated with the Documnet Type or the View Type with a string filter
        /// </summary>
        /// <param name="viewname"></param>
        /// <returns></returns>
        int Count(string viewname);

        /// <summary>
        /// Count all data associated with View name and string filter
        /// </summary>
        /// <param name="ViewName"></param>
        /// <param name="Filter"></param>
        /// <returns></returns>
        int Count(string ViewName, string Filter);

        /// <summary>
        /// Fetch a document by it's Guid
        /// </summary>
        /// <param name="guid"></param>
        /// <returns></returns>
        object Fetch(Guid guid);

        // new query model
        Result<T> Query<T>(Expression<Predicate<T>> Filter);
        Result<T> Query<T>(Expression<Predicate<T>> Filter, int start, int count);
        Result<T> Query<T>(string Filter);
        Result<T> Query<T>(string Filter, int start, int count);
        int Count<T>(Expression<Predicate<T>> Filter);
    }

    public interface IMapAPI : IQueryInterface
    {
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

        /// <summary>
        /// Get the next row number for this view
        /// </summary>
        /// <returns></returns>
        int NextRowNumber();

        IKeyStoreHF GetKVHF();
        //void EmitRow<V>(Guid docid, V row);
    }

    public interface IClientHandler
    {
        bool GenerateClientData(IQueryInterface api, string username, List<Guid> DocsToSend);
    }

    public enum COMMANDS
    {
        Save,
        SaveBytes,
        QueryType,
        QueryStr,
        Fetch,
        FetchBytes,
        Backup,
        Delete,
        DeleteBytes,
        Restore,
        AddUser,
        ServerSide,
        FullText,
        CountType,
        CountStr,
        GCount,
        DocHistory,
        FileHistory,
        FetchVersion,
        FetchFileVersion,
        CheckAssembly,
        FetchHistoryInfo,
        FetchByteHistoryInfo,
        ViewDelete,
        ViewDelete_t,
        ViewInsert,
        ViewInsert_t,
        DocCount,
        GetObjectHF,
        SetObjectHF,
        DeleteKeyHF,
        CountHF,
        ContainsHF,
        GetKeysHF,
        CompactStorageHF,
        IncrementHF,
        DecrementHF
    }
}
