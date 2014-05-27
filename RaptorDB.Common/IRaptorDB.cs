using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Linq.Expressions;

namespace RaptorDB.Common
{
    public delegate List<object> ServerSideFunc(IRaptorDB rap, string filter);

    public class HistoryInfo
    {
        public int Version;
        public DateTime ChangeDate;
    }

    public interface IRaptorDB
    {
        /// <summary>
        /// Save Bytes (files) to RptorDB storage
        /// </summary>
        /// <param name="docID"></param>
        /// <param name="bytes"></param>
        /// <returns></returns>
        bool SaveBytes(Guid fileID, byte[] bytes);
        /// <summary>
        /// Save a Document to RaptorDB
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="docID"></param>
        /// <param name="document"></param>
        /// <returns></returns>
        bool Save<T>(Guid docID, T document);


        /// <summary>
        /// Query all data in a view
        /// </summary>
        /// <param name="viewname"></param>
        /// <returns></returns>
        Result<object> Query(string viewname);
        /// <summary>
        /// Query all data in a view with paging
        /// </summary>
        /// <param name="viewname"></param>
        /// <param name="start"></param>
        /// <param name="count"></param>
        /// <returns></returns>
        Result<object> Query(string viewname, int start, int count);
        /// <summary>
        /// Query a View with a string filter
        /// </summary>
        /// <param name="viewname"></param>
        /// <param name="filter"></param>
        /// <returns></returns>
        Result<object> Query(string viewname, string filter);
        /// <summary>
        /// Query a View with a string filter with paging
        /// </summary>
        /// <param name="viewname"></param>
        /// <param name="filter"></param>
        /// <param name="start"></param>
        /// <param name="count"></param>
        /// <returns></returns>
        Result<object> Query(string viewname, string filter, int start, int count);
        /// <summary>
        /// Query a view with filter, paging and sorting
        /// </summary>
        /// <param name="viewname"></param>
        /// <param name="filter"></param>
        /// <param name="start"></param>
        /// <param name="count"></param>
        /// <returns></returns>
        Result<object> Query(string viewname, string filter, int start, int count, string orderby);
        /// <summary>
        /// Count all data associated with View name
        /// </summary>
        /// <param name="viewname"></param>
        /// <returns></returns>
        int Count(string viewname);
        /// <summary>
        /// Count all data associated with View name and string filter
        /// </summary>
        /// <param name="viewname"></param>
        /// <param name="filter"></param>
        /// <returns></returns>
        int Count(string viewname, string filter);

        #region del?
        ///// <summary>
        ///// Query all data associated with the Documnet Type or the View Type
        ///// </summary>
        ///// <param name="view"></param>
        ///// <returns></returns>
        //Result<object> Query(Type view);
        ///// <summary>
        ///// Query all data associated with the Documnet Type or the View Type with paging
        ///// </summary>
        ///// <param name="view"></param>
        ///// <param name="start"></param>
        ///// <param name="count"></param>
        ///// <returns></returns>
        //Result<object> Query(Type view, int start, int count); 

        ///// <summary>
        ///// Query a View with a LINQ filter
        ///// </summary>
        ///// <typeparam name="T"></typeparam>
        ///// <param name="viewname"></param>
        ///// <param name="filter"></param>
        ///// <returns></returns>
        //Result<object> Query<T>(string viewname, Expression<Predicate<T>> filter);
        ///// <summary>
        ///// Query a View with a LINQ filter with paging
        ///// </summary>
        ///// <typeparam name="T"></typeparam>
        ///// <param name="viewname"></param>
        ///// <param name="filter"></param>
        ///// <param name="start"></param>
        ///// <param name="count"></param>
        ///// <returns></returns>
        //Result<object> Query<T>(string viewname, Expression<Predicate<T>> filter, int start, int count);
        ///// <summary>
        ///// 
        ///// </summary>
        ///// <typeparam name="T"></typeparam>
        ///// <param name="viewname"></param>
        ///// <param name="filter"></param>
        ///// <param name="start"></param>
        ///// <param name="count"></param>
        ///// <param name="orderby"></param>
        ///// <returns></returns>
        //Result<object> Query<T>(string viewname, Expression<Predicate<T>> filter, int start, int count, string orderby);
        ///// <summary>
        ///// Query a View Type with a LINQ filter
        ///// </summary>
        ///// <typeparam name="T"></typeparam>
        ///// <param name="type"></param>
        ///// <param name="filter"></param>
        ///// <returns></returns>
        //Result<object> Query<T>(Type type, Expression<Predicate<T>> filter);
        ///// <summary>
        ///// Query a View Type with a LINQ filter with paging
        ///// </summary>
        ///// <typeparam name="T"></typeparam>
        ///// <param name="type"></param>
        ///// <param name="filter"></param>
        ///// <param name="start"></param>
        ///// <param name="count"></param>
        ///// <returns></returns>
        //Result<object> Query<T>(Type type, Expression<Predicate<T>> filter, int start, int count);
        ///// <summary>
        ///// Query a View Type with a string filter
        ///// </summary>
        ///// <typeparam name="T"></typeparam>
        ///// <param name="type"></param>
        ///// <param name="filter"></param>
        ///// <returns></returns>
        //Result<object> Query(Type type, string filter);
        ///// <summary>
        ///// Query a View Type with a string filter with paging
        ///// </summary>
        ///// <param name="type"></param>
        ///// <param name="filter"></param>
        ///// <returns></returns>
        //Result<object> Query(Type type, string filter, int start, int count); 

        ///// <summary>
        ///// Count all data associated with the Documnet Type or the View Type
        ///// </summary>
        ///// <param name="type"></param>
        ///// <returns></returns>
        //int Count(Type type);
        ///// <summary>
        ///// Count all data associated with the Documnet Type or the View Type with a string filter
        ///// </summary>
        ///// <param name="type"></param>
        ///// <param name="filter"></param>
        ///// <returns></returns>
        //int Count(Type type, string filter);
        ///// <summary>
        ///// Count all data associated with the Documnet Type or the View Type with a LINQ filter
        ///// </summary>
        ///// <param name="type"></param>
        ///// <param name="filter"></param>
        ///// <returns></returns>
        //int Count<T>(Type type, Expression<Predicate<T>> filter);
        ///// <summary>
        ///// Count all data associated with View name and LINQ filter
        ///// </summary>
        ///// <typeparam name="T"></typeparam>
        ///// <param name="viewname"></param>
        ///// <param name="filter"></param>
        ///// <returns></returns>
        //int Count<T>(string viewname, Expression<Predicate<T>> filter); 
        #endregion


        /// <summary>
        /// Fetch a Document
        /// </summary>
        /// <param name="docID"></param>
        /// <returns></returns>
        object Fetch(Guid docID);
        /// <summary>
        /// Fetch a file bytes
        /// </summary>
        /// <param name="fileID"></param>
        /// <returns></returns>
        byte[] FetchBytes(Guid fileID);
        ///// <summary>
        ///// Shutdown RaptorDB and flush all data to disk
        ///// </summary>
        //void Shutdown();
        /// <summary>
        /// Backup the document storage file incrementally to "Backup" folder
        /// </summary>
        /// <returns>True = done</returns>
        bool Backup();
        /// <summary>
        /// Start background restore of backups in the "Restore" folder
        /// </summary>
        void Restore();
        /// <summary>
        /// Delete a Document
        /// </summary>
        /// <param name="docid"></param>
        /// <returns></returns>
        bool Delete(Guid docid);
        /// <summary>
        /// Delete a File
        /// </summary>
        /// <param name="fileid"></param>
        /// <returns></returns>
        bool DeleteBytes(Guid fileid);
        /// <summary>
        /// Add users
        /// </summary>
        /// <param name="username"></param>
        /// <param name="oldpassword"></param>
        /// <param name="newpassword"></param>
        /// <returns></returns>
        bool AddUser(string username, string oldpassword, string newpassword);

        /// <summary>
        /// Do server side data aggregate queries, so you don't transfer large data rows to clients for processing 
        /// </summary>
        /// <param name="func"></param>
        /// <returns></returns>
        object[] ServerSide(ServerSideFunc func, string filter);

        /// <summary>
        /// Do server side data aggregate queries, so you don't transfer large data rows to clients for processing 
        /// </summary>
        /// <param name="func"></param>
        /// <returns></returns>
        object[] ServerSide<T>(ServerSideFunc func, Expression<Predicate<T>> filter);

        /// <summary>
        /// Full text search the entire original document
        /// </summary>
        /// <param name="filter"></param>
        /// <returns></returns>
        int[] FullTextSearch(string filter);


        // new query model
        /// <summary>
        /// Query a view with linq filter
        /// </summary>
        /// <typeparam name="T">Use the Row Schema type for your view</typeparam>
        /// <param name="filter"></param>
        /// <returns></returns>
        Result<T> Query<T>(Expression<Predicate<T>> filter);
        /// <summary>
        /// Query a view with paging
        /// </summary>
        /// <typeparam name="T">Use the Row Schema type for your view</typeparam>
        /// <param name="filter"></param>
        /// <param name="start"></param>
        /// <param name="count"></param>
        /// <returns></returns>
        Result<T> Query<T>(Expression<Predicate<T>> filter, int start, int count);
        /// <summary>
        /// Query a view with linq filter, paging and sorting
        /// </summary>
        /// <typeparam name="T">Use the Row Schema type for your view</typeparam>
        /// <param name="filter"></param>
        /// <param name="start"></param>
        /// <param name="count"></param>
        /// <param name="orderby"></param>
        /// <returns></returns>
        Result<T> Query<T>(Expression<Predicate<T>> filter, int start, int count, string orderby);
        /// <summary>
        /// Query a view with a string filter
        /// </summary>
        /// <typeparam name="T">Use the Row Schema type for your view</typeparam>
        /// <param name="filter"></param>
        /// <returns></returns>
        Result<T> Query<T>(string filter);
        /// <summary>
        /// Query a view with string filter and paging
        /// </summary>
        /// <typeparam name="T">Use the Row Schema type for your view</typeparam>
        /// <param name="filter"></param>
        /// <param name="start"></param>
        /// <param name="count"></param>
        /// <returns></returns>
        Result<T> Query<T>(string filter, int start, int count);
        /// <summary>
        /// Query a view with string filter, paging and sorting
        /// </summary>
        /// <typeparam name="T">Use the Row Schema type for your view</typeparam>
        /// <param name="filter"></param>
        /// <param name="start"></param>
        /// <param name="count"></param>
        /// <param name="orderby"></param>
        /// <returns></returns>
        Result<T> Query<T>(string filter, int start, int count, string orderby);
        /// <summary>
        /// Count rows with a linq filter
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="filter"></param>
        /// <returns></returns>
        int Count<T>(Expression<Predicate<T>> filter);

        /// <summary>
        /// Fetch the change history for a document
        /// </summary>
        /// <param name="docid"></param>
        /// <returns></returns>
        int[] FetchHistory(Guid docid);
        /// <summary>
        /// Fetch the change history for a document with dates
        /// </summary>
        /// <param name="docid"></param>
        /// <returns></returns>
        HistoryInfo[] FetchHistoryInfo(Guid docid);
        /// <summary>
        /// Fetch a change history for a file
        /// </summary>
        /// <param name="fileid"></param>
        /// <returns></returns>
        int[] FetchBytesHistory(Guid fileid);
        /// <summary>
        /// Fetch a change history for a file with dates
        /// </summary>
        /// <param name="docid"></param>
        /// <returns></returns>
        HistoryInfo[] FetchBytesHistoryInfo(Guid docid);
        /// <summary>
        /// Fetch the specific document version 
        /// </summary>
        /// <param name="versionNumber"></param>
        /// <returns></returns>
        object FetchVersion(int versionNumber);
        /// <summary>
        /// Fetch the specific file version
        /// </summary>
        /// <param name="versionNumber"></param>
        /// <returns></returns>
        byte[] FetchBytesVersion(int versionNumber);
    }
}
