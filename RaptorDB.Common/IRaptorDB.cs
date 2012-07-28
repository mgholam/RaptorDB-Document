using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Linq.Expressions;

namespace RaptorDB.Common
{
    public delegate List<object> ServerSideFunc(IRaptorDB rap, string filter);

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
        Result Query(string viewname);
        /// <summary>
        /// Query all data associated with the Documnet Type or the View Type
        /// </summary>
        /// <param name="view"></param>
        /// <returns></returns>
        Result Query(Type view);
        /// <summary>
        /// Query a View with a string filter
        /// </summary>
        /// <param name="viewname"></param>
        /// <param name="filter"></param>
        /// <returns></returns>
        Result Query(string viewname, string filter);
        /// <summary>
        /// Query a View with a LINQ filter
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="viewname"></param>
        /// <param name="filter"></param>
        /// <returns></returns>
        Result Query<T>(string viewname, Expression<Predicate<T>> filter);
        /// <summary>
        /// Query a View Type with a LINQ filter
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="type"></param>
        /// <param name="filter"></param>
        /// <returns></returns>
        Result Query<T>(Type type, Expression<Predicate<T>> filter);
        /// <summary>
        /// Query a View Type with a string filter
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="type"></param>
        /// <param name="filter"></param>
        /// <returns></returns>
        Result Query(Type type, string filter);
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
        /// <summary>
        /// Shutdown RaptorDB and flush all data to disk
        /// </summary>
        void Shutdown();

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

        Result FullTextSearch(string filter);
    }
}
