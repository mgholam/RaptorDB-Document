using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using RaptorDB.Common;
using System.Linq.Expressions;

namespace RaptorDB
{
    public class RaptorDBClient : IRaptorDB
    {
        public RaptorDBClient(string server, int port, string username, string password)
        {
            _username = username;
            _password = password;
            _client = new NetworkClient(server, port);
        }

        private NetworkClient _client;
        private string _username;
        private string _password;

        public bool Save<T>(Guid docID, T document)
        {
            Packet p = CreatePacket();
            p.Command = "save";
            p.Docid = docID;
            p.Data = document;
            ReturnPacket ret = (ReturnPacket)_client.Send(p);
            return ret.OK;
        }

        public bool SaveBytes(Guid docID, byte[] bytes)
        {
            Packet p = CreatePacket();
            p.Command = "savebytes";
            p.Docid = docID;
            p.Data = bytes;
            ReturnPacket ret = (ReturnPacket)_client.Send(p);
            return ret.OK;
        }

        /// <summary>
        /// Query any view -> get all rows
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="viewname"></param>
        /// <returns></returns>
        public Result<object> Query(string viewname)
        {
            return Query(viewname, 0, 0);
        }

        /// <summary>
        /// Query a primary view -> get all rows
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="view"></param>
        /// <returns></returns>
        public Result<object> Query(Type type)
        {
            return Query(type, 0, 0);
        }

        /// <summary>
        /// Query a view using a string filter
        /// </summary>
        /// <param name="viewname"></param>
        /// <param name="filter"></param>
        /// <returns></returns>
        public Result<object> Query(string viewname, string filter)
        {
            return Query(viewname, filter, 0, 0);
        }

        // FEATURE : add paging to queries -> start, count
        /// <summary>
        /// Query any view with filters
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="viewname">view name</param>
        /// <param name="filter"></param>
        /// <returns></returns>
        public Result<object> Query<T>(string viewname, Expression<Predicate<T>> filter)
        {
            return Query(viewname, filter, 0, 0);
        }

        /// <summary>
        /// Query a view with filters
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="view">base entity type, or typeof the view </param>
        /// <param name="filter"></param>
        /// <returns></returns>
        public Result<object> Query<T>(Type view, Expression<Predicate<T>> filter)
        {
            return Query<T>(view, filter, 0, 0);
        }

        /// <summary>
        /// Query a view with filters
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="view">base entity type, or typeof the view </param>
        /// <param name="filter"></param>
        /// <returns></returns>
        public Result<object> Query(Type view, string filter)
        {
            return Query(view, filter, 0, 0);
        }

        /// <summary>
        /// Fetch a document by it's ID
        /// </summary>
        /// <param name="docID"></param>
        /// <returns></returns>
        public object Fetch(Guid docID)
        {
            Packet p = CreatePacket();
            p.Command = "fetch";
            p.Docid = docID;
            ReturnPacket ret = (ReturnPacket)_client.Send(p);
            if (ret.OK)
                return ret.Data;
            else
                return null;
        }

        /// <summary>
        /// Fetch file data by it's ID
        /// </summary>
        /// <param name="fileID"></param>
        /// <returns></returns>
        public byte[] FetchBytes(Guid fileID)
        {
            Packet p = CreatePacket();
            p.Command = "fetchbytes";
            p.Docid = fileID;
            ReturnPacket ret = (ReturnPacket)_client.Send(p);
            if (ret.OK)
                return (byte[])ret.Data;
            else
                return null;
        }

        public void Shutdown()
        {
            _client.Close();
        }

        public bool Backup()
        {
            Packet p = CreatePacket();
            p.Command = "backup";
            ReturnPacket ret = (ReturnPacket)_client.Send(p);
            return ret.OK;
        }

        public void Restore()
        {
            Packet p = CreatePacket();
            p.Command = "restore";
            ReturnPacket ret = (ReturnPacket)_client.Send(p);
        }

        public bool Delete(Guid docid)
        {
            Packet p = CreatePacket();
            p.Command = "delete";
            p.Docid = docid;
            ReturnPacket ret = (ReturnPacket)_client.Send(p);
            return ret.OK;
        }

        public bool DeleteBytes(Guid fileid)
        {
            Packet p = CreatePacket();
            p.Command = "deletebytes";
            p.Docid = fileid;
            ReturnPacket ret = (ReturnPacket)_client.Send(p);
            return ret.OK;
        }

        public bool AddUser(string username, string oldpassword, string newpassword)
        {
            Packet p = CreatePacket();
            p.Command = "adduser";
            p.Data = new object[] { username, oldpassword, newpassword };
            ReturnPacket ret = (ReturnPacket)_client.Send(p);
            return ret.OK;
        }

        public object[] ServerSide(ServerSideFunc func, string filter)
        {
            Packet p = CreatePacket();
            p.Command = "serverside";
            p.Data = new object[] { func.Method.ReflectedType.AssemblyQualifiedName, func.Method.Name, filter };
            ReturnPacket ret = (ReturnPacket)_client.Send(p);
            return (object[])ret.Data;
        }

        public object[] ServerSide<T>(ServerSideFunc func, Expression<Predicate<T>> filter)
        {
            LINQString ls = new LINQString();
            ls.Visit(filter);

            Packet p = CreatePacket();
            p.Command = "serverside";
            p.Data = new object[] { func.Method.ReflectedType.AssemblyQualifiedName, func.Method.Name, ls.sb.ToString() };
            ReturnPacket ret = (ReturnPacket)_client.Send(p);
            return (object[])ret.Data;
        }

        public Result<object> FullTextSearch(string filter)
        {
            Packet p = CreatePacket();
            p.Command = "fulltext";
            p.Data = new object[] { filter };
            ReturnPacket ret = (ReturnPacket)_client.Send(p);
            return (Result<object>)ret.Data;
        }

        //public List<string> GetViews()
        //{
        //    return null;
        //}

        //public List<string> GetViewSchema(string viewname)
        //{
        //    return null;
        //}

        private Packet CreatePacket()
        {
            Packet p = new Packet();
            p.Username = _username;
            p.PasswordHash = Helper.MurMur.Hash(Encoding.UTF8.GetBytes(_username + "|" + _password)).ToString();

            return p;
        }

        /// <summary>
        /// Query all data in a view with paging
        /// </summary>
        /// <param name="viewname"></param>
        /// <param name="start"></param>
        /// <param name="count"></param>
        /// <returns></returns>
        public Result<object> Query(string viewname, int start, int count)
        {
            Packet p = CreatePacket();
            p.Command = "querystr";
            p.Viewname = viewname;
            p.Data = "";
            p.Start = start;
            p.Count = count;
            ReturnPacket ret = (ReturnPacket)_client.Send(p);
            return (Result<object>)ret.Data;
        }

        /// <summary>
        /// Query all data associated with the Documnet Type or the View Type with paging
        /// </summary>
        /// <param name="view"></param>
        /// <param name="start"></param>
        /// <param name="count"></param>
        /// <returns></returns>
        public Result<object> Query(Type view, int start, int count)
        {
            Packet p = CreatePacket();
            p.Command = "querytype";
            p.Start = start;
            p.Count = count;
            p.Data = new object[] { view.AssemblyQualifiedName, "" };
            ReturnPacket ret = (ReturnPacket)_client.Send(p);
            return (Result<object>)ret.Data;
        }

        /// <summary>
        /// Query a View with a string filter with paging
        /// </summary>
        /// <param name="viewname"></param>
        /// <param name="filter"></param>
        /// <param name="start"></param>
        /// <param name="count"></param>
        /// <returns></returns>
        public Result<object> Query(string viewname, string filter, int start, int count)
        {
            Packet p = CreatePacket();
            p.Command = "querystr";
            p.Viewname = viewname;
            p.Data = filter;
            p.Start = start;
            p.Count = count;
            ReturnPacket ret = (ReturnPacket)_client.Send(p);
            return (Result<object>)ret.Data;
        }

        /// <summary>
        /// Query a View with a LINQ filter with paging
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="viewname"></param>
        /// <param name="filter"></param>
        /// <param name="start"></param>
        /// <param name="count"></param>
        /// <returns></returns>
        public Result<object> Query<T>(string viewname, Expression<Predicate<T>> filter, int start, int count)
        {
            LINQString ls = new LINQString();
            ls.Visit(filter);
            Packet p = CreatePacket();
            p.Command = "querystr";
            p.Viewname = viewname;
            p.Start = start;
            p.Count = count;
            p.Data = ls.sb.ToString();
            ReturnPacket ret = (ReturnPacket)_client.Send(p);
            return (Result<object>)ret.Data;
        }

        /// <summary>
        /// Query a View Type with a LINQ filter with paging
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="type"></param>
        /// <param name="filter"></param>
        /// <param name="start"></param>
        /// <param name="count"></param>
        /// <returns></returns>
        public Result<object> Query<T>(Type type, Expression<Predicate<T>> filter, int start, int count)
        {
            LINQString ls = new LINQString();
            ls.Visit(filter);
            Packet p = CreatePacket();
            p.Command = "querytype";
            p.Start = start;
            p.Count = count;
            p.Data = new object[] { type.AssemblyQualifiedName, ls.sb.ToString() };
            ReturnPacket ret = (ReturnPacket)_client.Send(p);
            return (Result<object>)ret.Data;
        }


        /// <summary>
        /// Query a View Type with a string filter with paging
        /// </summary>
        /// <param name="type"></param>
        /// <param name="filter"></param>
        /// <param name="start"></param>
        /// <param name="count"></param>
        /// <returns></returns>
        public Result<object> Query(Type type, string filter, int start, int count)
        {
            Packet p = CreatePacket();
            p.Command = "querytype";
            p.Start = start;
            p.Count = count;
            p.Data = new object[] { type.AssemblyQualifiedName, filter };
            ReturnPacket ret = (ReturnPacket)_client.Send(p);
            return (Result<object>)ret.Data;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="type"></param>
        /// <returns></returns>
        public int Count(Type type)
        {
            return Count(type, "");
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="type"></param>
        /// <param name="filter"></param>
        /// <returns></returns>
        public int Count(Type type, string filter)
        {
            Packet p = CreatePacket();
            p.Command = "counttype";
            p.Data = new object[] { type.AssemblyQualifiedName, filter };
            ReturnPacket ret = (ReturnPacket)_client.Send(p);
            return (int)ret.Data;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="type"></param>
        /// <param name="filter"></param>
        /// <returns></returns>
        public int Count<T>(Type type, Expression<Predicate<T>> filter)
        {
            LINQString ls = new LINQString();
            ls.Visit(filter);
            Packet p = CreatePacket();
            p.Command = "counttype";
            p.Data = new object[] { type.AssemblyQualifiedName, ls.sb.ToString() };
            ReturnPacket ret = (ReturnPacket)_client.Send(p);
            return (int)ret.Data;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="viewname"></param>
        /// <returns></returns>
        public int Count(string viewname)
        {
            return Count(viewname, "");
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="viewname"></param>
        /// <param name="filter"></param>
        /// <returns></returns>
        public int Count(string viewname, string filter)
        {
            Packet p = CreatePacket();
            p.Command = "countstr";
            p.Viewname = viewname;
            p.Data = filter;
            ReturnPacket ret = (ReturnPacket)_client.Send(p);
            return (int)ret.Data;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="viewname"></param>
        /// <param name="filter"></param>
        /// <returns></returns>
        public int Count<T>(string viewname, Expression<Predicate<T>> filter)
        {
            LINQString ls = new LINQString();
            ls.Visit(filter);
            Packet p = CreatePacket();
            p.Command = "countstr";
            p.Viewname = viewname;
            p.Data = ls.sb.ToString();
            ReturnPacket ret = (ReturnPacket)_client.Send(p);
            return (int)ret.Data;
        }

        public Result<T> Query<T>(Expression<Predicate<T>> filter)
        {
            return Query<T>(filter, 0, 0);           
        }

        public Result<T> Query<T>(Expression<Predicate<T>> filter, int start, int count)
        {
            LINQString ls = new LINQString();
            ls.Visit(filter);
            Packet p = CreatePacket();
            p.Command = "querytype";
            p.Start = start;
            p.Count = count;
            p.Data = new object[] { typeof(T).AssemblyQualifiedName, ls.sb.ToString() };
            ReturnPacket ret = (ReturnPacket)_client.Send(p);
            Result<object> res = (Result<object>)ret.Data;
            return GenericResult<T>(res);
        }

        private static Result<T> GenericResult<T>(Result<object> res)
        {
            // dirty hack here to cleanup
            Result<T> result = new Result<T>();
            result.Count = res.Count;
            result.EX = res.EX;
            result.OK = res.OK;
            result.TotalCount = res.TotalCount;
            result.Rows = res.Rows.Cast<T>().ToList<T>();
            return result;
        }

        public Result<T> Query<T>(string filter)
        {
            return Query<T>(filter, 0, 0);
        }

        public Result<T> Query<T>(string filter, int start, int count)
        {
            Packet p = CreatePacket();
            p.Command = "querytype";
            p.Start = start;
            p.Count = count;
            p.Data = new object[] { typeof(T).AssemblyQualifiedName, filter };
            ReturnPacket ret = (ReturnPacket)_client.Send(p);
            Result<object> res = (Result<object>)ret.Data;
            return GenericResult<T>(res);
        }

        public int Count<T>(Expression<Predicate<T>> filter)
        {
            LINQString ls = new LINQString();
            ls.Visit(filter);
            Packet p = CreatePacket();
            p.Command = "gcount";
            p.Viewname = typeof(T).AssemblyQualifiedName;
            p.Data = ls.sb.ToString();
            ReturnPacket ret = (ReturnPacket)_client.Send(p);
            return (int)ret.Data;
        }
    }
}
