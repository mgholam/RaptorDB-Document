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
        public Result Query(string viewname)
        {
            Packet p = CreatePacket();
            p.Command = "querystr";
            p.Viewname = viewname;
            p.Data = "";
            ReturnPacket ret = (ReturnPacket)_client.Send(p);
            return (Result)ret.Data;
        }

        /// <summary>
        /// Query a primary view -> get all rows
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="view"></param>
        /// <returns></returns>
        public Result Query(Type view)
        {
            Packet p = CreatePacket();
            p.Command = "querytype";
            p.Data = new object[] { view.AssemblyQualifiedName, "" };
            ReturnPacket ret = (ReturnPacket)_client.Send(p);
            return (Result)ret.Data;
        }

        /// <summary>
        /// Query a view using a string filter
        /// </summary>
        /// <param name="viewname"></param>
        /// <param name="filter"></param>
        /// <returns></returns>
        public Result Query(string viewname, string filter)
        {
            Packet p = CreatePacket();
            p.Command = "querystr";
            p.Viewname = viewname;
            p.Data = filter;
            ReturnPacket ret = (ReturnPacket)_client.Send(p);
            return (Result)ret.Data;
        }

        // FEATURE : add paging to queries -> start, count
        /// <summary>
        /// Query any view with filters
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="viewname">view name</param>
        /// <param name="filter"></param>
        /// <returns></returns>
        public Result Query<T>(string viewname, Expression<Predicate<T>> filter)
        {
            LINQString ls = new LINQString();
            ls.Visit(filter);
            Packet p = CreatePacket();
            p.Command = "querystr";
            p.Viewname = viewname;
            p.Data = ls.sb.ToString();
            ReturnPacket ret = (ReturnPacket)_client.Send(p);
            return (Result)ret.Data;
        }

        /// <summary>
        /// Query a view with filters
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="view">base entity type, or typeof the view </param>
        /// <param name="filter"></param>
        /// <returns></returns>
        public Result Query<T>(Type type, Expression<Predicate<T>> filter)
        {
            LINQString ls = new LINQString();
            ls.Visit(filter);
            Packet p = CreatePacket();
            p.Command = "querytype";
            p.Data = new object[] { type.AssemblyQualifiedName, ls.sb.ToString() };
            ReturnPacket ret = (ReturnPacket)_client.Send(p);
            return (Result)ret.Data;
        }

        /// <summary>
        /// Query a view with filters
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="view">base entity type, or typeof the view </param>
        /// <param name="filter"></param>
        /// <returns></returns>
        public Result Query(Type type, string filter)
        {
            Packet p = CreatePacket();
            p.Command = "querytype";
            p.Data = new object[] { type.AssemblyQualifiedName, filter};
            ReturnPacket ret = (ReturnPacket)_client.Send(p);
            return (Result)ret.Data;
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
        
        public Result FullTextSearch(string filter)
        {
            Packet p = CreatePacket();
            p.Command = "fulltext";
            p.Data = new object[] { filter };
            ReturnPacket ret = (ReturnPacket)_client.Send(p);
            return (Result)ret.Data;
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

    }
}
