using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using RaptorDB.Common;
using System.Linq.Expressions;

namespace RaptorDB
{
    public class RaptorDBClient
    {
        public RaptorDBClient(string server, int port, string username, string password)
        {
            _username = username;
            _password = password;
            _client = new NetworkClient(server, port);
            _client.Connect();
        }

        private NetworkClient _client;
        private string _username;
        private string _password;


        public bool Save(Guid docID, object document)
        {
            Packet p = CreatePacket();
            p.Command = "save";
            p.Docid = docID;
            p.Data =  document;
            ReturnPacket ret = (ReturnPacket)_client.Send(p);
            return ret.OK;
        }

        public bool SaveBytes(Guid docID, byte[] bytes)
        {
            Packet p = CreatePacket();
            p.Command = "savebytes";
            p.Docid = docID;
            p.Data = bytes ;
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
            p.Data = new object[] { view.AssemblyQualifiedName , ""};
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
            p.Data =  ls.sb.ToString();
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

        public List<string> GetViews()
        {
            return null;
        }

        public List<string> GetViewSchema(string viewname)
        {
            return null;
        }


        private Packet CreatePacket()
        {
            Packet p = new Packet();
            p.Username = _username;
            p.PasswordHash = _password;
            
            return p;
        }
    }
}
