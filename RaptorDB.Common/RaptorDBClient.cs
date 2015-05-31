using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using RaptorDB.Common;
using System.Linq.Expressions;
using System.Reflection;
using System.IO;

namespace RaptorDB
{
    public class KVHF : IKeyStoreHF
    {
        public KVHF(NetworkClient client, string username, string password)
        {
            _client = client;
            _username = username;
            _password = password;
        }

        NetworkClient _client;
        private string _username;
        private string _password;


        public object GetObjectHF(string key)
        {
            Packet p = CreatePacket();
            p.Command = "" + COMMANDS.GetObjectHF;
            p.Data = key;
            ReturnPacket ret = (ReturnPacket)_client.Send(p);
            if (ret.OK)
                return ret.Data;
            else
                return null;
        }

        public bool SetObjectHF(string key, object obj)
        {
            Packet p = CreatePacket();
            p.Command = "" + COMMANDS.SetObjectHF;
            p.Data = new object[] { key, obj };
            ReturnPacket ret = (ReturnPacket)_client.Send(p);

            return ret.OK;
        }

        public bool DeleteKeyHF(string key)
        {
            Packet p = CreatePacket();
            p.Command = "" + COMMANDS.DeleteKeyHF;
            p.Data = key;
            ReturnPacket ret = (ReturnPacket)_client.Send(p);

            return (bool)ret.Data;
        }

        public int CountHF()
        {
            Packet p = CreatePacket();
            p.Command = "" + COMMANDS.CountHF;
            ReturnPacket ret = (ReturnPacket)_client.Send(p);

            return (int)ret.Data;
        }

        public bool ContainsHF(string key)
        {
            Packet p = CreatePacket();
            p.Command = "" + COMMANDS.ContainsHF;
            p.Data = key;
            ReturnPacket ret = (ReturnPacket)_client.Send(p);

            return (bool)ret.Data;
        }

        public string[] GetKeysHF()
        {
            Packet p = CreatePacket();
            p.Command = "" + COMMANDS.GetKeysHF;
            ReturnPacket ret = (ReturnPacket)_client.Send(p);

            return ((object[])ret.Data).Cast<string>().ToArray();
        }

        public void CompactStorageHF()
        {
            Packet p = CreatePacket();
            p.Command = "" + COMMANDS.CompactStorageHF;
            ReturnPacket ret = (ReturnPacket)_client.Send(p);

            return;
        }

        private Packet CreatePacket()
        {
            Packet p = new Packet();
            p.Username = _username;
            p.PasswordHash = Helper.MurMur.Hash(Encoding.UTF8.GetBytes(_username + "|" + _password)).ToString();

            return p;
        }
    }

    public class RaptorDBClient : IRaptorDB
    {
        public RaptorDBClient(string server, int port, string username, string password)
        {
            _username = username;
            _password = password;
            _client = new NetworkClient(server, port);
            // speed settings
            fastJSON.JSON.Parameters.ParametricConstructorOverride = true;
            fastBinaryJSON.BJSON.Parameters.ParametricConstructorOverride = true;
            _kv = new KVHF(_client, _username, _password);
        }

        private KVHF _kv;
        private NetworkClient _client;
        private string _username;
        private string _password;
        private SafeDictionary<string, bool> _assembly = new SafeDictionary<string, bool>();

        /// <summary>
        /// Save a document to RaptorDB
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="docID"></param>
        /// <param name="document"></param>
        /// <returns></returns>
        public bool Save<T>(Guid docID, T document)
        {
            Packet p = CreatePacket();
            p.Command = "" + COMMANDS.Save;
            p.Docid = docID;
            p.Data = document;
            ReturnPacket ret = (ReturnPacket)_client.Send(p);
            return ret.OK;
        }

        /// <summary>
        /// Save a file to RaptorDB
        /// </summary>
        /// <param name="fileID"></param>
        /// <param name="bytes"></param>
        /// <returns></returns>
        public bool SaveBytes(Guid fileID, byte[] bytes)
        {
            Packet p = CreatePacket();
            p.Command = "" + COMMANDS.SaveBytes;
            p.Docid = fileID;
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
            return Query(viewname, 0, -1);
        }

        /// <summary>
        /// Query a view using a string filter
        /// </summary>
        /// <param name="viewname"></param>
        /// <param name="filter"></param>
        /// <returns></returns>
        public Result<object> Query(string viewname, string filter)
        {
            return Query(viewname, filter, 0, -1);
        }

        /// <summary>
        /// Fetch a document by it's ID
        /// </summary>
        /// <param name="docID"></param>
        /// <returns></returns>
        public object Fetch(Guid docID)
        {
            Packet p = CreatePacket();
            p.Command = "" + COMMANDS.Fetch;
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
            p.Command = "" + COMMANDS.FetchBytes;
            p.Docid = fileID;
            ReturnPacket ret = (ReturnPacket)_client.Send(p);
            if (ret.OK)
                return (byte[])ret.Data;
            else
                return null;
        }

        /// <summary>
        /// Shutdown and cleanup 
        /// </summary>
        public void Shutdown()
        {
            _client.Close();
        }

        /// <summary>
        /// Backup the data file in incremental mode to the RaptorDB folder
        /// </summary>
        /// <returns></returns>
        public bool Backup()
        {
            Packet p = CreatePacket();
            p.Command = "" + COMMANDS.Backup;
            ReturnPacket ret = (ReturnPacket)_client.Send(p);
            return ret.OK;
        }

        /// <summary>
        /// Restore backup files stored in RaptorDB folder
        /// </summary>
        public void Restore()
        {
            Packet p = CreatePacket();
            p.Command = "" + COMMANDS.Restore;
            ReturnPacket ret = (ReturnPacket)_client.Send(p);
        }

        /// <summary>
        /// Delete a document (the actual data is not deleted just marked so) 
        /// </summary>
        /// <param name="docid"></param>
        /// <returns></returns>
        public bool Delete(Guid docid)
        {
            Packet p = CreatePacket();
            p.Command = "" + COMMANDS.Delete;
            p.Docid = docid;
            ReturnPacket ret = (ReturnPacket)_client.Send(p);
            return ret.OK;
        }

        /// <summary>
        /// Delete a file (the actual data is not deleted just marked so) 
        /// </summary>
        /// <param name="fileid"></param>
        /// <returns></returns>
        public bool DeleteBytes(Guid fileid)
        {
            Packet p = CreatePacket();
            p.Command = "" + COMMANDS.DeleteBytes;
            p.Docid = fileid;
            ReturnPacket ret = (ReturnPacket)_client.Send(p);
            return ret.OK;
        }

        /// <summary>
        /// Add a user for server mode login
        /// </summary>
        /// <param name="username"></param>
        /// <param name="oldpassword"></param>
        /// <param name="newpassword"></param>
        /// <returns></returns>
        public bool AddUser(string username, string oldpassword, string newpassword)
        {
            Packet p = CreatePacket();
            p.Command = "" + COMMANDS.AddUser;
            p.Data = new object[] { username, oldpassword, newpassword };
            ReturnPacket ret = (ReturnPacket)_client.Send(p);
            return ret.OK;
        }

        /// <summary>
        /// Execute server side queries
        /// </summary>
        /// <param name="func"></param>
        /// <param name="filter"></param>
        /// <returns></returns>
        public object[] ServerSide(ServerSideFunc func, string filter)
        {
            Packet p = CreatePacket();
            p.Command = "" + COMMANDS.ServerSide;
            p.Data = new object[] { func.Method.ReflectedType.AssemblyQualifiedName, func.Method.Name, filter };
            ReturnPacket ret = (ReturnPacket)_client.Send(p);
            return (object[])ret.Data;
        }

        /// <summary>
        /// Execute server side queries
        /// </summary>
        /// <typeparam name="TRowSchema"></typeparam>
        /// <param name="func"></param>
        /// <param name="filter"></param>
        /// <returns></returns>
        public object[] ServerSide<TRowSchema>(ServerSideFunc func, Expression<Predicate<TRowSchema>> filter)
        {
            LINQString ls = new LINQString();
            ls.Visit(filter);

            Packet p = CreatePacket();
            p.Command = "" + COMMANDS.ServerSide;
            p.Data = new object[] { func.Method.ReflectedType.AssemblyQualifiedName, func.Method.Name, ls.sb.ToString() };
            ReturnPacket ret = (ReturnPacket)_client.Send(p);
            return (object[])ret.Data;
        }

        /// <summary>
        /// Full text search the complete original document 
        /// </summary>
        /// <param name="filter"></param>
        /// <returns></returns>
        public int[] FullTextSearch(string filter)
        {
            Packet p = CreatePacket();
            p.Command = "" + COMMANDS.FullText;
            p.Data = new object[] { filter };
            ReturnPacket ret = (ReturnPacket)_client.Send(p);
            return (int[])ret.Data;
        }

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
            return Query(viewname, "", start, count);
        }

        /// <summary>
        /// Query a View with a string filter with paging
        /// </summary>
        /// <param name="viewname"></param>
        /// <param name="filter"></param>
        /// <param name="start"></param>
        /// <param name="count"></param>
        /// <returns></returns>
        public Result<object> Query(string viewname, string filter, int start, int count, string orderby)
        {
            bool b = false;
            // check if return type exists and copy assembly if needed
            if (_assembly.TryGetValue(viewname, out b) == false)
            {
                Packet pp = CreatePacket();
                pp.Command = "" + COMMANDS.CheckAssembly;
                pp.Viewname = viewname;
                ReturnPacket r = (ReturnPacket)_client.Send(pp);
                string type = r.Error;
                Type t = Type.GetType(type);
                if (t == null)
                {
                    if (r.Data != null)
                    {
                        var a = Assembly.Load((byte[])r.Data);
                        _assembly.Add(viewname, true);
                    }
                }
                else
                    _assembly.Add(viewname, true);
            }
            Packet p = CreatePacket();
            p.Command = "" + COMMANDS.QueryStr;
            p.Viewname = viewname;
            p.Data = filter;
            p.Start = start;
            p.Count = count;
            p.OrderBy = orderby;
            ReturnPacket ret = (ReturnPacket)_client.Send(p);
            return (Result<object>)ret.Data;
        }

        /// <summary>
        /// Query a View with a LINQ filter with paging
        /// </summary>
        /// <typeparam name="TRowSchema"></typeparam>
        /// <param name="viewname"></param>
        /// <param name="filter"></param>
        /// <param name="start"></param>
        /// <param name="count"></param>
        /// <returns></returns>
        public Result<object> Query<TRowSchema>(string viewname, Expression<Predicate<TRowSchema>> filter, int start, int count, string orderby)
        {
            LINQString ls = new LINQString();
            ls.Visit(filter);
            Packet p = CreatePacket();
            p.Command = "" + COMMANDS.QueryStr;
            p.Viewname = viewname;
            p.Start = start;
            p.Count = count;
            p.Data = ls.sb.ToString();
            p.OrderBy = orderby;
            ReturnPacket ret = (ReturnPacket)_client.Send(p);
            return (Result<object>)ret.Data;
        }

        /// <summary>
        /// Count rows
        /// </summary>
        /// <param name="viewname"></param>
        /// <returns></returns>
        public int Count(string viewname)
        {
            return Count(viewname, "");
        }

        /// <summary>
        /// Count rows with a string filter
        /// </summary>
        /// <param name="viewname"></param>
        /// <param name="filter"></param>
        /// <returns></returns>
        public int Count(string viewname, string filter)
        {
            Packet p = CreatePacket();
            p.Command = "" + COMMANDS.CountStr;
            p.Viewname = viewname;
            p.Data = filter;
            ReturnPacket ret = (ReturnPacket)_client.Send(p);
            return (int)ret.Data;
        }

        /// <summary>
        /// Query with LINQ filter
        /// </summary>
        /// <typeparam name="TRowSchema"></typeparam>
        /// <param name="filter"></param>
        /// <returns></returns>
        public Result<TRowSchema> Query<TRowSchema>(Expression<Predicate<TRowSchema>> filter)
        {
            return Query<TRowSchema>(filter, 0, -1, "");
        }

        /// <summary>
        /// Query with LINQ filter and paging
        /// </summary>
        /// <typeparam name="TRowSchema"></typeparam>
        /// <param name="filter"></param>
        /// <param name="start"></param>
        /// <param name="count"></param>
        /// <returns></returns>
        public Result<TRowSchema> Query<TRowSchema>(Expression<Predicate<TRowSchema>> filter, int start, int count, string orderby)
        {
            LINQString ls = new LINQString();
            ls.Visit(filter);
            Packet p = CreatePacket();
            p.Command = "" + COMMANDS.QueryType;
            p.Start = start;
            p.Count = count;
            p.OrderBy = orderby;
            p.Data = new object[] { typeof(TRowSchema).AssemblyQualifiedName, ls.sb.ToString() };
            ReturnPacket ret = (ReturnPacket)_client.Send(p);
            Result<object> res = (Result<object>)ret.Data;
            return GenericResult<TRowSchema>(res);
        }

        private static Result<TRowSchema> GenericResult<TRowSchema>(Result<object> res)
        {
            // FEATURE : dirty hack here to cleanup
            Result<TRowSchema> result = new Result<TRowSchema>();
            result.Count = res.Count;
            result.EX = res.EX;
            result.OK = res.OK;
            result.TotalCount = res.TotalCount;
            result.Rows = res.Rows.Cast<TRowSchema>().ToList<TRowSchema>();
            return result;
        }

        /// <summary>
        /// Query with string filter
        /// </summary>
        /// <typeparam name="TRowSchema"></typeparam>
        /// <param name="filter"></param>
        /// <returns></returns>
        public Result<TRowSchema> Query<TRowSchema>(string filter)
        {
            return Query<TRowSchema>(filter, 0, -1, "");
        }

        /// <summary>
        /// Query with string filter and paging
        /// </summary>
        /// <typeparam name="TRowSchema"></typeparam>
        /// <param name="filter"></param>
        /// <param name="start"></param>
        /// <param name="count"></param>
        /// <returns></returns>
        public Result<TRowSchema> Query<TRowSchema>(string filter, int start, int count, string orderby)
        {
            Packet p = CreatePacket();
            p.Command = "" + COMMANDS.QueryType;
            p.Start = start;
            p.Count = count;
            p.OrderBy = orderby;
            p.Data = new object[] { typeof(TRowSchema).AssemblyQualifiedName, filter };
            ReturnPacket ret = (ReturnPacket)_client.Send(p);
            Result<object> res = (Result<object>)ret.Data;
            return GenericResult<TRowSchema>(res);
        }

        /// <summary>
        /// Count with LINQ filter
        /// </summary>
        /// <typeparam name="TRowSchema"></typeparam>
        /// <param name="filter"></param>
        /// <returns></returns>
        public int Count<TRowSchema>(Expression<Predicate<TRowSchema>> filter)
        {
            LINQString ls = new LINQString();
            ls.Visit(filter);
            Packet p = CreatePacket();
            p.Command = "" + COMMANDS.GCount;
            p.Viewname = typeof(TRowSchema).AssemblyQualifiedName;
            p.Data = ls.sb.ToString();
            ReturnPacket ret = (ReturnPacket)_client.Send(p);
            return (int)ret.Data;
        }

        /// <summary>
        /// Fetch the document change history
        /// </summary>
        /// <param name="docid"></param>
        /// <returns></returns>
        public int[] FetchHistory(Guid docid)
        {
            Packet p = CreatePacket();
            p.Command = "" + COMMANDS.DocHistory;
            p.Docid = docid;
            ReturnPacket ret = (ReturnPacket)_client.Send(p);
            return (int[])ret.Data;
        }

        /// <summary>
        /// Fetch the file change history
        /// </summary>
        /// <param name="fileid"></param>
        /// <returns></returns>
        public int[] FetchBytesHistory(Guid fileid)
        {
            Packet p = CreatePacket();
            p.Command = "" + COMMANDS.FileHistory;
            p.Docid = fileid;
            ReturnPacket ret = (ReturnPacket)_client.Send(p);
            return (int[])ret.Data;
        }

        /// <summary>
        /// Fetch a specific document version
        /// </summary>
        /// <param name="versionNumber"></param>
        /// <returns></returns>
        public object FetchVersion(int versionNumber)
        {
            Packet p = CreatePacket();
            p.Command = "" + COMMANDS.FetchVersion;
            p.Data = versionNumber;
            ReturnPacket ret = (ReturnPacket)_client.Send(p);
            return ret.Data;
        }

        /// <summary>
        /// Fetch a specific file version
        /// </summary>
        /// <param name="versionNumber"></param>
        /// <returns></returns>
        public byte[] FetchBytesVersion(int versionNumber)
        {
            Packet p = CreatePacket();
            p.Command = "" + COMMANDS.FetchFileVersion;
            p.Data = versionNumber;
            ReturnPacket ret = (ReturnPacket)_client.Send(p);
            return (byte[])ret.Data;
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
            return this.Query(viewname, filter, start, count, "");
        }

        /// <summary>
        /// Query a view with paging
        /// </summary>
        /// <typeparam name="TRowSchema"></typeparam>
        /// <param name="viewname"></param>
        /// <param name="filter"></param>
        /// <param name="start"></param>
        /// <param name="count"></param>
        /// <returns></returns>
        public Result<object> Query<TRowSchema>(string viewname, Expression<Predicate<TRowSchema>> filter, int start, int count)
        {
            return this.Query(viewname, filter, start, count, "");
        }

        /// <summary>
        /// Query a view with paging
        /// </summary>
        /// <typeparam name="TRowSchema"></typeparam>
        /// <param name="filter"></param>
        /// <param name="start"></param>
        /// <param name="count"></param>
        /// <returns></returns>
        public Result<TRowSchema> Query<TRowSchema>(Expression<Predicate<TRowSchema>> filter, int start, int count)
        {
            return Query<TRowSchema>(filter, start, count, "");
        }

        /// <summary>
        /// Query a view with paging
        /// </summary>
        /// <typeparam name="TRowSchema"></typeparam>
        /// <param name="filter"></param>
        /// <param name="start"></param>
        /// <param name="count"></param>
        /// <returns></returns>
        public Result<TRowSchema> Query<TRowSchema>(string filter, int start, int count)
        {
            return Query<TRowSchema>(filter, start, count, "");
        }

        /// <summary>
        /// Fetch a change history for a document with dates
        /// </summary>
        /// <param name="docid"></param>
        /// <returns></returns>
        public HistoryInfo[] FetchHistoryInfo(Guid docid)
        {
            Packet p = CreatePacket();
            p.Command = "" + COMMANDS.FetchHistoryInfo;
            p.Docid = docid;
            ReturnPacket ret = (ReturnPacket)_client.Send(p);
            return (HistoryInfo[])ret.Data;
        }

        /// <summary>
        /// Fetch a change history for a file with dates
        /// </summary>
        /// <param name="docid"></param>
        /// <returns></returns>
        public HistoryInfo[] FetchBytesHistoryInfo(Guid docid)
        {
            Packet p = CreatePacket();
            p.Command = "" + COMMANDS.FetchByteHistoryInfo;
            p.Docid = docid;
            ReturnPacket ret = (ReturnPacket)_client.Send(p);
            return (HistoryInfo[])ret.Data;
        }

        /// <summary>
        /// Delete directly from a view using a filter
        /// </summary>
        /// <typeparam name="TRowSchema"></typeparam>
        /// <param name="filter"></param>
        /// <returns></returns>
        public int ViewDelete<TRowSchema>(Expression<Predicate<TRowSchema>> filter)
        {
            LINQString ls = new LINQString();
            ls.Visit(filter);
            Packet p = CreatePacket();
            p.Command = "" + COMMANDS.ViewDelete_t;
            p.Data = new object[] { typeof(TRowSchema).AssemblyQualifiedName, ls.sb.ToString() };
            ReturnPacket ret = (ReturnPacket)_client.Send(p);
            return (int)ret.Data;
        }

        /// <summary>
        /// Delete directly from a view using a filter
        /// </summary>
        /// <param name="viewname"></param>
        /// <param name="filter"></param>
        /// <returns></returns>
        public int ViewDelete(string viewname, string filter)
        {
            Packet p = CreatePacket();
            p.Command = "" + COMMANDS.ViewDelete;
            p.Data = new object[] { viewname, filter };
            ReturnPacket ret = (ReturnPacket)_client.Send(p);
            return (int)ret.Data;
        }

        /// <summary>
        /// Insert directly into a view
        /// </summary>
        /// <typeparam name="TRowSchema"></typeparam>
        /// <param name="id"></param>
        /// <param name="row"></param>
        /// <returns></returns>
        public bool ViewInsert<TRowSchema>(Guid id, TRowSchema row)
        {
            Packet p = CreatePacket();
            p.Command = "" + COMMANDS.ViewInsert_t;
            p.Docid = id;
            p.Data = new object[] { typeof(TRowSchema).AssemblyQualifiedName, row };
            ReturnPacket ret = (ReturnPacket)_client.Send(p);
            return (bool)ret.Data;
        }

        /// <summary>
        /// Insert directly into a view
        /// </summary>
        /// <param name="viewname"></param>
        /// <param name="id"></param>
        /// <param name="row"></param>
        /// <returns></returns>
        public bool ViewInsert(string viewname, Guid id, object row)
        {
            Packet p = CreatePacket();
            p.Command = "" + COMMANDS.ViewInsert;
            p.Docid = id;
            p.Data = new object[] { viewname, row };
            ReturnPacket ret = (ReturnPacket)_client.Send(p);
            return (bool)ret.Data;
        }

        /// <summary>
        ///  Get the number of documents in the storage file regardless of versions
        /// </summary>
        /// <returns></returns>
        public long DocumentCount()
        {
            Packet p = CreatePacket();
            p.Command = "" + COMMANDS.DocCount;
            ReturnPacket ret = (ReturnPacket)_client.Send(p);
            return (long)ret.Data;
        }

        public IKeyStoreHF GetKVHF()
        {
            return _kv;
        }
    }
}
