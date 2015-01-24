using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using RaptorDB.Common;
using System.Reflection;
using System.IO;
using System.Threading.Tasks;
using System.Threading;

namespace RaptorDB
{
    public class RaptorDBServer
    {
        public RaptorDBServer(int port, string DataPath)
        {
            _path = Directory.GetCurrentDirectory();
            AppDomain.CurrentDomain.AssemblyResolve += new ResolveEventHandler(CurrentDomain_AssemblyResolve);
            AppDomain.CurrentDomain.ProcessExit += new EventHandler(CurrentDomain_ProcessExit);
            _server = new NetworkServer();

            _datapath = DataPath;
            if (_datapath.EndsWith(_S) == false)
                _datapath += _S;
            _raptor = RaptorDB.Open(DataPath);
            register = _raptor.GetType().GetMethod("RegisterView", BindingFlags.Instance | BindingFlags.Public);
            save = _raptor.GetType().GetMethod("Save", BindingFlags.Instance | BindingFlags.Public);
            Initialize();
            _server.Start(port, processpayload);
        }

        void CurrentDomain_ProcessExit(object sender, EventArgs e)
        {
            //perform cleanup here
            log.Debug("process exited");
            Shutdown();
        }

        private string _S = Path.DirectorySeparatorChar.ToString();
        private Dictionary<string, uint> _users = new Dictionary<string, uint>();
        private string _path = "";
        private string _datapath = "";
        private ILog log = LogManager.GetLogger(typeof(RaptorDBServer));
        private NetworkServer _server;
        private RaptorDB _raptor;
        private MethodInfo register = null;
        private MethodInfo save = null;
        private SafeDictionary<Type, MethodInfo> _savecache = new SafeDictionary<Type, MethodInfo>();
        private SafeDictionary<string, ServerSideFunc> _ssidecache = new SafeDictionary<string, ServerSideFunc>();

        private Assembly CurrentDomain_AssemblyResolve(object sender, ResolveEventArgs args)
        {
            if (File.Exists(args.Name))
                return Assembly.LoadFrom(args.Name);
            string[] ss = args.Name.Split(',');
            string fname = ss[0] + ".dll";
            if (File.Exists(fname))
                return Assembly.LoadFrom(fname);
            fname = "Extensions" + _S + fname;
            if (File.Exists(fname))
                return Assembly.LoadFrom(fname);
            else return null;
        }

        private MethodInfo GetSave(Type type)
        {
            MethodInfo m = null;
            if (_savecache.TryGetValue(type, out m))
                return m;

            m = save.MakeGenericMethod(new Type[] { type });
            _savecache.Add(type, m);
            return m;
        }

        public void Shutdown()
        {
            WriteUsers();
            _server.Stop();
        }

        private void WriteUsers()
        {
            // write users to user.config file
            StringBuilder sb = new StringBuilder();
            sb.AppendLine("# FORMAT : username , pasword hash");
            sb.AppendLine("# To disable a user comment the line with the '#'");
            foreach (var kv in _users)
            {
                sb.AppendLine(kv.Key + " , " + kv.Value);
            }

            File.WriteAllText(_datapath + "RaptorDB-Users.config", sb.ToString());
        }

        private object processpayload(object data)
        {
            Packet p = (Packet)data;

            if (Authenticate(p) == false)
                return new ReturnPacket(false, "Authentication failed");

            ReturnPacket ret = new ReturnPacket(true);
            try
            {
                object[] param = null;

                switch (p.Command)
                {
                    case "save":
                        var m = GetSave(p.Data.GetType());
                        m.Invoke(_raptor, new object[] { p.Docid, p.Data });
                        break;
                    case "savebytes":
                        ret.OK = _raptor.SaveBytes(p.Docid, (byte[])p.Data);
                        break;
                    case "querytype":
                        param = (object[])p.Data;
                        Type t = Type.GetType((string)param[0]);
                        string viewname = _raptor.GetViewName(t);
                        ret.OK = true;
                        ret.Data = _raptor.Query(viewname, (string)param[1], p.Start, p.Count, p.OrderBy);
                        break;
                    case "querystr":
                        ret.OK = true;
                        ret.Data = _raptor.Query(p.Viewname, (string)p.Data, p.Start, p.Count, p.OrderBy);
                        break;
                    case "fetch":
                        ret.OK = true;
                        ret.Data = _raptor.Fetch(p.Docid);
                        break;
                    case "fetchbytes":
                        ret.OK = true;
                        ret.Data = _raptor.FetchBytes(p.Docid);
                        break;
                    case "backup":
                        ret.OK = _raptor.Backup();
                        break;
                    case "delete":
                        ret.OK = _raptor.Delete(p.Docid);
                        break;
                    case "deletebytes":
                        ret.OK = _raptor.DeleteBytes(p.Docid);
                        break;
                    case "restore":
                        ret.OK = true;
                        Task.Factory.StartNew(() => _raptor.Restore());
                        break;
                    case "adduser":
                        param = (object[])p.Data;
                        ret.OK = AddUser((string)param[0], (string)param[1], (string)param[2]);
                        break;
                    case "serverside":
                        param = (object[])p.Data;
                        ret.OK = true;
                        ret.Data = _raptor.ServerSide(GetServerSideFuncCache(param[0].ToString(), param[1].ToString()), param[2].ToString());
                        break;
                    case "fulltext":
                        param = (object[])p.Data;
                        ret.OK = true;
                        ret.Data = _raptor.FullTextSearch("" + param[0]);
                        break;
                    case "counttype":
                        // count type
                        param = (object[])p.Data;
                        Type t2 = Type.GetType((string)param[0]);
                        string viewname2 = _raptor.GetViewName(t2);
                        ret.OK = true;
                        ret.Data = _raptor.Count(viewname2, (string)param[1]);
                        break;
                    case "countstr":
                        // count str
                        ret.OK = true;
                        ret.Data = _raptor.Count(p.Viewname, (string)p.Data);
                        break;
                    case "gcount":
                        Type t3 = Type.GetType(p.Viewname);
                        string viewname3 = _raptor.GetViewName(t3);
                        ret.OK = true;
                        ret.Data = _raptor.Count(viewname3, (string)p.Data);
                        break;
                    case "dochistory":
                        ret.OK = true;
                        ret.Data = _raptor.FetchHistory(p.Docid);
                        break;
                    case "filehistory":
                        ret.OK = true;
                        ret.Data = _raptor.FetchBytesHistory(p.Docid);
                        break;
                    case "fetchversion":
                        ret.OK = true;
                        ret.Data = _raptor.FetchVersion((int)p.Data);
                        break;
                    case "fetchfileversion":
                        ret.OK = true;
                        ret.Data = _raptor.FetchBytesVersion((int)p.Data);
                        break;
                    case "checkassembly":
                        ret.OK = true;
                        string typ = "";
                        ret.Data = _raptor.GetAssemblyForView(p.Viewname, out typ);
                        ret.Error = typ;
                        break;
                    case "fetchhistoryinfo":
                        ret.OK = true;
                        ret.Data = _raptor.FetchHistoryInfo(p.Docid);
                        break;
                    case "fetchbytehistoryinfo":
                        ret.OK = true;
                        ret.Data = _raptor.FetchBytesHistoryInfo(p.Docid);
                        break;
                    case "viewdelete":
                        ret.OK = true;
                        param = (object[])p.Data;
                        ret.Data = _raptor.ViewDelete((string)param[0], (string)param[1]);
                        break;
                    case "viewdelete-t":
                        ret.OK = true;
                        param = (object[])p.Data;
                        Type t4 = Type.GetType((string)param[0]);
                        string viewname4 = _raptor.GetViewName(t4);
                        ret.Data = _raptor.ViewDelete(viewname4, (string)param[1]);
                        break;
                    case "viewinsert":
                        ret.OK = true;
                        param = (object[])p.Data;
                        ret.Data = _raptor.ViewInsert((string)param[0], p.Docid, param[1]);
                        break;
                    case "viewinsert-t":
                        ret.OK = true;
                        param = (object[])p.Data;
                        Type t5 = Type.GetType((string)param[0]);
                        string viewname5 = _raptor.GetViewName(t5);
                        ret.Data = _raptor.ViewInsert(viewname5, p.Docid, param[1]);
                        break;
                    case "doccount":
                        ret.OK = true;
                        ret.Data = _raptor.DocumentCount();
                        break;
                    case "getobjecthf":
                        ret.OK = true;
                        ret.Data = _raptor.GetKVHF().GetObjectHF((string)p.Data);
                        break;
                    case "setobjecthf":
                        ret.OK = true;
                        param = (object[])p.Data;
                        _raptor.GetKVHF().SetObjectHF((string)param[0], param[1]);
                        break;
                    case "deletekeyhf":
                        ret.OK = true;
                        ret.Data = _raptor.GetKVHF().DeleteKeyHF((string)p.Data);
                        break;
                    case "counthf":
                        ret.OK = true;
                        ret.Data = _raptor.GetKVHF().CountHF();
                        break;
                    case "containshf":
                        ret.OK = true;
                        ret.Data = _raptor.GetKVHF().ContainsHF((string)p.Data);
                        break;
                    case "getkeyshf":
                        ret.OK = true;
                        ret.Data = _raptor.GetKVHF().GetKeysHF();
                        break;
                    case "compactstoragehf":
                        ret.OK = true;
                        _raptor.GetKVHF().CompactStorageHF();
                        break;
                }
            }
            catch (Exception ex)
            {
                ret.OK = false;
                log.Error(ex);
            }
            return ret;
        }

        private ServerSideFunc GetServerSideFuncCache(string type, string method)
        {
            ServerSideFunc func;
            log.Debug("Calling Server side Function : " + method + " on type " + type);
            if (_ssidecache.TryGetValue(type + method, out func) == false)
            {
                Type tt = Type.GetType(type);

                func = (ServerSideFunc)Delegate.CreateDelegate(typeof(ServerSideFunc), tt, method);
                _ssidecache.Add(type + method, func);
            }
            return func;
        }

        private uint GenHash(string user, string pwd)
        {
            return Helper.MurMur.Hash(Encoding.UTF8.GetBytes(user.ToLower() + "|" + pwd));
        }

        private bool AddUser(string user, string oldpwd, string newpwd)
        {
            uint hash = 0;
            if (_users.TryGetValue(user.ToLower(), out hash) == false)
            {
                _users.Add(user.ToLower(), GenHash(user, newpwd));
                return true;
            }
            if (hash == GenHash(user, oldpwd))
            {
                _users[user.ToLower()] = GenHash(user, newpwd);
                return true;
            }
            return false;
        }

        private bool Authenticate(Packet p)
        {
            uint pwd;
            if (_users.TryGetValue(p.Username.ToLower(), out pwd))
            {
                uint hash = uint.Parse(p.PasswordHash);
                if (hash == pwd) return true;
            }
            log.Debug("Authentication failed for '" + p.Username + "' hash = " + p.PasswordHash);
            return false;
        }

        private void Initialize()
        {
            // load users here
            if (File.Exists(_datapath + "RaptorDB-Users.config"))
            {
                foreach (string line in File.ReadAllLines(_datapath + "RaptorDB-Users.config"))
                {
                    if (line.Contains("#") == false)
                    {
                        string[] s = line.Split(',');
                        _users.Add(s[0].Trim().ToLower(), uint.Parse(s[1].Trim()));
                    }
                }
            }
            // add default admin user if not exists
            if (_users.ContainsKey("admin") == false)
                _users.Add("admin", GenHash("admin", "admin"));

            // exe folder
            // |-Extensions
            Directory.CreateDirectory(_path + _S + "Extensions");

            // open extensions folder
            string path = _path + _S + "Extensions";

            foreach (var f in Directory.GetFiles(path, "*.dll"))
            {
                //        - load all dll files
                //        - register views 
                log.Debug("loading dll for views : " + f);
                Assembly a = Assembly.Load(f);
                foreach (var t in a.GetTypes())
                {
                    foreach (var att in t.GetCustomAttributes(typeof(RegisterViewAttribute), false))
                    {
                        try
                        {
                            object o = Activator.CreateInstance(t);
                            //  handle types when view<T> also
                            Type[] args = t.GetGenericArguments();
                            if (args.Length == 0)
                                args = t.BaseType.GetGenericArguments();
                            Type tt = args[0];
                            var m = register.MakeGenericMethod(new Type[] { tt });
                            m.Invoke(_raptor, new object[] { o });
                        }
                        catch (Exception ex)
                        {
                            log.Error(ex);
                        }
                    }
                }
            }
        }
    }
}
