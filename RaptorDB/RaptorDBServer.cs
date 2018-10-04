using System;
using System.Collections.Generic;
using System.Text;
using RaptorDB.Common;
using System.Reflection;
using System.IO;
using System.Threading.Tasks;

namespace RaptorDB
{
    public delegate void Handler(Packet data, ReturnPacket ret);

    public class instance_handler
    {
        public bool Initialized = false;
        public string dbpath;
        public MethodInfo register;
        public MethodInfo save;
        public RaptorDB rdb;
        public DateTime lastUsed = DateTime.MinValue;
        public bool hasExtensions = false;
        public SafeDictionary<Type, MethodInfo> saveCache = new SafeDictionary<Type, MethodInfo>();
        public SafeDictionary<string, ServerSideFunc> ssideCache = new SafeDictionary<string, ServerSideFunc>();
        public SafeDictionary<string, ServerSideFuncWithArgs> sswcideCache = new SafeDictionary<string, ServerSideFuncWithArgs>();
    }

    public class RaptorDBServer
    {
        public RaptorDBServer(int port, string DataPath)
        {
            _path = Directory.GetCurrentDirectory();
            AppDomain.CurrentDomain.AssemblyResolve += new ResolveEventHandler(CurrentDomain_AssemblyResolve);
            _server = new NetworkServer();

            if (_S == "/")// unix system
                _datapath = DataPath.Replace("\\", "/");
            else
                _datapath = DataPath;

            if (_datapath.EndsWith(_S) == false)
                _datapath += _S;

            // check if "instances" folder exist -> multi instance mode
            if (Directory.Exists(_datapath + "instances"))
            {
                _log.Debug("Insances exist, loading...");
                _multiInstance = true;
                foreach (var d in Directory.GetDirectories(_datapath + "instances"))
                {
                    var dn = new DirectoryInfo(d);
                    var i = new instance_handler();
                    i.dbpath = d;
                    if (Directory.Exists(d + _S + "Extensions"))
                        i.hasExtensions = true;
                    _instances.Add(dn.Name.ToLower(), i);
                }
            }
            _defaultInstance.rdb = RaptorDB.Open(DataPath);
            _defaultInstance.register = _defaultInstance.rdb.GetType().GetMethod("RegisterView", BindingFlags.Instance | BindingFlags.Public);
            _defaultInstance.save = _defaultInstance.rdb.GetType().GetMethod("Save", BindingFlags.Instance | BindingFlags.Public);

            Initialize();
            _server.Start(port, processpayload);

            // add timer to cleanup connected clients
            _concleanuptimer = new System.Timers.Timer(30 * 1000);
            _concleanuptimer.AutoReset = true;
            _concleanuptimer.Enabled = true;
            _concleanuptimer.Elapsed += _concleanuptimer_Elapsed;

            _unusedinstancetimer = new System.Timers.Timer(300 * 1000);// FIX : configuration here
            _unusedinstancetimer.AutoReset = true;
            _unusedinstancetimer.Enabled = true;
            _unusedinstancetimer.Elapsed += _unusedinstancetimer_Elapsed;
        }

        private object _lock = new object();
        private void _unusedinstancetimer_Elapsed(object sender, System.Timers.ElapsedEventArgs e)
        {
            lock (_lock)
            {
                bool freed = false;
                // clear unused rdb instances
                if (_multiInstance)
                {
                    foreach (var i in _instances)
                    {
                        if (i.Value.rdb != null &&
                            FastDateTime.Now.Subtract(i.Value.lastUsed).TotalMinutes > 60) // FIX : configuration here
                        {
                            var r = i.Value;
                            r.rdb.Shutdown();
                            r.Initialized = false;
                            r.register = null;
                            r.save = null;
                            r.saveCache = new SafeDictionary<Type, MethodInfo>();
                            r.ssideCache = new SafeDictionary<string, ServerSideFunc>();
                            r.sswcideCache = new SafeDictionary<string, ServerSideFuncWithArgs>();
                            r.rdb = null;

                            freed = true;
                        }
                    }
                    if (freed)
                        GC.Collect();
                }
            }
        }

        void _concleanuptimer_Elapsed(object sender, System.Timers.ElapsedEventArgs e)
        {
            _connectedClients.Clear();
        }

        private bool _multiInstance = false;
        private SafeDictionary<string, instance_handler> _instances = new SafeDictionary<string, instance_handler>();
        private instance_handler _defaultInstance = new instance_handler();

        private string _S = Path.DirectorySeparatorChar.ToString();
        private Dictionary<string, uint> _users = new Dictionary<string, uint>();
        private string _path = "";
        private string _datapath = "";
        private ILog _log = LogManager.GetLogger(typeof(RaptorDBServer));
        private NetworkServer _server;
        //private RaptorDB _raptor;
        //private MethodInfo register = null;
        //private MethodInfo save = null;
        //private SafeDictionary<Type, MethodInfo> _savecache = new SafeDictionary<Type, MethodInfo>();
        //private SafeDictionary<string, ServerSideFunc> _ssidecache = new SafeDictionary<string, ServerSideFunc>();
        //private SafeDictionary<string, ServerSideFuncWithArgs> _sswcidecache = new SafeDictionary<string, ServerSideFuncWithArgs>();
        private Dictionary<string, Handler> _handlers = new Dictionary<string, Handler>();
        private const string _RaptorDB_users_config = "RaptorDB-Users.config";
        private SafeDictionary<Guid, bool> _connectedClients = new SafeDictionary<Guid, bool>();
        private System.Timers.Timer _concleanuptimer;
        private System.Timers.Timer _unusedinstancetimer;

        public int ConnectedClients { get { return _connectedClients.Count(); } }

        private Assembly CurrentDomain_AssemblyResolve(object sender, ResolveEventArgs args) // FIX : handle instance??
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
            if (_defaultInstance.saveCache.TryGetValue(type, out m))
                return m;

            m = _defaultInstance.save.MakeGenericMethod(new Type[] { type });
            _defaultInstance.saveCache.Add(type, m);
            return m;
        }

        public void Shutdown()
        {
            WriteUsers();
            _server.Stop();
            _defaultInstance.rdb.Shutdown();

            foreach (var i in _instances)
            {
                _log.Debug("Shutting down instance : " + i.Key);
                if (i.Value.rdb != null)
                    i.Value.rdb.Shutdown();
            }
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

            File.WriteAllText(_datapath + _RaptorDB_users_config, sb.ToString());
        }

        private object processpayload(object data)
        {
            Packet p = (Packet)data;
            ReturnPacket ret = new ReturnPacket(true);

            if (Authenticate(p) == false)
                return new ReturnPacket(false, "Authentication failed");
            if (p.Command == "_close")
            {
                _connectedClients.Remove(p.ClientID);
                return ret;
            }
            else
                _connectedClients.Add(p.ClientID, true);

            try
            {
                Handler d = null;
                if (_handlers.TryGetValue(p.Command, out d))
                    d(p, ret);
                else
                    _log.Error("Command handler not found : " + p.Command);
            }
            catch (Exception ex)
            {
                ret.OK = false;
                _log.Error(ex);
            }
            return ret;
        }

        private RaptorDB GetInstance(string name)
        {
            // load or get instance
            instance_handler inst = null;
            _instances.TryGetValue(name.ToLower(), out inst);
            if(inst==null)
            {
                // no instance found -> err
                _log.Debug("instance name not found : " + name);
                return null;
            }
            if(inst.rdb==null)
            {
                // try loading raptordb instance
                var r = RaptorDB.Open(inst.dbpath);
                inst.rdb = r;
                // fix : create register and save 
                //inst.register 
                //inst.save

                if (inst.hasExtensions)
                {
                    // fix: load extension folder
                }
                else
                {
                    // fix: load default extenstions
                }
            }
            return inst.rdb;
        }

        private void InitializeCommandsDictionary()
        {
            // FIX : route to instance on p.InstanceName

            _handlers.Add("" + COMMANDS.Save,
                (p, ret) =>
                {
                    var m = GetSave(p.Data.GetType());
                    ret.OK = true;
                    m.Invoke(_defaultInstance.rdb, new object[] { p.Docid, p.Data });
                });

            _handlers.Add("" + COMMANDS.SaveBytes,
                (p, ret) =>
                {
                    ret.OK = _defaultInstance.rdb.SaveBytes(p.Docid, (byte[])p.Data);
                });

            _handlers.Add("" + COMMANDS.QueryType,
                (p, ret) =>
                {
                    var param = (object[])p.Data;
                    Type t = Type.GetType((string)param[0]);
                    string viewname = _defaultInstance.rdb.GetViewName(t);
                    ret.OK = true;
                    ret.Data = _defaultInstance.rdb.Query(viewname, (string)param[1], p.Start, p.Count, p.OrderBy);
                });

            _handlers.Add("" + COMMANDS.QueryStr,
                (p, ret) =>
                {
                    ret.OK = true;
                    ret.Data = _defaultInstance.rdb.Query(p.Viewname, (string)p.Data, p.Start, p.Count, p.OrderBy);
                });

            _handlers.Add("" + COMMANDS.Fetch,
                (p, ret) =>
                {
                    ret.OK = true;
                    ret.Data = _defaultInstance.rdb.Fetch(p.Docid);
                });

            _handlers.Add("" + COMMANDS.FetchBytes,
                (p, ret) =>
                {
                    ret.OK = true;
                    ret.Data = _defaultInstance.rdb.FetchBytes(p.Docid);
                });

            _handlers.Add("" + COMMANDS.Backup,
                (p, ret) =>
                {
                    ret.OK = _defaultInstance.rdb.Backup();
                });

            _handlers.Add("" + COMMANDS.Delete,
                (p, ret) =>
                {
                    ret.OK = _defaultInstance.rdb.Delete(p.Docid);
                });

            _handlers.Add("" + COMMANDS.DeleteBytes,
                (p, ret) =>
                {
                    ret.OK = _defaultInstance.rdb.DeleteBytes(p.Docid);
                });

            _handlers.Add("" + COMMANDS.Restore,
                (p, ret) =>
                {
                    ret.OK = true;
                    Task.Factory.StartNew(() => _defaultInstance.rdb.Restore());
                });

            _handlers.Add("" + COMMANDS.AddUser,
                (p, ret) =>
                {
                    var param = (object[])p.Data;
                    ret.OK = AddUser((string)param[0], (string)param[1], (string)param[2]);
                });

            _handlers.Add("" + COMMANDS.ServerSide,
                (p, ret) =>
                {
                    var param = (object[])p.Data;
                    ret.OK = true;
                    ret.Data = _defaultInstance.rdb.ServerSide(GetServerSideFuncCache(param[0].ToString(), param[1].ToString()), param[2].ToString());
                });

            _handlers.Add("" + COMMANDS.FullText,
                (p, ret) =>
                {
                    var param = (object[])p.Data;
                    ret.OK = true;
                    ret.Data = _defaultInstance.rdb.FullTextSearch("" + param[0]);
                });

            _handlers.Add("" + COMMANDS.CountType,
                (p, ret) =>
                {
                    // count type
                    var param = (object[])p.Data;
                    Type t = Type.GetType((string)param[0]);
                    string viewname2 = _defaultInstance.rdb.GetViewName(t);
                    ret.OK = true;
                    ret.Data = _defaultInstance.rdb.Count(viewname2, (string)param[1]);
                });

            _handlers.Add("" + COMMANDS.CountStr,
                (p, ret) =>
                {
                    // count str
                    ret.OK = true;
                    ret.Data = _defaultInstance.rdb.Count(p.Viewname, (string)p.Data);
                });

            _handlers.Add("" + COMMANDS.GCount,
                (p, ret) =>
                {
                    Type t = Type.GetType(p.Viewname);
                    string viewname3 = _defaultInstance.rdb.GetViewName(t);
                    ret.OK = true;
                    ret.Data = _defaultInstance.rdb.Count(viewname3, (string)p.Data);
                });

            _handlers.Add("" + COMMANDS.DocHistory,
                (p, ret) =>
                {
                    ret.OK = true;
                    ret.Data = _defaultInstance.rdb.FetchHistory(p.Docid);
                });

            _handlers.Add("" + COMMANDS.FileHistory,
                (p, ret) =>
                {
                    ret.OK = true;
                    ret.Data = _defaultInstance.rdb.FetchBytesHistory(p.Docid);
                });

            _handlers.Add("" + COMMANDS.FetchVersion,
                (p, ret) =>
                {
                    ret.OK = true;
                    ret.Data = _defaultInstance.rdb.FetchVersion((int)p.Data);
                });

            _handlers.Add("" + COMMANDS.FetchFileVersion,
                (p, ret) =>
                {
                    ret.OK = true;
                    ret.Data = _defaultInstance.rdb.FetchBytesVersion((int)p.Data);
                });

            _handlers.Add("" + COMMANDS.CheckAssembly,
                (p, ret) =>
                {
                    ret.OK = true;
                    string typ = "";
                    ret.Data = _defaultInstance.rdb.GetAssemblyForView(p.Viewname, out typ);
                    ret.Error = typ;
                });
            _handlers.Add("" + COMMANDS.FetchHistoryInfo,
                (p, ret) =>
                {
                    ret.OK = true;
                    ret.Data = _defaultInstance.rdb.FetchHistoryInfo(p.Docid);
                });

            _handlers.Add("" + COMMANDS.FetchByteHistoryInfo,
                (p, ret) =>
                {
                    ret.OK = true;
                    ret.Data = _defaultInstance.rdb.FetchBytesHistoryInfo(p.Docid);
                });

            _handlers.Add("" + COMMANDS.ViewDelete,
                (p, ret) =>
                {
                    ret.OK = true;
                    var param = (object[])p.Data;
                    ret.Data = _defaultInstance.rdb.ViewDelete((string)param[0], (string)param[1]);
                });

            _handlers.Add("" + COMMANDS.ViewDelete_t,
                (p, ret) =>
                {
                    ret.OK = true;
                    var param = (object[])p.Data;
                    Type t = Type.GetType((string)param[0]);
                    string viewname4 = _defaultInstance.rdb.GetViewName(t);
                    ret.Data = _defaultInstance.rdb.ViewDelete(viewname4, (string)param[1]);
                });

            _handlers.Add("" + COMMANDS.ViewInsert,
                (p, ret) =>
                {
                    ret.OK = true;
                    var param = (object[])p.Data;
                    ret.Data = _defaultInstance.rdb.ViewInsert((string)param[0], p.Docid, param[1]);
                });

            _handlers.Add("" + COMMANDS.ViewInsert_t,
                (p, ret) =>
                {
                    ret.OK = true;
                    var param = (object[])p.Data;
                    Type t = Type.GetType((string)param[0]);
                    string viewname5 = _defaultInstance.rdb.GetViewName(t);
                    ret.Data = _defaultInstance.rdb.ViewInsert(viewname5, p.Docid, param[1]);
                });

            _handlers.Add("" + COMMANDS.DocCount,
                (p, ret) =>
                {
                    ret.OK = true;
                    ret.Data = _defaultInstance.rdb.DocumentCount();
                });

            _handlers.Add("" + COMMANDS.GetObjectHF,
                (p, ret) =>
                {
                    ret.OK = true;
                    ret.Data = _defaultInstance.rdb.GetKVHF().GetObjectHF((string)p.Data);
                });

            _handlers.Add("" + COMMANDS.SetObjectHF,
                (p, ret) =>
                {
                    ret.OK = true;
                    var param = (object[])p.Data;
                    _defaultInstance.rdb.GetKVHF().SetObjectHF((string)param[0], param[1]);
                });

            _handlers.Add("" + COMMANDS.DeleteKeyHF,
                (p, ret) =>
                {
                    ret.OK = true;
                    ret.Data = _defaultInstance.rdb.GetKVHF().DeleteKeyHF((string)p.Data);
                });

            _handlers.Add("" + COMMANDS.CountHF,
                (p, ret) =>
                {
                    ret.OK = true;
                    ret.Data = _defaultInstance.rdb.GetKVHF().CountHF();
                });

            _handlers.Add("" + COMMANDS.ContainsHF,
                (p, ret) =>
                {
                    ret.OK = true;
                    ret.Data = _defaultInstance.rdb.GetKVHF().ContainsHF((string)p.Data);
                });

            _handlers.Add("" + COMMANDS.GetKeysHF,
                (p, ret) =>
                {
                    ret.OK = true;
                    ret.Data = _defaultInstance.rdb.GetKVHF().GetKeysHF();
                });

            _handlers.Add("" + COMMANDS.CompactStorageHF,
                (p, ret) =>
                {
                    ret.OK = true;
                    _defaultInstance.rdb.GetKVHF().CompactStorageHF();
                });

            _handlers.Add("" + COMMANDS.IncrementHF,
                (p, ret) =>
                {
                    ret.OK = true;
                    var param = (object[])p.Data;
                    if (param[1] is int)
                        ret.Data = _defaultInstance.rdb.GetKVHF().Increment((string)param[0], (int)param[1]);
                    else
                        ret.Data = _defaultInstance.rdb.GetKVHF().Increment((string)param[0], (decimal)param[1]);
                });

            _handlers.Add("" + COMMANDS.DecrementHF,
                (p, ret) =>
                {
                    ret.OK = true;
                    var param = (object[])p.Data;
                    if (param[1] is int)
                        ret.Data = _defaultInstance.rdb.GetKVHF().Decrement((string)param[0], (int)param[1]);
                    else
                        ret.Data = _defaultInstance.rdb.GetKVHF().Decrement((string)param[0], (decimal)param[1]);
                });

            _handlers.Add("" + COMMANDS.ServerSideWithArgs,
                (p, ret) =>
                {
                    var param = (object[])p.Data;
                    ret.OK = true;
                    ret.Data = _defaultInstance.rdb.ServerSide(GetServerSideFuncWithArgsCache(param[0].ToString(), param[1].ToString()), param[2].ToString(), param[3]);
                });

            _handlers.Add("" + COMMANDS.FreeMemory,
                (p, ret) =>
                {
                    ret.OK = true;
                    _log.Debug("Free memory called from client");
                    _defaultInstance.rdb.FreeMemory();
                });
        }

        private ServerSideFuncWithArgs GetServerSideFuncWithArgsCache(string type, string method)
        {
            ServerSideFuncWithArgs func;
            _log.Debug("Calling Server side Function with args : " + method + " on type " + type);
            if (_defaultInstance.sswcideCache.TryGetValue(type + method, out func) == false)
            {
                Type tt = Type.GetType(type);

                func = (ServerSideFuncWithArgs)Delegate.CreateDelegate(typeof(ServerSideFuncWithArgs), tt, method);
                _defaultInstance.sswcideCache.Add(type + method, func);
            }
            return func;
        }

        private ServerSideFunc GetServerSideFuncCache(string type, string method)
        {
            ServerSideFunc func;
            _log.Debug("Calling Server side Function : " + method + " on type " + type);
            if (_defaultInstance.ssideCache.TryGetValue(type + method, out func) == false)
            {
                Type tt = Type.GetType(type);

                func = (ServerSideFunc)Delegate.CreateDelegate(typeof(ServerSideFunc), tt, method);
                _defaultInstance.ssideCache.Add(type + method, func);
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
            _log.Debug("Authentication failed for '" + p.Username + "' hash = " + p.PasswordHash);
            return false;
        }

        private void Initialize()
        {
            // load users here
            if (File.Exists(_datapath + _RaptorDB_users_config))
            {
                foreach (string line in File.ReadAllLines(_datapath + _RaptorDB_users_config))
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
                _log.Debug("loading dll for views : " + f);
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
                            var m = _defaultInstance.register.MakeGenericMethod(new Type[] { tt });
                            m.Invoke(_defaultInstance.rdb, new object[] { o });
                        }
                        catch (Exception ex)
                        {
                            _log.Error(ex);
                        }
                    }
                }
            }

            InitializeCommandsDictionary();
        }
    }
}
