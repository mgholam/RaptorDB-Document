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
            AppDomain.CurrentDomain.AssemblyResolve += new ResolveEventHandler(CurrentDomain_AssemblyResolve);
            _server = new NetworkServer();//90, processpayload);
            _raptor = RaptorDB.Open(DataPath);
            register = _raptor.GetType().GetMethod("RegisterView", BindingFlags.Instance | BindingFlags.Public);
            save = _raptor.GetType().GetMethod("Save", BindingFlags.Instance | BindingFlags.Public);
            Initialize();
            _server.Start(port, processpayload);
        }

        Assembly CurrentDomain_AssemblyResolve(object sender, ResolveEventArgs args)
        {
            string[] ss = args.Name.Split(',');
            string fname = ss[0] + ".dll";
            if (File.Exists(fname))
                return Assembly.LoadFile(fname);
            fname = "Extensions\\" + fname;
            if (File.Exists(fname))
                return Assembly.LoadFile(fname);
            else return null;
        }

        private ILog log = LogManager.GetLogger(typeof(RaptorDBServer));
        private NetworkServer _server;
        private RaptorDB _raptor;
        private MethodInfo register = null;
        private MethodInfo save = null;
        SafeDictionary<Type, MethodInfo> _savecache = new SafeDictionary<Type, MethodInfo>();

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
            _server.Stop();
            _raptor.Shutdown();
        }


        private object processpayload(object data)
        {
            Packet p = (Packet)data;
            ReturnPacket ret = new ReturnPacket();
            ret.OK = true;
            try
            {
                object[] param;

                switch (p.Command)
                {
                    case "save":
                        var m = GetSave(p.Data.GetType());
                        m.Invoke(_raptor, new object[] { p.Docid, p.Data });
                        break;
                    case "savebytes":
                        break;
                    case "querytype":
                        param = (object[])p.Data;
                        Type t = Type.GetType((string)param[0]);
                        string viewname = _raptor.GetViewName(t);
                        ret.Data = _raptor.Query(viewname, (string)param[1]);
                        break;
                    case "querystr":
                        ret.Data = _raptor.Query(p.Viewname, (string)p.Data);
                        break;
                    case "fetch":
                        ret.Data = _raptor.Fetch(p.Docid);
                        break;
                    case "fetchbytes":
                        ret.Data = _raptor.FetchBytes(p.Docid);
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

        private void Initialize()
        {
            // exe folder
            // |-Data
            // |-LOGS
            // |-Extensions


            // FIX : - open extensions folder
            string path = Directory.GetCurrentDirectory() + "";

            foreach (var f in Directory.GetFiles(path, "*.dll"))
            {
                //        - load all dll files
                //        - register views 
                log.Debug("loading : " + f);
                Assembly a = Assembly.LoadFile(f);
                foreach (var t in a.GetTypes())
                {
                    foreach (var att in t.GetCustomAttributes(typeof(RegisterViewAttribute), false))
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
                }
            }
        }
    }
}
