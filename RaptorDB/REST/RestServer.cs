using System;
using RaptorDB.Common;
using System.Net;
using System.Text;
using System.IO;
using System.Threading.Tasks;
using RaptorDB;
using System.Reflection;
using System.Text.RegularExpressions;
using System.Collections.Generic;
using System.IO.Compression;
using System.Diagnostics;

namespace RaptorDBRest
{
    #region [    rdb rest helper classes    ]
    public class RDBRoute
    {
        public string URL;
        public Type EntityType;
        public string Viewname;
        public ServerSideFunc function;
        public override string ToString()
        {
            if (EntityType != null)
                return "POST-able type : " + EntityType.Name;
            else if (function != null)
                return "Server side function";
            else if (Viewname != "")
                return "View name : " + Viewname;

            return "Undefined";
        }
    }

    public interface IRouteAPI
    {
        void AddRoute(RDBRoute route);
        void RegisterView<T>(View<T> view);
    }

    public interface IRDBRouting
    {
        void Initialize(IRouteAPI api);
    }

    public class RDBJsonContainer
    {
        public string URL;
        public DateTime date;
        public string json;
        public string useragent;
        public Guid docid;
    }
    #endregion


    // TODO : add back routing support
    // TODO : add security check on connection

    public class RestServer : IRouteAPI
    {
        public RestServer(int HttpPort, RaptorDB.RaptorDB rdb, string routingpath, bool localonly)
        {
            _localonly = localonly;
            _port = HttpPort;
            _rdb = rdb;
            //save = _rdb.GetType().GetMethod("Save", BindingFlags.Instance | BindingFlags.Public);
            //register = _rdb.GetType().GetMethod("RegisterView", BindingFlags.Instance | BindingFlags.Public);
            if (_S == "/")
                _path = routingpath.Replace("\\", "/");
            else
                _path = routingpath;

            Task.Factory.StartNew(() => Start(), TaskCreationOptions.LongRunning);
        }

        delegate void Handler(HttpListenerContext ctx, string args, List<object> output);

        private Dictionary<string, string> _WebCache = new Dictionary<string, string>();
        private string _S = Path.DirectorySeparatorChar.ToString();
        private ILog _log = LogManager.GetLogger(typeof(RestServer));
        private bool _run = true;
        private RaptorDB.RaptorDB _rdb;
        private int _port;
        private string _path;
        private SafeDictionary<string, RDBRoute> _routing = new SafeDictionary<string, RDBRoute>();
        private HttpListener _server;
        //private KeyStore<Guid> _jsonstore;
        private bool _localonly = false;
        private Dictionary<string, Handler> _handler = new Dictionary<string, Handler>();


        public void Stop()
        {
            _run = false;
            //_jsonstore.Shutdown();
            //_rdb.Shutdown();
        }

        public void AddRoute(RDBRoute route)
        {
            _log.Debug("adding route : " + route.URL);
            _routing.Add(route.URL.ToLower(), route);
        }

        public void RegisterView<T>(View<T> view)
        {
            _log.Debug("registering view : " + view.Name);
            AddRoute(new RDBRoute { URL = "RaptorDB/Views/" + view.Name, Viewname = view.Name });
            _rdb.RegisterView(view);
        }

        void IRouteAPI.AddRoute(RDBRoute route)
        {
            AddRoute(route);
        }

        void IRouteAPI.RegisterView<T>(View<T> view)
        {
            RegisterView(view);
        }

        public IRaptorDB RDB { get { return _rdb; } }

        #region [   private   ]

        private void Start()
        {
            WriteResources();

            InitializeCommandHandler();
            //// compile _path\Routing\*.route files and register
            //Directory.CreateDirectory(_path + _S + "Routing");
            //// delete existing compiled files
            //foreach (var f in Directory.GetFiles(_path + _S + "Routing", "*.dll"))
            //    File.Delete(f);
            //// compile route files
            //CompileAndRegisterScriptRoutes(_path + _S + "Routing");

            //_jsonstore = new KeyStore<Guid>(_path + _S + "Datajson" + _S + "json.mgdat", true);

            _server = new HttpListener();
            //_server.AuthenticationSchemes = AuthenticationSchemes.Basic;
            if (_localonly)
            {
                _server.Prefixes.Add("http://localhost:" + _port + "/");
                _server.Prefixes.Add("http://127.0.0.1:" + _port + "/");
                _server.Prefixes.Add("http://" + Environment.MachineName + ":" + _port + "/");
            }
            else
                _server.Prefixes.Add("http://*:" + _port + "/");

            _server.Start();
            while (_run)
            {
                var context = _server.BeginGetContext(new AsyncCallback(ListenerCallback), _server);
                context.AsyncWaitHandle.WaitOne();
            }
        }

        private void InitializeCommandHandler()
        {
            _handler.Add("getroutes",
                (ctx, args, o) =>
                {
                    foreach (var rr in _routing)
                        o.Add(new { URL = rr.Value.URL, Description = rr.Value.ToString() });
                });

            _handler.Add("getviews",
                (ctx, args, o) =>
                {
                    foreach (var v in _rdb.GetViews())
                        o.Add(new { Name = v.Name, Description = v.Description, BackgroundIndexing = v.BackgroundIndexing, Version = v.Version, isPrimaryList = v.isPrimaryList });
                });

            _handler.Add("getschema",
                (ctx, qry, o) =>
                {
                    if (qry == "")
                    {
                        WriteResponse(ctx, 404, "GetSchema requires a viewname to be defined e.g. ?view=customerview");
                    }
                    else
                    {
                        string view = qry.Split('=')[1];
                        var sc = _rdb.GetSchema(view);
                        foreach (var i in sc.Columns)
                            o.Add(new { ColumnName = i.Key, Type = i.Value.Name });
                    }
                });

            _handler.Add("systeminfo",
                (ctx, args, o) =>
                {
                    var oo = GetInfo();
                    var s = fastJSON.JSON.ToJSON(oo, new fastJSON.JSONParameters { UseExtensions = false, UseFastGuid = false, EnableAnonymousTypes = true });
                    ctx.Response.ContentType = "application/json";
                    WriteResponse(ctx, 200, s);
                });

            _handler.Add("action",
                (ctx, action, o) =>
                {
                    switch (action)
                    {
                        case "backup":
                            _rdb.Backup();
                            WriteResponse(ctx, 200, "\"Done\"");
                            break;
                        case "compact":
                            _rdb.GetKVHF().CompactStorageHF();
                            WriteResponse(ctx, 200, "\"Done\"");
                            break;
                        case "getconfigs":
                            WriteResponse(ctx, 200, File.ReadAllText(_path + "raptordb.config"));
                            break;
                    }
                });

            _handler.Add("docget", // takes : guid 
                (ctx, qry, o) =>
                {
                    var g = Guid.Parse(qry);
                    _log.Debug("docid = " + qry);
                    var s = fastJSON.JSON.ToNiceJSON(_rdb.Fetch(g), new fastJSON.JSONParameters { UseExtensions = true, UseFastGuid = false, UseEscapedUnicode = false });
                    ctx.Response.ContentType = "application/json";
                    WriteResponse(ctx, 200, s);
                });

            _handler.Add("dochistory", // takes : guid 
                (ctx, qry, o) =>
                {
                    var g = Guid.Parse(qry);
                    var h = _rdb.FetchHistoryInfo(g);
                    _log.Debug("docid = " + qry);
                    var s = fastJSON.JSON.ToJSON(h, new fastJSON.JSONParameters { UseExtensions = false, UseFastGuid = false, UseEscapedUnicode = false });
                    ctx.Response.ContentType = "application/json";
                    WriteResponse(ctx, 200, s);
                });

            _handler.Add("docversion", // takes : version
                (ctx, qry, o) =>
                {
                    var v = int.Parse(qry);
                    var oo = _rdb.FetchVersion(v);
                    var s = fastJSON.JSON.ToNiceJSON(oo, new fastJSON.JSONParameters { UseExtensions = true, UseFastGuid = false, UseEscapedUnicode = false });
                    ctx.Response.ContentType = "application/json";
                    WriteResponse(ctx, 200, s);
                });

            _handler.Add("fileget", // takes : guid 
                (ctx, qry, o) =>
                {
                    var g = Guid.Parse(qry);
                    _log.Debug("fileid = " + qry);
                    var s = fastJSON.JSON.ToNiceJSON(_rdb.FetchBytes(g), new fastJSON.JSONParameters { UseExtensions = true, UseFastGuid = false, UseEscapedUnicode = false });
                    ctx.Response.ContentType = "application/json";
                    WriteResponse(ctx, 200, s);
                });

            _handler.Add("filehistory", // takes : guid 
                (ctx, qry, o) =>
                {
                    var g = Guid.Parse(qry);
                    var h = _rdb.FetchBytesHistoryInfo(g);
                    _log.Debug("fileid = " + qry);
                    var s = fastJSON.JSON.ToJSON(h, new fastJSON.JSONParameters { UseExtensions = false, UseFastGuid = false, UseEscapedUnicode = false });
                    ctx.Response.ContentType = "application/json";
                    WriteResponse(ctx, 200, s);
                });

            _handler.Add("fileversion", // takes : version
                (ctx, qry, o) =>
                {
                    var v = int.Parse(qry);
                    var oo = _rdb.FetchBytesVersion(v);
                    var s = fastJSON.JSON.ToNiceJSON(oo, new fastJSON.JSONParameters { UseExtensions = true, UseFastGuid = false, UseEscapedUnicode = false });
                    ctx.Response.ContentType = "application/json";
                    WriteResponse(ctx, 200, s);
                });

            _handler.Add("docsearch", // takes : string & count =x &start=y
                (ctx, qry, o) =>
                {
                    int start = 0;
                    int count = -1;

                    var m = _start_regex.Match(qry);
                    if (m.Success)
                    {
                        start = int.Parse(m.Groups["start"].Value);
                        qry = qry.Replace(m.Value, "");
                    }
                    m = _count_regex.Match(qry);
                    if (m.Success)
                    {
                        count = int.Parse(m.Groups["count"].Value);
                        qry = qry.Replace(m.Value, "");
                    }
                    var h = _rdb.FullTextSearch(qry);
                    List<int> list = new List<int>();
                    _log.Debug("search = " + qry);
                    if (count > -1 && h.Length > 0)
                    {
                        int c = list.Count;
                        for (int i = start; i < start + count; i++)
                            list.Add(h[i]);
                    }
                    var obj = new
                    {
                        Items = list,
                        Count = count,
                        TotalCount = h.Length
                    };
                    var s = fastJSON.JSON.ToJSON(obj, new fastJSON.JSONParameters { UseExtensions = false, UseFastGuid = false, UseEscapedUnicode = false, EnableAnonymousTypes = true });
                    ctx.Response.ContentType = "application/json";
                    WriteResponse(ctx, 200, s);
                });

            _handler.Add("hfkeys", // takes : count =x &start=y
                (ctx, qry, o) =>
                {
                    int start = 0;
                    int count = -1;

                    var m = _start_regex.Match(qry);
                    if (m.Success)
                    {
                        start = int.Parse(m.Groups["start"].Value);
                        qry = qry.Replace(m.Value, "");
                    }
                    m = _count_regex.Match(qry);
                    if (m.Success)
                    {
                        count = int.Parse(m.Groups["count"].Value);
                        qry = qry.Replace(m.Value, "");
                    }
                    var h = _rdb.GetKVHF().GetKeysHF();
                    List<string> list = new List<string>();
                    if (count > -1 && h.Length > 0)
                    {
                        for (int i = start; i < start + count; i++)
                            list.Add(h[i]);
                    }
                    var obj = new
                    {
                        Items = list,
                        Count = count,
                        TotalCount = h.Length
                    };
                    var s = fastJSON.JSON.ToJSON(obj, new fastJSON.JSONParameters { UseExtensions = false, UseFastGuid = false, UseEscapedUnicode = false, EnableAnonymousTypes = true });
                    ctx.Response.ContentType = "application/json";
                    WriteResponse(ctx, 200, s);
                });

            _handler.Add("hfget", // takes : string 
                (ctx, qry, o) =>
                {
                    var h = _rdb.GetKVHF().GetObjectHF(qry);
                    var s = fastJSON.JSON.ToNiceJSON(h, new fastJSON.JSONParameters { UseExtensions = false, UseFastGuid = false, UseEscapedUnicode = false, EnableAnonymousTypes = true });
                    ctx.Response.ContentType = "application/json";
                    WriteResponse(ctx, 200, s);
                });

            _handler.Add("viewinfo", // takes : viewname
                (ctx, qry, o) =>
                {
                    if (qry == "")
                    {
                        WriteResponse(ctx, 404, "ViewInfo requires a viewname to be defined e.g. ?customerview");
                    }
                    else
                    {
                        var vi = GetViewInfo(qry);
                        if (vi == "")
                            WriteResponse(ctx, 500, "View not found.");
                        else {
                            ctx.Response.ContentType = "application/json";
                            WriteResponse(ctx, 200, vi);
                        }
                    }
                });
        }

        internal string GetViewInfo(string name)
        {
            var v = _rdb.GetViews().Find(x => x.Name.ToLower() == name.ToLower());
            if (v == null)
                return "";

            var p = new Dictionary<string, string>();

            foreach (var pr in v.Schema.GetProperties())
                p.Add(pr.Name, pr.PropertyType.ToString());

            foreach (var pr in v.Schema.GetFields())
                p.Add(pr.Name, pr.FieldType.ToString());

            var obj = new
            {
                View = v,
                Schema = p
            };
            var s = fastJSON.JSON.ToJSON(obj, new fastJSON.JSONParameters { UseFastGuid = false, UseEscapedUnicode = false, EnableAnonymousTypes = true });
            return s;
        }

        private object GetInfo()
        {
            var ts = _rdb.Uptime();
            var s = "" + ts.Days + " days, " + ts.Hours + " hours, " + ts.Minutes + " mins, " + ts.Seconds + " secs";
            // get info here
            return new
            {
                DocumentCount = _rdb.DocumentCount(),
                FileCount = _rdb.FileCount(),
                OSVersion = Environment.OSVersion.ToString(),
                NumberOfViews = _rdb.GetViews().Count,
                HighFrequncyItems = _rdb.GetKVHF().CountHF(),
                RaptorDBVersion = FileVersionInfo.GetVersionInfo(this.GetType().Assembly.Location).ProductVersion.ToString(),
                DataFolderSize = _rdb.GetDataFolderSize(),
                Uptime = s,
                MemoryUsage = GetMemoryUsage(),
                LogItems = LogManager.GetLastLogs()
            };
        }

        private string GetMemoryUsage() // KLUDGE but works
        {
            try
            {
                string fname = Path.GetFileNameWithoutExtension(Assembly.GetEntryAssembly().Location);

                ProcessStartInfo ps = new ProcessStartInfo("tasklist");
                ps.Arguments = "/fi \"IMAGENAME eq " + fname + ".*\" /FO CSV /NH";
                ps.RedirectStandardOutput = true;
                ps.CreateNoWindow = true;
                ps.UseShellExecute = false;
                var p = Process.Start(ps);
                if (p.WaitForExit(1000))
                {
                    var s = p.StandardOutput.ReadToEnd().Split('\"');
                    return s[9].Replace("\"", "");
                }
            }
            catch { }
            return "Unable to get memory usage";
        }

        public void WriteCsv(List<object> data, Stream stream)
        {
            TextWriter output = new StreamWriter(stream, UTF8Encoding.UTF8);
            var o = data[0];

            foreach (var prop in o.GetType().GetProperties())
            {
                output.Write(prop.Name); // header
                output.Write(",");
            }
            foreach (var prop in o.GetType().GetFields())
            {
                output.Write(prop.Name); // header
                output.Write(",");
            }

            output.WriteLine();
            foreach (var item in data)
            {
                foreach (var prop in o.GetType().GetProperties())
                {
                    output.Write("\"" + prop.GetValue(item, null));
                    output.Write("\",");
                }
                foreach (var prop in o.GetType().GetFields())
                {
                    output.Write("\"" + prop.GetValue(item));
                    output.Write("\",");
                }
                output.WriteLine();
            }
            output.Flush();
        }

        //private MethodInfo register = null;
        //private void CompileAndRegisterScriptRoutes(string routefolder)
        //{
        //    // compile & register views
        //    string[] files = Directory.GetFiles(routefolder, "*.route");

        //    foreach (var fn in files)
        //    {
        //        Assembly a = CompileScript(fn);
        //        if (a != null)
        //        {
        //            foreach (var t in a.GetTypes())
        //            {
        //                if (typeof(IRDBRouting).IsAssignableFrom(t))
        //                {
        //                    IRDBRouting r = (IRDBRouting)Activator.CreateInstance(t);
        //                    r.Initialize(this);
        //                }
        //                // load views if exists
        //                foreach (var att in t.GetCustomAttributes(typeof(RegisterViewAttribute), false))
        //                {
        //                    try
        //                    {
        //                        object o = Activator.CreateInstance(t);
        //                        //  handle types when view<T> also
        //                        Type[] args = t.GetGenericArguments();
        //                        if (args.Length == 0)
        //                            args = t.BaseType.GetGenericArguments();
        //                        Type tt = args[0];
        //                        var m = register.MakeGenericMethod(new Type[] { tt });
        //                        m.Invoke(_rdb, new object[] { o });
        //                    }
        //                    catch (Exception ex)
        //                    {
        //                        _log.Error(ex);
        //                    }
        //                }
        //            }
        //        }
        //    }
        //}

        //private Assembly CompileScript(string file)
        //{
        //    try
        //    {
        //        _log.Debug("Compiling route script : " + file);
        //        CodeDomProvider compiler = CodeDomProvider.CreateProvider("CSharp");

        //        CompilerParameters compilerparams = new CompilerParameters();
        //        compilerparams.GenerateInMemory = false;
        //        compilerparams.GenerateExecutable = false;
        //        compilerparams.OutputAssembly = file.Replace(".route", ".dll");
        //        compilerparams.CompilerOptions = "/optimize";

        //        Regex regex = new Regex(
        //            @"\/\/\s*ref\s*\:\s*(?<refs>.*)",
        //            System.Text.RegularExpressions.RegexOptions.IgnoreCase);

        //        compilerparams.ReferencedAssemblies.Add(typeof(View<>).Assembly.Location); //raprotdb.common.dll
        //        compilerparams.ReferencedAssemblies.Add(typeof(object).Assembly.Location); //mscorlib.dll
        //        compilerparams.ReferencedAssemblies.Add(typeof(System.Uri).Assembly.Location); //system.dll
        //        compilerparams.ReferencedAssemblies.Add(typeof(System.Linq.Enumerable).Assembly.Location);//system.core.dll
        //        compilerparams.ReferencedAssemblies.Add(typeof(IRDBRouting).Assembly.Location); //raptordb.rest.dll

        //        foreach (Match m in regex.Matches(File.ReadAllText(file)))
        //        {
        //            string str = m.Groups["refs"].Value.Trim();
        //            Assembly a = Assembly.LoadWithPartialName(Path.GetFileNameWithoutExtension(str));//load from GAC if possible
        //            if (a != null)
        //                compilerparams.ReferencedAssemblies.Add(a.Location);
        //            else
        //            {
        //                string assm = Path.GetDirectoryName(this.GetType().Assembly.Location) + _S + str;
        //                a = Assembly.LoadFrom(assm);
        //                if (a != null)
        //                    compilerparams.ReferencedAssemblies.Add(a.Location);
        //                else
        //                    _log.Error("unable to find referenced file for view compiling : " + str);
        //            }
        //        }

        //        CompilerResults results = compiler.CompileAssemblyFromFile(compilerparams, file);

        //        if (results.Errors.HasErrors == true)
        //        {
        //            _log.Error("Error compiling route definition : " + file);
        //            foreach (var e in results.Errors)
        //                _log.Error(e.ToString());
        //            return null;
        //        }

        //        return results.CompiledAssembly;
        //    }
        //    catch (Exception ex)
        //    {
        //        _log.Error("Error compiling route definition : " + file);
        //        _log.Error(ex);
        //        return null;
        //    }
        //}

        private void WriteResources()
        {
            _log.Debug("Initializing web cache...");
            //if (Global.useWEBresources == false)
            //{
            //    Directory.CreateDirectory("WEB");
            //    Directory.CreateDirectory("WEB\\images");
            //}
            string name = this.GetType().Assembly.GetName().Name + ".";
            foreach (var r in this.GetType().Assembly.GetManifestResourceNames())
            {
                string s = r.Replace(name, "");
                if (s.StartsWith("WEB"))
                {
                    var ext = Path.GetExtension(s);
                    s = s.Replace(ext, "").Replace(".", "/");
                    var p = s + ext;
                    _WebCache.Add(p.ToLower(), r);
                    //if (Global.useWEBresources == false && File.Exists(folder + s) == false)
                    //{
                    //    using (MemoryStream ms = new MemoryStream())
                    //    {
                    //        Assembly.GetExecutingAssembly().GetManifestResourceStream(r).CopyTo(ms);
                    //        File.WriteAllBytes(folder + s, ms.ToArray());
                    //    }
                    //}
                }
            }
        }

        private byte[] ReadFromStream(string name)
        {
            using (MemoryStream ms = new MemoryStream())
            {
                Assembly.GetExecutingAssembly().GetManifestResourceStream(name).CopyTo(ms);
                return ms.ToArray();
            }
        }

        private void ListenerCallback(IAsyncResult ar)
        {
            var listener = ar.AsyncState as HttpListener;

            var ctx = listener.EndGetContext(ar);

            try
            {
                //do some stuff
                string path = ctx.Request.Url.GetComponents(UriComponents.Path, UriFormat.Unescaped).ToLower();
                _log.Debug("WEB request : " + path);
                //RDBRoute r = null;
                ctx.Response.ContentEncoding = UTF8Encoding.UTF8;
                string webpath = "WEB\\";
                //if (_S == "/")
                webpath = webpath.Replace("\\", "/");
                bool handled = false;
                //_log.Debug("path = " + path);
                if (path.StartsWith("raptordb"))
                {
                    string command = path.Replace("raptordb/", "");
                    if (command.Contains("?"))
                    {
                        command = command.Substring(0, command.IndexOf('?') - 1);
                    }
                    //_log.Debug("command = " + command);
                    Handler handler = null;
                    List<object> o = new List<object>();

                    if (_handler.TryGetValue(command, out handler))
                    {
                        string args = ctx.Request.Url.GetComponents(UriComponents.Query, UriFormat.Unescaped);
                        handler(ctx, args, o);

                        if (o.Count > 0)
                        {
                            Result<object> resf = new Result<object>(true);
                            resf.Rows = o;
                            resf.TotalCount = o.Count;
                            resf.Count = o.Count;
                            var s = fastJSON.JSON.ToJSON(resf, new fastJSON.JSONParameters { UseExtensions = false, UseFastGuid = false, EnableAnonymousTypes = true });
                            ctx.Response.ContentType = "application/json";
                            WriteResponse(ctx, 200, s);
                        }
                        handled = true;
                    }
                    else if (command.StartsWith("views"))
                    {
                        ProcessGET(_rdb, ctx, path.Replace("raptordb/views/", ""), null);
                        handled = true;
                    }
                    else if (command.StartsWith("excelexport"))
                    {
                        var data = DoQuery(_rdb, ctx, path.Replace("raptordb/excelexport/", ""), null);
                        ctx.Response.AddHeader("content-disposition", "attachment;filename='" + data.Title + ".csv'");
                        ctx.Response.AddHeader("Content-Type", "application/vnd.ms-excel");
                        _log.Debug("exporting to excel rows : " + data.Rows.Count);
                        WriteCsv(data.Rows, ctx.Response.OutputStream);
                        handled = true;
                    }
                }

                if (!handled)
                {
                    //if (_routing.TryGetValue(path, out r)) // lower case search 
                    //{
                    //    string mth = ctx.Request.HttpMethod;
                    //    if (mth == "POST" || mth == "PUT")
                    //        ProcessPOST(_rdb, ctx, path, r);
                    //    else
                    //        ProcessGET(_rdb, ctx, path, r);
                    //}
                    if (_WebCache.ContainsKey((webpath + path).ToLower()))// File.Exists(webpath + path)) // FIX : path here
                    {
                        switch (Path.GetExtension(path).ToLower())
                        {
                            case ".js":
                                ctx.Response.ContentType = "application/javascript"; break;
                            case ".css":
                                ctx.Response.ContentType = "text/css"; break;
                            case ".html":
                            case ".htm":
                                ctx.Response.ContentType = "text/html"; break;
                            default:
                                ctx.Response.ContentType = "application/octet-stream"; break;

                        }
                        _log.Debug("serving file : " + webpath + path);
                        WriteResponse(ctx, 200, ReadFromStream(_WebCache[(webpath + path).ToLower()]), true);// File.ReadAllBytes(webpath + path), true);
                    }
                    else if (path == "")
                    {
                        ctx.Response.ContentType = "text/html";
                        _log.Debug("serving file : " + webpath + "app.html");
                        WriteResponse(ctx, 200, ReadFromStream(_WebCache[(webpath + "app.html").ToLower()]), true);
                    }
                    else
                        WriteResponse(ctx, 404, "route path not found : " + ctx.Request.Url.GetComponents(UriComponents.Path, UriFormat.Unescaped));
                }
            }
            catch (Exception ex)
            {
                _log.Error(ex);
            }
            finally
            {
                ctx.Response.OutputStream.Close();
            }
        }

        //private void ProcessPOST(IRaptorDB rdb, HttpListenerContext ctx, string path, RDBRoute route)
        //{
        //    try
        //    {
        //        Guid docid = Guid.Empty;
        //        string qry = ctx.Request.Url.GetComponents(UriComponents.Query, UriFormat.Unescaped);
        //        if (qry == "")
        //        {
        //            _log.Error("POST-ing requires a GUID in route : " + route.URL);
        //            WriteResponse(ctx, 404, "POST-ing requires a GUID in route : " + route.URL);
        //            return;
        //        }
        //        else // parse out guid from url 
        //        {
        //            string[] s = qry.Split('=');
        //            docid = Guid.Parse(s[1].Trim().Replace("\"", "").Replace("\'", ""));
        //        }

        //        if (route.EntityType == null)
        //        {
        //            _log.Error("POST-ing to an undefined entity in route : " + route.URL);
        //            WriteResponse(ctx, 404, "POST-ing to an undefined entity in route : " + route.URL);
        //            return;
        //        }

        //        using (var reader = new StreamReader(ctx.Request.InputStream, ctx.Request.ContentEncoding))
        //        {
        //            string text = reader.ReadToEnd();

        //            // create a container
        //            RDBJsonContainer c = new RDBJsonContainer();
        //            c.date = FastDateTime.Now;
        //            c.json = text;
        //            c.URL = path;
        //            c.docid = docid;
        //            // TODO : save useragent info

        //            // convert json to object   
        //            object obj = fastJSON.JSON.ToObject(text, route.EntityType);

        //            // save object to RDB and handle transaction
        //            var m = GetSave(obj.GetType());
        //            bool ret = (bool)m.Invoke(_rdb, new object[] { docid, obj });
        //            if (ret)
        //            {
        //                // save json container to keystore
        //                _jsonstore.SetObject(docid, c);
        //                WriteResponse(ctx, 200, "posted : " + docid);
        //            }
        //            else
        //                WriteResponse(ctx, 500, "unable to save : " + text);
        //        }
        //    }
        //    catch (Exception ex)
        //    {
        //        _log.Error(ex);
        //        WriteResponse(ctx, 500, "" + ex);
        //    }
        //}

        private void WriteResponse(HttpListenerContext ctx, int code, string msg)
        {
            WriteResponse(ctx, code, Encoding.UTF8.GetBytes(msg), false);
        }

        private void WriteResponse(HttpListenerContext ctx, int code, byte[] data, bool skipcompress)
        {
            ctx.Response.StatusCode = code;
            ctx.Response.AppendHeader("Access-Control-Allow-Origin", "*");
            byte[] b = data;// Encoding.UTF8.GetBytes(msg);
            if (skipcompress == false && b.Length > 100 * 1024)
            {
                _log.Debug("original data size : " + b.Length.ToString("#,0"));
                using (var ms = new MemoryStream())
                {
                    using (var zip = new GZipStream(ms, CompressionMode.Compress, true))
                        zip.Write(b, 0, b.Length);
                    b = ms.ToArray();
                }
                _log.Debug("compressed size : " + b.Length.ToString("#,0"));
                ctx.Response.AddHeader("Content-Encoding", "gzip");
            }
            ctx.Response.ContentLength64 = b.LongLength;
            ctx.Response.OutputStream.Write(b, 0, b.Length);
        }

        //private MethodInfo save = null;
        //private SafeDictionary<Type, MethodInfo> _savecache = new SafeDictionary<Type, MethodInfo>();

        //private MethodInfo GetSave(Type type)
        //{
        //    MethodInfo m = null;
        //    if (_savecache.TryGetValue(type, out m))
        //        return m;

        //    m = save.MakeGenericMethod(new Type[] { type });
        //    _savecache.Add(type, m);
        //    return m;
        //}

        private Regex _start_regex = new Regex(@"[\?\&]?\s*start\s*\=\s*[-+]?(?<start>\d*)", RegexOptions.IgnoreCase | RegexOptions.IgnorePatternWhitespace | RegexOptions.Compiled);
        private Regex _count_regex = new Regex(@"[\?\&]?\s*count\s*\=\s*[-+]?(?<count>\d*)", RegexOptions.IgnoreCase | RegexOptions.IgnorePatternWhitespace | RegexOptions.Compiled);
        private Regex _order_regex = new Regex(@"[\?\&]?\s*orderby\s*\=\s*[-+]?(?<orderby>.*)", RegexOptions.IgnoreCase | RegexOptions.IgnorePatternWhitespace | RegexOptions.Compiled);

        private Result<object> DoQuery(IRaptorDB rdb, HttpListenerContext ctx, string path, RDBRoute route)
        {
            string qry = ctx.Request.Url.GetComponents(UriComponents.Query, UriFormat.Unescaped);
            string viewname = path;
            if (route != null)
            {
                //if (route.EntityType != null)
                //{
                //    if (qry != "")
                //    {
                //        // fetch the json document
                //        string[] s = qry.Split('=');
                //        object obj = null;
                //        if (_jsonstore.GetObject(Guid.Parse(s[1].Replace("\"", "")), out obj))
                //        {
                //            RDBJsonContainer d = (RDBJsonContainer)obj;
                //            WriteResponse(ctx, 200, d.json);
                //            return;
                //        }
                //    }

                //    WriteResponse(ctx, 404, "GUID not found :" + qry);
                //    return;
                //}
                if (route.Viewname == null && route.function != null)
                {
                    viewname = route.Viewname;
                    var o = route.function(_rdb, qry);
                    Result<object> resf = new Result<object>(true);
                    resf.Rows = o;
                    resf.TotalCount = o.Count;
                    resf.Count = o.Count;
                    resf.Title = route.Viewname;
                    return resf;
                }
            }

            // parse "start" and "count" from qry if exists
            int start = 0;
            int count = -1;
            string orderby = "";

            var m = _start_regex.Match(qry);
            if (m.Success)
            {
                start = int.Parse(m.Groups["start"].Value);
                qry = qry.Replace(m.Value, "");
            }
            m = _count_regex.Match(qry);
            if (m.Success)
            {
                count = int.Parse(m.Groups["count"].Value);
                qry = qry.Replace(m.Value, "");
            }
            m = _order_regex.Match(qry);
            if (m.Success)
            {
                orderby = m.Groups["orderby"].Value;
                qry = qry.Replace(m.Value, "");
            }

            var res = rdb.Query(viewname, qry, start, count, orderby);
            res.Title = viewname;
            return res;
        }

        private void ProcessGET(IRaptorDB rdb, HttpListenerContext ctx, string path, RDBRoute route)
        {
            try
            {
                var result = DoQuery(rdb, ctx, path, route);
                var s = fastJSON.JSON.ToJSON(result, new fastJSON.JSONParameters { UseExtensions = false, UseFastGuid = false, EnableAnonymousTypes = true });
                ctx.Response.ContentType = "application/json";
                WriteResponse(ctx, 200, s);
                return;
            }
            catch (Exception ex)
            {
                WriteResponse(ctx, 500, "" + ex);
            }
        }
        #endregion
    }
}