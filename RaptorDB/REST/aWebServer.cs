using System;
using System.Net;
using System.Text;
using System.IO;
using System.Threading.Tasks;
using System.Reflection;
using System.Collections.Generic;
using System.IO.Compression;

namespace RaptorDB
{
    public abstract class aWebServer
    {
        public aWebServer(int HttpPort, bool localonly, AuthenticationSchemes authenticationType, string apiPrefix)
        {
            _apiPrefix = apiPrefix;
            _authenticationType = authenticationType;
            _localonly = localonly;
            _port = HttpPort;
            Console.WriteLine("web port = " + _port);
            Task.Factory.StartNew(() => Start(), TaskCreationOptions.LongRunning);
        }

        public delegate void Handler(HttpListenerContext ctx);
        public abstract void InitializeCommandHandler(Dictionary<string, Handler> handler);

        #region [  properties  ]
        private string _S = Path.DirectorySeparatorChar.ToString();
        internal ILog _log = LogManager.GetLogger(typeof(aWebServer));
        private bool _run = true;
        private int _port;
        private HttpListener _server;
        private bool _localonly = false;
        private Dictionary<string, Handler> _handler = new Dictionary<string, Handler>();
        internal Dictionary<string, string> _WebCache = new Dictionary<string, string>();
        private AuthenticationSchemes _authenticationType = AuthenticationSchemes.None;
        private string _apiPrefix = "myapi";
        #endregion

        #region [  web call back handlers  ]
        private void ListenerCallback(IAsyncResult ar)
        {
            var listener = ar.AsyncState as HttpListener;

            var ctx = listener.EndGetContext(ar);
            _log.Debug("Remote Address = " + ctx.Request.RemoteEndPoint.Address);

            try
            {
                //do some stuff
                string path = ctx.Request.Url.GetComponents(UriComponents.Path, UriFormat.Unescaped).ToLower();
                if (ctx.User != null)
                    _log.Debug("user : " + ctx.User.Identity.Name);
                else
                {
                    //ctx.Response.Redirect("login");
                    //return;
                }

                string webpath = "WEB\\";
                webpath = webpath.Replace("\\", "/");
                bool handled = false;
                if (path.StartsWith(_apiPrefix))
                {
                    string command = path.Replace(_apiPrefix + "/", "");
                    if (command.Contains("?"))
                        command = command.Substring(0, command.IndexOf('?') - 1);
                    if (command.Contains("/"))
                        command = command.Substring(0, command.IndexOf('/'));

                    Handler handler = null;

                    if (_handler.TryGetValue(command, out handler))
                    {
                        handled = true;
                        handler(ctx);
                    }
                }

                if (!handled)
                {
                    if (path == "")
                    {
                        ctx.Response.ContentType = "text/html";
                        WriteResponse(ctx, 200, ReadFromStream(_WebCache[(webpath + "app.html").ToLower()]), false);
                    }
                    else
                    {
                        if (path.EndsWith(_apiPrefix + ".png") && File.Exists("logo.png"))
                        {
                            OutPutContentType(ctx, path);
                            WriteResponse(ctx, 200, File.ReadAllBytes("logo.png"), false);
                        }
                        else if (_WebCache.ContainsKey((webpath + path).ToLower()))
                        {
                            bool compress = OutPutContentType(ctx, path);
                            var o = _WebCache[(webpath + path).ToLower()];
                            WriteResponse(ctx, 200, ReadFromStream(o), compress);
                        }
                        else
                            WriteResponse(ctx, 404, "route path not found : " + ctx.Request.Url.GetComponents(UriComponents.Path, UriFormat.Unescaped));
                    }
                }
                ctx.Response.OutputStream.Close();
            }
            catch (Exception ex)
            {
                _log.Error(ex);
            }
        }


        #endregion

        #region [  internal  ]
        internal void Stop()
        {
            _run = false;
        }

        internal static bool OutPutContentType(HttpListenerContext ctx, string path)
        {
            bool compress = false;
            switch (Path.GetExtension(path).ToLower())
            {
                case ".jpg":
                case ".jpeg":
                    ctx.Response.ContentType = "image/jpeg"; break;
                case ".gif":
                    ctx.Response.ContentType = "image/gif"; break;
                case ".mht":
                    compress = true;
                    ctx.Response.ContentType = "multipart/related"; break;
                case ".eml":
                    compress = true;
                    ctx.Response.ContentType = "message/rfc822"; break;//"application/x-mimearchive"; break;
                case ".json":
                    ctx.Response.ContentEncoding = UTF8Encoding.UTF8;
                    ctx.Response.ContentType = "application/json"; break;
                case ".js":
                    ctx.Response.ContentEncoding = UTF8Encoding.UTF8;
                    ctx.Response.ContentType = "application/javascript"; break;
                case ".css":
                    ctx.Response.ContentType = "text/css"; break;
                case ".html":
                case ".htm":
                    compress = true;
                    ctx.Response.ContentEncoding = UTF8Encoding.UTF8;
                    ctx.Response.ContentType = "text/html"; break;
                case ".pdf":
                    ctx.Response.ContentType = "application/pdf"; break;
                case ".doc":
                case ".docx":
                    ctx.Response.ContentType = "application/msword"; break;
                case ".xls":
                case ".xlsx":
                    ctx.Response.ContentType = "application/vnd.ms-excel"; break;
                default:
                    ctx.Response.ContentType = "application/octet-stream"; break;

            }
            return compress;
        }

        internal void WriteResponse(HttpListenerContext ctx, int code, string msg)
        {
            WriteResponse(ctx, code, Encoding.UTF8.GetBytes(msg), false);
        }

        internal void WriteResponse(HttpListenerContext ctx, int code, string msg, bool compress)
        {
            WriteResponse(ctx, code, Encoding.UTF8.GetBytes(msg), compress);
        }

        internal void WriteResponse(HttpListenerContext ctx, int code, byte[] data, bool compress)
        {
            ctx.Response.StatusCode = code;
            ctx.Response.AppendHeader("Access-Control-Allow-Origin", "*");
            byte[] b = data;
            if (compress == true && b.Length > 100 * 1024)
            {
                _log.Debug("original data size : " + b.Length.ToString("#,0"));
                using (var ms = new MemoryStream())
                {
                    using (var zip = new GZipStream(ms, CompressionMode.Compress, true))
                        zip.Write(b, 0, b.Length);
                    b = ms.ToArray();
                }
                _log.Debug("compressed size : " + b.Length.ToString("#,0"));
                ctx.Response.AppendHeader("Content-Encoding", "gzip");
            }
            ctx.Response.ContentLength64 = b.LongLength;
            ctx.Response.OutputStream.Write(b, 0, b.Length);
        }
        #endregion

        #region [  private  ]
        private void Start()
        {
            try
            {
                InitializeCommandHandler(_handler);

                ReadResources();

                _server = new HttpListener();
                if (_authenticationType != AuthenticationSchemes.None)
                    _server.AuthenticationSchemes = _authenticationType;

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
            catch (Exception ex)
            {
                _log.Error(ex);
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

        private void ReadResources()
        {
            string name = Assembly.GetExecutingAssembly().GetName().Name + ".";
            foreach (var r in Assembly.GetExecutingAssembly().GetManifestResourceNames())
            {
                string s = r.Replace(name, "");
                if (s.StartsWith("WEB"))
                {
                    var ext = Path.GetExtension(s);
                    s = s.Replace(ext, "").Replace(".", "/");
                    var p = s + ext;
                    _WebCache.Add(p.ToLower(), r);
                }
            }
        }

        #endregion
    }
}