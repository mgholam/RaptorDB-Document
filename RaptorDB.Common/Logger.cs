using System;
using System.Text;
using System.Threading;
using System.IO;
using System.Collections.Generic;

namespace RaptorDB
{
    public interface ILog
    {
        void Debug(object msg, params object[] objs);
        void Error(object msg, params object[] objs);
        void Info(object msg, params object[] objs);
        void Warn(object msg, params object[] objs);
        void Fatal(object msg, params object[] objs);
    }

    internal class FileLogger
    {
        // Sinlgeton pattern 4 from : http://csharpindepth.com/articles/general/singleton.aspx
        private static readonly FileLogger instance = new FileLogger();
        // Explicit static constructor to tell C# compiler
        // not to mark type as beforefieldinit
        static FileLogger()
        {
        }
        private FileLogger()
        {
        }
        public static FileLogger Instance { get { return instance; } }

        private Queue<string> _que = new Queue<string>();
        private Queue<string> _log = new Queue<string>();
        private StreamWriter _output;
        private string _filename;
        private int _sizeLimit = 0;
        private long _lastSize = 0;
        private DateTime _lastFileDate;
        private bool _showMethodName = false;
        private string _FilePath = "";
        System.Timers.Timer _saveTimer;
        private int _lastLogsToKeep = 100;

        public bool ShowMethodNames
        {
            get { return _showMethodName; }
        }

        public void Init(string filename, int sizelimitKB, bool showmethodnames)
        {
            if (_output != null)
                return;
            _que = new Queue<string>();
            _showMethodName = showmethodnames;
            _sizeLimit = sizelimitKB;
            _filename = filename;
            // handle folder names as well -> create dir etc.
            _FilePath = Path.GetDirectoryName(filename);
            if (_FilePath != "")
            {
                _FilePath = Directory.CreateDirectory(_FilePath).FullName;
                if (_FilePath.EndsWith(Path.DirectorySeparatorChar.ToString()) == false)
                    _FilePath += Path.DirectorySeparatorChar.ToString();
            }
            _output = new StreamWriter(filename, true);
            FileInfo fi = new FileInfo(filename);
            _lastSize = fi.Length;
            _lastFileDate = fi.LastWriteTime;

            _saveTimer = new System.Timers.Timer(500);
            _saveTimer.Elapsed += new System.Timers.ElapsedEventHandler(_saveTimer_Elapsed);
            _saveTimer.Enabled = true;
            _saveTimer.AutoReset = true;
        }

        void _saveTimer_Elapsed(object sender, System.Timers.ElapsedEventArgs e)
        {
            WriteData();
        }

        public void ShutDown()
        {
            _saveTimer.Enabled = false;
            WriteData();
            if (_output != null)
            {
                _output.Flush();
                _output.Close();
                _output = null;
            }
        }

        private void WriteData()
        {
            if (_output == null)
                return;
            lock (_que)
            {
                while (_que.Count > 0)
                {
                    object o = _que.Dequeue();
                    if (_output != null && o != null)
                    {
                        if (_sizeLimit > 0)
                        {
                            // implement size limited logs
                            // implement rolling logs
                            #region [  rolling size limit ]
                            _lastSize += ("" + o).Length;
                            if (_lastSize > _sizeLimit * 1000)
                            {
                                _output.Flush();
                                _output.Close();
                                int count = 1;
                                while (File.Exists(_FilePath + Path.GetFileNameWithoutExtension(_filename) + "." + count.ToString("0000")))
                                    count++;

                                File.Move(_filename,
                                    _FilePath +
                                    Path.GetFileNameWithoutExtension(_filename) +
                                    "." + count.ToString("0000"));
                                _output = new StreamWriter(_filename, true);
                                _lastSize = 0;
                            }
                            #endregion
                        }
                        if (DateTime.Now.Subtract(_lastFileDate).Days > 0)
                        {
                            // implement date logs
                            #region [  rolling dates  ]
                            _output.Flush();
                            _output.Close();
                            int count = 1;
                            while (File.Exists(_FilePath + Path.GetFileNameWithoutExtension(_filename) + "." + count.ToString("0000")))
                            {
                                File.Move(_FilePath + Path.GetFileNameWithoutExtension(_filename) + "." + count.ToString("0000"),
                                   _FilePath +
                                   Path.GetFileNameWithoutExtension(_filename) +
                                   "." + count.ToString("0000") +
                                   "." + _lastFileDate.ToString("yyyy-MM-dd"));
                                count++;
                            }
                            File.Move(_filename,
                               _FilePath +
                               Path.GetFileNameWithoutExtension(_filename) +
                               "." + count.ToString("0000") +
                               "." + _lastFileDate.ToString("yyyy-MM-dd"));

                            _output = new StreamWriter(_filename, true);
                            _lastFileDate = DateTime.Now;
                            _lastSize = 0;
                            #endregion
                        }
                        _output.Write(o);
                    }
                }
                if (_output != null)
                    _output.Flush();
            }
            lock (_log)
            {
                while (_log.Count > _lastLogsToKeep)
                    _log.Dequeue();
            }
        }

        private string FormatLog(string log, string type, string meth, string msg, object[] objs)
        {
            StringBuilder sb = new StringBuilder();
            sb.Append(DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss"));
            sb.Append("|");
            sb.Append(log);
            sb.Append("|");
            sb.Append(Thread.CurrentThread.ManagedThreadId.ToString());
            sb.Append("|");
            sb.Append(type);
            sb.Append("|");
            sb.Append(meth);
            sb.Append("| ");
            sb.AppendLine(msg);

            if (objs != null)
                foreach (object o in objs)
                    sb.AppendLine("" + o);

            return sb.ToString();
        }

        public void Log(string logtype, string type, string meth, string msg, params object[] objs)
        {
            var l = FormatLog(logtype, type, meth, msg, objs);
            lock (_que)
                _que.Enqueue(l);
            lock (_log)
                _log.Enqueue(l);
        }

        internal List<string> GetLastLogs()
        {
            List<string> l = new List<string>();

            foreach (var s in _log)
            {
                l.Add(s);
            }

            return l;
        }
    }

    internal class logger : ILog
    {
        public logger(Type type)
        {
            typename = type.Namespace + "." + type.Name;
        }

        private string typename = "";

        private void log(string logtype, string msg, params object[] objs)
        {
            string meth = "";
            if (FileLogger.Instance.ShowMethodNames)
            {
                System.Diagnostics.StackTrace st = new System.Diagnostics.StackTrace(2);
                System.Diagnostics.StackFrame sf = st.GetFrame(0);
                meth = sf.GetMethod().Name;
            }
            FileLogger.Instance.Log(logtype, typename, meth, msg, objs);
        }

        #region ILog Members

        public void Debug(object msg, params object[] objs)
        {
            log("DEBUG", "" + msg, objs);
        }

        public void Error(object msg, params object[] objs)
        {
            log("ERROR", "" + msg, objs);
        }

        public void Info(object msg, params object[] objs)
        {
            log("INFO", "" + msg, objs);
        }

        public void Warn(object msg, params object[] objs)
        {
            log("WARN", "" + msg, objs);
        }

        public void Fatal(object msg, params object[] objs)
        {
            log("FATAL", "" + msg, objs);
        }
        #endregion
    }

    public static class LogManager
    {
        public static ILog GetLogger(Type obj)
        {
            return new logger(obj);
        }

        public static void Configure(string filename, int sizelimitKB, bool showmethodnames)
        {
            FileLogger.Instance.Init(filename, sizelimitKB, showmethodnames);
        }

        public static List<string> GetLastLogs()
        {
            return FileLogger.Instance.GetLastLogs();
        }

        public static void Shutdown()
        {
            FileLogger.Instance.ShutDown();
        }
    }
}
