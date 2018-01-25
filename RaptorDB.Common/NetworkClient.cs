using System;
using System.Net.Sockets;
using System.Threading;
using System.Net;
using System.Threading.Tasks;

namespace RaptorDB.Common
{
    //
    // Header bits format : 0 - json = 1 , bin = 0 
    //                      1 - binaryjson = 1 , text json = 0
    //                      2 - compressed = 1 , uncompressed = 0 
    //
    //     0   : data format
    //     1-4 : data length

    public class NetworkClient
    {
        public static class Config
        {
            /// <summary>
            /// Block buffer size (default = 32kb)
            /// </summary>
            public static int BufferSize = 32 * 1024;
            /// <summary>
            /// Log data if over (default = 1,000,000)
            /// </summary>
            public static int LogDataSizesOver = 1000000;
            /// <summary>
            /// Compress data if over (default = 1,000,000)
            /// </summary>
            public static int CompressDataOver = 1000000;
            /// <summary>
            /// Kill inactive client connections (default = 30sec)
            /// </summary>
            public static int KillConnectionSeconds = 30;
        }

        public NetworkClient(string server, int port)
        {
            _server = server;
            _port = port;
        }
        private ILog log = LogManager.GetLogger(typeof(NetworkClient));
        private TcpClient _client;
        private string _server;
        private int _port;

        public Guid ClientID = Guid.NewGuid();
        public bool UseBJSON = true;

        public void Connect()
        {
            _client = new TcpClient(_server, _port);
            _client.SendBufferSize = Config.BufferSize;
            _client.ReceiveBufferSize = _client.SendBufferSize;
        }

        public object Send(object data)
        {
            try
            {
                CheckConnection();

                byte[] hdr = new byte[5];
                hdr[0] = (UseBJSON ? (byte)3 : (byte)0);
                byte[] dat = fastBinaryJSON.BJSON.ToBJSON(data);
                bool compressed = false;
                if (dat.Length > NetworkClient.Config.CompressDataOver)
                {
                    log.Debug("compressing data over limit : " + dat.Length.ToString("#,#"));
                    compressed = true;
                    dat = MiniLZO.Compress(dat);
                    log.Debug("new size : " + dat.Length.ToString("#,#"));
                }
                byte[] len = Helper.GetBytes(dat.Length, false);
                hdr[0] = (byte)(3 + (compressed ? 4 : 0));
                Array.Copy(len, 0, hdr, 1, 4);
                _client.Client.Send(hdr);
                _client.Client.Send(dat);

                byte[] rechdr = new byte[5];
                using (NetworkStream n = new NetworkStream(_client.Client))
                {
                    n.Read(rechdr, 0, 5);
                    int c = Helper.ToInt32(rechdr, 1);
                    byte[] recd = new byte[c];
                    int bytesRead = 0;
                    int chunksize = 1;
                    while (bytesRead < c && chunksize > 0)
                        bytesRead +=
                          chunksize = n.Read
                            (recd, bytesRead, c - bytesRead);
                    if ((rechdr[0] & (byte)4) == (byte)4)
                        recd = MiniLZO.Decompress(recd);
                    if ((rechdr[0] & (byte)3) == (byte)3)
                        return fastBinaryJSON.BJSON.ToObject(recd);
                }
            }
            catch
            {

            }
            return null;
        }

        private void CheckConnection()
        {
            // check connected state before sending

            if (_client == null || !_client.Connected)
                Connect();
        }

        public void Close()
        {
            if (_client != null)
            {
                _client.Close();
            }
        }
    }

    public class NetworkServer
    {
        public delegate object ProcessPayload(object data);

        private ILog log = LogManager.GetLogger(typeof(NetworkServer));
        ProcessPayload _handler;
        private bool _run = true;
        private int count = 0;
        private int _port;

        public void Start(int port, ProcessPayload handler)
        {
            _handler = handler;
            _port = port;
            ThreadPool.SetMinThreads(50, 50);
            System.Timers.Timer t = new System.Timers.Timer(1000);
            t.AutoReset = true;
            t.Start();
            t.Elapsed += new System.Timers.ElapsedEventHandler(t_Elapsed);
            Task.Factory.StartNew(() => Run(), TaskCreationOptions.AttachedToParent);
        }

        private void Run()
        {
            TcpListener listener = new TcpListener(IPAddress.Any, _port);
            listener.Start();

            while (_run)
            {
                try
                {
                    TcpClient c = listener.AcceptTcpClient();
                    Task.Factory.StartNew(() => Accept(c));
                }
                catch (Exception ex) { log.Error(ex); }
            }
        }

        void t_Elapsed(object sender, System.Timers.ElapsedEventArgs e)
        {
            if (count > 0)
                log.Info("tcp connects/sec = " + count);
            count = 0;
        }

        public void Stop()
        {
            _run = false;
        }

        void Accept(TcpClient client)
        {
            using (NetworkStream n = client.GetStream())
            {
                while (client.Connected)
                {
                    this.count++;
                    byte[] c = new byte[5];
                    n.Read(c, 0, 5);
                    int count = BitConverter.ToInt32(c, 1);
                    byte[] data = new byte[count];
                    int bytesRead = 0;
                    int chunksize = 1;
                    while (bytesRead < count && chunksize > 0)
                        bytesRead +=
                          chunksize = n.Read
                            (data, bytesRead, count - bytesRead);

                    object o = fastBinaryJSON.BJSON.ToObject(data);
                    if ((c[0] & (byte)4) == (byte)4)
                        data = MiniLZO.Decompress(data);

                    object r = _handler(o);
                    bool compressed = false;
                    var dataret = fastBinaryJSON.BJSON.ToBJSON(r);
                    r = null;
                    if (dataret.Length > NetworkClient.Config.CompressDataOver)
                    {
                        log.Debug("compressing data over limit : " + dataret.Length.ToString("#,#"));
                        compressed = true;
                        dataret = MiniLZO.Compress(dataret);
                        log.Debug("new size : " + dataret.Length.ToString("#,#"));
                    }
                    if (dataret.Length > NetworkClient.Config.LogDataSizesOver)
                        log.Debug("data size (bytes) = " + dataret.Length.ToString("#,#"));

                    byte[] b = BitConverter.GetBytes(dataret.Length);
                    byte[] hdr = new byte[5];
                    hdr[0] = (byte)(3 + (compressed ? 4 : 0));
                    Array.Copy(b, 0, hdr, 1, 4);
                    n.Write(hdr, 0, 5);
                    n.Write(dataret, 0, dataret.Length);

                    //n.Flush();
                    //return;
                    int wait = 0;
                    bool close = false;
                    var dt = FastDateTime.Now;
                    while (n.DataAvailable == false && close == false)
                    {
                        wait++;
                        if (wait < 10000) // kludge : for insert performance
                            Thread.Sleep(0);
                        else
                        {
                            Thread.Sleep(1);
                            // wait done -> close connection 
                            if (FastDateTime.Now.Subtract(dt).TotalSeconds > NetworkClient.Config.KillConnectionSeconds)
                                close = true;
                        }
                    }
                    if (close)
                        break;
                }
                n.Close();
            }
            client.Close();
        }
    }
}
