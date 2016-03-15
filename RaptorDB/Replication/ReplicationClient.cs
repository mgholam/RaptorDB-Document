using System;
using System.IO;
using System.IO.Compression;
using RaptorDB.Common;
using System.Text.RegularExpressions;

namespace RaptorDB.Replication
{
    public class ClientRepConfig
    {
        public bool isConfigured;
        public string whencron = "* * * * *";
        public WhatItem what;
        public int lastHQCounter;
        public int lastCounter;
        public int outPackageNumber;
        public int inPackageNumber;
        public int lastPackageIndex;
    }

    public class ClientWhatWhenConfig
    {
        public WhatItem what;
        public string whencron;
    }

    /// <summary>
    /// Replication package processing is done in RaptorDB.cs
    /// </summary>
    internal class ReplicationClient
    {
        public ReplicationClient(string dataFolder, string config, IDocStorage<Guid> docs)
        {
            _log.Debug("starting replication client : " + dataFolder);
            _docs = docs;
            _path = dataFolder;
            // read client config file
            _config = fastJSON.JSON.ToObject<ClientConfiguration>(config);
            Initialize();
        }

        private void Initialize()
        {
            Directory.CreateDirectory(_path + "Replication");
            Directory.CreateDirectory(_path + "Replication" + _S + "Inbox");
            Directory.CreateDirectory(_path + "Replication" + _S + "Outbox");
            _InboxPath = _path + "Replication" + _S + "Inbox" + _S;
            _OutboxPath = _path + "Replication" + _S + "Outbox" + _S;
            // setup cron job
            _cron = new CronDaemon();

            _clientConfig = new ClientRepConfig();
            //  read what config
            if (File.Exists(_path + "Replication" + _S + "branch.dat"))
                _clientConfig = fastBinaryJSON.BJSON.ToObject<ClientRepConfig>(File.ReadAllBytes(_path + "Replication" + _S + "branch.dat"));
            // starting jobs
            _cron.AddJob(_clientConfig.whencron, Replicate);
        }


        private ILog _log = LogManager.GetLogger(typeof(ReplicationClient));
        IDocStorage<Guid> _docs;
        private CronDaemon _cron;
        private string _S = Path.DirectorySeparatorChar.ToString();
        private NetworkClient _client;
        private Replication.ClientConfiguration _config;
        private ClientRepConfig _clientConfig;
        private string _path;
        private string _OutboxPath;
        private string _InboxPath;
        private int INTERNALLIMIT = Global.PackageSizeItemCountLimit;

        public void Shutdown()
        {
            if (_cron != null)
                _cron.Stop();

            SaveConfig();
        }

        private void SaveConfig()
        {
            if (_clientConfig == null)
                return;
            if (_clientConfig.isConfigured == false)
                return;
            // write config to disk
            byte[] b = fastBinaryJSON.BJSON.ToBJSON(_clientConfig);
            File.WriteAllBytes(_path + "Replication" + _S + "branch.dat", b);
        }

        private object _lock = new object();
        private void Replicate()
        {
            lock (_lock)
            {
                try
                {
                    if (ConnectToHQ())
                    {
                        SendPackageToHQ();
                        GetPackageFormHQ();
                    }
                }
                catch (Exception ex)
                {
                    _log.Error(ex);
                }
                finally
                {
                    if (_client != null)
                    {
                        _client.Close();
                        _client = null;
                    }
                }
            }
        }

        private void GetPackageFormHQ()
        {
            ReplicationPacket p = createpacket();
            p.command = "getpackageforbranch";
            p.lastrecord = _clientConfig.lastHQCounter;
            ReturnPacket ret = (ReturnPacket)_client.Send(p);
            if (ret.OK)
            {
                if (ret.Data != null)
                {
                    ReplicationPacket pack = (ReplicationPacket)ret.Data;

                    if (pack.datahash == Helper.MurMur.Hash((byte[])pack.data))
                    {
                        _log.Debug("package recieved from server : " + pack.filename);
                        _log.Debug("package size : " + (pack.data as byte[]).Length.ToString("#,0"));
                        File.WriteAllBytes(_InboxPath + pack.filename, (byte[])pack.data);
                        p = createpacket();
                        p.command = "hqpackageok";
                        p.filename = pack.filename;
                        p.lastrecord = pack.lastrecord;
                        _clientConfig.lastHQCounter = pack.lastrecord;
                        SaveConfig();
                        ret = (ReturnPacket)_client.Send(p);
                        if (ret.OK)
                            return;
                    }
                }
            }
        }

        private void SendPackageToHQ()
        {
            string fn = CreatePackageForSend();
            if (fn != "")
            {
                ReplicationPacket p = createpacket();
                p.command = "packageforhq";
                p.data = File.ReadAllBytes(fn);
                p.datahash = Helper.MurMur.Hash((byte[])p.data);
                ReturnPacket ret = (ReturnPacket)_client.Send(p);
                string path = Path.GetDirectoryName(fn);
                string fnn = Path.GetFileNameWithoutExtension(fn);
                foreach (var f in Directory.GetFiles(path, fnn + ".*"))
                    File.Delete(f);
            }
        }

        private ReplicationPacket createpacket()
        {
            ReplicationPacket p = new ReplicationPacket();
            p.branchname = _config.BranchName;
            p.passwordhash = Helper.MurMur.Hash(Helper.GetBytes(_config.BranchName + "|" + _config.Password)).ToString();
            return p;
        }

        private bool ConnectToHQ()
        {

            if (_client == null)
            {
                _client = new NetworkClient(_config.ServerAddress, _config.ServerReplicationPort);
            }
            // authenticate and get branch config
            ReplicationPacket p = createpacket();
            p.command = "getbranchconfig";

            ReturnPacket ret = (ReturnPacket)_client.Send(p);
            if (ret.OK)
            {
                ClientWhatWhenConfig c = (ClientWhatWhenConfig)ret.Data;

                _clientConfig.what = c.what;

                _clientConfig.isConfigured = true;

                if (_clientConfig.whencron != c.whencron)
                {
                    _cron.Stop();
                    _clientConfig.whencron = c.whencron;
                    _cron = new CronDaemon();
                    _cron.AddJob(_clientConfig.whencron, Replicate);
                }

                SaveConfig();
            }
            return ret.OK;
        }

        private string CreatePackageForSend()
        {
            int maxc = INTERNALLIMIT;
            if (_clientConfig.what.PackageItemLimit > 0)
                maxc = _clientConfig.what.PackageItemLimit;
            string outFolder = _OutboxPath;
            int packageNumber = _clientConfig.outPackageNumber;
            int i = _clientConfig.lastCounter;
            string filename = outFolder + packageNumber.ToString("0000000000") + ".mgdat";
            int total = _docs.RecordCount();
            if (i < total)
            {
                StorageFile<Guid> package = new StorageFile<Guid>(filename, SF_FORMAT.JSON, true);
                while (maxc > 0 && i < total)
                {
                    var meta = _docs.GetMeta(i);
                    if (meta == null)
                        break;
                    if (meta.isReplicated == false && MatchType(meta.typename))
                    {
                        object obj = _docs.GetObject(i, out meta);
                        package.WriteObject(meta.key, obj);
                        maxc--;
                    }

                    i++;
                }
                package.Shutdown();
                packageNumber++;
                // compress the file
                using (FileStream read = File.OpenRead(filename))
                using (FileStream outp = File.Create(filename + ".gz"))
                    CompressForBackup(read, outp);

                // delete uncompressed file 
                File.Delete(filename);

                _clientConfig.lastCounter = i;
                _clientConfig.outPackageNumber = packageNumber;
                SaveConfig();
                return filename + ".gz";
            }
            return "";
        }

        private bool MatchType(string typename)
        {
            // match type filter
            foreach (var i in _clientConfig.what.B2HQtypes)
            {
                // do wildcard search
                Regex reg = new Regex("^" + i.Replace("*", ".*").Replace("?", "."), RegexOptions.IgnoreCase);
                if (reg.IsMatch(typename))
                    return true;
            }

            return false;
        }

        private static void CompressForBackup(Stream source, Stream destination)
        {
            using (GZipStream gz = new GZipStream(destination, CompressionMode.Compress))
                PumpDataForBackup(source, gz);
        }

        private static void PumpDataForBackup(Stream input, Stream output)
        {
            byte[] bytes = new byte[4096 * 2];
            int n;
            while ((n = input.Read(bytes, 0, bytes.Length)) != 0)
                output.Write(bytes, 0, n);
        }
    }
}
