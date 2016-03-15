using System;
using RaptorDB.Common;
using System.IO;
using System.IO.Compression;
using System.Text.RegularExpressions;

namespace RaptorDB.Replication
{
    internal class ReplicationServer
    {
        public ReplicationServer(string datapath, string config, IDocStorage<Guid> docs)
        {
            _docs = docs;
            _Path = datapath;
            Initialize(config);
        }
        IDocStorage<Guid> _docs;
        private string _S = Path.DirectorySeparatorChar.ToString();
        private string _Path;
        private ILog _log = LogManager.GetLogger(typeof(ReplicationServer));
        private ServerConfiguration _config;
        private NetworkServer _server;
        private string _InboxPath;
        private string _OutboxPath;
        private int INTERNALLIMIT = Global.PackageSizeItemCountLimit;
        private SafeDictionary<string, int> _branchLastDocs = new SafeDictionary<string, int>();


        private void Initialize(string config)
        {
            _log.Debug("Starting replication server...");
            Directory.CreateDirectory(_Path + "Replication");
            Directory.CreateDirectory(_Path + "Replication" + _S + "Inbox");
            Directory.CreateDirectory(_Path + "Replication" + _S + "Outbox");
            _InboxPath = _Path + "Replication" + _S + "Inbox";
            _OutboxPath = _Path + "Replication" + _S + "Outbox";

            _config = fastJSON.JSON.ToObject<ServerConfiguration>(config);
            if (_config == null)
            {
                _log.Error("unable to read the configuration for replication, check the config file");
                return;
            }

            // read branch lastdoc counts here
            foreach (var b in _config.Where)
            {
                int i = -1;
                if (File.Exists(_Path + "Replication" + _S + b.BranchName + ".last"))
                    i = Helper.ToInt32(File.ReadAllBytes(_Path + "Replication" + _S + b.BranchName + ".last"), 0);
                Directory.CreateDirectory(_Path + "Replication" + _S + "Inbox" + _S + b.BranchName);
                Directory.CreateDirectory(_Path + "Replication" + _S + "Outbox" + _S + b.BranchName);
                _branchLastDocs.Add(b.BranchName.ToLower(), i);
            }

            _server = new NetworkServer();
            _server.Start(_config.ReplicationPort, processpayload);
        }

        public void Shutdown()
        {
            WriteBranchCounters();
            // shutdown every thing
            _server.Stop();
        }

        private void WriteBranchCounters()
        {
            // write branch counts etc. to disk
            foreach (var b in _branchLastDocs)
            {
                File.WriteAllBytes(_Path + "Replication" + _S + b.Key + ".last", Helper.GetBytes(b.Value, false));
                _log.Debug("last counter for branch : " + b.Key + " = " + b.Value);
            }
        }

        private object processpayload(object data)
        {
            ReplicationPacket p = (ReplicationPacket)data;

            if (Authenticate(p) == false)
                return new ReturnPacket(false, "Authentication failed");

            ReturnPacket ret = new ReturnPacket(true);
            try
            {
                switch (p.command)
                {
                    case "getbranchconfig":
                        ret.OK = true;
                        ret.Data = GetBranchConfig(p.branchname);
                        break;
                    case "getpackageforbranch":
                        ret.OK = true;
                        ReplicationPacket pack = GetPackageForBranch(p);
                        ret.Data = pack;
                        break;
                    case "packageforhq":
                        ret.OK = PackageForHQ(p);
                        break;
                    case "hqpackageok":
                        ret.OK = true;
                        File.Delete(_OutboxPath + _S + p.branchname + _S + p.filename);
                        // set last rec on hq
                        _branchLastDocs.Add(p.branchname.ToLower(), p.lastrecord);
                        WriteBranchCounters();
                        break;
                }
            }
            catch (Exception ex)
            {
                ret.OK = false;
                _log.Error(ex);
            }
            return ret;
        }


        private ClientWhatWhenConfig GetBranchConfig(string branchname)
        {
            WhatItem ret = _config.What.Find((WhatItem w) => { return w.Name.ToLower() == branchname.ToLower(); });

            if (ret == null)
                ret = _config.What.Find((WhatItem w) => { return w.Name.ToLower() == "default"; });

            ClientWhatWhenConfig c = new ClientWhatWhenConfig();
            c.what = ret;
            var where = _config.Where.Find(w => { return w.BranchName.ToLower() == branchname.ToLower(); });
            if (where != null)
                c.whencron = where.When;
            else
                c.whencron = "* * * * *";

            return c;
        }

        private bool PackageForHQ(ReplicationPacket p)
        {
            uint hash = Helper.MurMur.Hash((byte[])p.data);
            if (hash != p.datahash)
                return false;
            // save file to \replication\inbox\branchname
            Directory.CreateDirectory(_InboxPath + _S + p.branchname);
            string fn = _InboxPath + _S + p.branchname + _S + p.filename;
            _log.Debug("package recieved from : " + p.branchname);
            _log.Debug("package name : " + p.filename);
            _log.Debug("package size : " + (p.data as byte[]).Length.ToString("#,0"));
            File.WriteAllBytes(fn, (byte[])p.data);
            return true;
        }

        private ReplicationPacket GetPackageForBranch(ReplicationPacket packet)
        {
            int last = _branchLastDocs[packet.branchname.ToLower()];
            // skip retry for the same package
            if (packet.lastrecord >= _branchLastDocs[packet.branchname.ToLower()])
            {
                string fn = CreatePackageForSend(packet, out last);
                ReplicationPacket p = new ReplicationPacket();
                p.filename = Path.GetFileName(fn);
                p.data = File.ReadAllBytes(fn);
                p.datahash = Helper.MurMur.Hash((byte[])p.data);
                p.lastrecord = last;
                return p;
            }
            else
                return null;
        }

        private bool Authenticate(ReplicationPacket p)
        {
            uint pwd = uint.Parse(p.passwordhash);
            bool auth = false;
            foreach (var w in _config.Where)
            {
                uint hash = Helper.MurMur.Hash(Helper.GetBytes(w.BranchName + "|" + w.Password));
                if (hash == pwd) auth = true;
            }
            if (auth == false)
                _log.Debug("Authentication failed for '" + p.branchname + "' hash = " + p.passwordhash);
            return auth;
        }

        private string CreatePackageForSend(ReplicationPacket packet, out int last)
        {
            int maxc = INTERNALLIMIT;
            WhatItem what = GetBranchConfig(packet.branchname).what;
            if (what.PackageItemLimit > 0)
                maxc = what.PackageItemLimit;
            string outFolder = _OutboxPath;
            int packageNumber = packet.lastrecord;
            int i = packet.lastrecord;
            string filename = outFolder + _S + packet.branchname + _S + packageNumber.ToString("0000000000") + ".mgdat";

            if (i < _docs.RecordCount())
            {
                StorageFile<Guid> package = new StorageFile<Guid>(filename, SF_FORMAT.JSON, true);
                while (maxc > 0)
                {
                    var meta = _docs.GetMeta(i);
                    if (meta == null)
                        break;
                    if (meta.isReplicated == false && MatchType(meta.typename, what))
                    {
                        if (meta.isDeleted == false || what.PropogateHQDeletes)
                        {
                            object obj = _docs.GetObject(i, out meta);
                            package.WriteObject(meta.key, obj);                
                            maxc--;
                        }
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
            }

            last = i;
            return filename + ".gz";
        }

        private bool MatchType(string typename, WhatItem what)
        {
            // match type filter
            foreach (var i in what.HQ2Btypes)
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
