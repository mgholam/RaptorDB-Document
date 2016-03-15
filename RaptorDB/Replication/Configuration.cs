using System.Collections.Generic;

namespace RaptorDB.Replication
{
    public class WhereItem
    {
        public string BranchName;
        public string Password;
        public string What;
        public string When;
    }

    public class WhatItem
    {
        public WhatItem()
        {
            HQ2Btypes = new List<string>();
            B2HQtypes = new List<string>();
        }
        public string Name;
        public int Version = 1;
        public bool PropogateHQDeletes = true;
        public int PackageItemLimit = 10000;
        public List<string> HQ2Btypes;
        public List<string> B2HQtypes;
    }

    public class ServerConfiguration
    {
        public ServerConfiguration()
        {
            Where = new List<WhereItem>();
            What = new List<WhatItem>();
            ReplicationPort = 9999;
        }
        public int ReplicationPort;
        //public string EmbeddedClientHandler;
        public List<WhereItem> Where;
        public List<WhatItem> What;
    }

    public enum REPMODE
    {
        Branch,
        Server
    }

    public class ClientConfiguration
    {
        public ClientConfiguration()
        {
            ServerReplicationPort = 9999;
        }
        public string ServerAddress = "";
        public int ServerReplicationPort;
        public string Password = "";
        public string BranchName = "";
    }
}
