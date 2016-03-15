namespace RaptorDB.Replication
{
    public class ReplicationPacket
    {
        //public int number;
        public string passwordhash;
        public string branchname;// source name
        public uint datahash; 
        public string filename;
        public object data;
        public string command;
        public int lastrecord;
    }
}
