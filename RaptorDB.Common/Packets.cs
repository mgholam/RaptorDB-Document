using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace RaptorDB.Common
{
    public class Packet
    {
        public string Username { get; set; }
        public string PasswordHash { get; set; }
        //public int Token { get; set; }
        //public int Session { get; set; }
        public string Command { get; set; }
        public object Data { get; set; }
        public Guid Docid { get; set; }
        public string Viewname { get; set; }
    }

    public class ReturnPacket
    {
        public bool OK { get; set; }
        //public int Token { get; set; }
        //public int Session { get; set; }
        public object Data { get; set; }
    }
}
