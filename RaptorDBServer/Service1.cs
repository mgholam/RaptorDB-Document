using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Diagnostics;
using System.Linq;
using System.ServiceProcess;
using System.Text;
using System.IO;
using System.Threading.Tasks;

namespace RaptorDBServer
{
    public partial class Service1 : ServiceBase
    {
        public Service1()
        {
            InitializeComponent();
        }

        RaptorDB.RaptorDBServer _raptor;

        protected override void OnStart(string[] args)
        {
            Directory.SetCurrentDirectory(Path.GetDirectoryName(this.GetType().Assembly.Location));
            _raptor = new RaptorDB.RaptorDBServer(Program.Port, Program.Path);
        }

        protected override void OnStop()
        {
            _raptor.Shutdown();
        }

        protected override void OnShutdown()
        {
            _raptor.Shutdown();
        }
    }
}
