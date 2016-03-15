using System.ServiceProcess;
using System.IO;

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
