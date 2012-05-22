using System;
using System.Collections.Generic;
using System.Linq;
using System.ServiceProcess;
using System.Text;

namespace RaptorDBServer
{
    static class Program
    {
        /// <summary>
        /// The main entry point for the application.
        /// </summary>
        static void Main(string[] args)
        {
            if (args.Length==0)
            {
                Console.WriteLine(@"
Run with : 
  -i install service
  -u uninstall service
  -n <service name> [default = RaptorDB]
  -p <port number> [default = 90]
  -f <data folder path>
");
                return;
            }
            ServiceBase[] ServicesToRun;
            ServicesToRun = new ServiceBase[] 
			{ 
				new Service1() 
			};
            ServiceBase.Run(ServicesToRun);
        }
    }
}
