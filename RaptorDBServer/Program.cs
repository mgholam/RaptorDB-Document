using System;
using System.ServiceProcess;
using System.IO;
using System.Reflection;
using System.Configuration.Install;

namespace RaptorDBServer
{
    static class Program
    {
        public static string InstallServiceName;
        public static int Port = 90;
        public static string Path = "";
        /// <summary>
        /// The main entry point for the application.
        /// </summary>
        static void Main(string[] args)
        {
            if (args.Length == 0)
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

            string name = "RaptorDB";
            string path = Directory.GetCurrentDirectory();
            int port = 90;
            bool install = false;
            bool uninstall = false;

            for (int i = 0; i < args.Length; i++)
            {
                if (args[i].Trim() == "-i") install = true;
                if (args[i].Trim() == "-u") uninstall = true;
                if (args[i].Trim() == "-p") port = int.Parse(args[++i]);
                if (args[i].Trim() == "-f") path = args[++i].Trim();
                if (args[i].Trim() == "-n") name = "RaptorDB - " + args[++i].Trim();
            }

            InstallServiceName = name;
            Port = port;
            Path = path;

            if (install)
            {
                if (IsServiceInstalled(name))
                {
                    Console.WriteLine();
                    Console.WriteLine("Service exists : " + name);
                    return;
                }
                // Install service
                ManagedInstallerClass.InstallHelper(new string[] { Assembly.GetExecutingAssembly().Location });
                return;
            }
            else if (uninstall)
            {
                if (IsServiceInstalled(name) == false)
                    return;
                // Uninstall service
                ManagedInstallerClass.InstallHelper(new string[] { "/u", Assembly.GetExecutingAssembly().Location });
                return;
            }

            if (Environment.UserInteractive == false)
                ServiceBase.Run(new Service1());
            else
                Dostart();
        }

        private static void Dostart()
        {
            var _raptor = new RaptorDB.RaptorDBServer(Port, Path);
            Console.WriteLine("Press Enter to shutdown...");
            Console.ReadLine();
            _raptor.Shutdown();
        }

        private static bool IsServiceInstalled(string serviceName)
        {
            // Get a list of current services
            ServiceController[] services = ServiceController.GetServices();

            // Look for our service
            foreach (ServiceController service in services)
                if (String.Compare(serviceName, service.ServiceName, true) == 0) 
                    return true;

            // Return
            return false;
        }
    }
}
