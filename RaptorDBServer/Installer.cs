using System.ComponentModel;
using System.Configuration.Install;
using System.ServiceProcess;

namespace RaptorDBServer
{
    [RunInstaller(true)]
    public class CustomServiceInstaller : Installer
    {
        private ServiceProcessInstaller process;
        private ServiceInstaller service;

        public CustomServiceInstaller()
        {
            process = new ServiceProcessInstaller();

            process.Account = ServiceAccount.LocalSystem;

            service = new ServiceInstaller();
            service.ServiceName = Program.InstallServiceName;
            
            Installers.Add(process);
            Installers.Add(service);
        }

        protected override void OnBeforeInstall(System.Collections.IDictionary savedState)
        {
            Context.Parameters["assemblypath"] = "\"" + this.GetType().Assembly.Location + "\" -p " + Program.Port + " -f \"" + Program.Path + "\"";
            base.OnBeforeInstall(savedState);
        }
    }
}
