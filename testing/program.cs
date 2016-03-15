using System;
using System.IO;
using RaptorDB;

namespace testing
{
    public class program
    {
        static RaptorDBServer server;
        public static void Main(string[] args)
        {
            AppDomain.CurrentDomain.UnhandledException += new UnhandledExceptionEventHandler(CurrentDomain_UnhandledException);
            server = new RaptorDBServer(90, @"..\..\RaptorDBdata");
            
            Console.WriteLine("Server started on port 90");
            Console.WriteLine("Press Enter to exit...");
            Console.CancelKeyPress += new ConsoleCancelEventHandler(Console_CancelKeyPress);
            Console.ReadLine();
            server.Shutdown();
            
            return;
        }

        static void Console_CancelKeyPress(object sender, ConsoleCancelEventArgs e)
        {
            Console.WriteLine("Shutting down...");
            server.Shutdown(); 
        }

        static void CurrentDomain_UnhandledException(object sender, UnhandledExceptionEventArgs e)
        {
            File.WriteAllText("error.txt", "" + e.ExceptionObject);
        }
    }
}
