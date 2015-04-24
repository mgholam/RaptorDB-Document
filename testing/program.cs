using System;
using System.Diagnostics;
using System.Collections;
using System.IO;
using System.Text;
using System.Threading;
using RaptorDB;
using System.Collections.Generic;
using System.Runtime.InteropServices;
using System.Reflection;
using RaptorDB.Views;
using System.Linq.Expressions;
using System.Linq;
using System.Collections.ObjectModel;
using System.Dynamic;
using RaptorDB.Common;

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
