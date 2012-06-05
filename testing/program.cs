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
        public static void Main()
        {
            AppDomain.CurrentDomain.UnhandledException += new UnhandledExceptionEventHandler(CurrentDomain_UnhandledException);
            server = new RaptorDBServer(90, @"..\..\..\RaptorDBdata");
            //RaptorDBClient client = new RaptorDBClient("127.0.0.1", 90, "admin", "admin");
            //var r = client.Query("salesinvoice", "serial<100");
            Console.WriteLine("Server started on port 90");
            Console.WriteLine("Press Enter to exit...");
            Console.CancelKeyPress += new ConsoleCancelEventHandler(Console_CancelKeyPress);
            
            Console.ReadLine();
            server.Shutdown();
            return;
            /*
            RaptorDB.RaptorDB rap = RaptorDB.RaptorDB.Open(@"..\..\..\RaptorDBdata");

            rap.RegisterView(new SalesInvoiceView());
            rap.RegisterView(new SalesItemRowsView());
            rap.RegisterView(new newview());
            bool end = false;

            Console.WriteLine("Press (P)redifined query, (I)nsert 100,000 docs, (S)tring query, (Q)uit");

        redo:
            ConsoleKey key = Console.ReadKey().Key;
            Console.WriteLine();

            switch (key)
            {
                case ConsoleKey.P:
                    predefinedquery(rap);
                    break;
                case ConsoleKey.I:
                    insertdata(rap);
                    break;
                case ConsoleKey.S:
                    stringquery(rap);
                    break;
                case ConsoleKey.Q:
                    end = true;
                    break;
                default:
                    Console.WriteLine("Press (P)redifined query, (I)nsert 100,000 docs, (S)tring query, (Q)uit");
                    break;
            }
            if (!end)
                goto redo;
            rap.Shutdown();
        }

        private static void stringquery(RaptorDB.RaptorDB rap)
        {
            Console.WriteLine("Enter you query in the following format :  viewname , query");
            string[] s = Console.ReadLine().Split(',');
            DateTime dt = FastDateTime.Now;
            try
            {
                var q = rap.Query(s[0].Trim(), s[1].Trim());
                Console.WriteLine("query time = " + FastDateTime.Now.Subtract(dt).TotalSeconds);
                Console.WriteLine("query count = " + q.Count);
                Console.WriteLine();
            }
            catch { Console.WriteLine("error in query."); }
        }

        private static void predefinedquery(RaptorDB.RaptorDB rap)
        {
            DateTime dt = FastDateTime.Now;
            int j = 100;
            var q = rap.Query(typeof(SalesInvoice), (SalesInvoice l) => (l.Serial == j));
            Console.WriteLine("query SalesInvoice time = " + FastDateTime.Now.Subtract(dt).TotalSeconds);
            Console.WriteLine("query count = " + q.Count);
                
            q = rap.Query(typeof(SalesItemRowsView), (LineItem l) => (l.Product == "prod 1" || l.Product == "prod 3"));
            
            // grouping
            List<SalesItemRowsView.RowSchema> list = q.Rows.Cast<SalesItemRowsView.RowSchema>().ToList();
            var e = from item in list group item by item.Product into grouped 
                    select new { Product = grouped.Key, 
                                 TotalPrice = grouped.Sum(product => product.Price),
                                 TotalQTY = grouped.Sum(product => product.QTY)
                    };

            //string str = fastJSON.JSON.Instance.ToJSON(e.ToList(), new fastJSON.JSONParamters { EnableAnonymousTypes = true });

            Console.WriteLine("query lineitems time = " + FastDateTime.Now.Subtract(dt).TotalSeconds);
            Console.WriteLine("query count = " + q.Count);
            Console.WriteLine();
        }

        private static void insertdata(RaptorDB.RaptorDB rap)
        {
            DateTime dt = FastDateTime.Now;

            for (int i = 0; i < 100000; i++)
            {
                var inv = new SalesInvoice()
                {
                    Date = FastDateTime.Now,
                    Serial = i % 10000,
                    CustomerName = "me " + i % 10,
                    Status = (byte)(i % 4),
                    Address = "df asd sdf asdf asdf"
                };
                inv.Items = new List<LineItem>();
                for (int k = 0; k < 5; k++)
                    inv.Items.Add(new LineItem() { Product = "prod " + k, Discount = 0, Price = 10 + k, QTY = 1 + k });
                if (i % 5000 == 0)
                    Console.Write(".");
                rap.Save(inv.ID, inv);
            }
            Console.WriteLine();
            Console.WriteLine("insert time secs = " + FastDateTime.Now.Subtract(dt).TotalSeconds);
             */
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
