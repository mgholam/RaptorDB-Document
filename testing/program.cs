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

namespace testing
{
    #region [  class definitions  ]
    public class LineItem
    {
        public decimal QTY { get; set; }
        public string Product { get; set; }
        public decimal Price { get; set; }
        public decimal Discount { get; set; }
    }

    public class SalesInvoice
    {
        public SalesInvoice()
        {
            ID = Guid.NewGuid();
        }
        public Guid ID { get; set; }
        public string CustomerName { get; set; }
        public string Address { get; set; }
        public List<LineItem> Items { get; set; }
        public DateTime Date { get; set; }
        public int Serial { get; set; }
        public byte Status { get; set; }
    }
    #endregion

    #region [  views  ]
    public class SalesInvoiceView : View<SalesInvoice>
    {
        public class RowSchema
        {
            [FullText]
            public string CustomerName;
            public DateTime InvoiceDate;
            public string Address;
            public int Serial;
            public byte Status;
        }

        public SalesInvoiceView()
        {
            this.Name = "SalesInvoice";
            this.Description = "A primary view for SalesInvoices";
            this.isPrimaryList = true;
            this.isActive = true;
            this.BackgroundIndexing = true;

            this.Schema = typeof(SalesInvoiceView.RowSchema);

            this.AddFireOnTypes(typeof(SalesInvoice));

            this.Mapper = (api, docid, doc) =>
            {
                api.Emit(docid, doc.CustomerName, doc.Date, doc.Address, doc.Serial, doc.Status);
            };
        }
    }

    public class SalesItemRowsView : View<SalesInvoice>
    {
        public class RowSchema
        {
            public string Product;
            public decimal QTY;
            public decimal Price;
            public decimal Discount;
        }

        public SalesItemRowsView()
        {
            this.Name = "SalesItemRows";
            this.Description = "";
            this.isPrimaryList = false;
            this.isActive = true;
            this.BackgroundIndexing = true;

            this.Schema = typeof(SalesItemRowsView.RowSchema);

            this.AddFireOnTypes(typeof(SalesInvoice));

            this.Mapper = (api, docid, doc) =>
                {
                    if (doc.Status == 3 && doc.Items != null)
                        foreach (var i in doc.Items)
                            api.Emit(docid, i.Product, i.QTY, i.Price, i.Discount);
                };
        }
    }

    public class newview : View<SalesInvoice>
    {
        public class RowSchema
        {
            public string Product;
            public decimal QTY;
            public decimal Price;
            public decimal Discount;
        }

        public newview()
        {
            this.Name = "newview";
            this.Description = "";
            this.isPrimaryList = false;
            this.isActive = true;
            this.BackgroundIndexing = true;
            this.Version = 1;

            this.Schema = typeof(SalesItemRowsView.RowSchema);

            this.AddFireOnTypes(typeof(SalesInvoice));

            this.Mapper = (api, docid, doc) =>
            {
                if (doc.Status == 3 && doc.Items != null)
                    foreach (var i in doc.Items)
                        api.Emit(docid, i.Product, i.QTY, i.Price, i.Discount);
            };
        }
    }
    #endregion

    public class program
    {
        public static void Main()
        {
            AppDomain.CurrentDomain.UnhandledException += new UnhandledExceptionEventHandler(CurrentDomain_UnhandledException);
            RaptorDB.RaptorDB rap = RaptorDB.RaptorDB.Open("RaptorDB");

            if (rap.RegisterView(new SalesInvoiceView()).OK == false)
            {
                Console.WriteLine("Error registering view");
                return;
            }

            if (rap.RegisterView(new SalesItemRowsView()).OK == false)
            {
                Console.WriteLine("Error registering view");
                return;
            }

            if (rap.RegisterView(new newview()).OK == false)
            {
                Console.WriteLine("Error registering view");
                return;
            } 
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
        }

        static void CurrentDomain_UnhandledException(object sender, UnhandledExceptionEventArgs e)
        {
            File.WriteAllText("error.txt", "" + e.ExceptionObject);
        }
    }
}
