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
    #region [  class definition  ]
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

    public class program
    {
        public class SalesInvoiceView : View<SalesInvoice>
        {
            public class RowSchema
            {
                public NormalString CustomerName;
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
                public NormalString Product;
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
                            {
                                api.Emit(docid, i.Product, i.QTY, i.Price, i.Discount);
                            }
                    };
            }
        }

        public static void Main()
        {
            AppDomain.CurrentDomain.UnhandledException += new UnhandledExceptionEventHandler(CurrentDomain_UnhandledException);


            RaptorDB.RaptorDB rap = RaptorDB.RaptorDB.Open("RaptorDB");

            SalesInvoiceView v = new SalesInvoiceView();

            if (rap.RegisterView(v).OK == false)
            {
                Console.WriteLine("Error registering view");
                return;
            }

            SalesItemRowsView v2 = new SalesItemRowsView();

            if (rap.RegisterView(v2).OK == false)
            {
                Console.WriteLine("Error registering view");
                return;
            }
            DateTime dt = FastDateTime.Now;
            var q = rap.Query("SalesItemRows", (LineItem l) => (l.Product == "asdas 1" || l.Product == "asdas 3"));
            Console.WriteLine("query lineitems = " + FastDateTime.Now.Subtract(dt).TotalSeconds);
            
            dt = FastDateTime.Now;

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
                {
                    inv.Items.Add(new LineItem() { Product = "asdas " + k, Discount = 0, Price = 10 + k, QTY = 1 + k });
                }
                rap.Save(inv.ID, inv);
            }

            Console.WriteLine("insert time secs = " + FastDateTime.Now.Subtract(dt).TotalSeconds);
            Console.WriteLine("Press (R) for redo query");
        //Thread.Sleep(4000);
        redo:
            dt = FastDateTime.Now;
            //q = rap.Query("SalesItemRows", (LineItem l) => (l.Price == 10));
            int j = 100;
            var res = rap.Query(//"SalesInvoice",
                typeof(SalesInvoice),
                (SalesInvoice s) => (s.Serial < j) && (s.Status == 1 || s.Status == 3));

            if (res.OK)
            {
                Console.WriteLine("count = " + res.Count);
            }
            Console.WriteLine("query time secs = " + FastDateTime.Now.Subtract(dt).TotalSeconds);
            dt = FastDateTime.Now;
            q = rap.Query("SalesItemRows", (LineItem l) => (l.Product == "asdas 1" || l.Product == "asdas 3"));
            Console.WriteLine("Count = " + q.Count);
            Console.WriteLine("query lineitems = " + FastDateTime.Now.Subtract(dt).TotalSeconds);
            if (Console.ReadKey().Key == ConsoleKey.R) { Console.WriteLine("edo"); goto redo; }
            rap.Dispose();
            return;
        }

        static void CurrentDomain_UnhandledException(object sender, UnhandledExceptionEventArgs e)
        {
            File.WriteAllText("error.txt", "" + e.ExceptionObject);
        }
    }
}
