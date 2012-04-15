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
    }



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
                    api.Emit(docid, doc.CustomerName, doc.Date, doc.Address, doc.Serial);
                };
            }
        }


        public static void Main()
        {
            AppDomain.CurrentDomain.UnhandledException += new UnhandledExceptionEventHandler(CurrentDomain_UnhandledException);
            DateTime dt = FastDateTime.Now;

            RaptorDB.RaptorDB rap = RaptorDB.RaptorDB.Open("RaptorDB");

            SalesInvoiceView v = new SalesInvoiceView();

            if (rap.RegisterView(v).OK == false)
            {
                Console.WriteLine("Error registering view");
                return;
            }              

            for (int i = 0; i < 100000; i++)
            {
                var inv = new SalesInvoice()
                {
                    Date = FastDateTime.Now,
                    Serial = i % 10000,
                    CustomerName = "me " + i % 10,
                    Address = "df asd sdf asdf asdf"
                };

                rap.Save(inv.ID, inv);
            }

            Console.WriteLine("insert time secs = " + FastDateTime.Now.Subtract(dt).TotalSeconds);
            Console.WriteLine("Press (R) for redo query");
            //Thread.Sleep(4000);
            redo:
            dt = FastDateTime.Now;

            int j = 50;
            var res = rap.Query(typeof(SalesInvoice), (SalesInvoice s) => (s.Serial < j) || (s.Serial <=10 && s.Serial>2));

            if (res.OK)
            {
                Console.WriteLine("count = " + res.Count);
            }
            Console.WriteLine("query time secs = " + FastDateTime.Now.Subtract(dt).TotalSeconds);
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
