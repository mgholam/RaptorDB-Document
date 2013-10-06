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
using System.Threading.Tasks;


namespace testing
{
    //class Program3
    //{
    //    static int waitTime = 5;
    //    static string dataPath = "_data";
    //    static RaptorDB.RaptorDB DB = null;
    //    static Sample[] SampleArray = 
    //    {
    //        new Sample
    //        {
    //            ID = Guid.NewGuid(),
    //            Name = "Object 1",
    //            OtherValue = "other value for object 1"
    //        },
    //        new Sample
    //        {
    //            ID = Guid.NewGuid(),
    //            Name = "Object 2",
    //            OtherValue = "other value for object 2"
    //        }
    //    };

    //    public static void Main3(string[] args)
    //    {
    //        //start with a clean slate
    //        if (Directory.Exists(dataPath))
    //        {
    //            Console.WriteLine("Delete database directory if it already exists");
    //            Directory.Delete(dataPath, true);
    //        }
    //        DBInit();

    //        //Write initial data and verify
    //        WriteSampleData();
    //        Wait();
    //        ReadAllObjects();

    //        //Delete 1 object and verify
    //        if (DB.Delete(SampleArray[0].ID))
    //            Console.WriteLine("Deleted {0}", SampleArray[0].Name);
    //        else
    //        {
    //            Console.WriteLine("Unable to delete object");
    //            return;
    //        }
    //        Wait();
    //        ReadAllObjects();

    //        //Remove view folder and demonstrate the bug
    //        RemoveViewFolder();
    //        Wait();
    //        ReadAllObjects();

    //        Console.Write("Press any key to continue . . . ");
    //        Console.ReadKey(true);
    //    }

    //    static void DBInit()
    //    {
    //        Console.WriteLine("Initialise database");
    //        DB = RaptorDB.RaptorDB.Open(dataPath);
    //        DB.RegisterView(new SampleView());
    //    }

    //    static void WriteSampleData()
    //    {
    //        foreach (Sample obj in SampleArray)
    //        {
    //            Console.WriteLine("{0} : {1} : {2}", obj.ID.ToString(), obj.Name, obj.OtherValue);
    //            DB.Save(obj.ID, obj);
    //        }
    //    }

    //    static void ReadAllObjects()
    //    {
    //        Console.WriteLine("Read back all data from DB");

    //        var list = DB.Query("SampleView");
    //        foreach (SampleView.RowSchema row in list.Rows)
    //        {
    //            Console.WriteLine("ViewRow {0}: {1}", row.docid.ToString(), row.Name);
    //            Sample obj = (Sample)DB.Fetch(row.docid);
    //            if (obj != null)
    //                Console.WriteLine("Object  {0} : {1} : {2}", obj.ID.ToString(), obj.Name, obj.OtherValue);
    //            else
    //                Console.WriteLine("Can't retrieve original Object");

    //            Console.WriteLine("");
    //        }
    //    }

    //    static void Wait()
    //    {
    //        Console.Write("\nWait a few seconds to make sure records are written");
    //        for (int i = 0; i < waitTime; i++)
    //        {
    //            Console.Write(".");
    //            System.Threading.Thread.Sleep(1000);
    //        }
    //        Console.WriteLine("\n");
    //    }

    //    static void RemoveViewFolder()
    //    {
    //        Console.WriteLine("Close connection to DB");
    //        DB.Shutdown();
    //        DB = null;

    //        Console.WriteLine("Remove 'Views' folder from DB folder to force the views to be re-populated");
    //        Directory.Delete(dataPath + @"\Views", true);

    //        Console.WriteLine("Re-connect to database");
    //        //Re-initialise DB
    //        DBInit();
    //    }
    //}

    //public class Sample
    //{
    //    public Guid ID;
    //    public string Name;
    //    public string OtherValue;

    //    public Sample()
    //    {
    //    }
    //}

    //public class SampleView : View<Sample>
    //{
    //    public class RowSchema : RDBSchema
    //    {
    //        public string Name;
    //    }

    //    public SampleView()
    //    {
    //        this.Name = "SampleView";
    //        this.Description = "A primary view for sample objects";
    //        this.isPrimaryList = true;
    //        this.isActive = true;
    //        this.BackgroundIndexing = false;

    //        this.Schema = typeof(SampleView.RowSchema);

    //        this.AddFireOnTypes(typeof(Sample));

    //        this.Mapper = (api, docid, doc) =>
    //        {
    //            api.EmitObject(docid, doc);
    //        };
    //    }
    //}
    
    /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    /*
    public class program2
    {
        static RaptorDB.RaptorDB rd;
        public static void Main2(string[] args)
        {
            if (args.Length < 2)
            {
                System.Console.WriteLine("Params: numthreads insertsforthreads (numthreads =  0 for insertion in the main thread )");
                return;
            }
            DateTime dt = FastDateTime.Now;
            int maxThread = int.Parse(args[0]);
            int maxDataToInsert = int.Parse(args[1]);
            RepositoryStart();
            System.Console.WriteLine("Starting inserts " + maxDataToInsert + " objects for " + maxThread + " thread/s....");
            if (maxThread == 0)
                InsertInvoice(maxDataToInsert);
            else
            {
                System.Threading.Tasks.Task[] tasks = new System.Threading.Tasks.Task[maxThread];
                Action<object> act = InsertInvoice;

                for (int i = 0; i < maxThread; i++)
                {
                    tasks[i] = System.Threading.Tasks.Task.Factory.StartNew(act, maxDataToInsert);
                }
                Task.WaitAll(tasks);
            }
            //System.Console.Write((maxDataToInsert * (maxThread == 0 ? 1 : maxThread)).ToString("#,###") + " inserted press a key to continue... ");
            //System.Console.ReadLine();
            RepositoryStop();
            System.Console.WriteLine("Re-opening RaptorDB for objects count....");
            RepositoryStart();
            System.Console.WriteLine(rd.Count(typeof(InvoiceView)));
            RepositoryStop();
            Console.WriteLine("time = " + FastDateTime.Now.Subtract(dt).TotalSeconds);
            return;
        }


        public static void RepositoryStart()
        {
            rd = RaptorDB.RaptorDB.Open(@"C:\temp\RaptorDBData\DMDATA");
            System.Console.WriteLine("Registering indexes....");
            rd.RegisterView(new InvoiceView());
            rd.RegisterView(new InvoiceAdditionalView());
            rd.RegisterView(new InvoiceItemView());
            System.Console.WriteLine("RaptorDB is Up....");
        }

        public static void RepositoryStop()
        {
            System.Console.WriteLine("RaptorDB shutdown....");
            rd.Shutdown();
            rd.Dispose();
            rd = null;
            System.Console.WriteLine("RaptorDB halted....");
        }



        public static void InsertInvoice(object maxDataToInsert)
        {
            System.Console.WriteLine("Thread " + System.Threading.Thread.CurrentThread.ManagedThreadId + " started...");
            int maxVal = (int)maxDataToInsert;
            for (int i = 1; i <= maxVal; i++)
            {

                Invoice invoice = new Invoice();
                invoice.UNIQUEID = Guid.NewGuid();
                invoice.Customer = "Customer1";
                invoice.InvoiceNumber = 10;
                invoice.InvoiceDate = DateTime.Now;
                invoice.items = new List<InvoiceItem>();
                invoice.items.Add(new InvoiceItem() { SKU = "ART" + i.ToString(), qty = 10.50M, UnitPrice = i, TotalPrice = (10.50M * i) });
                rd.Save<Invoice>(invoice.UNIQUEID, invoice);
                // System.Console.WriteLine(" Writing Thread " + System.Threading.Thread.CurrentThread.ManagedThreadId.ToString() + " Inserted doc " + i.ToString());
            }
            System.Console.WriteLine("Thread " + System.Threading.Thread.CurrentThread.ManagedThreadId + " ended with " + maxVal + " objects");
        }

        public static void SearchInvoice()
        {
            int count = rd.Query(typeof(InvoiceView)).Count;
        }



        public class BaseStore
        {
            public Guid UNIQUEID { get; set; }

        }



        public class Invoice : BaseStore
        {
            public DateTime InvoiceDate { get; set; }
            public String Customer { get; set; }
            public int InvoiceNumber { get; set; }
            public List<InvoiceItem> items { get; set; }
            public String TAG { get; set; }
        }


        public class InvoiceItem : BaseStore
        {
            public string SKU { get; set; }
            public Decimal qty { get; set; }
            public Decimal UnitPrice { get; set; }
            public Decimal TotalPrice { get; set; }
        }




        public class InvoiceView : RaptorDB.View<Invoice>
        {
            public class RowSchema : RaptorDB.RDBSchema
            {
                public DateTime InvoiceDate { get; set; }
                public String Customer { get; set; }
                public int InvoiceNumber { get; set; }

            }

            public InvoiceView()
            {
                this.Name = "InvoiceView";
                this.Schema = typeof(InvoiceView.RowSchema);
                this.isActive = true;
                this.isPrimaryList = true;
                this.ConsistentSaveToThisView = true;
                this.AddFireOnTypes(typeof(Invoice));
                this.Mapper = (api, docid, doc) => { api.Emit(docid, doc.InvoiceDate, doc.Customer, doc.InvoiceNumber); };
            }
        }

        public class InvoiceAdditionalView : RaptorDB.View<Invoice>
        {
            public class RowSchema : RaptorDB.RDBSchema
            {
                public String TAG { get; set; }
            }

            public InvoiceAdditionalView()
            {
                this.Name = "InvoiceAdditionalView";
                this.Schema = typeof(InvoiceView.RowSchema);
                this.isActive = true;
                this.isPrimaryList = false;
                this.ConsistentSaveToThisView = true;
                this.AddFireOnTypes(typeof(Invoice));
                this.Mapper = (api, docid, doc) => { api.Emit(docid, doc.TAG); };
            }
        }

        [RegisterView]
        public class InvoiceItemView : RaptorDB.View<Invoice>
        {
            public class RowSchema : RDBSchema
            {
                public string SKU { get; set; }
                public Decimal UnitPrice { get; set; }
                public Decimal qty { get; set; }
            }

            public InvoiceItemView()
            {
                this.Name = "InvoiceItem View";
                this.Schema = typeof(InvoiceItemView.RowSchema);
                this.isActive = true;
                this.isPrimaryList = false;
                this.AddFireOnTypes(typeof(Invoice));
                this.Mapper = (api, docid, doc) =>
                {
                    foreach (InvoiceItem item in doc.items)
                    {
                        api.Emit(docid, item.SKU, item.UnitPrice, item.qty);
                    }
                };
            }
        }
    }
    */
}