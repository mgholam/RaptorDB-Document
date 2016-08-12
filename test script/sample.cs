// ref : ..\output\raptordb.dll
// ref : ..\output\raptordb.common.dll
// ref : ..\faker.dll
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using RaptorDB;
using RaptorDB.Common;

namespace rdbtest
{

    #region [  entities  ]
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
        public string NoCase { get; set; }
        public string Address { get; set; }
        public List<LineItem> Items { get; set; }
        public DateTime Date { get; set; }
        public int Serial { get; set; }
        public byte Status { get; set; }
        public bool Approved { get; set; }
    }
    #endregion

    #region [  view definition  ]
    public class SalesInvoiceViewRowSchema : RDBSchema
    {
        public string CustomerName;
        public string NoCase;
        public DateTime Date;
        public string Address;
        public int Serial;
        public byte Status;
        public bool? Approved;
    }

    [RegisterView]
    public class SalesInvoiceView : View<SalesInvoice>
    {
        public SalesInvoiceView()
        {
            this.Name = "SalesInvoice";
            this.Description = "A primary view for SalesInvoices";
            this.isPrimaryList = true;
            this.isActive = true;
            this.BackgroundIndexing = true;
            this.Version = 1;

            this.Schema = typeof(SalesInvoiceViewRowSchema);

            this.Mapper = (api, docid, doc) =>
            {
                //if (doc.Status == 0)
                //    return;

                api.EmitObject(docid, doc);
            };
        }
    }
    #endregion

    class Program
    {
        static RaptorDB.RaptorDB rdb; // 1 instance

        static void Main(string[] args)
        {
            rdb = RaptorDB.RaptorDB.Open("data"); // a "data" folder beside the executable
            RaptorDB.Global.RequirePrimaryView = false;

            Console.WriteLine("Registering views..");
            rdb.RegisterView(new SalesInvoiceView());

            DoWork();

            Console.WriteLine("press any key...");
            Console.ReadKey();
            Console.WriteLine("\r\nShutting down...");
            rdb.Shutdown(); // explicit shutdown
        }

        static void DoWork()
        {
            long c = rdb.DocumentCount();
            if (c > 0) // not the first time running
            {
                var result = rdb.Query<SalesInvoiceViewRowSchema>(x => x.Serial < 100);
                // show the rows
                Console.WriteLine(fastJSON.JSON.ToNiceJSON(result.Rows, new fastJSON.JSONParameters { UseExtensions = false, UseFastGuid = false }));
                // show the count
                Console.WriteLine("Query result count = " + result.Count);
                return;
            }

            Console.Write("Inserting 100,000 documents...");
            int count = 100000;

            for (int i = 0; i < count; i++)
            {                
                var inv = CreateInvoice(i);

                // save here
                rdb.Save(inv.ID, inv);
            }

            Console.WriteLine("done.");
        }

        static SalesInvoice CreateInvoice(int counter)
        {
            // new invoice
            var inv = new SalesInvoice()
            {
                Date = Faker.DateTimeFaker.BirthDay(),
                Serial = counter % 10000,
                CustomerName = Faker.NameFaker.Name(),
                NoCase = "Me " + counter % 10,
                Status = (byte)(counter % 4),
                Address = Faker.LocationFaker.Street(),
                Approved = counter % 100 == 0 ? true : false
            };
            // new line items
            inv.Items = new List<LineItem>();
            for (int k = 0; k < 5; k++)
                inv.Items.Add(new LineItem() { Product = "prod " + k, Discount = 0, Price = 10 + k, QTY = 1 + k });

            return inv;
        }
    }
}