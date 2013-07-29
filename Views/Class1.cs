using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using RaptorDB;


namespace SampleViews
{
    #region [  class definitions  ]
    //public enum State
    //{
    //    Open,
    //    Closed,
    //    Approved
    //}

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
        public string NoCase {get; set ;}
        public string Address { get; set; }
        public List<LineItem> Items { get; set; }
        public DateTime Date { get; set; }
        public int Serial { get; set; }
        public byte Status { get; set; }
        public bool Approved { get; set; }
        //public State InvoiceState { get; set; }
    }
    #endregion

    #region [  views  ]
    [RegisterView]
    public class SalesInvoiceView : View<SalesInvoice>
    {
        public class RowSchema : RDBSchema
        {
            //[FullText]
            public string CustomerName;
            [CaseInsensitive]
            public string NoCase;
            public DateTime Date;
            public string Address;
            public int Serial;
            public byte Status;
            public bool Approved;
            //public State InvoiceState;
        }

        // define your own row schema below (you must define a 'docid' property)

        //public class RowSchema
        //{
        //    public string CustomerName { get; set; }
        //    public DateTime Date { get; set; }
        //    public string Address { get; set; }
        //    public int Serial { get; set; }
        //    public byte Status { get; set; }
        //    public bool Approved { get; set; }

        //    public Guid docid { get; set; }
        //}

        public SalesInvoiceView()
        {
            this.Name = "SalesInvoice";
            this.Description = "A primary view for SalesInvoices";
            this.isPrimaryList = true;
            this.isActive = true;
            this.BackgroundIndexing = true;
            this.Version = 3;
            //// uncomment the following for transaction mode
            //this.TransactionMode = true;

            this.Schema = typeof(SalesInvoiceView.RowSchema);

            this.FullTextColumns.Add("customername"); // this or the attribute

            this.CaseInsensitiveColumns.Add("nocase"); // this or the attribute

            this.AddFireOnTypes(typeof(SalesInvoice));

            this.Mapper = (api, docid, doc) =>
            {
                //int c = api.Count("SalesItemRows", "product = \"prod 1\"");
                if (doc.Serial == 0)
                    api.RollBack();
                api.EmitObject(docid, doc);
            };
        }
    }

    [RegisterView]
    public class SalesItemRowsView : View<SalesInvoice>
    {
        public class RowSchema : RDBSchema
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
                    foreach (var item in doc.Items)
                        api.EmitObject(docid, item);
            };
        }
    }

    [RegisterView]
    public class newview : View<SalesInvoice>
    {
        public class RowSchema : RDBSchema
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

            this.Schema = typeof(newview.RowSchema);

            this.AddFireOnTypes(typeof(SalesInvoice));

            this.Mapper = (api, docid, doc) =>
            {
                if (doc.Status == 3 && doc.Items != null)
                    foreach (var i in doc.Items)
                        api.EmitObject(docid, i);
            };
        }
    }
    #endregion
}
