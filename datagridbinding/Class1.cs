using System;
using System.Collections.Generic;
using RaptorDB;
using RaptorDB.Views;

namespace datagridbinding
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
        public class RowSchema : RDBSchema
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
                        foreach (var i in doc.Items)
                            api.Emit(docid, i.Product, i.QTY, i.Price, i.Discount);
                };
        }
    }

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
                        api.Emit(docid, i.Product, i.QTY, i.Price, i.Discount);
            };
        }
    }
    #endregion
}
