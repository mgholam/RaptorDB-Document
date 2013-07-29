using System;
using System.Collections.Generic;
using System.Linq;
using RaptorDB.Common;
using SampleViews;
using RaptorDB;

namespace Views
{
    public class ServerSide
    {
        // so the result can be serialized and is not an anonymous type
        // since this uses fields, derive from the BindableFields for data binding to work
        public class sumtype : RaptorDB.BindableFields
        {
            public string Product;
            public decimal TotalPrice;
            public decimal TotalQTY;
        }

        public static List<object> Sum_Products_based_on_filter(IRaptorDB rap, string filter)
        {
            var q = rap.Query(typeof(SalesItemRowsView), filter);

            List<SalesItemRowsView.RowSchema> list = q.Rows.Cast<SalesItemRowsView.RowSchema>().ToList();
            var res = from item in list
                      group item by item.Product into grouped
                      select new sumtype // avoid annymous types
                      {
                          Product = grouped.Key,
                          TotalPrice = grouped.Sum(product => product.Price),
                          TotalQTY = grouped.Sum(product => product.QTY)
                      };

            return res.ToList<object>();
        }
    }


    public class EmbeddedHandler : IClientHandler
    {
        public bool GenerateClientData(IQueryInterface api, string username, List<Guid> DocsToSend)
        {
            api.Log("generating data for user : " + username);

            // query data to send to client here as needed
            var r = api.Query<SalesInvoiceView.RowSchema>(x => x.Serial < 10);
            foreach (var p in r.Rows)
            {
                DocsToSend.Add(p.docid);
            }


            return true;
        }
    }
}
