using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using RaptorDB.Common;
using SampleViews;

namespace Views
{
    public class ServerSide
    {
        public class sumtype : RaptorDB.BindableFields
        {
            public string Product;
            public decimal TotalPrice;
            public decimal TotalQTY;
        }

        public static List<object> Sum_Product1_and_Product2(IRaptorDB rap)
        {
            var q = rap.Query(typeof(SalesItemRowsView), (LineItem l) => (l.Product == "prod 1" || l.Product == "prod 3"));

            List<SalesItemRowsView.RowSchema> list = q.Rows.Cast<SalesItemRowsView.RowSchema>().ToList();
            var res = from item in list
                      group item by item.Product into grouped
                      select new sumtype
                      {
                          Product = grouped.Key,
                          TotalPrice = grouped.Sum(product => product.Price),
                          TotalQTY = grouped.Sum(product => product.QTY)
                      };

            return res.ToList<object>();
        }
    }
}
