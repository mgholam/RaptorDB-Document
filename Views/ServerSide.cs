using System.Collections.Generic;
using System.Linq;
using RaptorDB.Common;
using SampleViews;

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
            var q = rap.Query<SalesItemRowsViewRowSchema>(filter);

            var res = from x in q.Rows
                      group x by x.Product into g
                      select new sumtype // avoid anonymous types
                      {
                          Product = g.Key,
                          TotalPrice = g.Sum(p => p.Price),
                          TotalQTY = g.Sum(p => p.QTY)
                      };

            return res.ToList<object>();
        }

        public static List<object> Sum_Products_based_on_filter_args(IRaptorDB rap, string filter, params object[] args)
        {
            if (args != null)
            {
                // get args here
            }
            var q = rap.Query<SalesItemRowsViewRowSchema>(filter);

            var res = from x in q.Rows
                      group x by x.Product into g
                      select new sumtype // avoid anonymous types
                      {
                          Product = g.Key,
                          TotalPrice = g.Sum(p => p.Price),
                          TotalQTY = g.Sum(p => p.QTY)
                      };

            return res.ToList<object>();
        }
    }
}
