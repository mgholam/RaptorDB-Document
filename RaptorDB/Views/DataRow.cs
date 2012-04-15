using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace RaptorDB.Views
{
    public struct Column
    {
        public string Name { get; set; }
        public Type DataType { get; set; }
    }

    public class DataRow
    {
        public DataRow()
        {
            Columns = new List<Column>();
            RowData = new object[0];
        }

        public object[] RowData { get; set; }
        public List<Column> Columns { get; set; }

        public object this[int index]
        {
            get { return RowData[index]; }
            set { RowData[index] = value; }
        }

        public object this[string name]
        {
            get { return RowData[columnindex(name)]; }
            set { RowData[columnindex(name)] = value; }
        }

        private int columnindex(string name)
        {
            int i = -1;
            i = Columns.FindIndex(delegate(Column c) { return c.Name.ToLower() == name.ToLower(); });
            return i;
        }
    }
}
