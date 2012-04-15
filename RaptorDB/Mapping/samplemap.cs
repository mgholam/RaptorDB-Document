using System;
using System.Collections.Generic;
using System.Text;

using RaptorDB.Mapping;

namespace mapnamespace
{

    #region [  VIEW ROWS  ]
    public class ViewRow
    {
        public ViewRow(object[] values)
        {
            d = values;
        }
        private object[] d;

        public string Name { get { return (string)d[1]; } set { d[1] = value; } }
        public string Address { get { return (string)d[2]; } set { d[2] = value; } }
        public int Code { get { return (int)d[3]; } set { d[3] = value; } }
    }
    #endregion

    public class mapfunction : IMAPFunction
    {
        private List<object[]> Rows = new List<object[]>();

        public List<object[]> GetRows()
        {
            return Rows;
        }

        public void CallMapper(object data, IMapAPI api)
        {
            map((RaptorDB.Views.View)data, api);
        }

        private void emit(Guid guid, string Name, string Address, int Code)
        {
            object[] data = new object[3 + 1];
            data[0] = guid;
            data[1] = Name;
            data[2] = Address;
            data[3] = Code;

            Rows.Add(data);
        }

        private void map(RaptorDB.Views.View data, IMapAPI api)
        {
        #region [  USER CODE  ]   

        #endregion
        }

    }


}


