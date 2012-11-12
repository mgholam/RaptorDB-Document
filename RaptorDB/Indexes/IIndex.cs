using System;
using System.Collections.Generic;
using System.Text;

namespace RaptorDB
{
    internal enum RDBExpression
    {
        Equal,
        Greater,
        GreaterEqual,
        Less,
        LessEqual,
        NotEqual,
        Between,
        Contains
    }

    internal interface IIndex
    {
        void Set(object key, int recnum);
        WAHBitArray Query(object fromkey, object tokey);
        WAHBitArray Query(RDBExpression ex, object from);
        void FreeMemory();
        void Shutdown();
        void SaveIndex();
    }
}
