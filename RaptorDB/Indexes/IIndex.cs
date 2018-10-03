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
        MGRB Query(object fromkey, object tokey, int maxsize);
        MGRB Query(RDBExpression ex, object from , int maxsize);
        void FreeMemory();
        void Shutdown();
        void SaveIndex();
        object[] GetKeys();
    }
}
