using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Linq.Expressions;

namespace RaptorDB.Common
{
    public interface IRaptorDB
    {
        bool SaveBytes(Guid docID, byte[] bytes);
        bool Save<T>(Guid docID, T document);
        Result Query(string viewname);
        Result Query(Type view);
        Result Query(string viewname, string filter);
        Result Query<T>(string viewname, Expression<Predicate<T>> filter);
        Result Query<T>(Type type, Expression<Predicate<T>> filter);
        object Fetch(Guid docID);
        byte[] FetchBytes(Guid fileID);
        void Shutdown();

        bool Backup();
        bool Delete(Guid docid);
        bool DeleteBytes(Guid fileid);
    }
}
