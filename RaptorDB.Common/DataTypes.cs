using System;
using System.Collections.Generic;

namespace RaptorDB
{
    /// <summary>
    /// Result of queries
    ///    OK : T = Query with data,  F = EX has the exception
    ///    Rows : query rows
    /// </summary>
    public class Result<T>
    {
        public Result()
        {

        }
        public Result(bool ok)
        {
            OK = ok;
        }
        public Result(bool ok, Exception ex)
        {
            OK = ok;
            EX = ex;
        }
        /// <summary>
        /// T=Values return, F=exceptions occurred 
        /// </summary>
        public bool OK { get; set; }
        public Exception EX { get; set; }
        /// <summary>
        /// Total number of rows of the query
        /// </summary>
        public int TotalCount { get; set; }
        /// <summary>
        /// Rows returned
        /// </summary>
        public int Count { get; set; }
        public List<T> Rows { get; set; }

        public string Title { get; set; }
        // FEATURE : data pending in results
        ///// <summary>
        ///// Data is being indexed, so results will not reflect all documents
        ///// </summary>
        //public bool DataPending { get; set; }
    }

    /// <summary>
    /// Base for row schemas : implements a docid property and is bindable
    /// </summary>
    public abstract class RDBSchema : BindableFields
    {
        public Guid docid;
    }
}
