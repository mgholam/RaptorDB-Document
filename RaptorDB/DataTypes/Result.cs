using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace RaptorDB
{
    /// <summary>
    /// Result of queries
    ///    OK : T = Query with data,  F = EX has the exception
    ///    Rows : query rows
    /// </summary>
    public class Result
    {
        public Result()
        {

        }
        public Result(bool ok)
        {
            OK = ok;
        }
        public Result(bool ok,Exception ex)
        {
            OK = ok;
            EX = ex;
        }
        public bool OK { get; set; }
        public Exception EX { get; set; }
        public int TotalCount { get; set; }
        public int Count { get; set; }
        public List<object[]> Rows { get; set; }

        // FIX : add schema for rows
    }
}
