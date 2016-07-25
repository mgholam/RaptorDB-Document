using System;
using System.Collections.Generic;
using System.Xml.Serialization;

namespace RaptorDB
{
    public abstract class ViewBase
    {
        public delegate void MapFunctionDelgate<V>(IMapAPI api, Guid docid, V doc);
        /// <summary>
        /// Increment this when you change view definitions so the engine can rebuild the contents
        /// </summary>
        public int Version { get; set; }
        
        /// <summary>
        /// Name of the view will be used for foldernames and filename and generated code
        /// </summary>
        public string Name { get; set;}
        
        /// <summary>
        /// A text for describing this views purpose for other developers 
        /// </summary>
        public string Description { get; set; }
        
        /// <summary>
        /// Column definitions for the view storage 
        /// </summary>
        [XmlIgnore]
        public Type Schema { get; set; }
        
        /// <summary>
        /// Is this the primary list and will be populated synchronously
        /// </summary>
        public bool isPrimaryList { get; set; }
        
        /// <summary>
        /// Is this view active and will recieve data
        /// </summary>
        public bool isActive { get; set; }
        
        /// <summary>
        /// Delete items on DocID before inserting new rows (default = true)
        /// </summary>
        public bool DeleteBeforeInsert { get; set; }

        /// <summary>
        /// Index in the background : better performance but reads might not have all the data
        /// </summary>
        public bool BackgroundIndexing { get; set; }

        /// <summary>
        /// Save documents to this view in the save process, like primary views
        /// </summary>
        public bool ConsistentSaveToThisView { get; set; }

        /// <summary>
        /// Apply to a Primary View and all the mappings of all views will be done in a transaction.
        /// You can use Rollback for failures.
        /// </summary>
        public bool TransactionMode { get; set; }

        /// <summary>
        /// When defining your own schema and you don't want dependancies to RaptorDB to propogate through your code
        /// define your full text columns here
        /// </summary>
        public List<string> FullTextColumns;
        
        /// <summary>
        /// When defining your own schems and you don't want dependancies to RaptorDB to propogate through your code 
        /// define your case insensitive columns here
        /// </summary>
        public List<string> CaseInsensitiveColumns;

        public Dictionary<string, byte> StringIndexLength;

        /// <summary>
        /// Columns that you don't want to index
        /// </summary>
        public List<string> NoIndexingColumns;
    }


    public class View<T> : ViewBase
    {
        public View()
        {
            isActive = true;
            DeleteBeforeInsert = true;
            BackgroundIndexing = true;
            FullTextColumns = new List<string>();
            CaseInsensitiveColumns = new List<string>();
            StringIndexLength = new Dictionary<string, byte>();
            NoIndexingColumns = new List<string>();
            isPrimaryList = true;
            ConsistentSaveToThisView = true;
        }

        /// <summary>
        /// Inline delegate for the mapper function used for quick applications 
        /// </summary>
        [XmlIgnore]
        public MapFunctionDelgate<T> Mapper { get; set; }

        public Result<object> Verify()
        {
            if (Name == null || Name == "") 
                throw new Exception("Name must be given");
            if (Schema == null) 
                throw new Exception("Schema must be defined");
            if (Schema.IsSubclassOf(typeof(RDBSchema)) == false)
            {
                var pi = Schema.GetProperty("docid");
                if (pi == null || pi.PropertyType != typeof(Guid))
                {
                    var fi = Schema.GetField("docid");
                    if( fi == null || fi.FieldType != typeof(Guid))
                        throw new Exception("The schema must be derived from RaptorDB.RDBSchema or must contain a 'docid' Guid field or property");
                }
            }
            if (Mapper == null) 
                throw new Exception("A map function must be defined");

            if (TransactionMode == true && isPrimaryList == false)
                throw new Exception("Transaction mode can only be enabled on Primary Views");
           
            // FEATURE : add more verifications
            return new Result<object>(true);
        }
    }
}
