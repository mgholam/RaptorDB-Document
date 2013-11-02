using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Xml.Serialization;

namespace RaptorDB
{
    public abstract class ViewBase
    {
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
        public Type Schema { get; set; }

        /// <summary>
        /// A list of Types that this view responds to (inheiratance is supported)
        /// Use AddFireOnTypes() to add to this list
        /// </summary>
        public List<Type> FireOnTypes { get; set; }
        
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
        /// Fire the mapper on these types
        /// </summary>
        /// <param name="type"></param>
        public void AddFireOnTypes(Type type)
        {
            FireOnTypes.Add(type);
        }

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
    }


    public class View<T> : ViewBase
    {
        public delegate void MapFunctionDelgate<V>(IMapAPI api, Guid docid, V doc);
        public View()
        {
            isActive = true;
            FireOnTypes = new List<Type>();
            DeleteBeforeInsert = true;
            BackgroundIndexing = true;
            FullTextColumns = new List<string>();
            CaseInsensitiveColumns = new List<string>();
            //AllowTransactions = false;
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
            if (FireOnTypes.Count == 0) 
                throw new Exception("No types have been defined to fire on");
            if (TransactionMode == true && isPrimaryList == false)
                throw new Exception("Transaction mode can only be enabled on Primary Views");
            // FEATURE : add more verifications
            return new Result<object>(true);
        }
    }
}
