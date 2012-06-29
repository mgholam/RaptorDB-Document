using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;
using System.Collections;
using System.Linq.Expressions;
using System.Threading.Tasks;
using System.Reflection;
using System.Reflection.Emit;
using Microsoft.CSharp;
using System.CodeDom.Compiler;
using System.ComponentModel;
using RaptorDB.Common;
using System.Threading;

namespace RaptorDB.Views
{
    internal class ViewRowDefinition
    {
        public ViewRowDefinition()
        {
            Columns = new List<KeyValuePair<string, Type>>();
        }
        public string Name { get; set; }
        internal List<KeyValuePair<string, Type>> Columns { get; set; }

        public void Add(string name, Type type)
        {
            Columns.Add(new KeyValuePair<string, Type>(name, type));
        }
    }

    // FEATURE : background save indexes to disk on timer
    internal class ViewHandler
    {
        private ILog _log = LogManager.GetLogger(typeof(ViewHandler));

        public ViewHandler(string path, ViewManager manager)
        {
            _Path = path;
            _viewmanager = manager;
        }

        private string _Path = "";
        private ViewManager _viewmanager;
        internal ViewBase _view;
        private SafeDictionary<string, IIndex> _indexes = new SafeDictionary<string, IIndex>();
        private StorageFile<Guid> _viewData;
        private BoolIndex _deletedRows;
        private string _docid = "docid";
        private List<string> _colnames = new List<string>();
        private IRowFiller _rowfiller;
        private ViewRowDefinition _schema;
        private SafeDictionary<int, Dictionary<Guid, List<object[]>>> _transactions = new SafeDictionary<int, Dictionary<Guid, List<object[]>>>();

        internal void SetView<T>(View<T> view, KeyStoreGuid docs)
        {
            bool rebuild = false;
            _view = view;
            // generate schemacolumns from schema
            GenerateSchemaColumns(_view);

            if (_Path.EndsWith("\\") == false) _Path += "\\";
            _Path += view.Name + "\\";
            if (Directory.Exists(_Path) == false)
            {
                Directory.CreateDirectory(_Path);
                rebuild = true;
            }
            else
            {
                // read version file and check with view
                int version = 0;
                if (File.Exists(_Path + "version_.dat"))
                {
                    version = Helper.ToInt32(File.ReadAllBytes(_Path + "version_.dat"), 0);
                    if (version < view.Version)
                    {
                        _log.Debug("Newer view version detected");
                        _log.Debug("Deleting view = " + view.Name);
                        Directory.Delete(_Path, true);
                        Directory.CreateDirectory(_Path);
                        rebuild = true;
                    }
                }
            }

            // load indexes here
            CreateLoadIndexes(_schema);

            LoadDeletedRowsBitmap();

            _viewData = new StorageFile<Guid>(_Path + view.Name + ".mgdat");
            _viewData.SkipDateTime = true;

            CreateResultRowFiller();

            if (rebuild)
                RebuildFromScratch(docs);
        }

        internal void FreeMemory()
        {
            foreach (var i in _indexes)
                i.Value.FreeMemory();

            _deletedRows.FreeMemory();
        }

        internal void Commit(int ID)
        {
            Dictionary<Guid, List<object[]>> rows = null;
            // save data to indexes
            if (_transactions.TryGetValue(ID, out rows))
                SaveAndIndex(rows);

            // remove in memory data
            _transactions.Remove(ID);
        }

        internal void RollBack(int ID)
        { 
            // remove in memory data
            _transactions.Remove(ID);
        }

        internal void Insert<T>(Guid guid, T doc)
        {
            apimapper api = new apimapper(_viewmanager);
            View<T> view = (View<T>)_view;

            if (view.Mapper != null)
                view.Mapper(api, guid, doc);

            // map objects to rows
            foreach (var d in api.emitobj)
                api.emit.Add(d.Key, ExtractRows(d.Value));

            SaveAndIndex(api.emit);
        }

        private void SaveAndIndex(Dictionary<Guid, List<object[]>> rows)
        {
            foreach (var d in rows)
            {
                // delete any items with docid in view
                if (_view.DeleteBeforeInsert)
                    DeleteRowsWith(d.Key);
                // insert new items into view
                InsertRowsWithIndexUpdate(d.Key, d.Value);
            }
        }

        internal bool InsertTransaction<T>(Guid docid, T doc)
        {
            apimapper api = new apimapper(_viewmanager);
            View<T> view = (View<T>)_view;

            if (view.Mapper != null)
                view.Mapper(api, docid, doc);

            if (api._RollBack == true)
                return false;

            // map emitobj -> rows
            foreach (var d in api.emitobj)
                api.emit.Add(d.Key, ExtractRows(d.Value));

            Dictionary<Guid, List<object[]>> rows = new Dictionary<Guid,List<object[]>>();
            if (_transactions.TryGetValue(Thread.CurrentThread.ManagedThreadId, out rows))
            {
                // FIX : exists -> merge data
            }
            else
            {
                _transactions.Add(Thread.CurrentThread.ManagedThreadId, api.emit);
            }

            return true;
        }

        SafeDictionary<string, LambdaExpression> _lambdacache = new SafeDictionary<string, LambdaExpression>();
        internal Result Query(string filter)
        {
            DateTime dt = FastDateTime.Now;
            _log.Debug("query : " + _view.Name);
            _log.Debug("query : " + filter);
            // FEATURE : add query caching here
            WAHBitArray ba = new WAHBitArray();

            LambdaExpression le = null;
            if (_lambdacache.TryGetValue(filter, out le) == false)
            {
                le = System.Linq.Dynamic.DynamicExpression.ParseLambda(_view.Schema, typeof(bool), filter, null);
                _lambdacache.Add(filter, le);
            }
            QueryVisitor qv = new QueryVisitor(QueryColumnExpression);
            qv.Visit(le.Body);
            ba = ((WAHBitArray)qv._bitmap.Pop()).AndNot(_deletedRows.GetBits());

            _log.Debug("query bitmap done (ms) : " + FastDateTime.Now.Subtract(dt).TotalMilliseconds);
            dt = FastDateTime.Now;
            // exec query return rows
            return ReturnRows(ba);
        }

        internal Result Query<T>(Expression<Predicate<T>> filter)
        {
            DateTime dt = FastDateTime.Now;
            _log.Debug("query : " + _view.Name);
            // FEATURE : add query caching here
            WAHBitArray ba = new WAHBitArray();

            QueryVisitor qv = new QueryVisitor(QueryColumnExpression);
            qv.Visit(filter);
            ba = ((WAHBitArray)qv._bitmap.Pop()).AndNot(_deletedRows.GetBits());
            if (_viewmanager.inTransaction())
            {
                // FIX : query from transaction own data

                //var rows = null;
                //if (_transactions.TryGetValue(Thread.CurrentThread.ManagedThreadId, out rows))
                //{
                //    var r = rows.Cast<T>().ToList().FindAll(filter.Compile());
                //    if (r.Count > 0)
                //    {

                //    }
                //}

            }

            _log.Debug("query bitmap done (ms) : " + FastDateTime.Now.Subtract(dt).TotalMilliseconds);
            dt = FastDateTime.Now;
            // exec query return rows
            return ReturnRows(ba);
        }

        internal Result Query()
        {
            // no filter query -> just show all the data
            DateTime dt = FastDateTime.Now;
            _log.Debug("query : " + _view.Name);
            int count = _viewData.Count();
            List<object> rows = new List<object>();
            Result ret = new Result();

            WAHBitArray del = _deletedRows.GetBits();
            for (int i = 0; i < count; i++)
            {
                if (del.Get(i) == true)
                    continue;
                byte[] b = _viewData.ReadData(i);
                if (b == null) continue;
                object o = FastCreateObject(_view.Schema);
                object[] data = ((ArrayList)fastBinaryJSON.BJSON.Instance.ToObject(b)).ToArray();
                rows.Add(_rowfiller.FillRow(o, data));
            }

            _log.Debug("query rows fetched (ms) : " + FastDateTime.Now.Subtract(dt).TotalMilliseconds);
            _log.Debug("query rows count : " + rows.Count);
            ret.OK = true;
            ret.Count = rows.Count;
            ret.TotalCount = rows.Count;
            ret.Rows = rows;
            return ret;
        }

        internal void Shutdown()
        {
            _log.Debug("Shutting down Viewhandler");
            // shutdown indexes
            foreach (var v in _indexes)
            {
                _log.Debug("Shutting down view index : " + v.Key);
                v.Value.Shutdown();
            }
            // save deletedbitmap
            _deletedRows.Shutdown();

            _viewData.Shutdown();

            // write view version
            File.WriteAllBytes(_Path + "version_.dat", Helper.GetBytes(_view.Version, false));
        }

        internal void Delete(Guid docid)
        {
            DeleteRowsWith(docid);
        }
        
        #region [  private methods  ]

        private void CreateResultRowFiller()
        {
            // create a row filler class
            string str = @"using System;
public class rf : RaptorDB.IRowFiller
{
    public object FillRow(object roww, object[] data)
    {
        {0} row = ({0}) roww;
        {1}
        return row;
    }
}";
            string src = str.Replace("{0}", _view.Schema.FullName.Replace("+", ".")).Replace("{1}", GenerateRowString());
            CSharpCodeProvider provider = new CSharpCodeProvider();
            var param = new CompilerParameters();
            param.GenerateInMemory = true;
            // FEATURE : load all required assemblies based on the view schema if required
            param.ReferencedAssemblies.Add(this.GetType().Assembly.Location);
            param.ReferencedAssemblies.Add(typeof(RDBSchema).Assembly.Location);
            param.ReferencedAssemblies.Add(_view.GetType().Assembly.Location);
            param.ReferencedAssemblies.Add(_view.Schema.Assembly.Location);
            param.ReferencedAssemblies.Add(typeof(ICustomTypeDescriptor).Assembly.Location);
            CompilerResults results = provider.CompileAssemblyFromSource(param, src);

            if (results.Errors.Count > 0)
                throw new FormatException(results.Errors[0].ErrorText);

            _rowfiller = (IRowFiller)results.CompiledAssembly.CreateInstance("rf");

            // init the row create 
            _createrow = null;
            FastCreateObject(_view.Schema);
        }

        private string GenerateRowString()
        {
            StringBuilder sb = new StringBuilder();
            int i = 0;
            sb.AppendLine("row.docid = (Guid)data[0];");
            foreach (var c in _schema.Columns)
            {
                if (c.Key == "docid")
                    continue;
                i++;
                sb.Append("row.");
                sb.Append(c.Key);
                sb.Append(" = (");
                sb.Append(c.Value.Name.Replace("FullTextString", "String"));
                sb.Append(")data[");
                sb.Append(i.ToString());
                sb.AppendLine("];");
            }
            return sb.ToString();
        }

        private Result ReturnRows(WAHBitArray ba)
        {
            DateTime dt = FastDateTime.Now;
            List<object> rows = new List<object>();
            Result ret = new Result();
            foreach (int i in ba.GetBitIndexes())
            {
                byte[] b = _viewData.ReadData(i);
                if (b == null) continue;
                object o = FastCreateObject(_view.Schema);
                object[] data = ((ArrayList)fastBinaryJSON.BJSON.Instance.ToObject(b)).ToArray();
                rows.Add(_rowfiller.FillRow(o, data));
            }
            _log.Debug("query rows fetched (ms) : " + FastDateTime.Now.Subtract(dt).TotalMilliseconds);
            _log.Debug("query rows count : " + rows.Count);
            ret.OK = true;
            ret.Count = rows.Count;
            ret.TotalCount = rows.Count;
            ret.Rows = rows;
            return ret;
        }

        private CreateRow _createrow = null;
        private delegate object CreateRow();
        private object FastCreateObject(Type objtype)
        {
            try
            {
                if (_createrow != null)
                    return _createrow();
                else
                {
                    DynamicMethod dynMethod = new DynamicMethod("_", objtype, null);
                    ILGenerator ilGen = dynMethod.GetILGenerator();

                    ilGen.Emit(OpCodes.Newobj, objtype.GetConstructor(Type.EmptyTypes));
                    ilGen.Emit(OpCodes.Ret);
                    _createrow = (CreateRow)dynMethod.CreateDelegate(typeof(CreateRow));
                    return _createrow();
                }
            }
            catch (Exception exc)
            {
                throw new Exception(string.Format("Failed to fast create instance for type '{0}' from assemebly '{1}'",
                    objtype.FullName, objtype.AssemblyQualifiedName), exc);
            }
        }

        MethodInfo view = null;
        private void RebuildFromScratch(KeyStoreGuid docs)
        {
            view = this.GetType().GetMethod("Insert", BindingFlags.Instance | BindingFlags.Public);
            _log.Debug("Rebuilding view from scratch...");
            _log.Debug("View = " + _view.Name);
            DateTime dt = FastDateTime.Now;

            int c = docs.RecordCount();
            for (int i = 0; i < c; i++)
            {
                Guid docid = Guid.Empty;
                bool isdeleted = false;
                byte[] b = docs.Get(i, out docid, out isdeleted);
                if (isdeleted)
                    Delete(docid);
                else
                {
                    if (b != null)
                    {
                        // FEATURE : optimize this by not creating the object if not in FireOnTypes
                        object obj = CreateObject(b);
                        Type t = obj.GetType();
                        if (_view.FireOnTypes.Contains(t.AssemblyQualifiedName))
                        {
                            var m = view.MakeGenericMethod(new Type[] { obj.GetType() });
                            m.Invoke(this, new object[] { docid, obj });
                        }
                    }
                    else
                        _log.Error("Doc is null : " + docid);
                }
            }
            _log.Debug("rebuild done (s) = " + FastDateTime.Now.Subtract(dt).TotalSeconds);
        }

        private object CreateObject(byte[] b)
        {
            if (b[0] < 32)
                return fastBinaryJSON.BJSON.Instance.ToObject(b);
            else
                return fastJSON.JSON.Instance.ToObject(Encoding.ASCII.GetString(b));
        }

        private void CreateLoadIndexes(ViewRowDefinition viewRowDefinition)
        {
            int i = 0;
            _indexes.Add(_docid, new TypeIndexes<Guid>(_Path, _docid, 16));
            // load indexes
            foreach (var c in viewRowDefinition.Columns)
            {
                if (c.Key == "docid")
                    continue;
                _indexes.Add(_schema.Columns[i].Key,
                          CreateIndex(
                            _schema.Columns[i].Key,
                            _schema.Columns[i].Value));
                i++;
            }
        }

        private void GenerateSchemaColumns(ViewBase _view)
        {
            // generate schema columns from schema
            _schema = new ViewRowDefinition();
            _schema.Name = _view.Name;

            foreach (var p in _view.Schema.GetProperties())
            {
                Type t = p.PropertyType;
                if (p.GetCustomAttributes(typeof(FullTextAttribute), true).Length > 0)
                    t = typeof(FullTextString);
                _schema.Add(p.Name, t);
            }

            foreach (var f in _view.Schema.GetFields())
            {
                Type t = f.FieldType;
                if (f.GetCustomAttributes(typeof(FullTextAttribute), true).Length > 0)
                    t = typeof(FullTextString);
                _schema.Add(f.Name, t);
            }

            foreach (var s in _schema.Columns)
                _colnames.Add(s.Key);
        }

        private void LoadDeletedRowsBitmap()
        {
            _deletedRows = new BoolIndex(_Path, "deleted_.idx");
        }

        private void InsertRowsWithIndexUpdate(Guid guid, List<object[]> rows)
        {
            foreach (var row in rows)
            {
                object[] r = new object[row.Length + 1];
                r[0] = guid;
                Array.Copy(row, 0, r, 1, row.Length);
                byte[] b = fastBinaryJSON.BJSON.Instance.ToBJSON(r);

                int rownum = _viewData.WriteData(guid, b, false);

                IndexRow(guid, row, rownum);
            }
        }

        private List<object[]> ExtractRows(List<object> rows)
        {
            List<object[]> output = new List<object[]>();
            // reflection match object properties to the schema row

            int colcount = _schema.Columns.Count ;

            foreach (var obj in rows)
            {
                object[] r = new object[colcount];
                int i = 0;
                List<fastJSON.Getters> getters = fastBinaryJSON.BJSON.Instance.GetGetters(obj.GetType());
                foreach (var c in _schema.Columns)
                {
                    foreach (var g in getters)
                    {
                        if (g.Name == c.Key)
                        {
                            r[i] = g.Getter(obj);
                            break;
                        }
                    }
                    i++;
                }
                output.Add(r);
            }

            return output;
        }

        private void IndexRow(Guid docid, object[] row, int rownum)
        {
            int i = 0;
            _indexes[_docid].Set(docid, rownum);
            // index the row
            foreach (var d in row)
                _indexes[_colnames[i++]].Set(d, rownum);
        }

        private IIndex CreateIndex(string name, Type type)
        {
            if (type == typeof(FullTextString))
                return new FullTextIndex(_Path, name);

            else if (type == typeof(string))
                return new TypeIndexes<string>(_Path, name, Global.DefaultStringKeySize);

            else if (type == typeof(bool))
                return new BoolIndex(_Path, name);

            else
                return (IIndex)Activator.CreateInstance(
                    typeof(TypeIndexes<>).MakeGenericType(type),
                    new object[] { _Path, name, Global.DefaultStringKeySize });
        }

        private void DeleteRowsWith(Guid guid)
        {
            // find bitmap for guid column
            WAHBitArray gc = QueryColumnExpression(_docid, RDBExpression.Equal, guid);
            _deletedRows.InPlaceOR(gc);
        }

        private WAHBitArray QueryColumnExpression(string colname, RDBExpression exp, object from)
        {
            return _indexes[colname].Query(exp, from);
        }
        #endregion
    }
}
