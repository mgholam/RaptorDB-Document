using System;
using System.Collections.Generic;
using System.Text;
using System.IO;
using System.Linq.Expressions;
using System.Threading.Tasks;
using System.Reflection;
using System.Reflection.Emit;
using RaptorDB.Common;
using System.Threading;
using fastJSON;
using fastBinaryJSON;

namespace RaptorDB.Views
{
    public class ViewRowDefinition
    {
        public ViewRowDefinition()
        {
            Columns = new List<KeyValuePair<string, Type>>();
        }
        public string Name { get; set; }
        public List<KeyValuePair<string, Type>> Columns { get; set; }

        public void Add(string name, Type type)
        {
            Columns.Add(new KeyValuePair<string, Type>(name, type));
        }
    }

    internal class tran_data
    {
        public Guid docid;
        public Dictionary<Guid, List<object[]>> rows;
    }

    internal class ViewHandler
    {
        private ILog _log = LogManager.GetLogger(typeof(ViewHandler));

        public ViewHandler(string path, ViewManager manager)
        {
            _Path = path;
            _viewmanager = manager;
        }

        private string _S = Path.DirectorySeparatorChar.ToString();
        private string _Path = "";
        private ViewManager _viewmanager;
        internal ViewBase _view;
        private Dictionary<string, IIndex> _indexes = new Dictionary<string, IIndex>();
        private StorageFile<Guid> _viewData;
        private BoolIndex _deletedRows;
        private string _docid = "docid";
        private List<string> _colnames = new List<string>();
        private RowFill _rowfiller;
        private ViewRowDefinition _schema;
        private SafeDictionary<int, tran_data> _transactions = new SafeDictionary<int, tran_data>();
        private SafeDictionary<string, int> _nocase = new SafeDictionary<string, int>();
        private Dictionary<string, byte> _idxlen = new Dictionary<string, byte>();

        private System.Timers.Timer _saveTimer;
        Type basetype; // used for mapper
        dynamic mapper;
        bool _isDirty = false;
        private string _dirtyFilename = "temp.$";
        private bool _stsaving = false;
        private int _RaptorDBVersion = 3; // used for engine changes to views
        private string _RaptorDBVersionFilename = "RaptorDB.version";

        private object _stlock = new object();
        void _saveTimer_Elapsed(object sender, System.Timers.ElapsedEventArgs e)
        {
            lock (_stlock)
            {
                _stsaving = true;
                foreach (var i in _indexes)
                    i.Value.SaveIndex();

                _deletedRows.SaveIndex();
                _stsaving = false;
            }
        }

        public Type GetFireOnType()
        {
            return basetype;
        }

        internal void SetView<T>(View<T> view, IDocStorage<Guid> docs)
        {
            bool rebuild = false;
            _view = view;
            // generate schemacolumns from schema
            GenerateSchemaColumns(_view);

            if (_Path.EndsWith(_S) == false) _Path += _S;
            _Path += view.Name + _S;
            if (Directory.Exists(_Path) == false)
            {
                Directory.CreateDirectory(_Path);
                rebuild = true;
            }
            else
            {
                // read version file and check with view
                int version = 0;
                if (File.Exists(_Path + _view.Name + ".version"))
                {
                    int.TryParse(File.ReadAllText(_Path + _view.Name + ".version"), out version);
                    if (version != view.Version)
                    {
                        _log.Debug("Newer view version detected");
                        rebuild = true;
                    }
                }
            }

            if (File.Exists(_Path + _dirtyFilename))
            {
                _log.Debug("Last shutdown failed, rebuilding view : " + _view.Name);
                rebuild = true;
            }

            if (File.Exists(_Path + _RaptorDBVersionFilename))
            {
                // check view engine version
                string s = File.ReadAllText(_Path + _RaptorDBVersionFilename);
                int version = 0;
                int.TryParse(s, out version);
                if (version != _RaptorDBVersion)
                {
                    _log.Debug("RaptorDB view engine upgrade, rebuilding view : " + _view.Name);
                    rebuild = true;
                }
            }
            else
            {
                _log.Debug("RaptorDB view engine upgrade, rebuilding view : " + _view.Name);
                rebuild = true;
            }

            if (rebuild)
            {
                _log.Debug("Deleting old view data folder = " + view.Name);
                Directory.Delete(_Path, true);
                Directory.CreateDirectory(_Path);
            }
            // load indexes here
            CreateLoadIndexes(_schema);

            _deletedRows = new BoolIndex(_Path, _view.Name, ".deleted");

            _viewData = new StorageFile<Guid>(_Path + view.Name + ".mgdat");

            CreateResultRowFiller();

            mapper = view.Mapper;
            // looking for the T in View<T>
            if (view.GetType().GetGenericArguments().Length == 1) // HACK : kludge change when possible 
                basetype = view.GetType().GetGenericArguments()[0];
            else
            {
                // or recurse until found
                basetype = view.GetType().BaseType.GetGenericArguments()[0];
            }

            if (rebuild)
                Task.Factory.StartNew(() => RebuildFromScratch(docs));

            _saveTimer = new System.Timers.Timer();
            _saveTimer.AutoReset = true;
            _saveTimer.Elapsed += new System.Timers.ElapsedEventHandler(_saveTimer_Elapsed);
            _saveTimer.Interval = Global.SaveIndexToDiskTimerSeconds * 1000;
            _saveTimer.Start();
        }

        internal void FreeMemory()
        {
            _log.Debug("free memory : " + _view.Name);
            foreach (var i in _indexes)
                i.Value.FreeMemory();

            _deletedRows.FreeMemory();
            InvalidateSortCache();
        }

        internal void Commit(int ID)
        {
            tran_data data = null;
            // save data to indexes
            if (_transactions.TryGetValue(ID, out data))
            {
                // delete any items with docid in view
                if (_view.DeleteBeforeInsert)
                    DeleteRowsWith(data.docid);
                SaveAndIndex(data.rows);
            }
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
            apimapper api = new apimapper(_viewmanager, this);

            if (basetype == doc.GetType())
            {
                View<T> view = _view as View<T>;
                if (view.Mapper != null)
                    view.Mapper(api, guid, doc);
            }
            else if (mapper != null)
                mapper(api, guid, doc);

            // map objects to rows
            foreach (var d in api.emitobj)
                api.emit.Add(d.Key, ExtractRows(d.Value));

            // delete any items with docid in view
            if (_view.DeleteBeforeInsert)
                DeleteRowsWith(guid);

            SaveAndIndex(api.emit);
        }

        private void SaveAndIndex(Dictionary<Guid, List<object[]>> rows)
        {
            foreach (var d in rows)
            {
                // insert new items into view
                InsertRowsWithIndexUpdate(d.Key, d.Value);
            }
            InvalidateSortCache();
        }

        internal bool InsertTransaction<T>(Guid docid, T doc)
        {
            apimapper api = new apimapper(_viewmanager, this);
            if (basetype == doc.GetType())
            {
                View<T> view = (View<T>)_view;

                try
                {
                    if (view.Mapper != null)
                        view.Mapper(api, docid, doc);
                }
                catch (Exception ex)
                {
                    _log.Error(ex);
                    return false;
                }
            }
            else if (mapper != null)
                mapper(api, docid, doc);

            if (api._RollBack == true)
                return false;

            // map emitobj -> rows
            foreach (var d in api.emitobj)
                api.emit.Add(d.Key, ExtractRows(d.Value));

            //Dictionary<Guid, List<object[]>> rows = new Dictionary<Guid, List<object[]>>();
            tran_data data = new tran_data();
            if (_transactions.TryGetValue(Thread.CurrentThread.ManagedThreadId, out data))
            {
                // TODO : exists -> merge data??
            }
            else
            {
                data = new tran_data();
                data.docid = docid;
                data.rows = api.emit;
                _transactions.Add(Thread.CurrentThread.ManagedThreadId, data);
            }

            return true;
        }

        // FEATURE : add query caching here
        SafeDictionary<string, LambdaExpression> _lambdacache = new SafeDictionary<string, LambdaExpression>();
        internal Result<object> Query(string filter, int start, int count)
        {
            return Query(filter, start, count, "");
        }

        internal Result<object> Query(string filter, int start, int count, string orderby)
        {
            filter = filter.Trim();
            if (filter == "")
                return Query(start, count, orderby);

            DateTime dt = FastDateTime.Now;
            _log.Debug("query : " + _view.Name);
            _log.Debug("query : " + filter);
            _log.Debug("orderby : " + orderby);

            WAHBitArray ba = new WAHBitArray();
            var delbits = _deletedRows.GetBits();
            if (filter != "")
            {
                LambdaExpression le = null;
                if (_lambdacache.TryGetValue(filter, out le) == false)
                {
                    le = System.Linq.Dynamic.DynamicExpression.ParseLambda(_view.Schema, typeof(bool), filter, null);
                    _lambdacache.Add(filter, le);
                }
                QueryVisitor qv = new QueryVisitor(QueryColumnExpression);
                qv.Visit(le.Body);

                ba = ((WAHBitArray)qv._bitmap.Pop()).AndNot(delbits);
            }
            else
                ba = WAHBitArray.Fill(_viewData.Count()).AndNot(delbits);

            var order = SortBy(orderby);
            bool desc = false;
            if (orderby.ToLower().Contains(" desc"))
                desc = true;
            _log.Debug("query bitmap done (ms) : " + FastDateTime.Now.Subtract(dt).TotalMilliseconds);
            dt = FastDateTime.Now;
            // exec query return rows
            return ReturnRows<object>(ba, null, start, count, order, desc);
        }

        internal Result<object> Query<T>(Expression<Predicate<T>> filter, int start, int count)
        {
            return Query<T>(filter, start, count, "");
        }

        // FEATURE : add query caching here
        internal Result<object> Query<T>(Expression<Predicate<T>> filter, int start, int count, string orderby)
        {
            if (filter == null)
                return Query(start, count);

            DateTime dt = FastDateTime.Now;
            _log.Debug("query : " + _view.Name);

            WAHBitArray ba = new WAHBitArray();

            QueryVisitor qv = new QueryVisitor(QueryColumnExpression);
            qv.Visit(filter);
            var delbits = _deletedRows.GetBits();
            ba = ((WAHBitArray)qv._bitmap.Pop()).AndNot(delbits);
            List<T> trows = null;
            if (_viewmanager.inTransaction())
            {
                // query from transaction own data
                tran_data data = null;
                if (_transactions.TryGetValue(Thread.CurrentThread.ManagedThreadId, out data))
                {
                    List<T> rrows = new List<T>();
                    foreach (var kv in data.rows)
                    {
                        foreach (var r in kv.Value)
                        {
                            object o = FastCreateObject(_view.Schema);
                            rrows.Add((T)_rowfiller(o, r));
                        }
                    }
                    trows = rrows.FindAll(filter.Compile());
                }
            }

            var order = SortBy(orderby);
            bool desc = false;
            if (orderby.ToLower().Contains(" desc"))
                desc = true;
            _log.Debug("query bitmap done (ms) : " + FastDateTime.Now.Subtract(dt).TotalMilliseconds);
            dt = FastDateTime.Now;
            // exec query return rows
            return ReturnRows<T>(ba, trows, start, count, order, desc);
        }

        internal Result<object> Query(int start, int count)
        {
            return Query(start, count, "");
        }

        internal Result<object> Query(int start, int count, string orderby)
        {
            // no filter query -> just show all the data
            DateTime dt = FastDateTime.Now;
            _log.Debug("query : " + _view.Name);
            int totalviewrows = _viewData.Count();
            List<object> rows = new List<object>();
            Result<object> ret = new Result<object>();
            int skip = start;
            int cc = 0;
            WAHBitArray del = _deletedRows.GetBits();
            ret.TotalCount = totalviewrows - (int)del.CountOnes();

            var order = SortBy(orderby);
            bool desc = false;
            if (orderby.ToLower().Contains(" desc"))
                desc = true;
            if (order.Count == 0)
                for (int i = 0; i < totalviewrows; i++)
                    order.Add(i);

            if (count == -1)
                count = totalviewrows;
            int len = order.Count;
            if (desc == false)
            {
                for (int idx = 0; idx < len; idx++)
                {
                    extractrowobject(count, rows, ref skip, ref cc, del, order, idx);
                    if (cc == count) break;
                }
            }
            else
            {
                for (int idx = len - 1; idx >= 0; idx--)
                {
                    extractrowobject(count, rows, ref skip, ref cc, del, order, idx);
                    if (cc == count) break;
                }
            }

            _log.Debug("query rows fetched (ms) : " + FastDateTime.Now.Subtract(dt).TotalMilliseconds);
            _log.Debug("query rows count : " + rows.Count.ToString("#,0"));
            ret.OK = true;
            ret.Count = rows.Count;
            //ret.TotalCount = rows.Count;
            ret.Rows = rows;
            return ret;
        }

        private void extractrowobject(int count, List<object> rows, ref int skip, ref int cc, WAHBitArray del, List<int> order, int idx)
        {
            int i = order[idx];
            if (del.Get(i) == false)
            {
                if (skip > 0)
                    skip--;
                else
                {
                    bool b = OutputRow<object>(rows, i);
                    if (b && count > 0)
                        cc++;
                }
            }
        }

        internal void Shutdown()
        {
            try
            {
                lock (_stlock)
                    _saveTimer.Enabled = false;
                while (_stsaving)
                    Thread.Sleep(1);

                if (_rebuilding)
                    _log.Debug("Waiting for view rebuild to finish... : " + _view.Name);

                while (_rebuilding)
                    Thread.Sleep(50);

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
                File.WriteAllText(_Path + _view.Name + ".version", _view.Version.ToString());

                File.WriteAllText(_Path + _RaptorDBVersionFilename, _RaptorDBVersion.ToString());
                // remove dirty file
                if (File.Exists(_Path + _dirtyFilename))
                    File.Delete(_Path + _dirtyFilename);
                _log.Debug("Viewhandler shutdown done.");
            }
            catch (Exception ex)
            {
                _log.Error(ex);
            }
        }

        internal void Delete(Guid docid)
        {
            DeleteRowsWith(docid);

            InvalidateSortCache();
        }

        #region [  private methods  ]

        private void CreateResultRowFiller()
        {
            _rowfiller = CreateRowFillerDelegate(_view.Schema, _schema);
            // init the row create 
            _createrow = null;
            FastCreateObject(_view.Schema);
        }

        public delegate object RowFill(object o, object[] data);
        public static RowFill CreateRowFillerDelegate(Type objtype, ViewRowDefinition schema)
        {
            DynamicMethod dynMethod = new DynamicMethod("rowfill", typeof(object), new Type[] { typeof(object), typeof(object[]) });
            ILGenerator il = dynMethod.GetILGenerator();
            var row = il.DeclareLocal(objtype);
            il.Emit(OpCodes.Ldarg_0);
            il.Emit(OpCodes.Castclass, objtype);
            il.Emit(OpCodes.Stloc, row);
            int i = 1;
            var val = il.DeclareLocal(typeof(object));
            var fields = objtype.GetFields();
            var properties = objtype.GetProperties();
            foreach (var col in schema.Columns)
            {
                FieldInfo c = null;
                PropertyInfo p = null;
                if (isField(fields, col.Key, out c)) 
                {
                    var end = il.DefineLabel();

                    il.Emit(OpCodes.Ldarg_1);
                    if (col.Key != "docid")
                        il.Emit(OpCodes.Ldc_I4, i);
                    else
                        il.Emit(OpCodes.Ldc_I4, 0);

                    il.Emit(OpCodes.Ldelem_Ref);
                    // check if value is not null
                    il.Emit(OpCodes.Stloc, val);
                    il.Emit(OpCodes.Ldloc, val);
                    il.Emit(OpCodes.Brfalse_S, end);
                    il.Emit(OpCodes.Ldloc, row);
                    il.Emit(OpCodes.Ldloc, val);

                    il.Emit(OpCodes.Unbox_Any, c.FieldType);
                    il.Emit(OpCodes.Stfld, c);
                    il.MarkLabel(end);
                    i++;
                }
                else if (isProperty(properties, col.Key, out p))
                {
                    var end = il.DefineLabel();
                    MethodInfo setMethod = p.GetSetMethod();
                    il.Emit(OpCodes.Ldarg_1);
                    if (col.Key != "docid")
                        il.Emit(OpCodes.Ldc_I4, i);
                    else
                        il.Emit(OpCodes.Ldc_I4, 0);

                    il.Emit(OpCodes.Ldelem_Ref);
                    // check if value is not null
                    il.Emit(OpCodes.Stloc, val);
                    il.Emit(OpCodes.Ldloc, val);
                    il.Emit(OpCodes.Brfalse_S, end);
                    il.Emit(OpCodes.Ldloc, row);
                    il.Emit(OpCodes.Ldloc, val);

                    il.Emit(OpCodes.Unbox_Any, p.PropertyType);
                    if (!p.DeclaringType.IsValueType)
                        il.EmitCall(OpCodes.Callvirt, setMethod, null);
                    else
                        il.EmitCall(OpCodes.Call, setMethod, null);
                    il.MarkLabel(end);
                    i++;
                }
            }
            il.Emit(OpCodes.Ldloc, row);
            il.Emit(OpCodes.Ret);

            return (RowFill)dynMethod.CreateDelegate(typeof(RowFill));
        }

        private static bool isProperty(PropertyInfo[] properties, string key, out PropertyInfo p)
        {
            foreach (var i in properties)
                if (i.Name == key)
                {
                    p = i;
                    return true;
                }
            p = null;
            return false;
        }

        private static bool isField(FieldInfo[] fields, string key, out FieldInfo c)
        {
            foreach (var i in fields)
                if (i.Name == key)
                {
                    c = i;
                    return true;
                }
            c = null;
            return false;
        }

        private Result<object> ReturnRows<T>(WAHBitArray ba, List<T> trows, int start, int count, List<int> orderby, bool descending)
        {
            DateTime dt = FastDateTime.Now;
            List<object> rows = new List<object>();
            Result<object> ret = new Result<object>();
            int skip = start;
            int c = 0;
            ret.TotalCount = (int)ba.CountOnes();
            if (count == -1) count = ret.TotalCount;
            if (count > 0)
            {
                int len = orderby.Count;
                if (len > 0)
                {
                    if (descending == false)
                    {
                        for (int idx = 0; idx < len; idx++)
                        {
                            extractsortrowobject(ba, count, orderby, rows, ref skip, ref c, idx);
                            if (c == count) break;
                        }
                    }
                    else
                    {
                        for (int idx = len - 1; idx >= 0; idx--)
                        {
                            extractsortrowobject(ba, count, orderby, rows, ref skip, ref c, idx);
                            if (c == count) break;
                        }
                    }
                }
                foreach (int i in ba.GetBitIndexes())
                {
                    if (c < count)
                    {
                        if (skip > 0)
                            skip--;
                        else
                        {
                            bool b = OutputRow<object>(rows, i);
                            if (b && count > 0)
                                c++;
                        }
                        if (c == count) break;
                    }
                }
            }
            if (trows != null) // TODO : move to start and decrement in count
                foreach (var o in trows)
                    rows.Add(o);
            _log.Debug("query rows fetched (ms) : " + FastDateTime.Now.Subtract(dt).TotalMilliseconds);
            _log.Debug("query rows count : " + rows.Count.ToString("#,0"));
            ret.OK = true;
            ret.Count = rows.Count;
            ret.Rows = rows;
            return ret;
        }

        private void extractsortrowobject(WAHBitArray ba, int count, List<int> orderby, List<object> rows, ref int skip, ref int c, int idx)
        {
            int i = orderby[idx];
            if (ba.Get(i))
            {
                if (skip > 0)
                    skip--;
                else
                {
                    bool b = OutputRow<object>(rows, i);
                    if (b && count > 0)
                        c++;
                }
                ba.Set(i, false);
            }
        }

        private bool OutputRow<T>(List<T> rows, int i)
        {
            byte[] b = _viewData.ViewReadRawBytes(i);
            if (b != null)
            {
                object o = FastCreateObject(_view.Schema);
                object[] data = (object[])fastBinaryJSON.BJSON.ToObject(b);
                rows.Add((T)_rowfiller(o, data));
                return true;
            }
            return false;
        }

        private Result<T> ReturnRows2<T>(WAHBitArray ba, List<T> trows, int start, int count, List<int> orderby, bool descending)
        {
            DateTime dt = FastDateTime.Now;
            List<T> rows = new List<T>();
            Result<T> ret = new Result<T>();
            int skip = start;
            int c = 0;
            ret.TotalCount = (int)ba.CountOnes();
            if (count == -1) count = ret.TotalCount;
            if (count > 0)
            {
                int len = orderby.Count;
                if (len > 0)
                {
                    if (descending == false)
                    {
                        for (int idx = 0; idx < len; idx++) //foreach (int i in orderby)
                        {
                            extractsortrowT(ba, count, orderby, rows, ref skip, ref c, idx);
                            if (c == count) break;
                        }
                    }
                    else
                    {
                        for (int idx = len - 1; idx >= 0; idx--) //foreach (int i in orderby)
                        {
                            extractsortrowT(ba, count, orderby, rows, ref skip, ref c, idx);
                            if (c == count) break;
                        }
                    }
                }
                foreach (int i in ba.GetBitIndexes())
                {
                    if (c < count)
                    {
                        if (skip > 0)
                            skip--;
                        else
                        {
                            bool b = OutputRow<T>(rows, i);
                            if (b && count > 0)
                                c++;
                        }
                        if (c == count) break;
                    }
                }
            }
            if (trows != null)// TODO : move to start and decrement in count
                foreach (var o in trows)
                    rows.Add(o);
            _log.Debug("query rows fetched (ms) : " + FastDateTime.Now.Subtract(dt).TotalMilliseconds);
            _log.Debug("query rows count : " + rows.Count.ToString("#,0"));
            ret.OK = true;
            ret.Count = rows.Count;
            ret.Rows = rows;
            return ret;
        }

        private void extractsortrowT<T>(WAHBitArray ba, int count, List<int> orderby, List<T> rows, ref int skip, ref int c, int idx)
        {
            int i = orderby[idx];
            if (ba.Get(i))
            {
                if (skip > 0)
                    skip--;
                else
                {
                    bool b = OutputRow<T>(rows, i);
                    if (b && count > 0)
                        c++;
                }
                ba.Set(i, false);
            }
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

        MethodInfo insertmethod = null;
        bool _rebuilding = false;
        private void RebuildFromScratch(IDocStorage<Guid> docs)
        {
            _rebuilding = true;
            try
            {
                insertmethod = this.GetType().GetMethod("Insert", BindingFlags.Instance | BindingFlags.NonPublic);
                _log.Debug("Rebuilding view from scratch...");
                _log.Debug("View = " + _view.Name);
                DateTime dt = FastDateTime.Now;

                int c = docs.RecordCount();
                int dc = 0;

                for (int i = 0; i < c; i++)
                {
                    StorageItem<Guid> meta = null;
                    object b = docs.GetObject(i, out meta);
                    if (meta != null && meta.isDeleted)
                        Delete(meta.key);
                    else
                    {
                        if (b != null)
                        {
                            object obj = b;
                            Type t = obj.GetType();
                            if (t == typeof(View_delete))
                            {
                                View_delete vd = (View_delete)obj;
                                if (vd.Viewname.ToLower() == this._view.Name.ToLower())
                                    ViewDelete(vd.Filter);
                            }
                            else if (t == typeof(View_insert))
                            {
                                View_insert vi = (View_insert)obj;
                                if (vi.Viewname.ToLower() == this._view.Name.ToLower())
                                    ViewInsert(vi.ID, vi.RowObject);
                            }
                            else if (t.IsSubclassOf(basetype) || t == basetype)
                            {
                                var m = insertmethod.MakeGenericMethod(new Type[] { obj.GetType() });
                                m.Invoke(this, new object[] { meta.key, obj });
                                dc++;
                            }
                        }
                        else
                            _log.Error("Doc is null : " + meta.key);
                    }
                }
                _log.Debug("Documents processed = " + dc);
                _log.Debug("rebuild view '" + _view.Name + "' done (s) = " + FastDateTime.Now.Subtract(dt).TotalSeconds);

                // write version.dat file when done
                File.WriteAllText(_Path + _view.Name + ".version", _view.Version.ToString());
            }
            catch (Exception ex)
            {
                _log.Error("Rebuilding View failed : " + _view.Name, ex);
            }
            _rebuilding = false;
        }

        private object CreateObject(byte[] b)
        {
            if (b[0] < 32)
                return BJSON.ToObject(b);
            else
                return JSON.ToObject(Encoding.ASCII.GetString(b));
        }

        private void CreateLoadIndexes(ViewRowDefinition viewRowDefinition)
        {
            int i = 0;
            _indexes.Add(_docid, new TypeIndexes<Guid>(_Path, _docid, 16));
            // load indexes
            foreach (var c in viewRowDefinition.Columns)
            {
                if (c.Key != "docid")
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

                if (_view.NoIndexingColumns.Contains(p.Name) || _view.NoIndexingColumns.Contains(p.Name.ToLower()))
                {
                    _schema.Add(p.Name, typeof(NoIndexing));
                }
                else
                {
                    if (p.GetCustomAttributes(typeof(FullTextAttribute), true).Length > 0)
                        t = typeof(FullTextString);
                    if (_view.FullTextColumns.Contains(p.Name) || _view.FullTextColumns.Contains(p.Name.ToLower()))
                        t = typeof(FullTextString);
                    if (p.Name != "docid")
                        _schema.Add(p.Name, t);

                    if (p.GetCustomAttributes(typeof(CaseInsensitiveAttribute), true).Length > 0)
                        _nocase.Add(p.Name, 0);
                    if (_view.CaseInsensitiveColumns.Contains(p.Name) || _view.CaseInsensitiveColumns.Contains(p.Name.ToLower()))
                        _nocase.Add(p.Name, 0);

                    var a = p.GetCustomAttributes(typeof(StringIndexLength), false);
                    if (a.Length > 0)
                    {
                        byte l = (a[0] as StringIndexLength).Length;
                        _idxlen.Add(p.Name, l);
                    }
                    if (_view.StringIndexLength.ContainsKey(p.Name) || _view.StringIndexLength.ContainsKey(p.Name.ToLower()))
                    {
                        byte b = 0;
                        if (_view.StringIndexLength.TryGetValue(p.Name, out b))
                            _idxlen.Add(p.Name, b);
                        if (_view.StringIndexLength.TryGetValue(p.Name.ToLower(), out b))
                            _idxlen.Add(p.Name, b);
                    }
                }
            }

            foreach (var f in _view.Schema.GetFields())
            {
                Type t = f.FieldType;
                if (_view.NoIndexingColumns.Contains(f.Name) || _view.NoIndexingColumns.Contains(f.Name.ToLower()))
                {
                    _schema.Add(f.Name, typeof(NoIndexing));
                }
                else
                {
                    if (f.GetCustomAttributes(typeof(FullTextAttribute), true).Length > 0)
                        t = typeof(FullTextString);
                    if (_view.FullTextColumns.Contains(f.Name) || _view.FullTextColumns.Contains(f.Name.ToLower()))
                        t = typeof(FullTextString);
                    if (f.Name != "docid")
                        _schema.Add(f.Name, t);

                    if (f.GetCustomAttributes(typeof(CaseInsensitiveAttribute), true).Length > 0)
                        _nocase.Add(f.Name, 0);
                    if (_view.CaseInsensitiveColumns.Contains(f.Name) || _view.CaseInsensitiveColumns.Contains(f.Name.ToLower()))
                        _nocase.Add(f.Name, 0);

                    var a = f.GetCustomAttributes(typeof(StringIndexLength), false);
                    if (a.Length > 0)
                    {
                        byte l = (a[0] as StringIndexLength).Length;
                        _idxlen.Add(f.Name, l);
                    }
                    if (_view.StringIndexLength.ContainsKey(f.Name) || _view.StringIndexLength.ContainsKey(f.Name.ToLower()))
                    {
                        byte b = 0;
                        if (_view.StringIndexLength.TryGetValue(f.Name, out b))
                            _idxlen.Add(f.Name, b);
                        if (_view.StringIndexLength.TryGetValue(f.Name.ToLower(), out b))
                            _idxlen.Add(f.Name, b);
                    }
                }
            }
            _schema.Add("docid", typeof(Guid));

            foreach (var s in _schema.Columns)
                _colnames.Add(s.Key);

            // set column index for nocase
            for (int i = 0; i < _colnames.Count; i++)
            {
                int j = 0;
                if (_nocase.TryGetValue(_colnames[i], out j))
                    _nocase[_colnames[i]] = i;
            }
        }

        private void InsertRowsWithIndexUpdate(Guid guid, List<object[]> rows)
        {
            if (_isDirty == false)
                WriteDirtyFile();

            foreach (var row in rows)
            {
                object[] r = new object[row.Length + 1];
                r[0] = guid;
                Array.Copy(row, 0, r, 1, row.Length);
                byte[] b = BJSON.ToBJSON(r);

                int rownum = (int)_viewData.WriteRawData(b);

                // case insensitve columns here
                foreach (var kv in _nocase)
                    row[kv.Value] = ("" + row[kv.Value]).ToLowerInvariant();

                IndexRow(guid, row, rownum);
            }
        }

        private List<object[]> ExtractRows(List<object> rows)
        {
            List<object[]> output = new List<object[]>();
            // reflection match object properties to the schema row

            int colcount = _schema.Columns.Count;

            foreach (var obj in rows)
            {
                object[] r = new object[colcount];
                Getters[] getters = Reflection.Instance.GetGetters(obj.GetType(), true, null);

                for (int i = 0; i < colcount; i++)
                {
                    var c = _schema.Columns[i];
                    foreach (var g in getters)
                    {
                        //var g = getters[ii];
                        if (g.Name == c.Key)
                        {
                            r[i] = g.Getter(obj);
                            break;
                        }
                    }
                }
                output.Add(r);
            }

            return output;
        }

        private void IndexRow(Guid docid, object[] row, int rownum)
        {
            int c = _colnames.Count - 1; // skip last docid 
            _indexes[_docid].Set(docid, rownum);

            for (int i = 0; i < c; i++)
            {
                object d = row[i];
                var idx = _indexes[_colnames[i]];
                if (idx != null)
                    idx.Set(d, rownum);
            }
        }

        private IIndex CreateIndex(string name, Type type)
        {
            if (type == typeof(NoIndexing))
                return new NoIndex();

            if (type == typeof(FullTextString))
                return new FullTextIndex(_Path, name, false, true);

            else if (type == typeof(string))
            {
                byte len = Global.DefaultStringKeySize;
                if (_idxlen.TryGetValue(name, out len) == false)
                    len = Global.DefaultStringKeySize;
                return new TypeIndexes<string>(_Path, name, len);
            }

            else if (type == typeof(bool) || type == typeof(bool?))
                return new BoolIndex(_Path, name, ".idx");

            else if (type.IsEnum)
                return (IIndex)Activator.CreateInstance(
                    typeof(EnumIndex<>).MakeGenericType(type),
                    new object[] { _Path, name });

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
            int i = 0;
            if (_nocase.TryGetValue(colname, out i)) // no case query
                return _indexes[colname].Query(exp, ("" + from).ToLowerInvariant(), _viewData.Count());
            else
                return _indexes[colname].Query(exp, from, _viewData.Count());
        }
        #endregion

        internal int Count<T>(Expression<Predicate<T>> filter)
        {
            int totcount = 0;
            DateTime dt = FastDateTime.Now;
            if (filter == null)
                totcount = internalCount();
            else
            {
                WAHBitArray ba = new WAHBitArray();

                QueryVisitor qv = new QueryVisitor(QueryColumnExpression);
                qv.Visit(filter);
                var delbits = _deletedRows.GetBits();
                ba = ((WAHBitArray)qv._bitmap.Pop()).AndNot(delbits);

                totcount = (int)ba.CountOnes();
            }
            _log.Debug("Count items = " + totcount);
            _log.Debug("Count time (ms) : " + FastDateTime.Now.Subtract(dt).TotalMilliseconds);
            return totcount;
        }

        internal int Count(string filter)
        {
            int totcount = 0;
            DateTime dt = FastDateTime.Now;
            filter = filter.Trim();
            if (filter == null || filter == "")
                totcount = internalCount();
            else
            {
                _log.Debug("Count filter : " + filter);
                WAHBitArray ba = new WAHBitArray();

                LambdaExpression le = null;
                if (_lambdacache.TryGetValue(filter, out le) == false)
                {
                    le = System.Linq.Dynamic.DynamicExpression.ParseLambda(_view.Schema, typeof(bool), filter, null);
                    _lambdacache.Add(filter, le);
                }
                QueryVisitor qv = new QueryVisitor(QueryColumnExpression);
                qv.Visit(le.Body);
                var delbits = _deletedRows.GetBits();
                ba = ((WAHBitArray)qv._bitmap.Pop()).AndNot(delbits);

                totcount = (int)ba.CountOnes();
            }
            _log.Debug("Count items = " + totcount);
            _log.Debug("Count time (ms) : " + FastDateTime.Now.Subtract(dt).TotalMilliseconds);
            return totcount;
        }

        private int internalCount()
        {
            if (_rebuilding)
                while (_rebuilding)
                    Thread.Sleep(10); // wait for rebuild to finish
            int c = _viewData.Count();
            int cc = (int)_deletedRows.GetBits().CountOnes();
            return c - cc;
        }

        internal Result<T> Query2<T>(Expression<Predicate<T>> filter, int start, int count)
        {
            return Query2<T>(filter, start, count, "");
        }

        internal Result<T> Query2<T>(Expression<Predicate<T>> filter, int start, int count, string orderby)
        {
            DateTime dt = FastDateTime.Now;
            _log.Debug("query : " + _view.Name);

            WAHBitArray ba = new WAHBitArray();

            QueryVisitor qv = new QueryVisitor(QueryColumnExpression);
            qv.Visit(filter);
            var delbits = _deletedRows.GetBits();
            if (qv._bitmap.Count > 0)
            {
                WAHBitArray qbits = (WAHBitArray)qv._bitmap.Pop();
                ba = qbits.AndNot(delbits);
            }
            List<T> trows = null;
            if (_viewmanager.inTransaction())
            {
                // query from transactions own data
                tran_data data = null;
                if (_transactions.TryGetValue(Thread.CurrentThread.ManagedThreadId, out data))
                {
                    List<T> rrows = new List<T>();
                    foreach (var kv in data.rows)
                    {
                        foreach (var r in kv.Value)
                        {
                            object o = FastCreateObject(_view.Schema);
                            rrows.Add((T)_rowfiller(o, r));
                        }
                    }
                    trows = rrows.FindAll(filter.Compile());
                }
            }
            var order = SortBy(orderby);
            bool desc = false;
            if (orderby.ToLower().Contains(" desc"))
                desc = true;
            _log.Debug("query bitmap done (ms) : " + FastDateTime.Now.Subtract(dt).TotalMilliseconds);
            dt = FastDateTime.Now;
            // exec query return rows
            return ReturnRows2<T>(ba, trows, start, count, order, desc);
        }

        internal Result<T> Query2<T>(string filter, int start, int count)
        {
            return Query2<T>(filter, start, count, "");
        }

        internal Result<T> Query2<T>(string filter, int start, int count, string orderby)
        {
            DateTime dt = FastDateTime.Now;
            _log.Debug("query : " + _view.Name);
            _log.Debug("query : " + filter);
            _log.Debug("order by : " + orderby);

            WAHBitArray ba = new WAHBitArray();
            var delbits = _deletedRows.GetBits();

            if (filter != "")
            {
                LambdaExpression le = null;
                if (_lambdacache.TryGetValue(filter, out le) == false)
                {
                    le = System.Linq.Dynamic.DynamicExpression.ParseLambda(_view.Schema, typeof(bool), filter, null);
                    _lambdacache.Add(filter, le);
                }
                QueryVisitor qv = new QueryVisitor(QueryColumnExpression);
                qv.Visit(le.Body);

                ba = ((WAHBitArray)qv._bitmap.Pop()).AndNot(delbits);
            }
            else
                ba = WAHBitArray.Fill(_viewData.Count()).AndNot(delbits);

            var order = SortBy(orderby);
            bool desc = false;
            if (orderby.ToLower().Contains(" desc"))
                desc = true;
            _log.Debug("query bitmap done (ms) : " + FastDateTime.Now.Subtract(dt).TotalMilliseconds);
            dt = FastDateTime.Now;
            // exec query return rows
            return ReturnRows2<T>(ba, null, start, count, order, desc);
        }

        private SafeDictionary<string, List<int>> _sortcache = new SafeDictionary<string, List<int>>();

        internal List<int> SortBy(string sortcol)
        {
            List<int> sortlist = new List<int>();
            if (sortcol == "")
                return sortlist;
            string col = "";
            foreach (var c in _schema.Columns)
                if (sortcol.ToLower().Contains(c.Key.ToLower()))
                {
                    col = c.Key;
                    break;
                }
            if (col == "")
            {
                _log.Debug("sort column not recognized : " + sortcol);
                return sortlist;
            }

            DateTime dt = FastDateTime.Now;

            if (_sortcache.TryGetValue(col, out sortlist) == false)
            {
                sortlist = new List<int>();
                int count = _viewData.Count();
                IIndex idx = _indexes[col];
                object[] keys = idx.GetKeys();
                Array.Sort(keys);

                foreach (var k in keys)
                {
                    var bi = idx.Query(RDBExpression.Equal, k, count).GetBitIndexes();
                    foreach (var i in bi)
                        sortlist.Add(i);
                }
                _sortcache.Add(col, sortlist);
            }
            _log.Debug("Sort column = " + col + ", time (ms) = " + FastDateTime.Now.Subtract(dt).TotalMilliseconds);
            return sortlist;
        }

        internal object GetAssembly(out string typename)
        {
            typename = _view.Schema.AssemblyQualifiedName;
            return File.ReadAllBytes(_view.Schema.Assembly.Location);
        }

        public ViewRowDefinition GetSchema()
        {
            return _schema;
        }

        int _lastrownumber = -1;
        object _rowlock = new object();
        internal int NextRowNumber()
        {
            lock (_rowlock)
            {
                if (_lastrownumber == -1)
                    _lastrownumber = internalCount();
                return ++_lastrownumber;
            }
        }

        internal int ViewDelete<T>(Expression<Predicate<T>> filter)
        {
            _log.Debug("delete : " + _view.Name);
            if (_isDirty == false)
                WriteDirtyFile();
            QueryVisitor qv = new QueryVisitor(QueryColumnExpression);
            qv.Visit(filter);
            var delbits = _deletedRows.GetBits();
            int count = qv._bitmap.Count;
            if (count > 0)
            {
                WAHBitArray qbits = (WAHBitArray)qv._bitmap.Pop();
                _deletedRows.InPlaceOR(qbits);
                count = (int)qbits.CountOnes();
            }
            _log.Debug("Deleted rows = " + count);

            InvalidateSortCache();
            return count;
        }

        private object _dfile = new object();
        private void WriteDirtyFile()
        {
            lock (_dfile)
            {
                _isDirty = true;
                if (File.Exists(_Path + _dirtyFilename) == false)
                    File.WriteAllText(_Path + _dirtyFilename, "dirty");
            }
        }

        internal int ViewDelete(string filter)
        {
            _log.Debug("delete : " + _view.Name);
            if (_isDirty == false)
                WriteDirtyFile();
            int count = 0;
            if (filter != "")
            {
                LambdaExpression le = null;
                if (_lambdacache.TryGetValue(filter, out le) == false)
                {
                    le = System.Linq.Dynamic.DynamicExpression.ParseLambda(_view.Schema, typeof(bool), filter, null);
                    _lambdacache.Add(filter, le);
                }
                QueryVisitor qv = new QueryVisitor(QueryColumnExpression);
                qv.Visit(le.Body);
                count = qv._bitmap.Count;
                if (count > 0)
                {
                    WAHBitArray qbits = (WAHBitArray)qv._bitmap.Pop();
                    _deletedRows.InPlaceOR(qbits);
                    count = (int)qbits.CountOnes();
                }
            }

            InvalidateSortCache();
            return count;
        }

        internal bool ViewInsert(Guid id, object row)
        {
            List<object> l = new List<object>();
            l.Add(row);

            var r = ExtractRows(l);
            InsertRowsWithIndexUpdate(id, r);

            InvalidateSortCache();
            return true;
        }

        private void InvalidateSortCache()
        {
            _sortcache = new SafeDictionary<string, List<int>>();
        }
    }
}
