using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;
using System.Collections;
using System.Linq.Expressions;
using System.Threading.Tasks;
using System.Reflection;

namespace RaptorDB.Views
{
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
            CreateLoadIndexes(_view.SchemaColumns);

            LoadDeletedRowsBitmap();

            _viewData = new StorageFile<Guid>(_Path + view.Name + ".mgdat");
            _viewData.SkipDateTime = true;

            if(rebuild)
                RebuildFromScratch(docs);
        }

        internal void FreeMemory()
        {
            foreach (var i in _indexes)
                i.Value.FreeMemory();

            _deletedRows.FreeMemory();
        }

        internal void Insert<T>(Guid guid, T doc)
        {
            apimapper api = new apimapper(_viewmanager);
            View<T> view = (View<T>)_view;

            if (view.Mapper != null)
                view.Mapper(api, guid, doc);
            // FEATURE : ELSE -> call map dll 

            foreach (var d in api.emit)
            {
                // delete any items with docid in view
                if (_view.DeleteBeforeInsert)
                    DeleteRowsWith(d.Key);
                // insert new items into view
                InsertRowsWithIndexUpdate(guid, d.Value);
            }

        }

        internal Result Query(string filter)
        {
            DateTime dt = FastDateTime.Now;
            _log.Debug("query : " + filter);
            // FEATURE : add query caching here
            Result ret = new Result();
            WAHBitArray ba = new WAHBitArray();
            List<object[]> rows = new List<object[]>();

            var e = System.Linq.Dynamic.DynamicExpression.ParseLambda(_view.Schema, typeof(bool), filter, null);

            QueryVisitor qv = new QueryVisitor(QueryColumnExpression);
            qv.Visit(e.Body);
            ba = ((WAHBitArray)qv._bitmap.Pop()).AndNot(_deletedRows.GetBits());

            _log.Debug("query bitmap done (ms) : " + FastDateTime.Now.Subtract(dt).TotalMilliseconds);
            dt = FastDateTime.Now;
            // exec query return rows
            return RetrunRows(ba);
        }

        internal Result Query<T>(Expression<Predicate<T>> filter)
        {
            DateTime dt = FastDateTime.Now;
            // FEATURE : add query caching here
            Result ret = new Result();
            WAHBitArray ba = new WAHBitArray();
            List<object[]> rows = new List<object[]>();

            QueryVisitor qv = new QueryVisitor(QueryColumnExpression);
            qv.Visit(filter);
            ba = ((WAHBitArray)qv._bitmap.Pop()).AndNot(_deletedRows.GetBits());

            _log.Debug("query bitmap done (ms) : " + FastDateTime.Now.Subtract(dt).TotalMilliseconds);
            dt = FastDateTime.Now;
            // exec query return rows
            return RetrunRows(ba);
        }

        internal Result Query()
        {
            // no filter query -> just show all the data
            DateTime dt = FastDateTime.Now;
            int count = _viewData.Count();
            List<object[]> rows = new List<object[]>();
            Result ret = new Result();

            WAHBitArray del = _deletedRows.GetBits();
            for (int i = 0; i < count; i++)
            {
                if (del.Get(i) == true)
                    continue;
                byte[] b = _viewData.ReadData(i);
                if (b == null) continue;
                rows.Add(
                    ((ArrayList)fastBinaryJSON.BJSON.Instance.ToObject(b)).ToArray()
                    );
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

        #region [  private methods  ]

        private Result RetrunRows(WAHBitArray ba)
        {
            DateTime dt = FastDateTime.Now;
            List<object[]> rows = new List<object[]>();
            Result ret = new Result();
            foreach (int i in ba.GetBitIndexes())
            {
                byte[] b = _viewData.ReadData(i);
                if (b == null) continue;
                rows.Add(
                    ((ArrayList)fastBinaryJSON.BJSON.Instance.ToObject(b)).ToArray()
                    );
            }
            _log.Debug("query rows fetched (ms) : " + FastDateTime.Now.Subtract(dt).TotalMilliseconds);
            _log.Debug("query rows count : " + rows.Count);
            ret.OK = true;
            ret.Count = rows.Count;
            ret.TotalCount = rows.Count;
            ret.Rows = rows;
            return ret;
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
                byte[] b = docs.Get(i, out docid);
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
                _indexes.Add(_view.SchemaColumns.Columns[i].Key,
                          CreateIndex(
                            _view.SchemaColumns.Columns[i].Key,
                            _view.SchemaColumns.Columns[i].Value));
                i++;
            }
        }

        private void GenerateSchemaColumns(ViewBase _view)
        {
            // generate schema columns from schema
            _view.SchemaColumns = new ViewRowDefinition();
            _view.SchemaColumns.Name = _view.Name;

            foreach (var p in _view.Schema.GetProperties())
            {
                Type t = p.PropertyType;
                if (p.GetCustomAttributes(typeof(FullTextAttribute), true).Length > 0)
                    t = typeof(FullTextString);
                _view.SchemaColumns.Add(p.Name, t);
            }

            foreach (var f in _view.Schema.GetFields())
            {
                Type t = f.FieldType;
                if (f.GetCustomAttributes(typeof(FullTextAttribute), true).Length > 0)
                    t = typeof(FullTextString);
                _view.SchemaColumns.Add(f.Name, t);
            }

            foreach (var s in _view.SchemaColumns.Columns)
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

        private void IndexRow(Guid docid, object[] row, int rownum)
        {
            //return;
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
