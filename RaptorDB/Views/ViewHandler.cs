using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;
using System.Collections;
using RaptorDB.Mapping;
using System.Linq.Expressions;
using System.Threading.Tasks;

namespace RaptorDB.Views
{
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
        //private WAHBitArray _deletedRows;
        BoolIndex _deletedRows;
        private string _docid = "docid";
        private List<string> _colnames = new List<string>();

        public void SetView<T>(View<T> view)
        {
            _view = view;
            // generate schemacolumns from schema
            GenerateSchemaColumns(_view);

            if (_Path.EndsWith("\\") == false) _Path += "\\";
            _Path += view.Name + "\\";
            Directory.CreateDirectory(_Path);
            // FIX : correct the following for column types and check for changes 
            //File.WriteAllBytes(_Path + view.Name + ".bjson",  fastBinaryJSON.BJSON.Instance.ToBJSON(Activator.CreateInstance( _view.Schema)));

            // load indexes here
            CreateLoadIndexes(_view.SchemaColumns);

            LoadDeletedRowsBitmap();

            _viewData = new StorageFile<Guid>(_Path + view.Name + ".mgdat", view.BackgroundIndexing);//, 1);
            _viewData.SkipDateTime = true;
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
                _view.SchemaColumns.Add(p.Name, p.PropertyType);

            foreach (var f in _view.Schema.GetFields())
                _view.SchemaColumns.Add(f.Name, f.FieldType);

            foreach (var s in _view.SchemaColumns.Columns)
                _colnames.Add(s.Key);
        }

        private void LoadDeletedRowsBitmap()
        {
            _deletedRows = new BoolIndex(_Path, "deleted_.idx");
            //if (File.Exists(_Path + "deleted.idx"))
            //{
            //    // load deleted rows here
            //    byte[] b = File.ReadAllBytes(_Path + "deleted.idx");
            //    List<uint> l = new List<uint>();
            //    for (int i = 0; i < b.Length; i += 4)
            //    {
            //        l.Add((uint)Helper.ToInt32(b, i));
            //    }

            //    _deletedRows = new WAHBitArray(WAHBitArray.TYPE.Uncompressed_WAH, l.ToArray());
            //}
            //else
            //    _deletedRows = new WAHBitArray();
        }

        public void FreeMemory()
        {
            foreach (var i in _indexes)
            {
                i.Value.FreeMemory();
            }
            _deletedRows.FreeMemory();
        }

        private object _lock = new object();
        public void Insert<T>(Guid guid, T doc)
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

        private void InsertRowsWithIndexUpdate(Guid guid, List<object[]> rows)
        {
            lock (_lock)
            {
                //return;
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
        }

        private void IndexRow(Guid docid, object[] row, int rownum)
        {
            //return;
            int i = 0;
            _indexes[_docid].Set(docid, rownum);
            // index the row
            foreach (var d in row)
            {
                _indexes[_colnames[i++]].Set(d, rownum);
                //i++;
            }
        }

        private IIndex CreateIndex(string name, Type type)
        {
            if (type != typeof(string))
            {
                if (type == typeof(NormalString))
                    return new TypeIndexes<string>(_Path, name, Global.DefaultStringKeySize);

                else if (type == typeof(bool))
                    return new BoolIndex(_Path, name);

                else
                    return (IIndex)Activator.CreateInstance(
                        typeof(TypeIndexes<>).MakeGenericType(type),
                        new object[] { _Path, name, Global.DefaultStringKeySize });
            }
            else
                return new FullTextIndex(_Path, name);
        }

        private void DeleteRowsWith(Guid guid)
        {
            // find bitmap for guid column
            WAHBitArray gc = QueryColumnExpression(_docid, RDBExpression.Equal, guid);
            _deletedRows.InPlaceOR(gc);
        }

        private WAHBitArray QueryColumnFromTo(string colname, object from, object to)
        {
            return _indexes[colname].Query(from, to);
        }

        private WAHBitArray QueryColumnExpression(string colname, RDBExpression exp, object from)
        {
            return _indexes[colname].Query(exp, from);
        }

        public Result Query<T>(Expression<Predicate<T>> filter, int start, int count)
        {
            // FEATURE : add query caching here
            Result ret = new Result();
            QueryVisitor qv = new QueryVisitor(QueryColumnFromTo, QueryColumnExpression);
            qv.Visit(filter);
            List<object[]> rows = new List<object[]>();
            WAHBitArray ba = (WAHBitArray)qv._bitmap.Pop();

            // exec query return rows
            foreach (int i in ba.GetBitIndexes(true))
            {
                byte[] b = _viewData.ReadData(i);
                rows.Add(
                    ((ArrayList)fastBinaryJSON.BJSON.Instance.ToObject(b)).ToArray()
                    );
            }
            ret.OK = true;
            ret.Count = rows.Count;
            ret.TotalCount = rows.Count;
            ret.Rows = rows;
            return ret;
        }

        internal void Shutdown()
        {
            // save deletedbitmap
            _deletedRows.Shutdown();

            _viewData.Shutdown();
            // shutdown indexes
            foreach (var v in _indexes)
            {
                v.Value.Shutdown();
            }
        }
    }
}
