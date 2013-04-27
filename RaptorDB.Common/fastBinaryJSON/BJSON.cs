using System;
using System.Collections;
using System.Collections.Generic;
#if !SILVERLIGHT
using System.Data;
#endif
using System.Globalization;
using System.IO;
using System.Reflection;
using System.Reflection.Emit;
using System.Xml;
using System.Text;
using fastJSON;
using RaptorDB.Common;

namespace fastBinaryJSON
{
    public sealed class TOKENS
    {
        public const byte DOC_START = 1;
        public const byte DOC_END = 2;
        public const byte ARRAY_START = 3;
        public const byte ARRAY_END = 4;
        public const byte COLON = 5;
        public const byte COMMA = 6;
        public const byte NAME = 7;
        public const byte STRING = 8;
        public const byte BYTE = 9;
        public const byte INT = 10;
        public const byte UINT = 11;
        public const byte LONG = 12;
        public const byte ULONG = 13;
        public const byte SHORT = 14;
        public const byte USHORT = 15;
        public const byte DATETIME = 16;
        public const byte GUID = 17;
        public const byte DOUBLE = 18;
        public const byte FLOAT = 19;
        public const byte DECIMAL = 20;
        public const byte CHAR = 21;
        public const byte BYTEARRAY = 22;
        public const byte NULL = 23;
        public const byte TRUE = 24;
        public const byte FALSE = 25;
        public const byte UNICODE_STRING = 26;
    }

    public delegate string Serialize(object data);
    public delegate object Deserialize(string data);

    public sealed class BJSONParameters
    {
        /// <summary> 
        /// Optimize the schema for Datasets (default = True)
        /// </summary>
        public bool UseOptimizedDatasetSchema = true;
        /// <summary>
        /// Serialize readonly properties (default = False)
        /// </summary>
        public bool ShowReadOnlyProperties = false;
        /// <summary>
        /// Use global types $types for more compact size when using a lot of classes (default = True)
        /// </summary>
        public bool UsingGlobalTypes = true;
        /// <summary>
        /// Use Unicode strings = T (faster), Use UTF8 strings = F (smaller) (default = True)
        /// </summary>
        public bool UseUnicodeStrings = true;
        /// <summary>
        /// Serialize Null values to the output (default = False)
        /// </summary>
        public bool SerializeNulls = false;
        /// <summary>
        /// 
        /// </summary>
        public bool UseExtensions = true;
        /// <summary>
        /// 
        /// </summary>
        public bool EnableAnonymousTypes = false;

        public bool UseUTCtimes = false;

        public void FixValues()
        {
            if (UseExtensions == false) // disable conflicting params
            {
                UsingGlobalTypes = false;
            }
        }
    }

    public sealed class BJSON
    {
        //public readonly static BJSON Instance = new BJSON();
        [ThreadStatic]
        private static BJSON _instance;

        public static BJSON Instance
        {
            get { return _instance ?? (_instance = new BJSON()); }
        }

        private BJSON()
        {
        }
        public BJSONParameters Parameters = new BJSONParameters();
        public UnicodeEncoding unicode = new UnicodeEncoding();
        public UTF8Encoding utf8 = new UTF8Encoding();
        private BJSONParameters _params;

        public byte[] ToBJSON(object obj)
        {
            _params = Parameters;
            return ToBJSON(obj, _params);
        }

        public byte[] ToBJSON(object obj, BJSONParameters param)
        {
            Reflection.Instance.ShowReadOnlyProperties = param.ShowReadOnlyProperties;
            param.FixValues();
            Type t = null;
            if (obj == null)
                return new byte[] { TOKENS.NULL };
            if (obj.GetType().IsGenericType)
                t = obj.GetType().GetGenericTypeDefinition();
            if (t == typeof(Dictionary<,>) || t == typeof(List<>))
                param.UsingGlobalTypes = false;
            _globalTypes = param.UsingGlobalTypes;
            return new BJSONSerializer(param).ConvertToBJSON(obj);
        }

        public object Parse(byte[] json)
        {
            _params = Parameters;
            _params.FixValues();
            _globalTypes = _params.UsingGlobalTypes;
            Reflection.Instance.ShowReadOnlyProperties = _params.ShowReadOnlyProperties;
            return new BJsonParser(json, _params.UseUTCtimes).Decode();
        }

        public T ToObject<T>(byte[] json)
        {
            return (T)ToObject(json, typeof(T));
        }

        public object ToObject(byte[] json)
        {
            return ToObject(json, null);
        }

        public object ToObject(byte[] json, Type type)
        {
            _params = Parameters;
            _params.FixValues();
            Type t = null;
            if (type != null && type.IsGenericType)
                t = type.GetGenericTypeDefinition();
            if (t == typeof(Dictionary<,>) || t == typeof(List<>))
                _params.UsingGlobalTypes = false;
            _globalTypes = _params.UsingGlobalTypes;

            Reflection.Instance.ShowReadOnlyProperties = _params.ShowReadOnlyProperties;

            var o = new BJsonParser(json, _params.UseUTCtimes).Decode();
#if !SILVERLIGHT
            if (type != null && type == typeof(DataSet))
                return CreateDataset(o as Dictionary<string, object>, null);

            if (type != null && type == typeof(DataTable))
                return CreateDataTable(o as Dictionary<string, object>, null);
#endif
            if (o is IDictionary)
            {
                if (type != null && type.IsGenericType && type.GetGenericTypeDefinition() == typeof(Dictionary<,>)) // deserialize a dictionary
                    return RootDictionary(o, type);
                else // deserialize an object
                    return ParseDictionary(o as Dictionary<string, object>, null, type, null);
            }

            if (o is List<object>)
            {
                if (type != null && type.GetGenericTypeDefinition() == typeof(Dictionary<,>)) // kv format
                    return RootDictionary(o, type);

                if (type != null && type.GetGenericTypeDefinition() == typeof(List<>)) // deserialize to generic list
                    return RootList(o, type);
                else
                    return (o as List<object>).ToArray();
            }

            return o;
        }

        public object FillObject(object input, byte[] json)
        {
            _params = Parameters;
            _params.FixValues();
            Reflection.Instance.ShowReadOnlyProperties = _params.ShowReadOnlyProperties;
            Dictionary<string, object> ht = new BJsonParser(json, _params.UseUTCtimes).Decode() as Dictionary<string, object>;
            if (ht == null) return null;
            return ParseDictionary(ht, null, input.GetType(), input);
        }


        internal SafeDictionary<Type, Serialize> _customSerializer = new SafeDictionary<Type, Serialize>();
        internal SafeDictionary<Type, Deserialize> _customDeserializer = new SafeDictionary<Type, Deserialize>();

        public void RegisterCustomType(Type type, Serialize serializer, Deserialize deserializer)
        {
            if (type != null && serializer != null && deserializer != null)
            {
                _customSerializer.Add(type, serializer);
                _customDeserializer.Add(type, deserializer);
                // reset property cache
                _propertycache = new SafeDictionary<string, SafeDictionary<string, myPropInfo>>();
            }
        }

        internal bool IsTypeRegistered(Type t)
        {
            if (_customSerializer.Count == 0) 
                return false;
            Serialize s;
            return _customSerializer.TryGetValue(t, out s);
        }


        #region [   BJSON specific reflection   ]
        internal enum myPropInfoType
        {
            Int,
            Long,
            String,
            Bool,
            DateTime,
            Enum,
            Guid,

            Array,
            ByteArray,
            Dictionary,
            StringDictionary,
#if !SILVERLIGHT
            Hashtable,
            DataSet,
            DataTable,
#endif
            Custom,
            Unknown,
        }

        [Flags]
        internal enum myPropInfoFlags
        {
            Filled = 1 << 0,
            CanWrite = 1 << 1
        }

        internal struct myPropInfo
        {
            public Type pt;
            public Type bt;
            public Reflection.GenericSetter setter;
            public Reflection.GenericGetter getter;
            public Type[] GenericTypes;
            public string Name;
            public myPropInfoType Type;
            public myPropInfoFlags Flags;

            public bool IsClass;
            public bool IsValueType;
            public bool IsGenericType;
        }

        SafeDictionary<string, SafeDictionary<string, myPropInfo>> _propertycache = new SafeDictionary<string, SafeDictionary<string, myPropInfo>>();
        internal SafeDictionary<string, myPropInfo> Getproperties(Type type, string typename)
        {
            SafeDictionary<string, myPropInfo> sd = null;
            if (_propertycache.TryGetValue(typename, out sd))
            {
                return sd;
            }
            else
            {
                sd = new SafeDictionary<string, myPropInfo>();
                PropertyInfo[] pr = type.GetProperties(BindingFlags.Public | BindingFlags.Instance | BindingFlags.Static);
                foreach (PropertyInfo p in pr)
                {
                    myPropInfo d = CreateMyProp(p.PropertyType, p.Name);
                    d.Flags |= myPropInfoFlags.CanWrite;
                    d.setter = Reflection.CreateSetMethod(type, p);
                    d.getter = Reflection.CreateGetMethod(type, p);
                    sd.Add(p.Name, d);
                }
                FieldInfo[] fi = type.GetFields(BindingFlags.Public | BindingFlags.Instance | BindingFlags.Static);
                foreach (FieldInfo f in fi)
                {
                    myPropInfo d = CreateMyProp(f.FieldType, f.Name);
                    d.setter = Reflection.CreateSetField(type, f);
                    d.getter = Reflection.CreateGetField(type, f);
                    sd.Add(f.Name, d);
                }

                _propertycache.Add(typename, sd);
                return sd;
            }
        }

        private myPropInfo CreateMyProp(Type t, string name)
        {
            myPropInfo d = new myPropInfo();
            myPropInfoType d_type = myPropInfoType.Unknown;
            myPropInfoFlags d_flags = myPropInfoFlags.Filled | myPropInfoFlags.CanWrite;


            if (t.IsEnum) d_type = myPropInfoType.Enum;
            else if (t.IsArray)
            {
                d.bt = t.GetElementType();
                if (t == typeof(byte[]))
                    d_type = myPropInfoType.ByteArray;
                else
                    d_type = myPropInfoType.Array;
            }
            else if (t.Name.Contains("Dictionary"))
            {
                d.GenericTypes = t.GetGenericArguments();
                if (d.GenericTypes.Length > 0 && d.GenericTypes[0] == typeof(string))
                    d_type = myPropInfoType.StringDictionary;
                else
                    d_type = myPropInfoType.Dictionary;
            }
#if !SILVERLIGHT
            else if (t == typeof(Hashtable)) d_type = myPropInfoType.Hashtable;
            else if (t == typeof(DataSet)) d_type = myPropInfoType.DataSet;
            else if (t == typeof(DataTable)) d_type = myPropInfoType.DataTable;
#endif
            else if (IsTypeRegistered(t))								
                d_type = myPropInfoType.Custom;

            d.IsClass = t.IsClass;
            d.IsValueType = t.IsValueType;
            if (t.IsGenericType)
            {
                d.IsGenericType = true;
                d.bt = t.GetGenericArguments()[0];
            }

            d.pt = t;
            d.Name = name;
            d.Type = d_type;
            d.Flags = d_flags;

            return d;
        }
        #endregion

        private object RootList(object parse, Type type)
        {
            Type[] gtypes = type.GetGenericArguments();
            IList o = (IList)Reflection.Instance.FastCreateInstance(type);
            foreach (var k in (IList)parse)
            {
                _globalTypes = false;
                object v = k;
                if (k is Dictionary<string, object>)
                    v = ParseDictionary(k as Dictionary<string, object>, null, gtypes[0], null);
                else
                    v = k;

                o.Add(v);
            }
            return o;
        }

        private object RootDictionary(object parse, Type type)
        {
            Type[] gtypes = type.GetGenericArguments();
            if (parse is Dictionary<string, object>)
            {
                IDictionary o = (IDictionary)Reflection.Instance.FastCreateInstance(type);

                foreach (var kv in (Dictionary<string, object>)parse)
                {
                    _globalTypes = false;
                    object v;
                    object k = kv.Key;
                    if (kv.Value is Dictionary<string, object>)
                        v = ParseDictionary(kv.Value as Dictionary<string, object>, null, gtypes[1], null);
                    else if (kv.Value is List<object>)
                        v = CreateArray(kv.Value as List<object>, typeof(object), typeof(object), null);
                    else
                        v = kv.Value;
                    o.Add(k, v);
                }

                return o;
            }
            if (parse is List<object>)
                return CreateDictionary(parse as List<object>, type, gtypes, null);

            return null;
        }

        private bool _globalTypes = false;
        private object ParseDictionary(Dictionary<string, object> d, Dictionary<string, object> globaltypes, Type type, object input)
        {
            object tn = "";
            if (d.TryGetValue("$types", out tn))
            {
                _globalTypes = true;
                if (globaltypes == null)
                    globaltypes = new Dictionary<string, object>();
                foreach (var kv in (Dictionary<string, object>)tn)
                {
                    globaltypes.Add((string)kv.Key, kv.Value);
                }
            }

            bool found = d.TryGetValue("$type", out tn);
#if !SILVERLIGHT
            if (found == false && type == typeof(System.Object))
            {
                return CreateDataset(d, globaltypes);
            }
#endif
            if (found)
            {
                if (_globalTypes && globaltypes != null)
                {
                    object tname = "";
                    if (globaltypes.TryGetValue((string)tn, out tname))
                        tn = tname;
                }
                type = Reflection.Instance.GetTypeFromCache((string)tn);
            }

            if (type == null)
                throw new Exception("Cannot determine type");

            string typename = type.FullName;
            object o = input;
            if (o == null)
                o = Reflection.Instance.FastCreateInstance(type);
            SafeDictionary<string, myPropInfo> props = Getproperties(type, typename);
            foreach (string name in d.Keys)
            {
                myPropInfo pi;
                if (props.TryGetValue(name, out pi) == false)
                    continue;
                if ((pi.Flags & (myPropInfoFlags.Filled | myPropInfoFlags.CanWrite)) != 0) 
                {
                    object v = d[name];

                    if (v != null)
                    {
                        object oset = v;

                        switch (pi.Type)
                        {
#if !SILVERLIGHT
                            case myPropInfoType.DataSet:
                                oset = CreateDataset((Dictionary<string, object>)v, globaltypes); 
                                break;
                            case myPropInfoType.DataTable:
                                oset = CreateDataTable((Dictionary<string, object>)v, globaltypes); 
                                break;
#endif                           
                            case myPropInfoType.Custom :
                                oset = CreateCustom((string)v, pi.pt);
                                break;
                            case myPropInfoType.Enum:
                                oset = CreateEnum(pi.pt, (string)v); 
                                break;
                            case myPropInfoType.StringDictionary:
                                oset = CreateStringKeyDictionary((Dictionary<string, object>)v, pi.pt, pi.GenericTypes, globaltypes); 
                                break;
                            case myPropInfoType.Hashtable:
                            case myPropInfoType.Dictionary:
                                oset = CreateDictionary((List<object>)v, pi.pt, pi.GenericTypes, globaltypes);
                                break;
                            case myPropInfoType.Array:
                                oset = CreateArray((List<object>)v, pi.pt, pi.bt, globaltypes); 
                                break;
                            default:
                                {
                                    if (pi.IsGenericType && pi.IsValueType == false)
                                        oset = CreateGenericList((List<object>)v, pi.pt, pi.bt, globaltypes);
                                    else if (pi.IsClass && v is Dictionary<string, object>)
                                        oset = ParseDictionary((Dictionary<string, object>)v, globaltypes, pi.pt, input);

                                    else if (v is List<object>)
                                        oset = CreateArray((List<object>)v, pi.pt, typeof(object), globaltypes);
                                    break;
                                }
                        }

                        o = pi.setter(o, oset);
                    }
                }
            }
            return o;
        }


        private object CreateCustom(string v, Type type)
        {
            Deserialize d;
            _customDeserializer.TryGetValue(type, out d);
            return d(v);
        }

        private object CreateEnum(Type pt, string v)
        {
            // TODO : optimize create enum
#if !SILVERLIGHT
            return Enum.Parse(pt, v);
#else
            return Enum.Parse(pt, v, true);
#endif
        }

        private object CreateArray(List<object> data, Type pt, Type bt, Dictionary<string, object> globalTypes)
        {
            Array col = Array.CreateInstance(bt, data.Count);
            // create an array of objects
            for (int i = 0; i < data.Count; i++)// each (object ob in data)
            {
                object ob = data[i];
                if (ob is IDictionary)
                    col.SetValue(ParseDictionary((Dictionary<string, object>)ob, globalTypes, bt, null), i);
                else
                    col.SetValue(ob, i);
            }

            return col;
        }

        private object CreateGenericList(List<object> data, Type pt, Type bt, Dictionary<string, object> globalTypes)
        {
            IList col = (IList)Reflection.Instance.FastCreateInstance(pt);
            // create an array of objects
            foreach (object ob in data)
            {
                if (ob is IDictionary)
                    col.Add(ParseDictionary((Dictionary<string, object>)ob, globalTypes, bt, null));

                else if (ob is List<object>)
                    col.Add(((List<object>)ob).ToArray());

                else
                    col.Add(ob);
            }
            return col;
        }

        private object CreateStringKeyDictionary(Dictionary<string, object> reader, Type pt, Type[] types, Dictionary<string, object> globalTypes)
        {
            var col = (IDictionary)Reflection.Instance.FastCreateInstance(pt);
            Type t1 = null;
            Type t2 = null;
            if (types != null)
            {
                t1 = types[0];
                t2 = types[1];
            }

            foreach (KeyValuePair<string, object> values in reader)
            {
                var key = values.Key;
                object val = null;
                if (values.Value is Dictionary<string, object>)
                    val = ParseDictionary((Dictionary<string, object>)values.Value, globalTypes, t2, null);
                else
                    val = values.Value;
                col.Add(key, val);
            }

            return col;
        }

        private object CreateDictionary(List<object> reader, Type pt, Type[] types, Dictionary<string, object> globalTypes)
        {
            IDictionary col = (IDictionary)Reflection.Instance.FastCreateInstance(pt);
            Type t1 = null;
            Type t2 = null;
            if (types != null)
            {
                t1 = types[0];
                t2 = types[1];
            }

            foreach (Dictionary<string, object> values in reader)
            {
                object key = values["k"];
                object val = values["v"];

                if (key is Dictionary<string, object>)
                    key = ParseDictionary((Dictionary<string, object>)key, globalTypes, t1, null);

                if (val is Dictionary<string, object>)
                    val = ParseDictionary((Dictionary<string, object>)val, globalTypes, t2, null);

                col.Add(key, val);
            }

            return col;
        }

#if !SILVERLIGHT
        private DataSet CreateDataset(Dictionary<string, object> reader, Dictionary<string, object> globalTypes)
        {
            DataSet ds = new DataSet();
            ds.EnforceConstraints = false;
            ds.BeginInit();

            // read dataset schema here
            ReadSchema(reader, ds, globalTypes);

            foreach (KeyValuePair<string, object> pair in reader)
            {
                if (pair.Key == "$type" || pair.Key == "$schema") continue;

                List<object> rows = (List<object>)pair.Value;
                if (rows == null) continue;

                DataTable dt = ds.Tables[pair.Key];
                ReadDataTable(rows, dt);
            }

            ds.EndInit();

            return ds;
        }

        private void ReadSchema(Dictionary<string, object> reader, DataSet ds, Dictionary<string, object> globalTypes)
        {
            var schema = reader["$schema"];

            if (schema is string)
            {
                TextReader tr = new StringReader((string)schema);
                ds.ReadXmlSchema(tr);
            }
            else
            {
                DatasetSchema ms = (DatasetSchema)ParseDictionary((Dictionary<string, object>)schema, globalTypes, typeof(DatasetSchema), null);
                ds.DataSetName = ms.Name;
                for (int i = 0; i < ms.Info.Count; i += 3)
                {
                    if (ds.Tables.Contains(ms.Info[i]) == false)
                        ds.Tables.Add(ms.Info[i]);
                    ds.Tables[ms.Info[i]].Columns.Add(ms.Info[i + 1], Type.GetType(ms.Info[i + 2]));
                }
            }
        }

        private void ReadDataTable(List<object> rows, DataTable dt)
        {
            dt.BeginInit();
            dt.BeginLoadData();

            foreach (List<object> row in rows)
            {
                object[] v = new object[row.Count];
                row.CopyTo(v, 0);
                dt.Rows.Add(v);
            }

            dt.EndLoadData();
            dt.EndInit();
        }

        DataTable CreateDataTable(Dictionary<string, object> reader, Dictionary<string, object> globalTypes)
        {
            var dt = new DataTable();

            // read dataset schema here
            var schema = reader["$schema"];

            if (schema is string)
            {
                TextReader tr = new StringReader((string)schema);
                dt.ReadXmlSchema(tr);
            }
            else
            {
                var ms = (DatasetSchema)this.ParseDictionary((Dictionary<string, object>)schema, globalTypes, typeof(DatasetSchema), null);
                dt.TableName = ms.Info[0];
                for (int i = 0; i < ms.Info.Count; i += 3)
                {
                    dt.Columns.Add(ms.Info[i + 1], Type.GetType(ms.Info[i + 2]));
                }
            }

            foreach (var pair in reader)
            {
                if (pair.Key == "$type" || pair.Key == "$schema")
                    continue;

                var rows = (List<object>)pair.Value;
                if (rows == null)
                    continue;

                if (!dt.TableName.Equals(pair.Key, StringComparison.InvariantCultureIgnoreCase))
                    continue;

                ReadDataTable(rows, dt);
            }

            return dt;
        }
#endif
    }
}