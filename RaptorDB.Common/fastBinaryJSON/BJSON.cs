using System;
using System.Collections;
using System.Collections.Generic;
#if !SILVERLIGHT
using System.Data;
#endif
using System.IO;
using System.Collections.Specialized;
using fastJSON;

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
        public const byte DATETIMEOFFSET = 27;
        public const byte ARRAY_TYPED = 28;
        public const byte TYPES_POINTER = 29;
        public const byte TIMESPAN = 30;
        public const byte ARRAY_TYPED_LONG = 31;
        public const byte NAME_UNI = 32;
    }

    public class typedarray
    {
        public string typename;
        public int count;
        public List<object> data = new List<object>();
    }



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
        /// Enable fastBinaryJSON extensions $types, $type, $map (default = True)
        /// </summary>
        public bool UseExtensions = true;
        /// <summary>
        /// Anonymous types have read only properties 
        /// </summary>
        public bool EnableAnonymousTypes = false;
        /// <summary>
        /// Use the UTC date format (default = False)
        /// </summary>
        public bool UseUTCDateTime = false;
        /// <summary>
        /// Ignore attributes to check for (default : XmlIgnoreAttribute, NonSerialized)
        /// </summary>
        public List<Type> IgnoreAttributes = new List<Type> { typeof(System.Xml.Serialization.XmlIgnoreAttribute), typeof(NonSerializedAttribute) };
        /// <summary>
        /// If you have parametric and no default constructor for you classes (default = False)
        /// 
        /// IMPORTANT NOTE : If True then all initial values within the class will be ignored and will be not set
        /// </summary>
        public bool ParametricConstructorOverride = false;
        /// <summary>
        /// Maximum depth the serializer will go to to avoid loops (default = 20 levels)
        /// </summary>
        public short SerializerMaxDepth = 20;
        /// <summary>
        /// Use typed arrays t[] into object = t[] not object[] (default = true)
        /// </summary>
        public bool UseTypedArrays = true;
        /// <summary>
        /// Backward compatible Typed array type name as UTF8 (default = false -> fast v1.5 unicode)
        /// </summary>
        public bool v1_4TypedArray = false;

        public void FixValues()
        {
            if (UseExtensions == false) // disable conflicting params
                UsingGlobalTypes = false;

            if (EnableAnonymousTypes)
                ShowReadOnlyProperties = true;
        }

        internal BJSONParameters MakeCopy()
        {
            return new BJSONParameters
            {  
                UseOptimizedDatasetSchema = UseOptimizedDatasetSchema,
                ShowReadOnlyProperties = ShowReadOnlyProperties,
                EnableAnonymousTypes = EnableAnonymousTypes,
                UsingGlobalTypes = UsingGlobalTypes,
                IgnoreAttributes = new List<Type>(IgnoreAttributes),
                UseUnicodeStrings = UseUnicodeStrings,
                SerializeNulls = SerializeNulls,
                ParametricConstructorOverride = ParametricConstructorOverride,
                SerializerMaxDepth = SerializerMaxDepth,
                UseTypedArrays = UseTypedArrays,
                UseExtensions = UseExtensions,
                UseUTCDateTime = UseUTCDateTime,
                v1_4TypedArray = v1_4TypedArray//,
                //OptimizeSize = OptimizeSize

            };
        }
    }

    public static class BJSON
    {
        /// <summary>
        /// Globally set-able parameters for controlling the serializer
        /// </summary>
        public static BJSONParameters Parameters = new BJSONParameters();
        /// <summary>
        /// Parse a json and generate a Dictionary&lt;string,object&gt; or List&lt;object&gt; structure
        /// </summary>
        /// <param name="json"></param>
        /// <returns></returns>
        public static object Parse(byte[] json)
        {
            return new BJsonParser(json, Parameters.UseUTCDateTime, Parameters.v1_4TypedArray).Decode();
        }
#if NET4
        /// <summary>
        /// Create a .net4 dynamic object from the binary json byte array
        /// </summary>
        /// <param name="json"></param>
        /// <returns></returns>
        public static dynamic ToDynamic(byte[] json)
        {
            return new DynamicJson(json);
        }
#endif
        /// <summary>
        /// Register custom type handlers for your own types not natively handled by fastBinaryJSON
        /// </summary>
        /// <param name="type"></param>
        /// <param name="serializer"></param>
        /// <param name="deserializer"></param>
        public static void RegisterCustomType(Type type, Reflection.Serialize serializer, Reflection.Deserialize deserializer)
        {
            Reflection.Instance.RegisterCustomType(type, serializer, deserializer);
        }
        /// <summary>
        /// Create a binary json representation for an object
        /// </summary>
        /// <param name="obj"></param>
        /// <returns></returns>
        public static byte[] ToBJSON(object obj)
        {
            return ToBJSON(obj, Parameters);
        }
        /// <summary>
        /// Create a binary json representation for an object with parameter override on this call
        /// </summary>
        /// <param name="obj"></param>
        /// <param name="param"></param>
        /// <returns></returns>
        public static byte[] ToBJSON(object obj, BJSONParameters param)
        {
            param.FixValues();
            param = param.MakeCopy();
            Type t = null;
            if (obj == null)
                return new byte[] { TOKENS.NULL };
            if (obj.GetType().IsGenericType)
                t = Reflection.Instance.GetGenericTypeDefinition(obj.GetType());// obj.GetType().GetGenericTypeDefinition();
            if (t == typeof(Dictionary<,>) || t == typeof(List<>))
                param.UsingGlobalTypes = false;
            // FEATURE : enable extensions when you can deserialize anon types
            if (param.EnableAnonymousTypes) { param.UseExtensions = false; param.UsingGlobalTypes = false; }

            return new BJSONSerializer(param).ConvertToBJSON(obj);
        }
        /// <summary>
        /// Fill a given object with the binary json represenation
        /// </summary>
        /// <param name="input"></param>
        /// <param name="json"></param>
        /// <returns></returns>
        public static object FillObject(object input, byte[] json)
        {
            return new deserializer(Parameters).FillObject(input, json);
        }
        /// <summary>
        /// Create a generic object from the json
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="json"></param>
        /// <returns></returns>
        public static T ToObject<T>(byte[] json)
        {
            return new deserializer(Parameters).ToObject<T>(json);
        }
        /// <summary>
        /// Create a generic object from the json with parameter override on this call
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="json"></param>
        /// <param name="param"></param>
        /// <returns></returns>
        public static T ToObject<T>(byte[] json, BJSONParameters param)
        {
            return new deserializer(param).ToObject<T>(json);
        }
        /// <summary>
        /// Create an object from the json 
        /// </summary>
        /// <param name="json"></param>
        /// <returns></returns>
        public static object ToObject(byte[] json)
        {
            return new deserializer(Parameters).ToObject(json, null);
        }
        /// <summary>
        /// Create an object from the json with parameter override on this call
        /// </summary>
        /// <param name="json"></param>
        /// <param name="param"></param>
        /// <returns></returns>
        public static object ToObject(byte[] json, BJSONParameters param)
        {
            param.FixValues();
            param = param.MakeCopy();
            return new deserializer(param).ToObject(json, null);
        }
        /// <summary>
        /// Create a typed object from the json
        /// </summary>
        /// <param name="json"></param>
        /// <param name="type"></param>
        /// <returns></returns>
        public static object ToObject(byte[] json, Type type)
        {
            return new deserializer(Parameters).ToObject(json, type);
        }
        /// <summary>
        /// Clear the internal reflection cache so you can start from new (you will loose performance)
        /// </summary>
        public static void ClearReflectionCache()
        {
            Reflection.Instance.ClearReflectionCache();
        }
        /// <summary>
        /// Deep copy an object i.e. clone to a new object
        /// </summary>
        /// <param name="obj"></param>
        /// <returns></returns>
        public static object DeepCopy(object obj)
        {
            return new deserializer(Parameters).ToObject(ToBJSON(obj));
        }
    }

    internal class deserializer
    {
        public deserializer(BJSONParameters param)
        {
            _params = param;
            _params = param.MakeCopy();
        }

        private BJSONParameters _params;
        private Dictionary<object, int> _circobj = new Dictionary<object, int>();
        private Dictionary<int, object> _cirrev = new Dictionary<int, object>();

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
            //_params.FixValues();
            Type t = null;
            if (type != null && type.IsGenericType)
                t = Reflection.Instance.GetGenericTypeDefinition(type);// type.GetGenericTypeDefinition();
            _globalTypes = _params.UsingGlobalTypes;
            if (t == typeof(Dictionary<,>) || t == typeof(List<>))
                _globalTypes = false;

            var o = new BJsonParser(json, _params.UseUTCDateTime, _params.v1_4TypedArray).Decode();
            if (type?.IsEnum == true) return CreateEnum(type, o);
#if !SILVERLIGHT
            if (type != null && type == typeof(DataSet))
                return CreateDataset(o as Dictionary<string, object>, null);

            if (type != null && type == typeof(DataTable))
                return CreateDataTable(o as Dictionary<string, object>, null);
#endif
            if (o is typedarray)
            {
                return ParseTypedArray(new Dictionary<string, object>(), o);
            }
            if (o is IDictionary)
            {
                if (type != null && t == typeof(Dictionary<,>)) // deserialize a dictionary
                    return RootDictionary(o, type);
                else // deserialize an object
                    return ParseDictionary(o as Dictionary<string, object>, null, type, null);
            }

            if (o is List<object>)
            {
                if (type != null && t == typeof(Dictionary<,>)) // kv format
                    return RootDictionary(o, type);

                if (type != null && t == typeof(List<>)) // deserialize to generic list
                    return RootList(o, type);

                if (type == typeof(Hashtable))
                    return RootHashTable((List<object>)o);
                else if (type == null)
                {
                    List<object> l = (List<object>)o;
                    if (l.Count > 0 && l[0].GetType() == typeof(Dictionary<string, object>))
                    {
                        Dictionary<string, object> globals = new Dictionary<string, object>();
                        List<object> op = new List<object>();
                        // try to get $types 
                        foreach (var i in l)
                            op.Add(ParseDictionary((Dictionary<string, object>)i, globals, null, null));
                        return op;
                    }
                    return l.ToArray();
                }
            }
            else if (type != null && o.GetType() != type)
                return ChangeType(o, type);

            return o;
        }

        private object ChangeType(object o, Type type)
        {
            if (Reflection.Instance.IsTypeRegistered(type))
                return Reflection.Instance.CreateCustom((string)o, type);
            else
                return o;
        }

        public object FillObject(object input, byte[] json)
        {
            _params.FixValues();
            Dictionary<string, object> ht = new BJsonParser(json, _params.UseUTCDateTime, _params.v1_4TypedArray).Decode() as Dictionary<string, object>;
            if (ht == null) return null;
            return ParseDictionary(ht, null, input.GetType(), input);
        }

        private object RootHashTable(List<object> o)
        {
            Hashtable h = new Hashtable();

            foreach (Dictionary<string, object> values in o)
            {
                object key = values["k"];
                object val = values["v"];
                if (key is Dictionary<string, object>)
                    key = ParseDictionary((Dictionary<string, object>)key, null, typeof(object), null);

                if (val is Dictionary<string, object>)
                    val = ParseDictionary((Dictionary<string, object>)val, null, typeof(object), null);

                h.Add(key, val);
            }

            return h;
        }

        private object RootList(object parse, Type type)
        {
            Type[] gtypes = Reflection.Instance.GetGenericArguments(type);// type.GetGenericArguments();
            IList o = (IList)Reflection.Instance.FastCreateList(type, ((IList)parse).Count);
            Dictionary<string, object> globals = new Dictionary<string, object>();

            foreach (var k in (IList)parse)
            {
                _globalTypes = false;
                object v = k;
                if (k is Dictionary<string, object>)
                    v = ParseDictionary(k as Dictionary<string, object>, globals, gtypes[0], null);
                else
                    v = k;

                o.Add(v);
            }
            return o;
        }

        private object RootDictionary(object parse, Type type)
        {
            Type[] gtypes = Reflection.Instance.GetGenericArguments(type);
            Type t1 = null;
            Type t2 = null;
            if (gtypes != null)
            {
                t1 = gtypes[0];
                t2 = gtypes[1];
            }
            var arraytype = t2.GetElementType();

            if (parse is Dictionary<string, object>)
            {
                IDictionary o = (IDictionary)Reflection.Instance.FastCreateInstance(type);

                foreach (var kv in (Dictionary<string, object>)parse)
                {
                    _globalTypes = false;
                    object v;
                    object k = kv.Key;
                    if (t2.Name.StartsWith("Dictionary")) // deserialize a dictionary
                        v = RootDictionary(kv.Value, t2);

                    else if (kv.Value is Dictionary<string, object>)
                        v = ParseDictionary(kv.Value as Dictionary<string, object>, null, t2, null);

                    else if (t2 == typeof(byte[]))
                        v = kv.Value;

                    else if (gtypes != null && t2.IsArray)
                        v = CreateArray((List<object>)kv.Value, t2, arraytype, null);

                    else if (kv.Value is IList)
                        v = CreateGenericList((List<object>)kv.Value, t2, t1, null);

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
            if (type == typeof(NameValueCollection))
                return CreateNV(d);
            if (type == typeof(StringDictionary))
                return CreateSD(d);

            if (d.TryGetValue("$i", out tn))
            {
                object v = null;
                _cirrev.TryGetValue((int)tn, out v);
                return v;
            }

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

            if (globaltypes != null)
                _globalTypes = true;

            bool found = d.TryGetValue("$type", out tn);
#if !SILVERLIGHT
            if (found == false && type == typeof(System.Object))
            {
                return d;  // CreateDataset(d, globaltypes);
            }
#endif
            if (found)
            {
                if (_globalTypes && globaltypes != null)
                {
                    object tname = "";
                    if (globaltypes != null && globaltypes.TryGetValue((string)tn, out tname))
                        tn = tname;
                }
                type = Reflection.Instance.GetTypeFromCache((string)tn);
            }

            if (type == null)
                throw new Exception("Cannot determine type");

            string typename = type.FullName;
            object o = input;
            if (o == null)
            {
                if (_params.ParametricConstructorOverride)
                    o = System.Runtime.Serialization.FormatterServices.GetUninitializedObject(type);
                else
                    o = Reflection.Instance.FastCreateInstance(type);
            }

            int circount = 0;
            if (_circobj.TryGetValue(o, out circount) == false)
            {
                circount = _circobj.Count + 1;
                _circobj.Add(o, circount);
                _cirrev.Add(circount, o);
            }

            Dictionary<string, myPropInfo> props = Reflection.Instance.Getproperties(type, typename, _params.ShowReadOnlyProperties);//, Reflection.Instance.IsTypeRegistered(type));
            foreach (var kv in d)
            {
                var n = kv.Key;
                var v = kv.Value;
                string name = n.ToLowerInvariant();
                myPropInfo pi;
                if (props.TryGetValue(name, out pi) == false)
                    continue;
                if (pi.CanWrite)
                {
                    //object v = d[n];

                    if (v != null)
                    {
                        object oset = v;
                        if (v is typedarray)
                        {
                            oset = ParseTypedArray(globaltypes, v);
                        }
                        else
                        {
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
                                case myPropInfoType.Custom:
                                    oset = Reflection.Instance.CreateCustom((string)v, pi.pt);
                                    break;
                                case myPropInfoType.Enum:
                                    oset = CreateEnum(pi.pt, v);
                                    break;
                                case myPropInfoType.StringKeyDictionary:
                                    oset = CreateStringKeyDictionary((Dictionary<string, object>)v, pi.pt, pi.GenericTypes, globaltypes);
                                    break;
                                case myPropInfoType.Hashtable:
                                case myPropInfoType.Dictionary:
                                    oset = CreateDictionary((List<object>)v, pi.pt, pi.GenericTypes, globaltypes);
                                    break;
                                case myPropInfoType.NameValue: oset = CreateNV((Dictionary<string, object>)v); break;
                                case myPropInfoType.StringDictionary: oset = CreateSD((Dictionary<string, object>)v); break;
                                case myPropInfoType.Array:
                                    oset = CreateArray((List<object>)v, pi.pt, pi.bt, globaltypes);
                                    break;
                                default:
                                    {
                                        if (pi.IsGenericType && pi.IsValueType == false)
                                            oset = CreateGenericList((List<object>)v, pi.pt, pi.bt, globaltypes);
                                        else if ((pi.IsClass || pi.IsStruct || pi.IsInterface) && v is Dictionary<string, object>)
                                        {
                                            var oo = (Dictionary<string, object>)v;
                                            if (oo.ContainsKey("$schema"))
                                                oset = CreateDataset(oo, globaltypes);
                                            else
                                                oset = ParseDictionary(oo, globaltypes, pi.pt, input);
                                        }
                                        else if (v is List<object>)
                                            oset = CreateArray((List<object>)v, pi.pt, typeof(object), globaltypes);
                                        break;
                                    }
                            }
                        }
                        o = pi.setter(o, oset);
                    }
                }
            }
            return o;
        }

        private object ParseTypedArray(Dictionary<string, object> globaltypes, object v)
        {
            object oset;
            var ta = (typedarray)v;
            var t = Reflection.Instance.GetTypeFromCache(ta.typename);
            IList a = Array.CreateInstance(t, ta.count);
            int i = 0;
            foreach (var dd in ta.data)
            {
                object oo = null;
                if (dd == null)
                    oo = null;
                else if (dd is typedarray)
                    oo = ParseTypedArray(globaltypes, dd);
                else if (dd is Dictionary<string, object>)
                    oo = ParseDictionary((Dictionary<string, object>)dd, globaltypes, t, null);
                else if (dd is List<object>)
                    oo = CreateArray((List<object>)dd, t, t.GetElementType(), globaltypes);
                else
                    oo = dd;
                a[i++] = oo;
            }
            oset = a;
            return oset;
        }

        private StringDictionary CreateSD(Dictionary<string, object> d)
        {
            StringDictionary nv = new StringDictionary();

            foreach (var o in d)
                nv.Add(o.Key, (string)o.Value);

            return nv;
        }

        private NameValueCollection CreateNV(Dictionary<string, object> d)
        {
            NameValueCollection nv = new NameValueCollection();

            foreach (var o in d)
                nv.Add(o.Key, (string)o.Value);

            return nv;
        }

        private object CreateEnum(Type pt, object v)
        {
            // FEATURE : optimize create enum
#if !SILVERLIGHT
            return Enum.Parse(pt, v.ToString());
#else
            return Enum.Parse(pt, v, true);
#endif
        }

        private object CreateArray(List<object> data, Type pt, Type bt, Dictionary<string, object> globalTypes)
        {
            if (bt == null)
                bt = typeof(object);

            Array col = Array.CreateInstance(bt, data.Count);
            var arraytype = bt.GetElementType();
            // create an array of objects
            for (int i = 0; i < data.Count; i++)// each (object ob in data)
            {
                object ob = data[i];
                if (ob == null)
                {
                    continue;
                }
                if (ob is IDictionary)
                    col.SetValue(ParseDictionary((Dictionary<string, object>)ob, globalTypes, bt, null), i);
                else if (ob is ICollection)
                    col.SetValue(CreateArray((List<object>)ob, bt, arraytype, globalTypes), i);
                else
                    col.SetValue(ob, i);
            }

            return col;
        }

        private object CreateGenericList(List<object> data, Type pt, Type bt, Dictionary<string, object> globalTypes)
        {
            if (pt != typeof(object))
            {
                IList col = (IList)Reflection.Instance.FastCreateList(pt, data.Count);
                // create an array of objects
                foreach (object ob in data)
                {
                    if (ob is IDictionary)
                        col.Add(ParseDictionary((Dictionary<string, object>)ob, globalTypes, bt, null));

                    else if (ob is List<object>)
                    {
                        if (bt.IsGenericType)
                            col.Add((List<object>)ob);
                        else
                            col.Add(((List<object>)ob).ToArray());
                    }
                    else if(ob is typedarray)
                        col.Add(((typedarray)ob).data.ToArray());
                    else
                        col.Add(ob);
                }
                return col;
            }
            return data;
        }

        private object CreateStringKeyDictionary(Dictionary<string, object> reader, Type pt, Type[] types, Dictionary<string, object> globalTypes)
        {
            var col = (IDictionary)Reflection.Instance.FastCreateInstance(pt);
            Type arraytype = null;
            Type t2 = null;
            if (types != null)
                t2 = types[1];

            Type generictype = null;
            var ga = Reflection.Instance.GetGenericArguments(t2);
            if (ga.Length > 0)
                generictype = ga[0];
            arraytype = t2.GetElementType();

            foreach (KeyValuePair<string, object> values in reader)
            {
                var key = values.Key;
                object val = null;

                if (values.Value is Dictionary<string, object>)
                    val = ParseDictionary((Dictionary<string, object>)values.Value, globalTypes, t2, null);

                else if (types != null && t2.IsArray)
                {
                    if (values.Value is Array)
                        val = values.Value;
                    else
                        val = CreateArray((List<object>)values.Value, t2, arraytype, globalTypes);
                }
                else if (values.Value is IList)
                    val = CreateGenericList((List<object>)values.Value, t2, generictype, globalTypes);

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

                if (typeof(IDictionary).IsAssignableFrom(t2))
                    val = RootDictionary(val, t2);

                else if (val is Dictionary<string, object>)
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