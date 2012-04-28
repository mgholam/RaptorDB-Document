using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.CodeDom.Compiler;
using System.IO;
using System.Reflection;
using RaptorDB.Views;

namespace RaptorDB.Mapping
{
    public class MapEngine 
    {
        public MapEngine(IMapAPI vm)
        {
            _api = vm;
        }

        private IMapAPI _api;
        SafeDictionary<string, IMAPFunction> _mapcache = new SafeDictionary<string, IMAPFunction>();
        public bool DebugMode = false;
        private ILog _log = LogManager.GetLogger(typeof(MapEngine));


//        public void test()
//        {
//            Views.View v = new Views.View();
//            v.SchemaColumns = new ViewRowDefinition();
//            v.Name = "test";
//            v.FireOnTypes = new List<string>();
//            v.FireOnTypes.Add("BizFX.Entity.Return, BizFX.Entity, Version=2.0.0.0, Culture=neutral, PublicKeyToken=e5d192f5e46064af");
//            v.SchemaColumns.Columns.Add("Name", DataTypes.String);
//            v.SchemaColumns.Columns.Add("BirthDay", DataTypes.DateTime);
//            v.SchemaColumns.Columns.Add("Address", DataTypes.String);
//            v.SchemaColumns.Columns.Add("Code", DataTypes.Int);
//            v.MapFunctionCode = @"
//List<object[]> q = api.Query(""someview"",""a=1"",0, -1);
//foreach(object[] rr in q)
//{
//    testRow r = new testRow(rr);
//    emit(data.GUID, r.Name, r.BirthDay, r.Address, r.Code*2);
//}
//if(data.IsOK)
//    emit(data.GUID, ""ppp"",DateTime.Now,""hello"",123);
//else
//    api.Log(""error"");
//";
//            //v.SchemaColumns = v.Name;
//            //v.ViewsUsed = new List<Views.ViewRowDefinition>();
//            //v.ViewsUsed.Add(v.SchemaColumns);
//            Compile(v,"RaptorDB\\Views\\");
//        }

        #region [  M A P   C O M P I L E R  ]
        public void Compile(Views.ViewBase view, string mapfolder)
        {
            Directory.CreateDirectory(mapfolder);
            CodeDomProvider cs = CodeDomProvider.CreateProvider("CSharp");
            var _Parameters = new CompilerParameters();

            // create source file
            string code = CreateSourceFile(view, _Parameters);

            // Compile code
            _Parameters.IncludeDebugInformation = false;
            _Parameters.GenerateExecutable = false;
            _Parameters.CompilerOptions = "/optimize";
            _Parameters.OutputAssembly = mapfolder + 
                view.Name +
                //DocID.ToString().Replace("-", "") + 
                ".dll";
            var compilerresult = cs.CompileAssemblyFromSource(_Parameters, code);

            if (compilerresult.Errors.HasErrors)
            {
                foreach (var p in compilerresult.Errors)
                    _log.Error(p.ToString());
            }
            else
            {
                _mapcache.Remove(mapfolder + view.Name + ".dll");
            }
        }

        private string CreateSourceFile(Views.ViewBase view, CompilerParameters _Parameters)
        {
            // create source file from template
            string code = Properties.Resources.CodeTemplate
                .Replace("%USER_CODE%", view.MapFunctionCode)
                .Replace("%COLUMN_COUNT%", view.SchemaColumns.Columns.Count.ToString())
                .Replace("%VIEW_ROW%", CreateViewRows(view.ViewsUsed))
                .Replace("%BASE_TYPE%", Type.GetType(view.FireOnTypes[0]).FullName)
                .Replace("%COLUMNS_PARAMS%", CreateColumnParams(view.SchemaColumns))
                .Replace("%DATA_ASSIGN%", CreateDataAssign(view.SchemaColumns))
                ;

            // extract type information for compile references
            Dictionary<string, string> references = new Dictionary<string, string>();

            foreach (string aqn in view.FireOnTypes)
            {
                Type t = Type.GetType(aqn);
                if (references.ContainsKey(t.FullName) == false)
                    references.Add(t.FullName, t.Assembly.Location);
                // TODO :traverse hierarchy and add all references
            }

            _Parameters.ReferencedAssemblies.Add(this.GetType().Assembly.Location);
            // set reference assemblies
            foreach (string s in references.Values)
                _Parameters.ReferencedAssemblies.Add(s);

            return code;
        }

        private string CreateViewRows(List<Views.ViewRowDefinition> list)
        {
            StringBuilder sb = new StringBuilder();

            string temp = Properties.Resources.ViewRow;

            foreach (var v in list)
            {
                sb.Append( ViewBase.GenerateViewRow(v));
            }

            return sb.ToString();
        }

        private string CreateColumnParams(Views.ViewRowDefinition viewRowDefinition)
        {
            StringBuilder sb = new StringBuilder();

            int i = 0;
            foreach (KeyValuePair<string, DataTypes> kv in viewRowDefinition.Columns)
            {
                sb.Append(kv.Value);
                sb.Append(" ");
                sb.Append(kv.Key);
                i++;
                if (i < viewRowDefinition.Columns.Count)
                    sb.Append(",");
            }

            return sb.ToString();
        }

        private string CreateDataAssign(Views.ViewRowDefinition viewRowDefinition)
        {
            StringBuilder sb = new StringBuilder();
            int i = 1;
            foreach (KeyValuePair<string, DataTypes> kv in viewRowDefinition.Columns)
            {
                sb.Append("data[");
                sb.Append(i.ToString());
                sb.Append("] = ");
                sb.Append(kv.Key);
                sb.AppendLine(";");
                i++;
            }

            return sb.ToString();
        }
        #endregion

        public DataList Execute(string filename,Guid docid, object data)
        {
            IMAPFunction map;
            if (_mapcache.TryGetValue(filename, out map)==false)
            {
                byte[] b = File.ReadAllBytes(filename);
                Assembly a= Assembly.Load(b);
                map = (IMAPFunction)a.CreateInstance("mapnamespace.mapfunction");
                _mapcache.Add(filename, map);
            }
            if (map != null)
            {
                map.CallMapper(docid, data, _api);
                return map.GetRows();
            }
            return null;
        }
    }
}
