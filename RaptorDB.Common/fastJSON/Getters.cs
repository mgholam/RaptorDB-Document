using System;
using System.Collections.Generic;

namespace fastJSON
{
    public class Getters
    {
        public string Name;
        public JSON.GenericGetter Getter;
        public Type propertyType;
    }

    public class DatasetSchema
    {
        public List<string> Info { get; set; }
        public string Name { get; set; }
    }
}
