using DataMasker.Models;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DataMasker.Examples
{
    class JsonClass
    {
    }
    public class Config1
    {
        public string connectionString { get; set; }
        public string connectionStringPrd { get; set; }
        public string Databasename { get; set; }
        public string Hostname { get; set; }
    }

    public class DataSource
    {
        public string type { get; set; }
        public Config1 config { get; set; }
    }

    public class DataGeneration
    {
        public string locale { get; set; }
    }

    public class Column
    {
        public string name { get; set; }
        public string type { get; set; }
        public string min { get; set; }
        public string max { get; set; }
        public bool ignore { get; set; }
        public string Operator { get; set; }
        public bool? retainNullValues { get; set; }
        public bool? RetainEmptyStringValues { get; set; }
        public string useGenderColumn { get; set; }
        public string StringFormatPattern { get; set; }
        [DefaultValue(null)]
        [JsonProperty(DefaultValueHandling = DefaultValueHandling.Populate)]
        public object UseValue { get; set; }

        [DefaultValue(null)]
        [JsonProperty(DefaultValueHandling = DefaultValueHandling.Populate)]
        public bool? Preview { get; set; }
    }

    public class Table
    {
        public string name { get; set; }
        public string primaryKeyColumn { get; set; }
        public string RowCount { get; set; }
        public string Schema { get; set; }
        public string TargetSchema { get; set; }
        public List<Column> columns { get; set; }
    }

    public class RootObject1
    {
        public DataSource dataSource { get; set; }
        public DataGeneration dataGeneration { get; set; }
        public IList<TableConfig> tables { get; set; }
    }
}
