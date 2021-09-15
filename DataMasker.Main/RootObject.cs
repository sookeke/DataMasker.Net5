using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DataMasker.Examples
{
    public class RootObject
    {
        [DefaultValue("")]
        [JsonProperty("TABLE_NAME", DefaultValueHandling = DefaultValueHandling.Populate)]

        public string TableName { get; set; }
        [DefaultValue("")]
        [JsonProperty("COLUMN_NAME", DefaultValueHandling = DefaultValueHandling.Populate)]
        public string ColumnName { get; set; }

        [DefaultValue("max")]
        [JsonProperty("ROW_COUNT", DefaultValueHandling = DefaultValueHandling.Populate)]
        public string RowCount { get; set; }
        [DefaultValue("")]
        [JsonProperty("DATA_TYPE", DefaultValueHandling = DefaultValueHandling.Populate)]
        public string DataType { get; set; }

        [JsonProperty("NULLABLE")]
        public string Nullable { get; set; }

        [JsonProperty("DATA_DEFAULT")]
        public string DataDefault { get; set; }
        [DefaultValue(0)]
        [JsonProperty("COLUMN_ID", DefaultValueHandling = DefaultValueHandling.Populate)]
        public long? ColumnId { get; set; } = 0;

        [DefaultValue("")]
        // [JsonProperty(NullValueHandling = NullValueHandling.Ignore)]
        [JsonProperty("COMMENTS", DefaultValueHandling = DefaultValueHandling.Populate, NullValueHandling = NullValueHandling.Ignore)]
        public string Comments { get; set; } = "";

        [JsonProperty("Description")]
        public string Description { get; set; }

        [JsonProperty("Public")]
        public string Public { get; set; }

        [JsonProperty("Personal")]
        public string Personal { get; set; }
        [DefaultValue("")]
        [JsonProperty("PKconstraintName", DefaultValueHandling = DefaultValueHandling.Populate)]
        public string PKconstraintName { get; set; } = "";
        [JsonRequired]
        [JsonProperty("SCHEMA")]
        public string Schema { get; set; }

        [JsonRequired]
        [JsonProperty("TARGETSCHEMA")]
        public string TargetSchema { get; set; }

        [DefaultValue("TRUE")]
        [JsonProperty("Retain NULL", DefaultValueHandling = DefaultValueHandling.Populate)]
        public string RetainNull { get; set; } = "TRUE";

        [DefaultValue("FALSE")]
        [JsonProperty("RetainEmptyString", DefaultValueHandling = DefaultValueHandling.Populate)]
        public string RetainEmptyString { get; set; } = "FALSE";

        [DefaultValue("FALSE")]
        [JsonProperty("Preview", DefaultValueHandling = DefaultValueHandling.Populate)]
        public string Preview { get; set; } = "FALSE";

        [DefaultValue("")]
        [JsonProperty("Min", DefaultValueHandling = DefaultValueHandling.Populate)]
        public string Min { get; set; }
        [DefaultValue("")]
        [JsonProperty("max", DefaultValueHandling = DefaultValueHandling.Populate)]
        public string Max { get; set; }

        [JsonProperty("Sensitive")]
        public string Sensitive { get; set; }
        [DefaultValue("")]
        [JsonProperty("Masking Rule", DefaultValueHandling = DefaultValueHandling.Populate)]
        public string MaskingRule { get; set; }

        [JsonProperty("Rule set by")]
        public string RuleSetBy { get; set; }

        [JsonProperty("Rule Reasoning")]
        public string RuleReasoning { get; set; }

        [JsonProperty("COMPLETED")]
        public string Completed { get; set; }
        [DefaultValue("")]
        [JsonProperty("StringFormat", DefaultValueHandling = DefaultValueHandling.Populate)]
        public string StringFormat { get; set; }
        [DefaultValue(null)]
        [JsonProperty("UseValue", DefaultValueHandling = DefaultValueHandling.Populate)]
        public string UseValue { get; set; }
        [JsonProperty("Conversion Consideration (NEEDS BUSINESS DISCUSSION)")]
        public string Consideration { get; set; }
    }
}
