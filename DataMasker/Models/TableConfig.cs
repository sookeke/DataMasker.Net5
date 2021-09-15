using System.Collections.Generic;
using System.ComponentModel;
using System.Configuration;
using System.IO;
using Newtonsoft.Json;

namespace DataMasker.Models
{
    //private static readonly string _schema = Directory.GetCurrentDirectory() + ConfigurationManager.AppSettings["_exceptionpath"];
    /// <summary>
    /// TableConfig
    /// </summary>
    public class TableConfig
    {
        //private const string _schema = Directory.GetCurrentDirectory() + ConfigurationManager.AppSettings["_exceptionpath"];
        /// <summary>
        /// The name of the table
        /// </summary>
        /// <value>
        /// The name.
        /// </value>
        [JsonRequired]
        public string Name { get; set; }

        /// <summary>
        /// The primary key of the target table, used to update the data
        /// </summary>
        /// <value>
        /// The primary key column.
        /// </value>
        [JsonRequired]
        public string PrimaryKeyColumn { get; set; }
        public string RowCount { get; set; }

        [JsonRequired]
        public string Schema { get; set; }
        [JsonRequired]
        public string TargetSchema { get; set; }

        /// <summary>
        /// List of <see cref="ColumnConfig"/>
        /// </summary>
        /// <value>
        /// The columns.
        /// </value>
        [JsonRequired]
        public IList<ColumnConfig> Columns { get; set; }


    }
}
