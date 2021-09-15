using MySqlX.XDevAPI.Common;
using System;
using System.Collections.Generic;
using System.Text;

namespace DataMasker.Main
{
    public class DataMaskerAppConfig
    {
        public string ExcelSheetPath { get; set; }
        public string TestJson { get; set; }
        public string RunTestJson { get; set; }
        public string DatabaseName { get; set; }
        public string WriteDML { get; set; }
        public string MaskedCopyDatabase { get; set; }
        public MaskValidation Validation { get; set; }
 
        public string Hostname { get; set; }
        public string DataSourceType { get; set; }
    }
}
