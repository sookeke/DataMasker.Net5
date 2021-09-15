using ExcelDataReader;
using Newtonsoft.Json;
using Newtonsoft.Json.Serialization;
using QuickType;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Data.OleDb;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
//using System.Web.Script.Serialization;

namespace DataMasker.Models
{
    public static class ExcelToJson
    {
        public static string DateTimeFormat { get; set; }

        public static string ToJson(string szFilePath)
        {
            Encoding.RegisterProvider(System.Text.CodePagesEncodingProvider.Instance);
            if (szFilePath == null) { throw new ArgumentNullException("input excel sheet"); }
            var settings = new JsonSerializerSettings { ContractResolver = new SpecialContractResolver() };
            string jsonPath = Path.GetDirectoryName(szFilePath) + "\\" + Path.GetFileNameWithoutExtension(szFilePath) + ".json";
            try
            {
                using (FileStream fileStream = new FileStream(szFilePath, FileMode.Open, FileAccess.Read))
                {
                    IExcelDataReader excelReader = null;
                    if (Path.GetExtension(szFilePath).ToUpper() == ".XLSX")
                    {
                        excelReader = ExcelReaderFactory.CreateOpenXmlReader(fileStream);
                    }
                    else if (Path.GetExtension(szFilePath).ToUpper() == ".XLS")
                    {
                        //1. Reading from a binary Excel file ('97-2003 format; *.xls)
                        excelReader = ExcelReaderFactory.CreateBinaryReader(fileStream);
                    }
                    else
                        Console.WriteLine(new ArgumentException("Invalid sheet type",Path.GetFileName(szFilePath)));
                    if (excelReader != null)
                    {
                        System.Data.DataSet result = excelReader.AsDataSet(new ExcelDataSetConfiguration()
                        {
                            ConfigureDataTable = (_) => new ExcelDataTableConfiguration()
                            {
                                UseHeaderRow = true
                            }
                        });


                        //Set to Table
                        var dataTable = result.Tables[0].AsDataView().ToTable();                       
                        var json = JsonConvert.SerializeObject(dataTable, Formatting.Indented, new JsonSerializerSettings { ContractResolver = new SpecialContractResolver() });                       
                        var o = json.Replace("null", "\"\"");
                        using (var tw = new StreamWriter(jsonPath, false))
                        {
                            tw.WriteLine(o.ToString());
                            tw.Close();
                        }
                        var oo = JsonObject(o);
                        //Console.WriteLine("{0}{1}", "converted output: ".ToUpper() + Environment.NewLine,o);
                    }
                  
                }
            }
            catch (IOException ex)
            {

                Console.WriteLine("Exception caught: {0}", ex.Message);
            }
            finally
            {
                //Console.WriteLine("Result: {0}",);
            }
            if (File.Exists(jsonPath))
            {
                return jsonPath;
            }
            else
            {
                Console.WriteLine(new IOException("exception caught: could not find file: " + jsonPath));
                Console.WriteLine("Program will exit: Press ENTER to exist..");
                Console.ReadLine();
                System.Environment.Exit(1);
            }
            return "";
        }
        public static object[] FromJson(string input)
        {
            if (input == null) { throw new ArgumentNullException("input"); }
            //var serializer = new JavaScriptSerializer();
            object[] result = JsonConvert.DeserializeObject(input, typeof(object[])) as object[];

            return result;
        }
        public static object[] JsonObject(string json)
        {
            var settings = new JsonSerializerSettings { ContractResolver = new SpecialContractResolver() };

            if (json == null) { throw new ArgumentNullException("input json"); }
            object[] result = JsonConvert.DeserializeObject(json, typeof(object[]),settings) as object[];
            return result;
        }
        public class NullableValueProvider : IValueProvider
        {
            private readonly object _defaultValue;
            private readonly IValueProvider _underlyingValueProvider;


            public NullableValueProvider(MemberInfo memberInfo, Type underlyingType)
            {
                _underlyingValueProvider = new ReflectionValueProvider(memberInfo);
                _defaultValue = Activator.CreateInstance(underlyingType);
            }

            public void SetValue(object target, object value)
            {
                _underlyingValueProvider.SetValue(target, value);
            }

            public object GetValue(object target)
            {
                return _underlyingValueProvider.GetValue(target) ?? _defaultValue;
            }
        }

        public class SpecialContractResolver : DefaultContractResolver
        {
            protected override IValueProvider CreateMemberValueProvider(MemberInfo member)
            {
                if (member.MemberType == MemberTypes.Property)
                {
                    var pi = (PropertyInfo)member;
                    if (pi.PropertyType.IsGenericType && pi.PropertyType.GetGenericTypeDefinition() == typeof(Nullable<>))
                    {
                        return new NullableValueProvider(member, pi.PropertyType.GetGenericArguments().First());
                    }
                }
                else if (member.MemberType == MemberTypes.Field)
                {
                    var fi = (FieldInfo)member;
                    if (fi.FieldType.IsGenericType && fi.FieldType.GetGenericTypeDefinition() == typeof(Nullable<>))
                        return new NullableValueProvider(member, fi.FieldType.GetGenericArguments().First());
                }

                return base.CreateMemberValueProvider(member);
            }
        }

        public static string ToJson(DataTable table)
        {
            if (table == null) { throw new ArgumentNullException("table"); }

            StringBuilder result = new StringBuilder(string.Empty);
            if (table.Rows.Count > 0)
            {
                result.Append("[");
                foreach (DataRow row in table.Rows)
                {
                    result.Append(ToJson(row));
                }
                result.Append("]");
            }
            return result.ToString();
        }
        public static string ToJson(DataSet dataSet)
        {
            if (dataSet == null) { throw new ArgumentNullException("dataSet"); }

            StringBuilder result = new StringBuilder(string.Empty);
            if (dataSet.Tables.Count > 0)
            {
                result.Append("[");
                foreach (DataTable table in dataSet.Tables)
                {
                    result.Append(ToJson(table));
                }
                result.Append("]");
            }
            return result.ToString();
        }
        public static string ToJson(DataRow row)
        {
            DateTimeFormat = JsonHelper();
            if (row == null) { throw new ArgumentNullException("row"); }
            if (string.IsNullOrWhiteSpace(DateTimeFormat)) { throw new ArgumentNullException("DateTimeFormat"); }

            StringBuilder result = new StringBuilder(string.Empty);
            if (row.ItemArray.Count() > 0)
            {
                //var serializer = new JavaScriptSerializer();
                string json = JsonConvert.SerializeObject(row.ItemArray);

                // Replace Date(...) by a string in the format found in the property [DateTimeFormat].
                var matchEvaluator = new MatchEvaluator(ConvertJsonDateToDateString);
                var regex = new Regex(@"\/Date\((-?\d+)\)\/");
                json = regex.Replace(json, matchEvaluator);

                result.Append(json);
            }
            return result.ToString();
        }
        public static string ConvertJsonDateToDateString(Match match)
        {
            if (match == null) { throw new ArgumentNullException("match"); }
            if (string.IsNullOrWhiteSpace(DateTimeFormat)) { throw new ArgumentNullException("DateTimeFormat"); }

            string result = string.Empty;
            DateTime dt = new DateTime(1970, 1, 1); // Epoch date, used by the JavaScriptSerializer to represent starting point of datetime in JSON.
            dt = dt.AddMilliseconds(long.Parse(match.Groups[1].Value));
            dt = dt.ToLocalTime();
            result = dt.ToString(DateTimeFormat);
            return result;
        }
        public static string JsonHelper()
        {
            DateTimeFormat = "yyyy-MM-dd HH:mm:ss";
            return DateTimeFormat;
        }

    }

}
