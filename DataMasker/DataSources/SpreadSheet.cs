using DataMasker.Interfaces;
using DataMasker.Models;
using ExcelDataReader;
using KellermanSoftware.CompareNetObjects;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DataMasker.DataSources
{
    public class SpreadSheet : IDataSource
    {
        //create object = > SpreadSheetTable = > mask = >
        //private static TextFieldParser cvsReader;
        private readonly DataSourceConfig _sourceConfig;
        private readonly string _connectionString;
        private static readonly List<KeyValuePair<object, object>> exceptionBuilder = new List<KeyValuePair<object, object>>();
        public object[] Values { get; private set; }
        private static readonly string _exceptionpath = Directory.GetCurrentDirectory() + ConfigurationManager.AppSettings["_exceptionpath"];
        private static readonly string _successfulCommit = Directory.GetCurrentDirectory() + ConfigurationManager.AppSettings["_successfulCommit"];
        private static List<IDictionary<string, object>> rawData = new List<IDictionary<string, object>>();
        private DataSet result;
        private int o;
        private object value;

        public SpreadSheet(
        DataSourceConfig sourceConfig)
        {
            _sourceConfig = sourceConfig;
            if (sourceConfig.Config.connectionString != null && !string.IsNullOrWhiteSpace(sourceConfig.Config.connectionString.ToString()))
            {
                _connectionString = sourceConfig.Config.connectionString;
            }
            else
            {
                _connectionString =
                    $"User ID={sourceConfig.Config.userName};Password={sourceConfig.Config.password};Data Source={sourceConfig.Config.server};Initial Catalog={sourceConfig.Config.name};Persist Security Info=False;";
            }

        }
        public string RemoveWhitespace(string str)
        {
            return string.Join("", str.Split(default(string[]), StringSplitOptions.RemoveEmptyEntries));
        }
        public IEnumerable<IDictionary<string, object>> CreateObject(DataTable dataTable)
        {
            List<Dictionary<string, object>> _sheetObject = new List<Dictionary<string, object>>();
            rawData = new List<IDictionary<string, object>>();
            foreach (DataRow row in dataTable.Rows)
            {

                var dictionary = row.Table.Columns
                                .Cast<DataColumn>()
                                .ToDictionary(col => col.ColumnName, col => row.Field<object>(col.ColumnName));
                _sheetObject.Add(dictionary);
                rawData.Add(new Dictionary<string, object>(dictionary));

            }
            return _sheetObject;
        }

        public IEnumerable<T> CreateObjecttst<T>(DataTable dataTable)
        {
            throw new NotImplementedException();
        }

        public DataTable CreateTable(IEnumerable<IDictionary<string, object>> obj)
        {
            var table = new DataTable();

            //excuse the meaningless variable names

            var c = obj.FirstOrDefault(x => x.Values
                                                 .OfType<IEnumerable<IDictionary<string, object>>>()
                                                 .Any());
            var p = c ?? obj.FirstOrDefault();
            if (p == null)
                return table;

            var headers = p.Where(x => x.Value is string)
                           .Select(x => x.Key)
                           .Concat(c == null ?
                                   Enumerable.Empty<string>() :
                                   c.Values
                                    .OfType<IEnumerable<IDictionary<string, object>>>()
                                    .First()
                                    .SelectMany(x => x.Keys))
                           .Select(x => new DataColumn(x))
                           .ToArray();
            table.Columns.AddRange(headers);

            foreach (var parent in obj)
            {
                var children = parent.Values
                                     .OfType<IEnumerable<IDictionary<string, object>>>()
                                     .ToArray();

                var length = children.Any() ? children.Length : 1;

                var parentEntries = parent.Where(x => x.Value is string)
                                          .Repeat(length)
                                          .ToLookup(x => x.Key, x => x.Value);
                var childEntries = children.SelectMany(x => x.First())
                                           .ToLookup(x => x.Key, x => x.Value);

                var allEntries = parentEntries.Concat(childEntries)
                                              .ToDictionary(x => x.Key, x => x.ToArray());

                var addedRows = Enumerable.Range(0, length)
                                          .Select(x => new
                                          {
                                              relativeIndex = x,
                                              actualIndex = table.Rows.IndexOf(table.Rows.Add())
                                          })
                                          .ToArray();

                foreach (DataColumn col in table.Columns)
                {
                    if (!allEntries.TryGetValue(col.ColumnName, out object[] columnRows))
                        continue;

                    foreach (var row in addedRows)
                        table.Rows[row.actualIndex][col] = columnRows[row.relativeIndex];
                }
            }

            return table;
        }

        public DataTableCollection DataTableFromCsv(string csvPath, TableConfig tableConfig)
        {
            var t = Path.GetExtension(csvPath).ToUpper();
            System.Text.Encoding.RegisterProvider(CodePagesEncodingProvider.Instance);
            if (csvPath == null) { throw new ArgumentException("spreadsheet path cannot is be null"); }
            DataTable dataTable = new DataTable();
            if (Path.GetExtension(csvPath).ToUpper().Equals(".CSV"))
            {
                result = new DataSet();
                dataTable.Columns.Clear();
                dataTable.Rows.Clear();
                dataTable.Clear();
                dataTable.TableName = "SpreadSheet Table";
                List<string> allEmails = new List<string>();
                using (FileStream fileStream = new FileStream(csvPath, FileMode.Open, FileAccess.Read))
                {
                    var reader = ExcelReaderFactory.CreateCsvReader(fileStream, new ExcelReaderConfiguration()
                    {
                        FallbackEncoding = Encoding.GetEncoding(1252),
                        AutodetectSeparators = new char[] { ',', ';', '\t', '|', '#' }
                    });
                    if (reader != null)
                    {
                        result = reader.AsDataSet(new ExcelDataSetConfiguration()
                        {
                            ConfigureDataTable = (_) => new ExcelDataTableConfiguration()
                            {
                                UseHeaderRow = true,
                                
                            }
                        });


                        //Set to Table
                        //dataTable = result.Tables[0].AsDataView().ToTable();
                    }
                    result.Tables[0].TableName = tableConfig.Name;
                }

                   

                //using (cvsReader = new TextFieldParser(csvPath))
                //{
                //    cvsReader.SetDelimiters(new string[] { "," });

                //    //cvsReader.HasFieldsEnclosedInQuotes = true;
                //    //read column
                //    string[] colfield = cvsReader.ReadFields();
                //    //colfield
                //    //specila chra string
                //    string specialChar = @"\|!#$%&/()=?»«@£§€{}.-;'<>,";
                //    string repclace = @"_";
                //    repclace.ToCharArray();
                //    foreach (string column in colfield)
                //    {
                //        foreach (var item in specialChar)
                //        {
                //            if (column.Contains(item))
                //            {
                //                column.Replace(item, repclace[0]);

                //            }
                //        }
                //        DataColumn datacolumn = new DataColumn(column)
                //        {
                //            AllowDBNull = true
                //        };
                //        var dcol = Regex.Replace(datacolumn.ColumnName, @"[^a-zA-Z0-9_.]+", "_");
                //        dataTable.Columns.Add(dcol);


                //    }

                //    while (!cvsReader.EndOfData)
                //    {

                //        try
                //        {
                //            string[] fieldData = cvsReader.ReadFields();
                //            for (int i = 0; i < fieldData.Length; i++)
                //            {
                //                if (fieldData[i] == "")
                //                {
                //                    fieldData[i] = null;
                //                }


                //            }
                //            dataTable.Rows.Add(fieldData);
                //        }
                //        catch (Exception ex)
                //        {
                //            Console.WriteLine(ex.Message);
                //            return null;
                //        }

                //    }
                //    result.Tables.Add(dataTable);
                //}
            }
            else if (Path.GetExtension(csvPath).ToUpper().Equals(".XLXS") || Path.GetExtension(csvPath).ToUpper().Equals(".XLS") || t == ".XLSX")
            {
                dataTable.Columns.Clear();
                dataTable.Rows.Clear();
                dataTable.Clear();
                result = new DataSet();
                System.Text.Encoding.RegisterProvider(CodePagesEncodingProvider.Instance);
                using (FileStream fileStream = new FileStream(csvPath, FileMode.Open, FileAccess.Read))
                {
                    IExcelDataReader excelReader = null;
                    if (Path.GetExtension(csvPath).ToUpper() == ".XLSX")
                    {
                        excelReader = ExcelReaderFactory.CreateOpenXmlReader(fileStream);
                    }
                    else if (Path.GetExtension(csvPath).ToUpper() == ".XLS")
                    {
                        //1. Reading from a binary Excel file ('97-2003 format; *.xls)
                        excelReader = ExcelReaderFactory.CreateBinaryReader(fileStream);
                    }
                    else
                        throw new ArgumentOutOfRangeException();
                    if (excelReader != null)
                    {
                        result = excelReader.AsDataSet(new ExcelDataSetConfiguration()
                        {
                            ConfigureDataTable = (_) => new ExcelDataTableConfiguration()
                            {
                                UseHeaderRow = true
                            }
                        });


                        //Set to Table
                        //dataTable = result.Tables[0].AsDataView().ToTable();
                    }
                }
            }
            else
                throw new ArgumentException("Invalid sheet extension", csvPath);

  
             

            return result.Tables;
        }

        public int GetCount(TableConfig config)
        {
            throw new NotImplementedException();
        }

        public IEnumerable<IDictionary<string, object>> GetData(TableConfig tableConfig, Config config, int rowCount, int? fetch = null, int? offset = null)
        {
            throw new NotImplementedException();
        }

        public object GetData(string column, string table)
        {
            throw new NotImplementedException();
        }

        public IEnumerable<IDictionary<string, object>> RawData(IEnumerable<IDictionary<string, object>> PrdData)
        {
            if (!(File.Exists(_successfulCommit) && File.Exists(_exceptionpath)))
            {


                //write to the file
                File.Create(_successfulCommit).Close();

                //write to the file
                File.Create(_exceptionpath).Close();



            }
            using (System.IO.StreamWriter sw = System.IO.File.AppendText(_exceptionpath))
            {
                if (new FileInfo(_exceptionpath).Length == 0)
                {
                    sw.WriteLine("exceptions for " + ConfigurationManager.AppSettings["DatabaseName"] + ".........." + Environment.NewLine + Environment.NewLine);
                    //  File.WriteAllText(_exceptionpath, "exceptions for " + ConfigurationManager.AppSettings["APP_NAME"] + ".........." + Environment.NewLine + Environment.NewLine);
                }
                // sw.WriteLine(""); 
            }
            using (System.IO.StreamWriter sw = System.IO.File.AppendText(_successfulCommit))
            {
                //write my text 
                if (new FileInfo(_successfulCommit).Length == 0)
                {
                    // File.WriteAllText(_successfulCommit, "Successful Commits for " + ConfigurationManager.AppSettings["APP_NAME"] + ".........." + Environment.NewLine + Environment.NewLine);

                    sw.WriteLine("Successful Commits for " + ConfigurationManager.AppSettings["DatabaseName"] + ".........." + Environment.NewLine + Environment.NewLine);
                }
            }
            return rawData;
        }

        public object Shuffle(string schema, string table, string column, object existingValue, bool retainNull, IEnumerable<IDictionary<string, object>> _dataTable)
        {
            
            Random rnd = new Random();
            TableConfig config = new TableConfig();
            var dTable = SpreadSheetTable(_dataTable, config);
            var result = new DataView(dTable).ToTable(false, new string[] { column}).AsEnumerable().Select(n => n[0]).ToList();
            //Randomizer randomizer = new Randomizer();

            CompareLogic compareLogic = new CompareLogic();


            //var values = Array();
            //Randomizer randomizer = new Randomizer();
            try
            {
                if (retainNull)
                {
                    Values = result.Where(n => n != DBNull.Value).Distinct().ToArray();
                }
                else
                    Values = result.Distinct().ToArray();


                //var find = values.Count();
                value = Values[rnd.Next(Values.Count())];
                if (Values.Count() <= 1)
                {
                    o = o + 1;
                    if (o == 1)
                    {
                        File.WriteAllText(_exceptionpath, "");
                    }
                    if (!(exceptionBuilder.Where(n => n.Key.Equals(table)).Count() > 0 && exceptionBuilder.Where(n => n.Value.Equals(column)).Count() > 0))
                    {
                        exceptionBuilder.Add(new KeyValuePair<object, object>(table, column));
                        File.AppendAllText(_exceptionpath, "Cannot generate unique shuffle value" + " on table " + table + " for column " + column + Environment.NewLine + Environment.NewLine);
                    }
                    //if (!(exceptionBuilder.ContainsKey(table) && exceptionBuilder.ContainsValue(column)))
                    //{
                        
                    //}
                    //o = o + 1;
                    //File.AppendAllText(_exceptionpath, "Cannot generate unique shuffle value" + " on table " + table + " for column " + column + Environment.NewLine + Environment.NewLine);
                    return value;
                }
                if (value is DBNull && retainNull)
                {
                    //var nt = values.Where(n => n != null).Select(n => n).ToArray()[rnd.Next(0, values.Where(n => n != null).ToArray().Count())];
                    return Values.Where(n => n != null).Select(n => n).ToArray()[rnd.Next(0, Values.Where(n => n != null).ToArray().Count())];
                }

                if (compareLogic.Compare(Convert.ToString(value), Convert.ToString(null)).AreEqual && retainNull)
                {
                    //var nt = values.Where(n => n != null).Select(n => n).ToArray()[rnd.Next(0, values.Where(n => n != null).ToArray().Count())];
                    return Values.Where(n => n != null).Select(n => n).ToArray()[rnd.Next(0, Values.Where(n => n != null).ToArray().Count())];
                }
                else
                {
                    while (compareLogic.Compare(Convert.ToString(value), Convert.ToString(existingValue)).AreEqual)
                    {

                        value = Values[rnd.Next(0, Values.Count())];
                    }
                }
               
                return value;
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.ToString());
                File.AppendAllText(_exceptionpath, ex.ToString() + Environment.NewLine);
                return null;

            }
            //return value;



        }

        public DataTable SpreadSheetTable(IEnumerable<IDictionary<string, object>> parents, TableConfig config)
        {
            var table = new DataTable();



            var c = parents.FirstOrDefault(x => x.Values
                                           .OfType<IEnumerable<IDictionary<string, object>>>()
                                           .Any());
            var p = c ?? parents.FirstOrDefault();
            if (p == null)
                return table;

            //var ccc = p.Where(x => x.Value is object)
            //               .Select(x => x.Key);



            //var headers1 = p.Where(x => x.Value is object)
            //               .Select(x => x.Key)
            //               .Concat(c == null ?
            //                       Enumerable.Empty<object>() :
            //                       c.Values
            //                        .OfType<IEnumerable<IDictionary<string, object>>>()
            //                        .First()
            //                        .SelectMany(x => x.Keys)).ToArray();





            foreach (var parent in parents)
            {
                var children = parent.Values
                                     .OfType<IEnumerable<IDictionary<string, object>>>()
                                     .ToArray();

                var length = children.Any() ? children.Length : 1;
                var parentEntries1 = parent.Where(x => x.Value is object).ToLookup(x => x.Key, x => x.Value);


                var parentEntries = parent
                                          .Repeat(length)
                                          .ToLookup(x => x.Key, x => x.Value);

                var childEntries = children.SelectMany(x => x.First())
                                           .ToLookup(x => x.Key, x => x.Value);


                var allEntries = parentEntries.Concat(childEntries)
                                              .ToDictionary(x => x.Key, x => x.ToArray());

                var headers = allEntries.Select(x => x.Key)
                                        .Except(table.Columns
                                                     .Cast<DataColumn>()
                                                     .Select(x => x.ColumnName))
                                        .Select(x => new DataColumn(x))
                                        .ToArray();
                foreach (var header in headers)
                {
                    if (config.Columns != null)
                    {
                        if (config.Columns.Where(n => n.Name.Equals(header.ColumnName)).Count() != 0)
                        {
                            foreach (ColumnConfig columnConfig in config.Columns.Where(n => n.Name.Equals(header.ColumnName)))
                            {
                                if (columnConfig.Type == DataType.Geometry || columnConfig.Type == DataType.Shufflegeometry)
                                {
                                    header.DataType = typeof(SdoGeometry);
                                }
                                else if (columnConfig.Type == DataType.Blob)
                                {
                                    header.DataType = typeof(byte[]);
                                }

                            }
                        }
                    }
                 



                    table.Columns.Add(header);
                }


                var addedRows = new int[length];
                //var xxx = table.Rows.Add();
                for (int i = 0; i < length; i++)
                    addedRows[i] = table.Rows.IndexOf(table.Rows.Add());

                foreach (DataColumn col in table.Columns)
                {
                    if (!allEntries.TryGetValue(col.ColumnName, out object[] columnRows))
                        continue;

                    for (int i = 0; i < addedRows.Length; i++)
                    {
                        if (columnRows[i] is SdoGeometry)
                        {
                            table.Rows[addedRows[i]][col] = (SdoGeometry)columnRows[i];
                        }
                        else if (columnRows[i] is byte[])
                        {
                            table.Rows[addedRows[i]][col] = (byte[])columnRows[i];
                        }
                        else
                        {
                            table.Rows[addedRows[i]][col] = columnRows[i];
                        }
                    }
                }
            }

            return table;
        }

        public void UpdateRow(IDictionary<string, object> row, TableConfig tableConfig, Config config)
        {
            throw new NotImplementedException();
        }

        public bool UpdateRows(IEnumerable<IDictionary<string, object>> rows, int rowCount, TableConfig tableConfig, Config config, IDictionary<string, KeyValuePair<string, string>> cmdParameters, Action<int> updatedCallback = null)
        {
            throw new NotImplementedException();
        }

        public DataTable GetDataTable(string table, string schema, string connection, string rowCount)
        {
            throw new NotImplementedException();
        }

        public Task<IEnumerable<IDictionary<string, object>>> GetAsyncData(TableConfig tableConfig, Config config)
        {
            throw new NotImplementedException();
        }
    }
}
