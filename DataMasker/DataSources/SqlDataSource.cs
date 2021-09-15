using System;
using Dapper;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using DataMasker.Interfaces;
using DataMasker.Models;
using DataMasker.Utils;
using System.Configuration;
using System.IO;
using KellermanSoftware.CompareNetObjects;
using System.Security;
using Bogus;
using System.Globalization;
using System.Threading.Tasks;

namespace DataMasker.DataSources
{
    /// <summary>
    /// SqlDataSource
    /// </summary>
    /// <seealso cref="IDataSource"/>
    public class SqlDataSource : IDataSource
    {
        private readonly DataSourceConfig _sourceConfig;
        //global::Dapper.SqlMapper.AddTypeHandler(typeof(DbGeography), new GeographyMapper());
        private static readonly string _exceptionpath = Directory.GetCurrentDirectory() + ConfigurationManager.AppSettings["_exceptionpath"];
        private static readonly string _successfulCommit = Directory.GetCurrentDirectory() + ConfigurationManager.AppSettings["_successfulCommit"];
        private static readonly DateTime DEFAULT_MIN_DATE = new DateTime(1900, 1, 1, 0, 0, 0, 0);
        //private static readonly string _exceptionpath = Directory.GetCurrentDirectory() + ConfigurationManager.AppSettings["_exceptionpath"];
        private static readonly DateTime DEFAULT_MAX_DATE = DateTime.Now;
        //private IEnumerable<IDictionary<string, object>> getData { get; set; }
        public object[] Values { get; private set; }
        public bool isRolledBack { get; private set; }

        public int o = 0;

        private static List<IDictionary<string, object>> rawData = new List<IDictionary<string, object>>();
        private static Dictionary<string, string> exceptionBuilder = new Dictionary<string, string>();

        private readonly string _connectionString;
        private readonly string _connectionStringPrd;

        /// <summary>
        /// Initializes a new instance of the <see cref="SqlDataSource"/> class.
        /// </summary>
        /// <param name="sourceConfig"></param>
        public SqlDataSource(
            DataSourceConfig sourceConfig)
        {
            _sourceConfig = sourceConfig;
            if (sourceConfig.Config.connectionString!=null && !string.IsNullOrWhiteSpace(sourceConfig.Config.connectionString.ToString()))
            {
                _connectionString = sourceConfig.Config.connectionString;
            }
            else
            {
                _connectionString =
                    $"User ID={sourceConfig.Config.userName};Password={sourceConfig.Config.password};Data Source={sourceConfig.Config.server};Initial Catalog={sourceConfig.Config.name};Persist Security Info=False;";
            }
            if (sourceConfig.Config.connectionStringPrd != null && !string.IsNullOrWhiteSpace(sourceConfig.Config.connectionStringPrd.ToString()))
            {
                _connectionStringPrd = sourceConfig.Config.connectionStringPrd;
            }

        }


        /// <summary>
        /// Gets the data.
        /// </summary>
        /// <param name="tableConfig">The table configuration.</param>
        /// <returns></returns>
        /// <inheritdoc/>
        public IEnumerable<IDictionary<string, object>> GetData(
            TableConfig tableConfig, Config config, int rowCount, int? fetch = null, int? offset = null)
        {
            //SqlCredential credentials = new SqlCredential(UserName, ReadPassword());
            using (SqlConnection connection = new SqlConnection(_connectionStringPrd))
            {
                //Stopwatch watch = new Stopwatch();
                //watch.Start();
                try
                {
                    connection.Open();
                    //Console.WriteLine("Database Connection established");
                }
                catch (Exception e)
                {
                    Console.WriteLine("Exception occurs {0}", e.Message);
                    Console.WriteLine("Program will exit: Press ENTER to exist..");
                    Console.ReadLine();

                    File.WriteAllText(_exceptionpath, e.Message + Environment.NewLine + Environment.NewLine);
                    System.Environment.Exit(1);
                }
                rawData = new List<IDictionary<string, object>>();
                var _prdData = (IEnumerable<IDictionary<string, object>>)connection.Query(BuildSelectSql(tableConfig, config), buffered: true, commandTimeout: 0);
                foreach (IDictionary<string, object> prd in _prdData)
                {
                    rawData.Add(new Dictionary<string, object>(prd));
                }
                //watch.Stop();
                //TimeSpan timeSpan = watch.Elapsed;
                //var timeElapse = string.Format("{0}h {1}m {2}s {3}ms", timeSpan.Hours, timeSpan.Minutes, timeSpan.Seconds, timeSpan.Milliseconds);
                //Console.WriteLine(timeElapse);
             
                return _prdData;
            }
        }
        public static string ByteArrayToString(byte[] ba)
        {

            return BitConverter.ToString(ba).Replace("-", "");
        }
        /// <summary>
        /// Updates the row.
        /// </summary>
        /// <param name="row">The row.</param>
        /// <param name="tableConfig">The table configuration.</param>
        /// <inheritdoc/>
        public void UpdateRow(
            IDictionary<string, object> row,
            TableConfig tableConfig, Config config)
        {
            using (SqlConnection connection = new SqlConnection(_connectionString))
            {
                connection.Open();
                connection.Execute(BuildUpdateSql(tableConfig, config), row, null, commandType: CommandType.Text);
            }
        }
        private string BuildCountSql(
            TableConfig tableConfig)
        {            
            return $"SELECT COUNT(*) FROM [{tableConfig.Schema}].[{tableConfig.Name}]";
        }
        private static SecureString ReadPassword()
        {
            var secured = new SecureString();
            ConsoleKeyInfo keyInfo = Console.ReadKey(true);

            while (keyInfo.Key != ConsoleKey.Enter)
            {
                secured.AppendChar(keyInfo.KeyChar);
                keyInfo = Console.ReadKey(true);
            }

            secured.MakeReadOnly();
            return secured;
        }
        /// <inheritdoc/>
        public bool UpdateRows(
            IEnumerable<IDictionary<string, object>> rows,
            int rowCount,
            TableConfig tableConfig, Config config,
          IDictionary<string, KeyValuePair<string, string>> cmdParameters,
            Action<int> updatedCallback)
        {
            if (rowCount > 200000)
            {
                _sourceConfig.UpdateBatchSize = 100000;
            }
            if (rowCount >= 50000 && rowCount <= 200000)
            {
                _sourceConfig.UpdateBatchSize = 50000;
            }
            int? batchSize = _sourceConfig.UpdateBatchSize;
            isRolledBack = false;
            if (batchSize == null ||
                batchSize <= 0)
            {
                batchSize = rowCount;
            }

            IEnumerable<Batch<IDictionary<string, object>>> batches = Batch<IDictionary<string, object>>.BatchItems(
                rows,
                (
                    objects,
                    enumerable) => enumerable.Count() < batchSize);

            int totalUpdated = 0;
            

           
            using (SqlConnection connection = new SqlConnection(_connectionString))
            {
                connection.Open();
                foreach (Batch<IDictionary<string, object>> batch in batches)
                {
                    SqlTransaction sqlTransaction = connection.BeginTransaction();


                    string sql = BuildUpdateSql(tableConfig, config);
                    try
                    {

                        connection.Execute(sql, batch.Items, sqlTransaction,0,CommandType.Text);
                        
                        if (_sourceConfig.DryRun)
                        {
                            sqlTransaction.Rollback();
                            isRolledBack = true;
                        }
                        else
                        {
                            sqlTransaction.Commit();
                           // File.AppendAllText(_successfulCommit, $"Successful Commit on table {tableConfig.Schema}.{tableConfig.Name}" + Environment.NewLine + Environment.NewLine);
                            File.AppendAllText(_successfulCommit, $"Batch {batch.BatchNo} - Successfully Commit {batch.Items.Count} records on table  {tableConfig.TargetSchema}.{tableConfig.Name} - " + DateTime.Now.ToString() + Environment.NewLine + Environment.NewLine);
                        }


                        if (updatedCallback != null)
                        {
                            totalUpdated += batch.Items.Count;
                            updatedCallback.Invoke(totalUpdated);
                        }
                    }
                    catch (Exception ex)
                    {
                        sqlTransaction.Rollback();
                        isRolledBack = true;
                        Console.WriteLine(ex.Message);
                        File.AppendAllText(_exceptionpath, ex.Message + $" on table  {tableConfig.Schema}.{tableConfig.Name}" + Environment.NewLine + Environment.NewLine); 
                    }
                }
            }
            return isRolledBack;
        }

        /// <summary>
        /// Builds the update SQL.
        /// </summary>
        /// <param name="tableConfig">The table configuration.</param>
        /// <returns></returns>
        private string BuildUpdateSql(
            TableConfig tableConfig, Config config)
        {
            string sql = $"UPDATE [{tableConfig.TargetSchema}].[{tableConfig.Name}] SET ";

            sql += tableConfig.Columns.GetUpdateColumns(config);
            sql += $" WHERE [{tableConfig.PrimaryKeyColumn}] = @{tableConfig.PrimaryKeyColumn}";
            return sql;
        }


        /// <summary>
        /// Builds the select SQL.
        /// </summary>
        /// <param name="tableConfig">The table configuration.</param>
        /// <returns></returns>
        private string BuildSelectSql(
            TableConfig tableConfig, Config config)
        {
            string sql = "";
            if (int.TryParse(tableConfig.RowCount, out int n))
            {
                sql = $"SELECT TOP ({n})  {tableConfig.Columns.GetSelectColumns(tableConfig.PrimaryKeyColumn,config)} FROM [{tableConfig.Schema}].[{tableConfig.Name}]";
            }
            else
                sql = $"SELECT  {tableConfig.Columns.GetSelectColumns(tableConfig.PrimaryKeyColumn, config)} FROM [{tableConfig.Schema}].[{tableConfig.Name}]"; ;
            return sql;
        }
       
        public object Shuffle(string schema, string table, string column, object existingValue, bool retainNull, IEnumerable<IDictionary<string,object>> dataTable)
        {
            CompareLogic compareLogic = new CompareLogic();
            //ArrayList list = new ArrayList();
          
            Random rnd = new Random();
                try
                {
                    if (retainNull)
                    {
                        Values = dataTable.Select(n => n.Values).SelectMany(x => x).ToList().Where(n => n != null).Distinct().ToArray();
                       
                    }
                    else
                        Values = dataTable.Select(n => n.Values).SelectMany(x => x).ToList().Distinct().ToArray();


                    //var find = values.Count();
                    object value = Values[rnd.Next(Values.Count())];
                    if (Values.Count() <= 1)
                    {
                        o = o + 1;
                        if (o == 1)
                        {
                            File.WriteAllText(_exceptionpath, "");
                        }

                        if (!(exceptionBuilder.ContainsKey(table) && exceptionBuilder.ContainsValue(column)))
                        {
                            exceptionBuilder.Add(table, column);
                            File.AppendAllText(_exceptionpath, "Cannot generate unique shuffle value" + " on table " + table + " for column " + column + Environment.NewLine + Environment.NewLine);
                        }
                        //o = o + 1;
                        //File.AppendAllText(_exceptionpath, "Cannot generate unique shuffle value" + " on table " + table + " for column " + column + Environment.NewLine + Environment.NewLine);
                        return value;
                    }

                    if (compareLogic.Compare(value, null).AreEqual && retainNull)
                    {

                        //var tt = values.Where(n => n != null).Select(n => n).ToArray()[rnd.Next(0, values.Where(n => n != null).ToArray().Count())];
                        //var nt = values.Where(n => n != null).Select(n => n).ToArray()[rnd.Next(0, values.Where(n => n != null).ToArray().Count())];
                        return Values.Where(n => n != null).Select(n => n).ToArray()[rnd.Next(0, Values.Where(n => n != null).ToArray().Count())];
                    }
                    else
                    {



                        while (compareLogic.Compare(value, existingValue).AreEqual)
                        {

                            value = Values[rnd.Next(0, Values.Count())];
                        }
                        if (value == existingValue)
                        {

                        }
                        return value;


                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine(ex.ToString());
                    File.AppendAllText(_exceptionpath, ex.ToString() + Environment.NewLine);
                    return null;
                }


          //  }
        }
        public static bool Isonull(object T)
        {
            return T == null ? true : false;
        }
        public object GetData(string column, string table)
        {
            throw new NotImplementedException();
        }

        public DataTableCollection DataTableFromCsv(string csvPath, TableConfig tableConfig)
        {
            throw new NotImplementedException();
        }

        public IEnumerable<IDictionary<string, object>> CreateObject(DataTable dataTable)
        {
            List<Dictionary<string, object>> _sheetObject = new List<Dictionary<string, object>>();
            foreach (DataRow row in dataTable.Rows)
            {

                var dictionary = row.Table.Columns
                                .Cast<DataColumn>()
                                .ToDictionary(col => col.ColumnName, col => row.Field<object>(col.ColumnName));
                _sheetObject.Add(dictionary);

            }
            return _sheetObject;
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


            string[] formats = {"M/d/yyyy h:mm:ss tt", "M/d/yyyy h:mm tt",
                     "MM/dd/yyyy hh:mm:ss", "M/d/yyyy h:mm:ss",
                     "M/d/yyyy hh:mm tt", "M/d/yyyy hh tt",
                     "M/d/yyyy h:mm", "M/d/yyyy h:mm",
                     "MM/dd/yyyy hh:mm", "M/dd/yyyy hh:mm",

                     "M-d-yyyy h:mm:ss tt", "M-d-yyyy h:mm tt",
                     "MM-dd-yyyy hh:mm:ss", "M-d-yyyy h:mm:ss",
                     "M-d-yyyy hh:mm tt", "M-d-yyyy hh tt",
                     "M-d-yyyy h:mm", "M-d-yyyy h:mm",
                     "MM-dd-yyyy hh:mm", "M-dd-yyyy hh:mm",

                     "yyyy-d-M h:mm:ss tt", "yyyy-d-M h:mm tt",
                     "yyyy-dd-MM hh:mm:ss", "yyyy-d-M h:mm:ss",
                     "yyyy-d-M hh:mm tt", "yyyy-d-M hh tt",
                     "yyyy-d-M h:mm", "yyyy-d-M h:mm",
                     "yyyy-dd-MM hh:mm", "yyyy-dd-M hh:mm",

                     "yyyy-M-d h:mm:ss tt", "yyyy-M-d h:mm tt",
                     "yyyy-MM-dd hh:mm:ss", "yyyy-M-d h:mm:ss",
                     "yyyy-M-d hh:mm tt", "yyyy-M-d hh tt",
                     "yyyy-M-d h:mm", "yyyy-M-d h:mm",
                     "yyyy-MM-dd hh:mm", "yyyy-M-dd hh:mm",
                            "yyyyMMdd", "yyyyMdd hh:mm",


            };
            Faker faker = new Faker();


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
                        else if (col.DataType == typeof(DateTime))
                        {
                            if (columnRows[i] is string && string.IsNullOrWhiteSpace(columnRows[i].ToString()))
                            {
                                
                                columnRows[i] = RemoveWhitespace(columnRows[i].ToString());
                                //columnRows[i] = DateTime.Parse(columnRows[i].ToString());
                                //Clear nullspace date record;
                                columnRows[i] = DateTime.TryParse(columnRows[i].ToString(), out DateTime temp) ? temp : faker.Date.Between(DEFAULT_MIN_DATE, DEFAULT_MAX_DATE);

                                table.Rows[addedRows[i]][col] = columnRows[i];
                            }
                            else
                                table.Rows[addedRows[i]][col] = DateTime.TryParseExact(columnRows[i].ToString(), formats, CultureInfo.InvariantCulture, DateTimeStyles.None, out DateTime temp) ? temp : DateTime.Now;

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
        public string RemoveWhitespace(string str)
        {
            return string.Join("", str.Split(default(string[]), StringSplitOptions.RemoveEmptyEntries));
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

        public IEnumerable<IDictionary<string, object>> RawData(IEnumerable<IDictionary<string, object>> PrdData)
        {
            if (!(File.Exists(_successfulCommit) && File.Exists(_exceptionpath)))
            {               
                File.Create(_successfulCommit).Close();
                File.Create(_exceptionpath).Close();
            }
            using (StreamWriter sw = File.AppendText(_exceptionpath))
            {
                if (new FileInfo(_exceptionpath).Length == 0)
                {
                    sw.WriteLine("Exceptions for " + ConfigurationManager.AppSettings["DatabaseName"] + " Database.........." + Environment.NewLine + Environment.NewLine);
                }
            }
            using (StreamWriter sw = File.AppendText(_successfulCommit))
            {
                if (new FileInfo(_successfulCommit).Length == 0)
                {
                    sw.WriteLine("Successful Commits for " + ConfigurationManager.AppSettings["DatabaseName"] + " database.........." + Environment.NewLine + Environment.NewLine);
                }
            }
            return rawData;
        }

        public int GetCount(TableConfig config)
        {
            using (SqlConnection connection = new SqlConnection(_connectionString))
            {
                try
                {
                    connection.Open();
                    //Console.WriteLine("Database Connection established");
                }
                catch (Exception e)
                {
                    Console.WriteLine("Exception occurs {0}", e.Message);
                    Console.WriteLine("Program will exit: Press ENTER to exist..");
                    Console.ReadLine();

                    File.WriteAllText(_exceptionpath, e.Message + Environment.NewLine + Environment.NewLine);
                    System.Environment.Exit(1);
                }

                var count = connection.ExecuteScalar(BuildCountSql(config));
                return Convert.ToInt32(count);
            }
        }

        public IEnumerable<T> CreateObjecttst<T>(DataTable dataTable)
        {
            throw new NotImplementedException();
        }
        public DataTable GetDataTable(string table,string schema, string connection, string rowCount)
        {
            DataTable dataTable = new DataTable();
            using (SqlConnection sqlConnection = new SqlConnection(connection))
            {
                var obj = "OBJECTPROPERTYCHECK";
                string squery = "";
                if (schema == obj)
                {
                    squery = $"SELECT OBJECTPROPERTY(OBJECT_ID({table}), 'TableHasIdentity') AS 'IDENTITY'";
                }
                else
                {
                    if (int.TryParse(rowCount, out int n))
                    {
                        squery = $"SELECT TOP ({n}) * from [{schema}].[{table}]";
                    }
                    else
                        squery = $"Select * from [{schema}].[{table}]";
                    //squery = $"Select * from [{schema}].[{table}]";
                }
                sqlConnection.Open();
                using (SqlDataAdapter adapter = new SqlDataAdapter(squery, sqlConnection))
                {

                    try
                    {
                        //Fill the data table with select statement's query results:
                        int recordsAffectedSubscriber = 0;
                        recordsAffectedSubscriber = adapter.Fill(dataTable);
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine(ex.Message);
                    }
                }
                return dataTable;
            }
        }
        public Task<IEnumerable<IDictionary<string, object>>> GetAsyncData(TableConfig tableConfig, Config config)
        {
            throw new NotImplementedException();
        }
    }
}
