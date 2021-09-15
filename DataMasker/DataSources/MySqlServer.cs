using DataMasker.Interfaces;
using DataMasker.Models;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using MySql.Data.MySqlClient;
using Dapper;
using DataMasker.Utils;
using KellermanSoftware.CompareNetObjects;

namespace DataMasker.DataSources
{
    public class MySqlServer : IDataSource
    {
        private readonly DataSourceConfig _sourceConfig;
        //global::Dapper.SqlMapper.AddTypeHandler(typeof(DbGeography), new GeographyMapper());
        private static readonly string _exceptionpath = Directory.GetCurrentDirectory() + ConfigurationManager.AppSettings["_exceptionpath"];
        private static readonly string _successfulCommit = Directory.GetCurrentDirectory() + ConfigurationManager.AppSettings["_successfulCommit"];

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
        public MySqlServer(
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
            using (MySqlConnection connection = new MySqlConnection(_connectionString))
            {
                //connection.Open();
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


                var _prdData = (IEnumerable<IDictionary<string, object>>)connection.Query(BuildSelectSql(tableConfig, config), buffered: true);
                //rawData.AddRange(_prdData.Select(n => n.ToDictionary(x => x.Key, x => x.Value).Select(x => x) as IDictionary<string, object>));

                foreach (IDictionary<string, object> prd in _prdData)
                {

                    rawData.Add(new Dictionary<string, object>(prd));
                }
                //rawData.AddRange(new List<IDictionary<string, object>>(_prdData));


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
            using (MySqlConnection connection = new MySqlConnection(_connectionString))
            {
                connection.Open();
                connection.Execute(BuildUpdateSql(tableConfig, config), row, null, commandType: CommandType.Text);
            }
        }
        private string BuildCountSql(
            TableConfig tableConfig)
        {
            return $"SELECT COUNT(*) FROM `{tableConfig.Schema}`.`{tableConfig.Name}`";
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
            if (rowCount >= 50000 && rowCount <= 180000)
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



            using (MySqlConnection connection = new MySqlConnection(_connectionString))
            {
                connection.Open();
                foreach (Batch<IDictionary<string, object>> batch in batches)
                {
                    MySqlTransaction sqlTransaction = connection.BeginTransaction();


                    string sql = BuildUpdateSql(tableConfig, config);
                    try
                    {


                        connection.Execute(sql, batch.Items, sqlTransaction, null);

                        if (_sourceConfig.DryRun)
                        {
                            sqlTransaction.Rollback();
                            isRolledBack = true;
                        }
                        else
                        {
                            sqlTransaction.Commit();
                            File.AppendAllText(_successfulCommit, $"Successful Commit on table {tableConfig.Schema}.{tableConfig.Name}" + Environment.NewLine + Environment.NewLine);
                        }


                        if (updatedCallback != null)
                        {
                            totalUpdated += batch.Items.Count;
                            updatedCallback.Invoke(totalUpdated);
                        }
                    }
                    catch (Exception ex)
                    {

                        Console.WriteLine(ex.ToString());
                        isRolledBack = true;
                        File.AppendAllText(_exceptionpath, ex.ToString() + $" on table  {tableConfig.Schema}.{tableConfig.Name}" + Environment.NewLine + Environment.NewLine); ;
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
            string sql = $"UPDATE `{tableConfig.TargetSchema}`.`{tableConfig.Name}` SET ";

            sql += tableConfig.Columns.GetUpdateColumns(config);
            sql += $" WHERE `{tableConfig.PrimaryKeyColumn}` = @{tableConfig.PrimaryKeyColumn}";
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
                sql = $"SELECT  {tableConfig.Columns.GetSelectColumns(tableConfig.PrimaryKeyColumn, config)} FROM `{tableConfig.Schema}`.`{tableConfig.Name}` LIMIT {n}";
            }
            else
                sql = $"SELECT  {tableConfig.Columns.GetSelectColumns(tableConfig.PrimaryKeyColumn, config)} FROM `{tableConfig.Schema}`.`{tableConfig.Name}`"; ;
            return sql;
        }

        public object Shuffle(string schema, string table, string column, object existingValue, bool retainNull, IEnumerable<IDictionary<string, object>> dataTable)
        {
            CompareLogic compareLogic = new CompareLogic();
            //ArrayList list = new ArrayList();
            Random rnd = new Random();
            //string sql = $"SELECT `{column}` FROM  `{schema}`.`{table}`";
            //using (var connection = new MySqlConnection(_connectionStringPrd))
            //{
            try
            {


                //connection.Open();
                //var result = (IEnumerable<IDictionary<string, object>>)connection.Query(sql);
                //var values = Array();
                //Randomizer randomizer = new Randomizer();
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
                    return value;


                }

            }
            catch (Exception ex)
            {

                Console.WriteLine(ex.ToString());
                File.AppendAllText(_exceptionpath, ex.ToString() + Environment.NewLine);
                return null;
            }

            //}
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
                                columnRows[i] = DateTime.TryParse(columnRows[i].ToString(), out DateTime temp) ? temp : DateTime.MinValue.AddHours(9);


                            }
                            table.Rows[addedRows[i]][col] = columnRows[i];
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

        public int GetCount(TableConfig config)
        {
            using (MySqlConnection connection = new MySqlConnection(_connectionString))
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

        public DataTable GetDataTable(string table, string schema, string connection, string rowCount)
        {
            DataTable dataTable = new DataTable();
            using (MySqlConnection mySqlConnection = new MySqlConnection(connection))
            {
                string squery = "";
                if (int.TryParse(rowCount, out int n))
                {
                    squery = $"Select * from `{schema}`.`{table}` LIMIT {n}";
                }
                else
                    squery = $"Select * from `{schema}`.`{table}`";

                mySqlConnection.Open();
                using (MySqlDataAdapter oda = new MySqlDataAdapter(squery, mySqlConnection))
                {
                    try
                    {
                        //Fill the data table with select statement's query results:
                        int recordsAffectedSubscriber = 0;
                        recordsAffectedSubscriber = oda.Fill(dataTable);
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine(ex.Message);
                    }
                }
            }
            return dataTable;
        }

        public Task<IEnumerable<IDictionary<string, object>>> GetAsyncData(TableConfig tableConfig, Config config)
        {
            throw new NotImplementedException();
        }
    }
}
