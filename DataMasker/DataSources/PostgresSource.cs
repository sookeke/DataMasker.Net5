using Dapper;
using DataMasker.Interfaces;
using DataMasker.Models;
using DataMasker.Utils;
using KellermanSoftware.CompareNetObjects;
using Npgsql;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

namespace DataMasker.DataSources
{
    class PostgresSource : IDataSource
    {
        private readonly DataSourceConfig _sourceConfig;
        //global::Dapper.SqlMapper.AddTypeHandler(typeof(DbGeography), new GeographyMapper());
        private static readonly string _exceptionpath = Directory.GetCurrentDirectory() + ConfigurationManager.AppSettings["_exceptionpath"];
        private static readonly string _successfulCommit = Directory.GetCurrentDirectory() + ConfigurationManager.AppSettings["_successfulCommit"];

        //private IEnumerable<IDictionary<string, object>> getData { get;  set; }
        public object[] Values { get; private set; }
        public bool isRolledBack { get; private set; }

        public int o = 0;

        private static List<IDictionary<string, object>> rawData = new List<IDictionary<string, object>>();
        private static Dictionary<string, string> exceptionBuilder = new Dictionary<string, string>();

        private readonly string _connectionString;
        private readonly string _connectionStringPrd;

        public PostgresSource(
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
        public IEnumerable<IDictionary<string, object>> GetData(TableConfig tableConfig, Config config, int rowCount, int? fetch = null, int? offset = null)
        {
            //string _connectionStringGet = ConfigurationManager.AppSettings["ConnectionStringPrd"];

            using (var connection = new NpgsqlConnection(_connectionStringPrd))
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
                string query = "";
                IDictionary<string, object> idict = new Dictionary<string, object>();
                IEnumerable<IDictionary<string, object>> row = new List<IDictionary<string, object>>();
                List<IDictionary<string, object>> rows = new List<IDictionary<string, object>>();
                rawData = new List<IDictionary<string, object>>();
                //var rowCount = GetCount(tableConfig);
                query = BuildSelectSql(tableConfig, config);
                //var retu = connection.Query(BuildSelectSql(tableConfig));
                rawData = new List<IDictionary<string, object>>();
                var _prdData = (IEnumerable<IDictionary<string, object>>)connection.Query(query, buffered: true);
               // rawData.AddRange(new List<IDictionary<string, object>>(_prdData));

                foreach (IDictionary<string, object> prd in _prdData)
                {

                    rawData.Add(new Dictionary<string, object>(prd));
                }

                return _prdData;
                //var retu = connection.Query(BuildSelectSql(tableConfig));
                // return (IEnumerable<IDictionary<string, object>>)connection.Query(BuildSelectSql(tableConfig));
            }
        }
       
        public void UpdateRow(IDictionary<string, object> row, TableConfig tableConfig, Config config)
        {
            using (var connection = new NpgsqlConnection(_connectionString))
            {
                connection.Open();
                connection.Execute(BuildUpdateSql(tableConfig,config), row, null, commandType: System.Data.CommandType.Text);
            }
        }
        public string RemoveWhitespace(string str)
        {
            return string.Join("", str.Split(default(string[]), StringSplitOptions.RemoveEmptyEntries));
        }
        public bool UpdateRows(IEnumerable<IDictionary<string, object>> rows, int rowCount, TableConfig tableConfig, Config config, IDictionary<string, KeyValuePair<string, string>> cmdParameters, Action<int> updatedCallback = null)
        {
            SqlMapper.AddTypeHandler(new GeographyMapper());
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

            if (!(File.Exists(_successfulCommit) && File.Exists(_exceptionpath)))
            {

                //write to the file
                File.Create(_successfulCommit).Close();

                //write to the file
                File.Create(_exceptionpath).Close();



            }
            else
                File.WriteAllText(_successfulCommit, String.Empty);
            using (StreamWriter sw = File.AppendText(_exceptionpath))
            {
                if (new FileInfo(_exceptionpath).Length == 0)
                {
                    sw.WriteLine("exceptions for " + ConfigurationManager.AppSettings["DatabaseName"] + ".........." + Environment.NewLine + Environment.NewLine);
                    //  File.WriteAllText(_exceptionpath, "exceptions for " + ConfigurationManager.AppSettings["APP_NAME"] + ".........." + Environment.NewLine + Environment.NewLine);
                }
                // sw.WriteLine(""); 
            }
            using (StreamWriter sw = File.AppendText(_successfulCommit))
            {
                //write my text 
                if (new FileInfo(_successfulCommit).Length == 0)
                {
                    // File.WriteAllText(_successfulCommit, "Successful Commits for " + ConfigurationManager.AppSettings["APP_NAME"] + ".........." + Environment.NewLine + Environment.NewLine);

                    sw.WriteLine("Successful Commits for database" + ConfigurationManager.AppSettings["DatabaseName"] + ".........." + Environment.NewLine + Environment.NewLine);
                }
            }
            using (NpgsqlConnection connection = new NpgsqlConnection(_connectionString))
            {
                connection.Open();
                foreach (Batch<IDictionary<string, object>> batch in batches)
                {
                    using (IDbTransaction sqlTransaction = connection.BeginTransaction())
                    {
                        //OracleBulkCopy oracleBulkCopy = new OracleBulkCopy(connection, OracleBulkCopyOptions.UseInternalTransaction);


                        string sql = BuildUpdateSql(tableConfig, config);


                        try
                        {
                            //File.AppendAllText(_successfulCommit, "Successful Commit on table " + config.Name + Environment.NewLine + Environment.NewLine);

                            connection.Execute(sql, batch.Items, sqlTransaction);

                            if (_sourceConfig.DryRun)
                            {
                                sqlTransaction.Rollback();
                                isRolledBack = true;
                            }
                            else
                            {
                                sqlTransaction.Commit();
                                File.AppendAllText(_successfulCommit, $"Successful Commit on table  {tableConfig.Schema}.{ tableConfig.Name}" + Environment.NewLine + Environment.NewLine);
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
                            Console.WriteLine(ex.Message);
                            isRolledBack = true;
                            File.AppendAllText(_exceptionpath, ex.Message + $" on table  {tableConfig.Schema}.{tableConfig.Name}" + Environment.NewLine + Environment.NewLine);

                        }


                    }
                }
            }
            return isRolledBack;
        }
        public string BuildUpdateSql(
           TableConfig tableConfig, Config config)
        {
            var charsToRemove = new string[] { "[", "]" };
            string sql = $"UPDATE {tableConfig.TargetSchema.AddDoubleQuotes()}.{tableConfig.Name.AddDoubleQuotes()} SET ";

            sql += tableConfig.Columns.GetUpdateColumns(config);
            sql += $" WHERE {tableConfig.PrimaryKeyColumn.AddDoubleQuotes()} = @{tableConfig.PrimaryKeyColumn}";
           
            var sqltOrc = new string[] { "@" };
            foreach (var c in charsToRemove)
            {
                sql = sql.Replace(c, string.Empty);
            }
            foreach (var c in sqltOrc)
            {
                sql = sql.Replace(c, ":");
            }
            return sql;
        }
        private string BuildSelectSql(
           TableConfig tableConfig, Config config)
        {
            //var clumns = tableConfig.Columns.GetSelectColumns(tableConfig.PrimaryKeyColumn)
            string sql = "";
            if (int.TryParse(tableConfig.RowCount, out int n))
            {
                sql = $"SELECT {tableConfig.Columns.GetSelectColumns(tableConfig.PrimaryKeyColumn, config)} FROM {tableConfig.Schema.AddDoubleQuotes()}.{tableConfig.Name.AddDoubleQuotes()} LIMIT {n}";
            }
            else
                sql = $"SELECT {tableConfig.Columns.GetSelectColumns(tableConfig.PrimaryKeyColumn, config)} FROM {tableConfig.Schema.AddDoubleQuotes()}.{tableConfig.Name.AddDoubleQuotes()}";
            //return sql;
            //string sql = $"SELECT  {tableConfig.Columns.GetSelectColumns(tableConfig.PrimaryKeyColumn)} FROM {tableConfig.Name}";
            if (sql.Contains("[") || sql.Contains("]"))
            {
                var charsToRemove = new string[] { "[", "]" };
                foreach (var c in charsToRemove)
                {
                    sql = sql.Replace(c, string.Empty);
                }
            }
            return sql;
        }
        public object Shuffle(string schema, string table, string column, object existingValue, bool retainNull, IEnumerable<IDictionary<string, object>> dataTable)
        {
            //ArrayList list = new ArrayList();
            CompareLogic compareLogic = new CompareLogic();
            Random rnd = new Random();
            //string sql = $"SELECT {column.AddDoubleQuotes()} FROM {schema.AddDoubleQuotes()}.{table.AddDoubleQuotes()}";
            //using (var connection = new NpgsqlConnection(_connectionStringPrd))
            //{
                try
                {


                    //connection.Open();
                    //var result = (IEnumerable<IDictionary<string, object>>)connection.Query(sql);
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
                        //var nt = values.Where(n => n != null).Select(n => n).ToArray()[rnd.Next(0, values.Where(n => n != null).ToArray().Count())];
                        return Values.Where(n => n != null).Select(n => n).ToArray()[rnd.Next(0, Values.Where(n => n != null).ToArray().Count())];
                    }
                    while (compareLogic.Compare(value, existingValue).AreEqual)
                    {

                        value = Values[rnd.Next(0, Values.Count())];
                    }

                    return value;
                }
                catch (Exception ex)
                {
                    Console.WriteLine(ex.ToString());
                    File.AppendAllText(_exceptionpath, ex.ToString() + Environment.NewLine);
                    return null;
                }
           // }

            //return list;
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
            List<IDictionary<string, object>> _sheetObject = new List<IDictionary<string, object>>();
            Dictionary<string, object> dictionary = new Dictionary<string, object>();
            foreach (DataRow row in dataTable.Rows)
            {

                dictionary = row.Table.Columns
                                .Cast<DataColumn>()
                                .ToDictionary(col => col.ColumnName, col => row.Field<object>(col.ColumnName));
                _sheetObject.Add(dictionary);
            }
          
            return _sheetObject;
        } 
        public IEnumerable<T> CreateObjecttst<T>(DataTable dataTable)
        {
           
            List<IDictionary<string,object>> _sheetObject = new List<IDictionary<string,object>>();
            Dictionary<string, object> dictionary = new Dictionary<string, object>();
            

            
            foreach (DataRow row in dataTable.Rows)
            {

                dictionary = row.Table.Columns
                                .Cast<DataColumn>()
                                .ToDictionary(col => col.ColumnName, col => row.Field<object>(col.ColumnName));
                _sheetObject.Add(dictionary);
            }
            return (IEnumerable<T>)_sheetObject;
        }
       
        public DataTable SpreadSheetTable(IEnumerable<IDictionary<string, object>> parents, TableConfig config)
        {
            var table = new DataTable();

            foreach (var parent in parents)
            {
                var children = parent.Values
                                     .OfType<IEnumerable<IDictionary<string, object>>>()
                                     .ToArray();

                var length = children.Any() ? children.Length : 1;

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
                table.Columns.AddRange(headers);

                var addedRows = new int[length];

               

                for (int i = 0; i < length; i++)
                    addedRows[i] = table.Rows.IndexOf(table.Rows.Add());

                foreach (DataColumn col in table.Columns)
                {
                    if (!allEntries.TryGetValue(col.ColumnName, out object[] columnRows))
                        continue;

                    for (int i = 0; i < addedRows.Length; i++)
                    {
                        if (columnRows[i] is byte[])
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
            //rawData = getData; 
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

                    sw.WriteLine("Successful Commits for database" + ConfigurationManager.AppSettings["DatabaseName"] + ".........." + Environment.NewLine + Environment.NewLine);
                }
            }
            return rawData;
        }

        public int GetCount(TableConfig config)
        {
            using (NpgsqlConnection connection = new NpgsqlConnection(_connectionStringPrd))
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
        private string BuildCountSql(
         TableConfig tableConfig)
        {
            return $"SELECT COUNT(*) FROM {tableConfig.Schema}.{tableConfig.Name}";
        }

        public DataTable GetDataTable(string table,string schema, string connection, string rowCount)
        {
            DataTable dataTable = new DataTable();
            using (NpgsqlConnection oracleConnection = new NpgsqlConnection(connection))
            {
                string squery = $"Select * from {schema.AddDoubleQuotes()}.{table.AddDoubleQuotes()}";
                if (int.TryParse(rowCount, out int n))
                {
                    squery = $"Select * from {schema.AddDoubleQuotes()}.{table.AddDoubleQuotes()} LIMIT {n}";
                }
                else
                    squery = $"Select * from {schema.AddDoubleQuotes()}.{table.AddDoubleQuotes()}";

                oracleConnection.Open();

                using (NpgsqlDataAdapter oda = new NpgsqlDataAdapter(squery, oracleConnection))
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
