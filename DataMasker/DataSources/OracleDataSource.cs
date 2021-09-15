using DataMasker.Interfaces;
using DataMasker.Models;
using Dapper;
using Oracle.DataAccess.Client;
using System;
using System.Collections.Generic;
using System.Linq;
using DataMasker.Utils;
using System.Data;
using System.IO;
using System.Configuration;
using DataMasker.DataLang;
using KellermanSoftware.CompareNetObjects;
using System.Globalization;
using Bogus;
using System.Threading.Tasks;
//using SqlBulkTools;
using System.Transactions;
//using Oracle.ManagedDataAccess.Client;

namespace DataMasker.DataSources
{
    public class OracleDataSource : IDataSource
    {
        private readonly DataSourceConfig _sourceConfig;
        //global::Dapper.SqlMapper.AddTypeHandler(typeof(DbGeography), new GeographyMapper());
        private static readonly string _exceptionpath = Directory.GetCurrentDirectory() + ConfigurationManager.AppSettings["_exceptionpath"];
        private static readonly string _successfulCommit = Directory.GetCurrentDirectory() + ConfigurationManager.AppSettings["_successfulCommit"];
        private static readonly DateTime DEFAULT_MIN_DATE = new DateTime(1900, 1, 1, 0, 0, 0, 0);
        private static readonly DateTime DEFAULT_MAX_DATE = DateTime.Now;
        public object[] Values { get; private set; }
        public bool IsRolledBack { get; private set; }

        public int o = 0;

        private static List<IDictionary<string, object>> rawData = new List<IDictionary<string, object>>();
        private static Dictionary<string, string> exceptionBuilder = new Dictionary<string, string>();
        private readonly string _connectionString;
        private readonly string _connectionStringPrd;

        public OracleDataSource(
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
            using (OracleConnection connection = new OracleConnection(_connectionStringPrd))
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
                IEnumerable<IDictionary<string, object>> row = null;
                List<IDictionary<string, object>> rows = new List<IDictionary<string, object>>();
                rawData = new List<IDictionary<string, object>>();
                if (rowCount != 0 && rowCount >= 10000)
                {
                    query = BuildSelectSql(tableConfig, config, rowCount, null, null);
                    using (OracleCommand cmd = new OracleCommand(query, connection))
                    {
                        cmd.InitialLOBFetchSize = 1;
                        using (OracleDataReader reader = cmd.ExecuteReader())
                        {
                            reader.FetchSize = cmd.RowSize * 10000;
                            //cmd.FetchSize = 100000;
                            if (reader.HasRows)
                            {
                                while (reader.Read())
                                {
                                    var o = Enumerable.Range(0, reader.FieldCount).ToDictionary(reader.GetName, reader.GetValue);
                                    rows.Add(o);
                                    rawData.Add(new Dictionary<string, object>(o));
                                }
                                row = rows;
                                GC.Collect();
                                GC.WaitForPendingFinalizers();
                            }
                        }
                        cmd.Connection = null;
                        OracleConnection.ClearAllPools();
                        connection.Dispose();
                    }
                    return row;
                }
                else
                {
                    query = BuildSelectSql(tableConfig, config, rowCount, offset, fetch);
                    rawData = new List<IDictionary<string, object>>();
                    var _prdData = (IEnumerable<IDictionary<string, object>>)connection.Query(query, buffered: true);
                    foreach (IDictionary<string, object> prd in _prdData)
                    {
                        rawData.Add(new Dictionary<string, object>(prd));
                    }
                    connection.Close();
                    return _prdData;
                }
            }
        }
        public static string ByteArrayToString(byte[] ba)
        {
            return BitConverter.ToString(ba).Replace("-", "");
        }
        private string BuildCountSql(
           TableConfig tableConfig)
        {
            return $"SELECT COUNT(*) FROM {tableConfig.Schema}.{tableConfig.Name}";
        }
        public void UpdateRow(IDictionary<string, object> row, TableConfig tableConfig, Config config)
        {
            using (var connection = new OracleConnection(_connectionString))
            {
                connection.Open();
                connection.Execute(BuildUpdateSql(tableConfig, config), row, null, commandType: System.Data.CommandType.Text);
            }
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
            IsRolledBack = false;
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
                    sw.WriteLine("Successful Commits for " + ConfigurationManager.AppSettings["DatabaseName"] + "on schema " + tableConfig.TargetSchema + ".........." + Environment.NewLine + Environment.NewLine);
                }
            }
            using (OracleConnection connection = new OracleConnection(_connectionString))
            {
                connection.Open();
                OracleCommand cmd = connection.CreateCommand();
                cmd.Parameters.Clear();
                var Parameters = Mapping(cmdParameters);
                OracleParameterCollection orp = cmd.Parameters;


                foreach (Batch<IDictionary<string, object>> batch in batches)
                {
                    using (OracleTransaction sqlTransaction = connection.BeginTransaction())
                    {
                        string sql = BuildUpdateSql(tableConfig, config);
                        try
                        {
                            foreach (var columnParameter in Parameters)
                            {
                                if (columnParameter.Value.Key == OracleDbType.NVarchar2)
                                {
                                    cmd.Parameters.Add(new OracleParameter(columnParameter.Key, columnParameter.Value.Key)
                                    {
                                        Value = batch.Items.SelectMany(n => n.ToList()).Where(n => n.Key.Equals(columnParameter.Key)).Select(n => n.Value).ToArray(),
                                        UdtTypeName = "MDSYS.SDO_GEOMETRY"
                                    });

                                }
                                else if (columnParameter.Value.Key == OracleDbType.Varchar2 || columnParameter.Value.Key == OracleDbType.NVarchar2)
                                {
                                    int size = Convert.ToInt32(columnParameter.Value.Value);
                                    cmd.Parameters.Add(new OracleParameter(columnParameter.Key, columnParameter.Value.Key, size)
                                    {
                                        Value = batch.Items.SelectMany(n => n.ToList()).Where(n => n.Key.Equals(columnParameter.Key)).Select(n => n.Value).ToArray()
                                    });
                                }
                                else
                                    cmd.Parameters.Add(new OracleParameter(columnParameter.Key, columnParameter.Value.Key)
                                    {
                                        Value = batch.Items.SelectMany(n => n.ToList()).Where(n => n.Key.Equals(columnParameter.Key)).Select(n => n.Value).ToArray()
                                    });

                            }
                            cmd.CommandText = sql;
                            cmd.CommandTimeout = 0;
                            cmd.BindByName = true;
                            cmd.ArrayBindCount = batch.Items.Count;
                            int affectedRows = cmd.ExecuteNonQuery();
                            if (affectedRows > 0)
                            {
                                sqlTransaction.Commit();
                                cmd.Parameters.Clear();
                                File.AppendAllText(_successfulCommit, $"Batch {batch.BatchNo} - Successfully Commit {affectedRows} records on table  {tableConfig.TargetSchema}.{tableConfig.Name} - " + DateTime.Now.ToString() + Environment.NewLine + Environment.NewLine);
                            }
                            else
                                IsRolledBack = true;



                            //var columns = tableConfig.Columns.Where(n => !n.Ignore).Select(n => n.Name);
                            //IBulkOperations bulk = new BulkOperations();
                            //bulk.Setup().ForCollection(batch.Items)
                            //    .WithTable(tableConfig.Name)
                            //    .AddColumns(x=>columns)
                            //    .BulkUpdate()
                            //    .SetIdentityColumn(x=>tableConfig.PrimaryKeyColumn)
                            //    .MatchTargetOn(x => tableConfig.PrimaryKeyColumn)
                            //    .Commit(connection);

                            // bulk.Setup<TableConfig>(x=>x.ForCollection)

                            //connection.Execute(sql, batch.Items, sqlTransaction);
                            if (_sourceConfig.DryRun)
                            {
                                sqlTransaction.Rollback();
                                IsRolledBack = true;
                            }
                            if (updatedCallback != null)
                            {
                                totalUpdated += batch.Items.Count;
                                updatedCallback.Invoke(totalUpdated);
                            }
                        }
                        catch (Exception ex)
                        {
                            IsRolledBack = true;
                            Console.WriteLine(ex.Message);
                            File.AppendAllText(_exceptionpath, ex.Message + $" on table {tableConfig.TargetSchema}.{tableConfig.Name}" + Environment.NewLine + Environment.NewLine);
                        }
                    }
                    cmd.Parameters.Clear();
                }
            }
            return IsRolledBack;
        }
        public string BuildUpdateSql(
           TableConfig tableConfig, Config config)
        {
            var charsToRemove = new string[] { "[", "]" };
            string sql = $"UPDATE {tableConfig.TargetSchema}.{tableConfig.Name} SET ";

            sql += tableConfig.Columns.GetUpdateColumns(config);
            sql += $" WHERE {tableConfig.PrimaryKeyColumn} = @{tableConfig.PrimaryKeyColumn}";
            //thisis oracle replace @ WITH :
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
           TableConfig tableConfig, Config config, int rowCount, int? offset = null, int? fetch = null)
        {
            //var clumns = tableConfig.Columns.GetSelectColumns(tableConfig.PrimaryKeyColumn)
            string sql = "";
            if (int.TryParse(tableConfig.RowCount, out int n))
            {
                sql = $"SELECT  {tableConfig.Columns.GetSelectColumns(tableConfig.PrimaryKeyColumn, config)} FROM {tableConfig.Schema}.{tableConfig.Name} WHERE rownum <=" + n;
            }
            else if (tableConfig.RowCount.Contains("-") && tableConfig.RowCount.Split('-').Count() == 2)
            {
                int j = Convert.ToInt32(tableConfig.RowCount.Split('-').FirstOrDefault());
                int k = Convert.ToInt32(tableConfig.RowCount.Split('-')[1]);
                sql = $"SELECT * FROM (SELECT {tableConfig.Columns.GetSelectColumns(tableConfig.PrimaryKeyColumn, config)}, rownum r FROM {tableConfig.Schema}.{tableConfig.Name}) WHERE r BETWEEN {j} AND {k}";
            }
            else if (rowCount > 100000 && offset != null && fetch != null)
            {
                sql = $"SELECT  {tableConfig.Columns.GetSelectColumns(tableConfig.PrimaryKeyColumn, config)} FROM {tableConfig.Schema}.{tableConfig.Name} ORDER BY {tableConfig.PrimaryKeyColumn} OFFSET {offset} ROWS FETCH NEXT {fetch} ROWS ONLY";
            }
            else
                sql = $"SELECT  {tableConfig.Columns.GetSelectColumns(tableConfig.PrimaryKeyColumn, config)} FROM {tableConfig.Schema}.{tableConfig.Name}";

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
            CompareLogic compareLogic = new CompareLogic();
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
                    return Values.Where(n => n != null).Select(n => n).ToArray()[rnd.Next(0, Values.Where(n => n != null).ToArray().Count())];
                }
                while (compareLogic.Compare(value, existingValue).AreEqual)
                {

                    value = Values[rnd.Next(0, Values.Count())];
                }
                if (value is SdoGeometry)
                {
                    return (SdoGeometry)value;
                }
                return value;
            }
            catch (Exception ex)
            {

                Console.WriteLine(ex.ToString());
                File.AppendAllText(_exceptionpath, ex.ToString() + Environment.NewLine);
                return null;
            }


            //}

            //return list;
        }


        public object GetData(string column, string table)
        {
            throw new NotImplementedException();
        }
        public static Dictionary<string, KeyValuePair<OracleDbType, string>> Mapping(IDictionary<string, KeyValuePair<string, string>> oracleColumn)
        {
            Dictionary<string, KeyValuePair<OracleDbType, string>> val = new Dictionary<string, KeyValuePair<OracleDbType, string>>();
            foreach (var item in oracleColumn)
            {
                switch (ToEnum(item.Value.Key, Orctypes.none))
                {
                    case Orctypes.VARCHAR2:
                        val.Add(item.Key, new KeyValuePair<OracleDbType, string>(OracleDbType.Varchar2, item.Value.Value));
                        break;
                    case Orctypes.NVARCHAR2:
                        val.Add(item.Key, new KeyValuePair<OracleDbType, string>(OracleDbType.NVarchar2, item.Value.Value));
                        break;
                    case Orctypes.VARCHAR:
                        val.Add(item.Key, new KeyValuePair<OracleDbType, string>(OracleDbType.Varchar2, item.Value.Value));
                        break;
                    case Orctypes.CHAR:
                        val.Add(item.Key, new KeyValuePair<OracleDbType, string>(OracleDbType.Char, item.Value.Value));
                        break;
                    case Orctypes.NCHAR:
                        val.Add(item.Key, new KeyValuePair<OracleDbType, string>(OracleDbType.NChar, item.Value.Value));
                        break;
                    case Orctypes.NUMBER:
                        if (string.IsNullOrEmpty(item.Value.Value))
                        {
                            val.Add(item.Key, new KeyValuePair<OracleDbType, string>(OracleDbType.Decimal, item.Value.Value));
                            break;
                        }
                        else if (Convert.ToInt32(item.Value.Value) >= 2 && Convert.ToInt32(item.Value.Value) >= 9)
                        {
                            val.Add(item.Key, new KeyValuePair<OracleDbType, string>(OracleDbType.Int32, item.Value.Value));
                            break;
                        }
                        else if (Convert.ToInt32(item.Value.Value) >= 10 && Convert.ToInt32(item.Value.Value) >= 18)
                        {
                            val.Add(item.Key, new KeyValuePair<OracleDbType, string>(OracleDbType.Int64, item.Value.Value));
                            break;
                        }
                        else
                        {
                            val.Add(item.Key, new KeyValuePair<OracleDbType, string>(OracleDbType.Int32, item.Value.Value));
                            break;
                        }
                    case Orctypes.BINARY_FLOAT:
                        val.Add(item.Key, new KeyValuePair<OracleDbType, string>(OracleDbType.BinaryFloat, item.Value.Value));
                        break;
                    case Orctypes.BINARY_DOUBLE:
                        val.Add(item.Key, new KeyValuePair<OracleDbType, string>(OracleDbType.BinaryDouble, item.Value.Value));
                        break;
                    case Orctypes.BOOLEAN:
                        val.Add(item.Key, new KeyValuePair<OracleDbType, string>(OracleDbType.Boolean, item.Value.Value));
                        break;
                    case Orctypes.PLS_INTEGER:
                        val.Add(item.Key, new KeyValuePair<OracleDbType, string>(OracleDbType.Int64, item.Value.Value));
                        break;
                    case Orctypes.LONG:
                        val.Add(item.Key, new KeyValuePair<OracleDbType, string>(OracleDbType.Long, item.Value.Value));
                        break;
                    case Orctypes.DATE:
                        val.Add(item.Key, new KeyValuePair<OracleDbType, string>(OracleDbType.Date, item.Value.Value));
                        break;
                    case Orctypes.TIMESTAMP:
                        val.Add(item.Key, new KeyValuePair<OracleDbType, string>(OracleDbType.TimeStamp, item.Value.Value));
                        break;
                    case Orctypes.RAW:
                        val.Add(item.Key, new KeyValuePair<OracleDbType, string>(OracleDbType.Raw, item.Value.Value));
                        break;
                    case Orctypes.CLOB:
                        val.Add(item.Key, new KeyValuePair<OracleDbType, string>(OracleDbType.Clob, item.Value.Value));
                        break;
                    case Orctypes.NCLOB:
                        val.Add(item.Key, new KeyValuePair<OracleDbType, string>(OracleDbType.NClob, item.Value.Value));
                        break;
                    case Orctypes.BLOB:
                        val.Add(item.Key, new KeyValuePair<OracleDbType, string>(OracleDbType.Blob, item.Value.Value));
                        break;
                    case Orctypes.BFILE:
                        val.Add(item.Key, new KeyValuePair<OracleDbType, string>(OracleDbType.BFile, item.Value.Value));
                        break;
                    case Orctypes.XMLType:
                        val.Add(item.Key, new KeyValuePair<OracleDbType, string>(OracleDbType.XmlType, item.Value.Value));
                        break;
                    case Orctypes.SDO_GEOMETRY:
                        val.Add(item.Key, new KeyValuePair<OracleDbType, string>(OracleDbType.Object, item.Value.Value));
                        break;
                    case Orctypes.none:
                        throw new ArgumentOutOfRangeException();
                    default:
                        throw new ArgumentOutOfRangeException();
                }
            }
            return val;
        }
        public enum Orctypes
        {
            VARCHAR2,
            NVARCHAR2,
            VARCHAR,
            CHAR,
            NCHAR,
            NUMBER,
            BINARY_FLOAT,
            BINARY_DOUBLE,
            BOOLEAN,
            PLS_INTEGER,
            LONG,
            DATE,
            TIMESTAMP,
            RAW,
            CLOB,
            NCLOB,
            BLOB,
            BFILE,
            XMLType,
            SDO_GEOMETRY,
            none
        }
        public static T ToEnum<T>(string value, T defaultValue) where T : struct
        {
            if (string.IsNullOrEmpty(value))
            {
                return defaultValue;
            }

            try
            {
                if (!Enum.TryParse(value, true, out T enumValue))
                {
                    return defaultValue;
                }
                return enumValue;
            }
            catch (Exception)
            {
                return defaultValue;
            }
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
                            "yyyyMMdd", "yyyyMdd hh:mm","yyyy",


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
                            if (columnConfig.Type == DataType.Geometry || columnConfig.Type == DataType.Shufflegeometry || columnConfig.Type == DataType.ShufflePolygon)
                            {
                                header.DataType = typeof(SdoGeometry);
                            }
                            else if (columnConfig.Type == DataType.Blob)
                            {
                                header.DataType = typeof(byte[]);
                            }
                            else if (columnConfig.Type == DataType.DateOfBirth)
                            {
                                header.DataType = typeof(DateTime);
                            }

                        }
                    }



                    table.Columns.Add(header);
                }


                var addedRows = new int[length];
                //var xxx = table.Rows.Add();
                for (int i = 0; i < length; i++)
                    addedRows[i] = table.Rows.IndexOf(table.Rows.Add());

                try
                {


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
                                    columnRows[i] = DateTime.TryParse(columnRows[i].ToString(), out DateTime temp) ? temp : faker.Date.Between(DEFAULT_MIN_DATE, DEFAULT_MAX_DATE);

                                    table.Rows[addedRows[i]][col] = columnRows[i];
                                }
                                else if (columnRows[i] is null)
                                {
                                    table.Rows[addedRows[i]][col] = DBNull.Value;
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
                catch (Exception ex)
                {

                    Console.WriteLine(ex.ToString());
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
            //rawData = getData; 
            if (!(File.Exists(_successfulCommit) && File.Exists(_exceptionpath)))
            {


                //write to the file
                File.Create(_successfulCommit).Close();

                //write to the file
                File.Create(_exceptionpath).Close();



            }
            using (StreamWriter sw = File.AppendText(_exceptionpath))
            {
                if (new FileInfo(_exceptionpath).Length == 0)
                {
                    sw.WriteLine("exceptions for " + ConfigurationManager.AppSettings["APP_NAME"] + ".........." + Environment.NewLine + Environment.NewLine);
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

                    sw.WriteLine("Successful Commits for " + ConfigurationManager.AppSettings["APP_NAME"] + ".........." + Environment.NewLine + Environment.NewLine);
                }
            }
            return rawData;
        }

        public int GetCount(TableConfig config)
        {
            using Oracle.DataAccess.Client.OracleConnection connection = new Oracle.DataAccess.Client.OracleConnection(_connectionStringPrd);
            try
            {
                connection.Open();
                Console.ForegroundColor = ConsoleColor.Green;
                Console.WriteLine("Database Connection established");
                Console.ResetColor();
            }
            catch (Exception e)
            {
                Console.ForegroundColor = ConsoleColor.Red;
                Console.WriteLine("Exception occurs {0}", e.Message);
                Console.WriteLine("Program will exit: Press ENTER to exist..");
                Console.ReadLine();

                File.WriteAllText(_exceptionpath, e.Message + Environment.NewLine + Environment.NewLine);
                System.Environment.Exit(1);
            }
            var count = connection.ExecuteScalar(BuildCountSql(config));
            return Convert.ToInt32(count);
        }

        public IEnumerable<T> CreateObjecttst<T>(DataTable dataTable)
        {
            throw new NotImplementedException();
        }
        class HazSqlGeo
        {
            //public int Id { get; set; }
            public SdoDimArray Geo { get; set; }

        }
        public DataTable GetDataTable(string table, string schema, string connection, string rowCount)
        {
            DataTable dataTable = new DataTable();
            List<object> geoInfoList = new List<object>();
            using (OracleConnection oracleConnection = new OracleConnection(connection))
            {
                string squery = "";
                if (schema == "MDSYS")
                {
                    var view = "USER_SDO_GEOM_METADATA";
                    squery = $"Select * from {schema}.{view} where TABLE_NAME = {table.AddSingleQuotes()}";
                }
                else
                {
                    if (int.TryParse(rowCount, out int n))
                    {
                        squery = $"Select * from {schema}.{table} WHERE rownum <=" + n;
                    }
                    else
                        squery = $"Select * from {schema}.{table}";

                    //squery = $"Select * from {schema}.{table}";
                }
                oracleConnection.Open();
                using (OracleDataAdapter oda = new OracleDataAdapter(squery, oracleConnection))
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

        public async Task<IEnumerable<IDictionary<string, object>>> GetAsyncData(TableConfig tableConfig, Config config)
        {
            using (var connection = new OracleConnection(_connectionStringPrd))
            {
                await connection.OpenAsync();
                var query = BuildSelectSql(tableConfig, config, 0, null, null);
                rawData = new List<IDictionary<string, object>>();
                var result = await connection.QueryAsync<IDictionary<string, object>>(query, commandTimeout: 0);
                return result;
            }
        }


    }
}
