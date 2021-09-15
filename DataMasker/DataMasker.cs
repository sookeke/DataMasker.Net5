using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Bogus.DataSets;
using DataMasker.Interfaces;
using DataMasker.Models;
using System.Runtime.Serialization.Formatters.Binary;
using System.IO;
using System.Data;
using System.Xml;
using System.Xml.Serialization;
using System.Configuration;
using ChoETL;
using Bogus;

namespace DataMasker
{
    /// <summary>
    /// DataMasker
    /// </summary>
    /// <seealso cref="DataMasker.Interfaces.IDataMasker"/>
    public class DataMasker : IDataMasker
    {
        /// <summary>
        /// The maximum iterations allowed when attempting to retrieve a unique value per column
        /// </summary>
        private const int MAX_UNIQUE_VALUE_ITERATIONS = 5000;
        /// <summary>
        /// The data generator
        /// </summary>
        private readonly IDataGenerator _dataGenerator;
        private static readonly string _exceptionpath = Directory.GetCurrentDirectory() + ConfigurationManager.AppSettings["_exceptionpath"];
        //private static readonly DataSourceProvider dataSourceProvider;
        private static Randomizer _randomizer;
        /// <summary>
        /// A dictionary key'd by {tableName}.{columnName} containing a <see cref="HashSet{T}"/> of values which have been previously used for this table/column
        /// </summary>
        private readonly ConcurrentDictionary<string, HashSet<object>> _uniqueValues = new ConcurrentDictionary<string, HashSet<object>>();
        private readonly DataTable _location = new DataTable() { Columns = { "Country", "States", "Province", "City", "Address"} };
        private static readonly List<KeyValuePair<object, object>> uniquevalue = new List<KeyValuePair<object, object>>();
        //private readonly IDataSource _dataSource;



        /// <summary>
        /// Initializes a new instance of the <see cref="DataMasker"/> class.
        /// </summary>
        /// <param name="dataGenerator">The data generator.</param>
        public DataMasker(
            IDataGenerator dataGenerator)
        {
            _dataGenerator = dataGenerator;
        }
        //public DataSources()

        /// <summary>
        /// Masks the specified object with new data
        /// </summary>
        /// <param name="obj">The object to mask</param>
        /// <param name="tableConfig">The table configuration.</param>
        /// <returns></returns>
        public IDictionary<string, object> Mask(
            IDictionary<string, object> obj,
            TableConfig tableConfig, IDataSource dataSource,int rowCount, IEnumerable<IDictionary<string,object>> data, DataTable dataTable)
        {
            var addr = new DataTable();
            _location.Rows.Clear();
            foreach (ColumnConfig columnConfig in tableConfig.Columns.Where(x => !x.Ignore && x.Type != DataType.Computed))
            {
                try
                {
                    object existingValue = obj[columnConfig.Name];
                    string uniqueCacheKey = $"{tableConfig.Name}.{columnConfig.Name}";
                    Name.Gender? gender = null;
                    if (!string.IsNullOrEmpty(columnConfig.UseGenderColumn) && obj[columnConfig.UseGenderColumn] != null)
                    {
                        object g = obj[columnConfig.UseGenderColumn];
                        gender = Utils.Utils.TryParseGender(g?.ToString());
                    }
                    if (columnConfig.Unique)
                    {
                        existingValue = GetUniqueValue(tableConfig.Name, columnConfig, existingValue, gender);
                    }
                    else if (columnConfig.Type == DataType.Unmask)
                    {
                        obj[columnConfig.Name] = existingValue;
                    }
                    else if (columnConfig.Type == DataType.Shuffle || columnConfig.Type == DataType.Shufflegeometry || columnConfig.Type == DataType.ShufflePolygon)
                    {
                        var columndata = data.Select(n => n.Where(x => x.Key.Equals(columnConfig.Name)).ToDictionary());
                        if (string.IsNullOrEmpty(tableConfig.Schema))
                        {
                            existingValue = _dataGenerator.GetValueShuffle(columnConfig, "", $"{tableConfig.Name}", columnConfig.Name, dataSource, columndata, existingValue, gender);
                        }
                        else
                            existingValue = _dataGenerator.GetValueShuffle(columnConfig, $"{tableConfig.Schema}", $"{tableConfig.Name}", columnConfig.Name, dataSource, columndata, existingValue, gender);
                    }
                    else if (columnConfig.Type == DataType.File)
                    {
                        if (existingValue.ToString().Contains("."))
                        {
                            columnConfig.StringFormatPattern = existingValue.ToString().Substring(existingValue.ToString().LastIndexOf('.') + 1);
                        }
                        else
                        {
                            //columnConfig.StringFormatPattern = "{{SYSTEM.FILENAME}}";
                            columnConfig.Type = DataType.Filename;
                        }

                        existingValue = _dataGenerator.GetValue(columnConfig, existingValue, tableConfig.Name, gender);

                        // existingValue = _dataGenerator.get(columnConfig, tableConfig.Name, columnConfig.Name, dataSource, existingValue, columnConfig.StringFormatPattern, gender)
                    }
                    else if (columnConfig.Type == DataType.math)
                    {
                        if (!string.IsNullOrEmpty(columnConfig.UseValue))
                        {
                            existingValue = ConvertValue(columnConfig.Type, columnConfig.UseValue);
                        }
                        else
                        {
                            try
                            {
                                List<object> source = new List<object>();
                                if (string.IsNullOrEmpty(columnConfig.StringFormatPattern) && columnConfig.Max == null && columnConfig.StringFormatPattern.Contains(","))
                                {
                                    throw new InvalidOperationException("StringFormatPattern and Max Cannot be empty");
                                }
                                if (columnConfig.Name.ToUpper() == columnConfig.StringFormatPattern.ToUpper())
                                {
                                    //only number objects
                                    if (IsNumeric(obj[columnConfig.Name]))
                                    {
                                        source.Add(obj[columnConfig.Name]);
                                    }
                                    else
                                        throw new InvalidOperationException(columnConfig.Name + " must be Numeric type for " + columnConfig.Operator);

                                }
                                else
                                {
                                    //check position of stringformat pattern objects
                                    var columnPosition = tableConfig.Columns.Select(n => n.Name).ToList();
                                    foreach (var item in columnConfig.StringFormatPattern.Split(','))
                                    {
                                        //column A should have been masked alongside column B to apply operation: Col A + Col B = Col C
                                        if (!(columnPosition.IndexOf(columnConfig.Name) > columnPosition.IndexOf(item)))
                                        {
                                            throw new InvalidOperationException(columnConfig.Name + " Index must be Greater than " + item);
                                        }
                                        else
                                        {
                                            //only number objects
                                            if (IsNumeric(obj[item]))
                                            {
                                                source.Add(obj[item]);
                                            }
                                            else
                                                throw new InvalidOperationException(item + " must be Numeric type for " + columnConfig.Operator);

                                        }

                                    }
                                }
                                existingValue = _dataGenerator.MathOperation(columnConfig, existingValue, source.ToArray(), columnConfig.Operator, Convert.ToInt32(Math.Round(Convert.ToDecimal(columnConfig.Max))));

                            }
                            catch (Exception ex)
                            {
                                File.AppendAllText(_exceptionpath, "Masking Operation InvalidOperationException: " + ex.Message + Environment.NewLine);
                                // throw;
                            }
                        }
                    }
                    else if (columnConfig.Type == DataType.MaskingOut)
                    {
                        if (!string.IsNullOrEmpty(columnConfig.UseValue))
                        {
                            existingValue = ConvertValue(columnConfig.Type, columnConfig.UseValue);
                        }
                        else
                        {
                            if (columnConfig.StringFormatPattern.ToUpper().Contains("MaskingRight".ToUpper()))
                            {
                                existingValue = MaskingRight(existingValue, Convert.ToInt32(columnConfig.Max), tableConfig.Name, columnConfig);
                            }
                            else if (columnConfig.StringFormatPattern.ToUpper().Contains("MaskingLeft".ToUpper()))
                            {
                                existingValue = MaskingLeft(existingValue, Convert.ToInt32(columnConfig.Max), tableConfig.Name, columnConfig);
                            }
                            else if (columnConfig.StringFormatPattern.ToUpper().Contains("MaskingMiddle".ToUpper()))
                            {
                                existingValue = MaskingMiddle(existingValue, Convert.ToInt32(columnConfig.Max), tableConfig.Name, columnConfig);
                            }
                            else
                                throw new ArgumentException("Invalid MaskingOut Operation", columnConfig.StringFormatPattern);
                        }
                    }
                    else if (columnConfig.Type == DataType.Scramble)
                    {
                        existingValue = DataScramble(existingValue, tableConfig.Name, columnConfig);
                    }
                    else if (columnConfig.Type == DataType.exception)
                    {
                        var cc = existingValue.ToString().Length;
                        if (existingValue.ToString().Length > Convert.ToInt32(columnConfig.StringFormatPattern))
                        {
                            columnConfig.Ignore = false;
                            existingValue = _dataGenerator.GetValue(columnConfig, existingValue, tableConfig.Name, gender);
                        }
                    }
                    else if (_location.Columns.Cast<DataColumn>().Where(s => columnConfig.Name.ToUpper().Contains(s.ColumnName.ToUpper())).Count() == 1)
                    {
                        //check if a column in the table contains 1 column in the location table column
                        //check for multi line addressin the table 
                        //var multiLine = tableConfig.Columns.Where(n => columnConfig.Name.Contains(n.Name)).ToList().Count > 2;
                        bool u;
                        var cname = _location.Columns.Cast<DataColumn>().Where(s => columnConfig.Name.ToUpper().Contains(s.ColumnName.ToUpper())).ToList().FirstOrDefault();//get the exact column match
                        try
                        {
                            //if (columnConfig.Type == DataType.SecondaryAddress || columnConfig.Type == DataType.StreetAddress)
                            //{
                            //    existingValue = _dataGenerator.GetValue(columnConfig, existingValue, tableConfig.Name, gender);
                           // }
                            //else
                            //{
                                if (_location.Rows.Count == 0)
                                {
                                    u = tableConfig.Columns.Any(n => n.Name.ToUpper().Equals("CITY")) && (tableConfig.Columns.Any(n => n.Name.ToUpper().Equals("STATE")) || tableConfig.Columns.Any(n => n.Name.ToUpper().Equals("PROVINCE")));
                                    addr = (DataTable)_dataGenerator.OpenDatabaseAddress(columnConfig, existingValue, _location, u);
                                }
                                if (addr == null)
                                {
                                    existingValue = null;

                                }
                                else if (_location.Rows.Count > 0)
                                {
                                        if (columnConfig.RetainNullValues &&
                                            existingValue == null)
                                        {
                                            existingValue = null;
                                        }
                                        else
                                            existingValue = _location.Rows[0][cname.ColumnName];
                                }
                                else
                                    File.AppendAllText(_exceptionpath, "Could not Generate addresses on " + $"{tableConfig.Name}.{columnConfig.Name}" + " and will return original: " + Environment.NewLine);
                           // }
                        }
                        catch (Exception ex)
                        {
                            File.AppendAllText(_exceptionpath, "Could not Generate addresses on " + $"{tableConfig.Name}.{columnConfig.Name}" + "  and will return original: " + ex.Message + Environment.NewLine);
                            existingValue = _dataGenerator.GetValue(columnConfig, existingValue, tableConfig.Name, gender);
                        }
                    }
                    else
                    {
                        existingValue = _dataGenerator.GetValue(columnConfig, existingValue, tableConfig.Name, gender);
                    }
                    //replace the original value
                    obj[columnConfig.Name] = existingValue;
                }
                catch (Exception ex)
                {
                   // Console.WriteLine(ex.ToString());
                    Console.WriteLine(ex.ToString());
                    File.AppendAllText(_exceptionpath, $"Unable to mask column {columnConfig.Name} with the following error: {ex.Message}" + Environment.NewLine);
                }
            }

          foreach (ColumnConfig columnConfig in tableConfig.Columns.Where(x => !x.Ignore && x.Type == DataType.Computed))
          {
            var separator = columnConfig.Separator ?? " ";
            StringBuilder colValue = new StringBuilder();
            bool first = true;
            foreach (var sourceColumn in columnConfig.SourceColumns)
            {
              if (!obj.ContainsKey(sourceColumn))
              {
                throw new Exception($"Source column {sourceColumn} could not be found.");
              }

              if (first)
              {
                first = false;
              }
              else
              {
                colValue.Append(separator);
              }
              colValue.Append(obj[sourceColumn] ?? String.Empty);
             }
            obj[columnConfig.Name] = colValue.ToString();
          }
          return obj;
        }

        public static bool IsNumeric(object Expression)
        {
            //double retNum;
            if (Expression == null)
            {
                Expression = 0;
            }
            bool isNum = double.TryParse(Convert.ToString(Expression), System.Globalization.NumberStyles.Any, System.Globalization.NumberFormatInfo.InvariantInfo, out double retNum);
            return isNum;
        }
        public IDictionary<string, object> MaskBLOB(IDictionary<string, object> obj,
            TableConfig tableConfig, IDataSource dataSource, IEnumerable<IDictionary<string, object>> data, string filename, FileTypes fileExtension, string blobLocation)
        {
            var addr = new DataTable();
            _location.Rows.Clear();
            foreach (ColumnConfig columnConfig in tableConfig.Columns.Where(x => !x.Ignore && x.Type != DataType.Computed))
            {
                object existingValue = obj[columnConfig.Name];
                Name.Gender? gender = null;
                if (!string.IsNullOrEmpty(columnConfig.UseGenderColumn))
                {
                    object g = obj[columnConfig.UseGenderColumn];
                    gender = Utils.Utils.TryParseGender(g?.ToString());
                }

                if (columnConfig.Unique)
                {
                    existingValue = GetUniqueValue(tableConfig.Name, columnConfig, existingValue, gender);
                }
                else if (columnConfig.Unmask == true)
                {
                    obj[columnConfig.Name] = existingValue;
                }
                else if (columnConfig.Type == DataType.Filename && !string.IsNullOrEmpty(columnConfig.StringFormatPattern))
                {
                    //   // existingValue = _dataGenerator.GetBlobValue(columnConfig, tableConfig.Name, columnConfig.Name, dataSource, existingValue, columnConfig.StringFormatPattern, gender)
                    existingValue = _dataGenerator.GetBlobValue(columnConfig, dataSource, existingValue, filename, fileExtension, blobLocation, gender);
                }
                else if (columnConfig.Type == DataType.Blob)
                {
                    existingValue = _dataGenerator.GetBlobValue(columnConfig, dataSource, existingValue, filename, fileExtension, blobLocation, gender);
                }
                else if (columnConfig.Type == DataType.Shuffle || columnConfig.Type == DataType.Shufflegeometry)
                {
                    var columndata = data.Select(n => n.Where(x => x.Key.Equals(columnConfig.Name)).ToDictionary());
                    existingValue = _dataGenerator.GetValueShuffle(columnConfig, tableConfig.Schema, tableConfig.Name, columnConfig.Name, dataSource, columndata, existingValue, gender);
                }
                else if (columnConfig.Type == DataType.File)
                {
                    if (existingValue.ToString().Contains("."))
                    {
                        columnConfig.StringFormatPattern = existingValue.ToString().Substring(existingValue.ToString().LastIndexOf('.') + 1);
                    }
                    else
                    {
                        //columnConfig.StringFormatPattern = "{{SYSTEM.FILENAME}}";
                        columnConfig.Type = DataType.Filename;
                    }

                    existingValue = _dataGenerator.GetValue(columnConfig, existingValue, tableConfig.Name, gender);

                    // existingValue = _dataGenerator.get(columnConfig, tableConfig.Name, columnConfig.Name, dataSource, existingValue, columnConfig.StringFormatPattern, gender)
                }
                else if (columnConfig.Type == DataType.math)
                {
                    if (!string.IsNullOrEmpty(columnConfig.UseValue))
                    {
                        existingValue = ConvertValue(columnConfig.Type, columnConfig.UseValue);
                    }
                    else
                    {
                        try
                        {
                            List<object> source = new List<object>();
                            if (string.IsNullOrEmpty(columnConfig.StringFormatPattern) && columnConfig.Max == null && columnConfig.StringFormatPattern.Contains(","))
                            {
                                throw new InvalidOperationException("StringFormatPattern and Max Cannot be empty");
                            }
                            if (columnConfig.Name.ToUpper() == columnConfig.StringFormatPattern.ToUpper())
                            {
                                //only number objects
                                if (IsNumeric(obj[columnConfig.Name]))
                                {
                                    source.Add(obj[columnConfig.Name]);
                                }
                                else
                                    throw new InvalidOperationException(columnConfig.Name + " must be Numeric type for " + columnConfig.Operator);

                            }
                            else
                            {
                                //check position of stringformat pattern objects
                                var columnPosition = tableConfig.Columns.Select(n => n.Name).ToList();
                                foreach (var item in columnConfig.StringFormatPattern.Split(','))
                                {
                                    //column A should have been masked alongside column B to apply operation: Col A + Col B = Col C
                                    if (!(columnPosition.IndexOf(columnConfig.Name) > columnPosition.IndexOf(item)))
                                    {
                                        throw new InvalidOperationException(columnConfig.Name + " Index must be Greater than " + item);
                                    }
                                    else
                                    {
                                        //only number objects
                                        if (IsNumeric(obj[item]))
                                        {
                                            source.Add(obj[item]);
                                        }
                                        else
                                            throw new InvalidOperationException(item + " must be Numeric type for " + columnConfig.Operator);

                                    }

                                }
                            }
                            existingValue = _dataGenerator.MathOperation(columnConfig, existingValue, source.ToArray(), columnConfig.Operator, Convert.ToInt32(Math.Round(Convert.ToDecimal(columnConfig.Max))));

                        }
                        catch (Exception ex)
                        {
                            File.AppendAllText(_exceptionpath, "Masking Operation InvalidOperationException: " + ex.Message + Environment.NewLine);
                            // throw;
                        }
                    }
                }
                else if (columnConfig.Type == DataType.MaskingOut)
                {
                    if (!string.IsNullOrEmpty(columnConfig.UseValue))
                    {
                        existingValue = ConvertValue(columnConfig.Type, columnConfig.UseValue);
                    }
                    else
                    {
                        if (columnConfig.StringFormatPattern.ToUpper().Contains("MaskingRight".ToUpper()))
                        {
                            existingValue = MaskingRight(existingValue, Convert.ToInt32(columnConfig.Max), tableConfig.Name, columnConfig);
                        }
                        else if (columnConfig.StringFormatPattern.ToUpper().Contains("MaskingLeft".ToUpper()))
                        {
                            existingValue = MaskingLeft(existingValue, Convert.ToInt32(columnConfig.Max), tableConfig.Name, columnConfig);
                        }
                        else if (columnConfig.StringFormatPattern.ToUpper().Contains("MaskingMiddle".ToUpper()))
                        {
                            existingValue = MaskingMiddle(existingValue, Convert.ToInt32(columnConfig.Max), tableConfig.Name, columnConfig);
                        }
                        else
                            throw new ArgumentException("Invalid MaskingOut Operation", columnConfig.StringFormatPattern);
                    }
                }
                else if (columnConfig.Type == DataType.Scramble)
                {
                    existingValue = DataScramble(existingValue, tableConfig.Name, columnConfig);
                }
                else if (columnConfig.Type == DataType.exception)
                {
                    var cc = existingValue.ToString().Length;
                    if (existingValue.ToString().Length > Convert.ToInt32(columnConfig.StringFormatPattern))
                    {
                        columnConfig.Ignore = false;
                        existingValue = _dataGenerator.GetValue(columnConfig, existingValue, tableConfig.Name, gender);
                    }
                }
                else if (_location.Columns.Cast<DataColumn>().Where(s => columnConfig.Name.ToUpper().Contains(s.ColumnName.ToUpper())).Count() == 1)
                {
                    //check if a column in the table contains 1 column in the location table column
                    //check for multi line addressin the table 
                    //var multiLine = tableConfig.Columns.Where(n => columnConfig.Name.Contains(n.Name)).ToList().Count > 2;
                    bool u;
                    var cname = _location.Columns.Cast<DataColumn>().Where(s => columnConfig.Name.ToUpper().Contains(s.ColumnName.ToUpper())).ToList().FirstOrDefault();//get the exact column match
                    try
                    {
                        if (columnConfig.Type == DataType.SecondaryAddress || columnConfig.Type == DataType.StreetAddress)
                        {
                            existingValue = _dataGenerator.GetValue(columnConfig, existingValue, tableConfig.Name, gender);
                        }
                        else
                        {
                            if (_location.Rows.Count == 0)
                            {
                                u = tableConfig.Columns.Any(n => n.Name.ToUpper().Contains("CITY")) && (tableConfig.Columns.Any(n => n.Name.ToUpper().Contains("STATE")) || tableConfig.Columns.Any(n => n.Name.ToUpper().Contains("PROVINCE")));
                                addr = (DataTable)_dataGenerator.GetAddress(columnConfig, existingValue, _location, u);
                            }
                            if (addr == null)
                            {
                                existingValue = null;

                            }
                            else if (_location.Rows.Count > 0)
                            {
                                existingValue = _location.Rows[0][cname.ColumnName];
                            }
                            else
                                File.AppendAllText(_exceptionpath, "Could not Generate addresses on " + $"{tableConfig.Name}.{columnConfig.Name}" + " and will return original: " + Environment.NewLine);
                        }
                    }
                    catch (Exception ex)
                    {
                        File.AppendAllText(_exceptionpath, "Could not Generate addresses on " + $"{tableConfig.Name}.{columnConfig.Name}" + "  and will return original: " + ex.Message + Environment.NewLine);
                        existingValue = _dataGenerator.GetValue(columnConfig, existingValue, tableConfig.Name, gender);
                    }
                }
                else
                {
                    existingValue = _dataGenerator.GetValue(columnConfig, existingValue, tableConfig.Name, gender);
                }
                //replace the original value
                obj[columnConfig.Name] = existingValue;
            }          
            foreach (ColumnConfig columnConfig in tableConfig.Columns.Where(x => !x.Ignore && x.Type == DataType.Computed))
            {
                var separator = columnConfig.Separator ?? " ";
                StringBuilder colValue = new StringBuilder();
                bool first = true;
                foreach (var sourceColumn in columnConfig.SourceColumns)
                {
                    if (!obj.ContainsKey(sourceColumn))
                    {
                        throw new Exception($"Source column {sourceColumn} could not be found.");
                    }

                    if (first)
                    {
                        first = false;
                    }
                    else
                    {
                        colValue.Append(separator);
                    }
                    colValue.Append(obj[sourceColumn] ?? String.Empty);
                }
                obj[columnConfig.Name] = colValue.ToString();
            }
            return obj;
        }
        private int GetObjectSize(object TestObject)
        {
            BinaryFormatter bf = new BinaryFormatter();
            MemoryStream ms = new MemoryStream();
            byte[] Array;
            bf.Serialize(ms, TestObject);
            Array = ms.ToArray();
            return Array.Count();
        }
        private object DataScramble(object o, string tableName, ColumnConfig columnConfig)
        {
            _randomizer = new Randomizer();
            int totalIterations = 0;
            string uniqueCacheKey = $"{tableName}.{columnConfig.Name}";
            if (!string.IsNullOrEmpty(columnConfig.UseValue))
            {
                return ConvertValue(columnConfig.Type, columnConfig.UseValue);
            }
            if (columnConfig.RetainNullValues &&
              o == null)
            {
                return null;
            }
            if (columnConfig.RetainEmptyStringValues &&
               (o is string && string.IsNullOrWhiteSpace((string)o)))
            {
                return o;
            }
            if (o is string)
            {
                if (Convert.ToString(o).Length > 1)
                {
                    object newValue = Convert.ToString(string.Join("", _randomizer.Shuffle(Convert.ToString(o).ToArray()).ToArray()));
                    while (Convert.ToString(newValue).Equals(Convert.ToString(o)))
                    {
                        totalIterations++;
                        if (totalIterations >= MAX_UNIQUE_VALUE_ITERATIONS)
                        {
                            if (!(uniquevalue.Where(n => n.Key.Equals(tableName)).Count() > 0 && uniquevalue.Where(n => n.Value.Equals(columnConfig.Name)).Count() > 0))
                            {
                                File.AppendAllText(_exceptionpath, $"Unable to generate Scramble value for {uniqueCacheKey}, attempt to resolve value {totalIterations} times" + Environment.NewLine + Environment.NewLine);
                                uniquevalue.Add(new KeyValuePair<object, object>(tableName, columnConfig.Name));
                                //break;
                            }
                            //File.AppendAllText(_exceptionpath, $"Unable to generate unique value for {uniqueCacheKey}, attempt to resolve value {totalIterations} times" + Environment.NewLine + Environment.NewLine);
                            break;
                        }
                        newValue = Convert.ToString(string.Join("", _randomizer.Shuffle(Convert.ToString(o).ToArray()).ToArray()));
                    }
                    return newValue;
                }
                else
                {
               
                    //throw new ArgumentException("cannot scramble string of lenght less than 2", columnConfig.Name);
                    if (!(uniquevalue.Where(n => n.Key.Equals(tableName)).Count() > 0 && uniquevalue.Where(n => n.Value.Equals(columnConfig.Name)).Count() > 0))
                    {
                        File.AppendAllText(_exceptionpath, "cannot scramble string "+ Convert.ToString(o) + " of lenght less than 2 on " + uniqueCacheKey + Environment.NewLine);
                        uniquevalue.Add(new KeyValuePair<object, object>(tableName, columnConfig.Name));
                        //break;
                    }
                    
                    return o;
                }
            }
            else if (o is decimal)
            {

                if (Convert.ToString(o).Contains(".") && Convert.ToString(o).Split('.').FirstOrDefault().Length != 0)
                {
                    var s = Convert.ToString(o).Split('.');

                    ;                    //_randomizer = new Randomizer();
                    return Convert.ToDecimal(string.Join("", _randomizer.Shuffle(Convert.ToString(s.FirstOrDefault()).ToArray()).ToArray())) + "." + s.LastOrDefault().ToString();
                }
                else
                {
                    File.AppendAllText(_exceptionpath, "cannot scramble decimal " + Convert.ToString(o) + " of left lenght less than 2 on " + uniqueCacheKey + Environment.NewLine);
                    return Convert.ToDecimal(string.Join("", _randomizer.Shuffle(Convert.ToString(o)).ToArray()));
                }
            }
            else if (o is double)
            {
                if (Convert.ToString(o).Contains(".") && Convert.ToString(o).Split('.').FirstOrDefault().Length != 0)
                {
                    var s = Convert.ToString(o).Split('.');

                    //_randomizer = new Randomizer();
                    return Convert.ToDouble(string.Join("", _randomizer.Shuffle(Convert.ToString(s.FirstOrDefault()).ToArray()).ToArray())) + "." + s.LastOrDefault().ToString();
                }
                else
                {
                    File.AppendAllText(_exceptionpath, "cannot scramble double " + Convert.ToString(o) + " of left lenght less than 2 on " + uniqueCacheKey + Environment.NewLine);
                    return Convert.ToDouble(string.Join("", _randomizer.Shuffle(Convert.ToString(o)).ToArray()));

                }// _randomizer = new Randomizer();

            }
            else if (o is int)
            {
                //_randomizer = new Randomizer();
               
                if (Convert.ToString(o).Length > 1)
                {
                    object newValue = Convert.ToInt32(string.Join("", _randomizer.Shuffle(Convert.ToString(o).ToArray()).ToArray()));
                    while (Convert.ToString(newValue).Equals(Convert.ToString(o)))
                    {
                        totalIterations++;
                        if (totalIterations >= MAX_UNIQUE_VALUE_ITERATIONS)
                        {
                            if (!(uniquevalue.Where(n => n.Key.Equals(tableName)).Count() > 0 && uniquevalue.Where(n => n.Value.Equals(columnConfig.Name)).Count() > 0))
                            {
                                File.AppendAllText(_exceptionpath, $"Unable to generate Scramble value for {uniqueCacheKey}, attempt to resolve value {totalIterations} times" + Environment.NewLine + Environment.NewLine);
                                uniquevalue.Add(new KeyValuePair<object, object>(tableName, columnConfig.Name));
                                //break;
                            }
                            //File.AppendAllText(_exceptionpath, $"Unable to generate unique value for {uniqueCacheKey}, attempt to resolve value {totalIterations} times" + Environment.NewLine + Environment.NewLine);
                            break;
                        }
                        newValue = Convert.ToInt32(string.Join("", _randomizer.Shuffle(Convert.ToString(o).ToArray()).ToArray()));
                    }
                    return newValue;
                }
                else
                {
                    File.AppendAllText(_exceptionpath, "cannot scramble Integer " + Convert.ToString(o) + " of lenght less than 2 on " + uniqueCacheKey + Environment.NewLine);
                    return Convert.ToInt32(string.Join("", _randomizer.Shuffle(Convert.ToString(o).ToArray()).ToArray()));
                }
                
            }
            else if (o is long)
            {
                //_randomizer = new Randomizer();
                return Convert.ToInt64(string.Join("", _randomizer.Shuffle(Convert.ToString(o).ToArray()).ToArray()));
            }
            else
                throw new ArgumentException(columnConfig.Type.ToString() + " does not apply to " +  o.GetType().ToString());
          
        }
        private object MaskingLeft(object o, int position, string tableName, ColumnConfig columnConfig)
        {
            if (columnConfig.RetainNullValues &&
              o == null)
            {
                return null;
            }
            if (columnConfig.RetainEmptyStringValues &&
               (o is string && string.IsNullOrWhiteSpace((string)o)))
            {
                return o;
            }
            List<string> slist = new List<string>();
            if (o is string && !string.IsNullOrWhiteSpace((string)o) && Convert.ToString(o).Length > position)
            {
                int i = 0;
                var s = Convert.ToString(o).Batch(position).Select(r => new String(r.ToArray())).ToList();
                s.ToList().ForEach(u =>
                {
                    if (i == 0)
                    {
                        var tu = string.Join("", u.Select(n => n.ToString().Replace(n, '*')));                     
                        slist.Add(tu);
                    }
                    else
                        slist.Add(u);
                    i = i + 1;
                });
                return string.Join("", slist.ToArray());
            }
            File.AppendAllText(_exceptionpath, "MaskingOut Lenght " + Convert.ToString(o) + " must be greater than " + position + " in " + $"{tableName}.{columnConfig.Name}" + Environment.NewLine);
            return Convert.ToString(o);
        }
        private object MaskingRight(object o, int position, string tableName, ColumnConfig columnConfig)
        {
            if (columnConfig.RetainNullValues &&
           o == null)
            {
                return null;
            }
            if (columnConfig.RetainEmptyStringValues &&
               (o is string && string.IsNullOrWhiteSpace((string)o)))
            {
                return o;
            }
            List<string> slist = new List<string>();
            if (o is string && !string.IsNullOrWhiteSpace((string)o) && Convert.ToString(o).Length > position)
            {
                int i = 0;
                var s = Convert.ToString(o).Batch(position).Select(r => new String(r.ToArray())).ToList();
                s.ToList().ForEach(u =>
                {
                    if (i == s.Count - 1)
                    {
                        var tu = string.Join("", u.Select(n => n.ToString().Replace(n, '*')));                     
                        slist.Add(tu);
                    }
                    else
                        slist.Add(u);
                    i = i + 1;
                });
                return string.Join("", slist.ToArray());
            }
            File.AppendAllText(_exceptionpath, "MaskingOut Lenght " + Convert.ToString(o) + " must be greater than " + position + " in " + $"{tableName}.{columnConfig.Name}" + Environment.NewLine);
            return Convert.ToString(o);
        }
        private object  MaskingMiddle(object o, int position, string tableName, ColumnConfig columnConfig)
        {
            List<string> slist = new List<string>();
            if (columnConfig.RetainNullValues &&
           o == null)
            {
                return null;
            }
            if (columnConfig.RetainEmptyStringValues &&
               (o is string && string.IsNullOrWhiteSpace((string)o)))
            {
                return o;
            }
            if (o is string && !string.IsNullOrWhiteSpace((string)o) && Convert.ToString(o).Length > (position * 2))
            {
                int i = 0;
                var s = Convert.ToString(o).Batch(position).Select(r => new String(r.ToArray())).ToList();
                s.ToList().ForEach(u =>
                {
                    if ( i == 0 || i == s.Count - 1)
                    {
                        slist.Add(u);
                    }
                    else
                    {
                        var tu = string.Join("", u.Select(n => n.ToString().Replace(n, '*')));
                        slist.Add(tu);                        
                    }
                    i = i + 1;
                });
                return string.Join("", slist.ToArray());
            }
            File.AppendAllText(_exceptionpath, "MaskingOut Lenght " + Convert.ToString(o) + " must be greater than 2X " + position + " in " + $"{tableName}.{columnConfig.Name}" + Environment.NewLine);
            return Convert.ToString(o);
        }
        private object GetUniqueValue(string tableName,
            ColumnConfig columnConfig,
            object existingValue,
            Name.Gender? gender)
        {
            //create a unique key
            string uniqueCacheKey = $"{tableName}.{columnConfig.Name}";

            //if this table/column combination hasn't been seen before add an empty hash set
            if (!_uniqueValues.ContainsKey(uniqueCacheKey))
            {
                _uniqueValues.AddOrUpdate(uniqueCacheKey, new HashSet<object>(), (a, b) => b);
            }
            //grab the hash set for this table/column 
            HashSet<object> uniqueValues = _uniqueValues[uniqueCacheKey];

            int totalIterations = 0;
            do
            {

                existingValue = _dataGenerator.GetValue(columnConfig, existingValue, tableName, gender);
                totalIterations++;
                if (totalIterations >= MAX_UNIQUE_VALUE_ITERATIONS)
                {
                    throw new Exception($"Unable to generate unique value for {uniqueCacheKey}, attempt to resolve value {totalIterations} times");
                }
            }
            while (uniqueValues.Contains(existingValue));

            uniqueValues.Add(existingValue);
            return existingValue;
        }
        private static object ConvertValue(
        DataType dataType,
        string val)
        {
            if (val.ToUpper() == "NULL")
            {
                return null;
            }
            switch (dataType)
            {
                case DataType.FirstName:
                case DataType.LastName:
                case DataType.Rant:
                case DataType.Vehicle:
                case DataType.Lorem:
                case DataType.StringFormat:
                case DataType.Company:
                case DataType.FullName:
                case DataType.CompanyPersonName:
                case DataType.PostalCode:
                case DataType.RandomUsername:
                case DataType.RandomYear:
                case DataType.RandomSeason:
                case DataType.RandomInt:
                case DataType.RandomDec:
                case DataType.TimeSpan:
                case DataType.PickRandom:
                case DataType.FullAddress:
                case DataType.State:
                case DataType.City:
                case DataType.Bogus:
                case DataType.StringConcat:
                case DataType.PhoneNumber:
                case DataType.None:
                    return val;
                case DataType.DateOfBirth:
                    return DateTime.Parse(val);
                case DataType.math:
                    return Convert.ToDecimal(val);
            }

            throw new ArgumentOutOfRangeException(nameof(dataType) + " not implemented for UseValue", dataType, null);
        }
        private string Serialize<T>(T value)
        {
            if (value == null)
            {
                return string.Empty;
            }
            try
            {
                System.Xml.Serialization.XmlSerializer xmlserializer = new XmlSerializer(typeof(T));
                StringWriter stringWriter = new StringWriter();
                XmlWriter writer = XmlWriter.Create(stringWriter);
                xmlserializer.Serialize(writer, value);
                string serializeXml = stringWriter.ToString();
                writer.Close();
                return serializeXml;
            }
            catch (Exception)
            {
                return string.Empty;
            }
        }
        public DataTable DictionaryToDataTable(IEnumerable<IDictionary<string, object>> parents, TableConfig config)
        {
            var table = new DataTable();
            var c = parents.FirstOrDefault(x => x.Values
                                           .OfType<IEnumerable<IDictionary<string, object>>>()
                                           .Any());
            var p = c ?? parents.FirstOrDefault();
            if (p == null)
                return table;
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
    }


}

