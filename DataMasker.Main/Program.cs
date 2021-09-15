using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.IO;
using System.Linq;
using System.Text;
using DataMasker.Interfaces;
using DataMasker.Models;
using Newtonsoft.Json;
using Newtonsoft.Json.Schema;
using Newtonsoft.Json.Schema.Generation;
using Newtonsoft.Json.Serialization;
using OfficeOpenXml;
using Newtonsoft.Json.Linq;
using DataMasker.DataLang;
using DataMasker.MaskingValidation;
using KellermanSoftware.CompareNetObjects;
using System.Diagnostics;
using System.Reflection;
using System.Security.Principal;
using SharpCompress.Archives.Rar;
using SharpCompress.Common;
using SharpCompress.Archives;
using System.Net;
using ChoETL;
using DataMasker.Runner;

/*
    Author: Stanley Okeke
    Company: MOTI IMB
    Title: Senior Technical Analyst IMB
    Version copies : SVN
 * */

namespace DataMasker.Examples
{
    public class Program
    {
        #region system declarations
        #region read-only and const app config
        private static readonly string _exceptionpath = Directory.GetCurrentDirectory() + $@"\output\MaskExceptions.txt";
        private static readonly string path = Directory.GetCurrentDirectory() + $@"\output\Validation\ValidationResult.txt";
        private static string copyjsonPath;
        private static readonly string fromEmail = ConfigurationManager.AppSettings["fromEmail"];
        private static readonly string Recipients = ConfigurationManager.AppSettings["RecipientEmail"];
        private static readonly string sheetPath = ConfigurationManager.AppSettings["ExcelSheetPath"];
        private static readonly string TSchema = ConfigurationManager.AppSettings["APP_NAME"];
        private static readonly string Stype = ConfigurationManager.AppSettings["DataSourceType"];
        private static readonly string _testJson = ConfigurationManager.AppSettings["TestJson"];
        private const string ExcelSheetPath = "ExcelSheetPath"; private const string DatabaseName = "DatabaseName"; private const string WriteDML = "WriteDML";
        private const string MaskTabletoSpreadsheet = "MaskTabletoSpreadsheet"; private const string Schema = "APP_NAME"; private const string ConnectionString = "ConnectionString"; private const string ConnectionStringPrd = "ConnectionStringPrd";
        private const string MaskedCopyDatabase = "MaskedCopyDatabase"; private const string RunValidation = "RunValidation"; private const string EmailValidation = "EmailValidation"; private const string jsonMapPath = "jsonMapPath"; private const string RunValidationONLY = "RunValidationONLY";
        private const string SourceType = "DataSourceType";
        private const string RunTestJson = "RunTestJson"; private const string TestJson = "TestJson";
        private const string AutoUpdate = "AutoUpdate"; private const string CurrentVersionURL = "CurrentVersionURL"; private const string CurrentInstallerURL = "CurrentInstallerURL";

        private static readonly string exceptionPath = Directory.GetCurrentDirectory() + ConfigurationManager.AppSettings["_exceptionpath"];
        #endregion
        private static string _nameDatabase;
        private static string _SpreadSheetPath;
        private static DataTable PrdTable = null;
        private static DataTable _dmlTable = null;
        private static DataTable MaskTable = null;
        private static int count;
        private static DataTable report = new DataTable();  
        private static List<string> _colError = new List<string>();
        private static List<KeyValuePair<string, string>> collist = new List<KeyValuePair<string, string>>();
        private static List<KeyValuePair<TableConfig,ColumnConfig>> jsconfigTable = new List<KeyValuePair<TableConfig, ColumnConfig>>();
        private static List<KeyValuePair<TableConfig, ColumnConfig>> copyJsTable = new List<KeyValuePair<TableConfig, ColumnConfig>>();
        private static List<KeyValuePair<string, Dictionary<string, string>>> _allNull = new List<KeyValuePair<string, Dictionary<string, string>>>();
        private static int rowCount;
        private static bool isBinary;
        private static bool isRollback;
        private static readonly Dictionary<ProgressType, ProgressbarUpdate> _progressBars = new Dictionary<ProgressType, ProgressbarUpdate>();
        private static readonly Dictionary<string, object> allkey = new Dictionary<string, object>();
        private static Dictionary<string, KeyValuePair<string,string>> TableParameter = new Dictionary<string, KeyValuePair<string,string>>();

        private static readonly List<KeyValuePair<string, Dictionary<string, KeyValuePair<string,string>>>> ColumnParameter = new List<KeyValuePair<string, Dictionary<string, KeyValuePair<string,string>>>>();
        private static readonly string _successfulCommit = Directory.GetCurrentDirectory() + ConfigurationManager.AppSettings["_successfulCommit"];

        public static string Jsonpath { get; private set; }
        public static string CreateDir { get; private set; }
        public static string ColumnMapping { get; private set; }
        public static string DownloadMSI { get; private set; }
        public static string InstallMsiPath { get; private set; }
        public static string MaskerVersion { get; private set; }
        #endregion
        public static bool IsAdministrator()
        {
#pragma warning disable CA1416 // Validate platform compatibility
            WindowsIdentity identity = WindowsIdentity.GetCurrent();
#pragma warning restore CA1416 // Validate platform compatibility
#pragma warning disable CA1416 // Validate platform compatibility
            WindowsPrincipal principal = new WindowsPrincipal(identity);
#pragma warning restore CA1416 // Validate platform compatibility
#pragma warning disable CA1416 // Validate platform compatibility
            return principal.IsInRole(WindowsBuiltInRole.Administrator);
#pragma warning restore CA1416 // Validate platform compatibility
        }
        public static bool ExtractMSI(string destination)
        {
            try
            {
                RarArchive archive = RarArchive.Open(DownloadMSI);
                foreach (var entry in archive.Entries.Where(entry => !entry.IsDirectory))
                {
                    entry.WriteToDirectory(destination, new ExtractionOptions()
                    {
                        ExtractFullPath = true,
                        Overwrite = true
                    });
                }
                if (File.Exists(destination + @"\DataMaskerInstaller\DataMaskerInstaller.msi"))
                {
                    InstallMsiPath = destination + @"\DataMaskerInstaller\DataMaskerInstaller.msi";
                    return true;
                }
            }
            catch (Exception)
            {

                throw;
            }
            return false;
        }
        public static bool Download(string urlPath, string path)
        {
            try
            {
                //var https = new HttpClient();
                Console.WriteLine("Downloading latest version...");
                WebClient Clients = new WebClient
                {
                    UseDefaultCredentials = true
                };
                Clients.DownloadFile(urlPath, path + @"\DataMaskerInstaller.rar");
                if (File.Exists(path + @"\DataMaskerInstaller.rar"))
                {
                    Clients.Dispose();
                    DownloadMSI = path + @"\DataMaskerInstaller.rar";
                    return true;
                }
            }
            catch (Exception)
            {

                throw;
            }

            return false;
        }
        public static bool Install(string sMSIPath)
        {
            try
            {
                Console.WriteLine("Installing latest version...");
                string configfile = Directory.GetCurrentDirectory() + @"\DataMasker.Mask.exe.config";
                File.Copy(configfile, Path.Combine(Path.GetDirectoryName(configfile)
                    , Path.GetFileName(configfile) + ".bak"), true);
                Process process = new Process();
                process.StartInfo.Arguments = string.Format("/i \"{0}\" ALLUSERS=1 /qn", sMSIPath);
                process.StartInfo.FileName = "msiexec";
                //process.StartInfo = processInfo;
                process.Start();
                process.WaitForExit();
                return true;
            }
            catch (Exception ex)
            {

                Console.WriteLine("Failed to update: {0}", ex.Message);
            }
            return false;
        }
        public static bool CheckUpdate(string urlVersion, string urlFile)
        {
            try
            {


                WebClient Client = new WebClient
                {
                    UseDefaultCredentials = true
                };
                string versionString = Client.DownloadString(urlVersion);
                Version latestVersion = new Version(versionString);
                //get my own version to compare against latest.
                Assembly assembly = Assembly.GetExecutingAssembly();
                FileVersionInfo fvi = FileVersionInfo.GetVersionInfo(assembly.Location);
                Version myVersion = new Version(fvi.ProductVersion);
                Client.Dispose();
                if (latestVersion > myVersion)
                {
                    Console.WriteLine(string.Format("You've got version {0} of DataMasker for Windows. Would you like to update to the latest version {1} [Yes/No]?", myVersion, latestVersion));
                    var key = Console.ReadLine();
                    if (key.ToUpper() == "YES" || key.ToUpper() == "Y")
                    {
                        string mPath = @"C:\DataMaskerInstaller";
                        if (!Directory.Exists(mPath))
                        {
                            Directory.CreateDirectory(mPath);
                        }
                        if (Download(urlFile, mPath))
                        {
                            if (ExtractMSI(mPath))
                            {
                                //install
                                if (Install(InstallMsiPath))
                                {
                                    // Restart and run as admin
                                    var exeName = Process.GetCurrentProcess().MainModule.FileName;
                                    ProcessStartInfo startInfo = new ProcessStartInfo(exeName)
                                    {
                                        Verb = "runas",
                                        Arguments = "restart"
                                    };
                                    Process.Start(startInfo);
                                    Environment.Exit(1);
                                }
                            }
                        }
                    }
                   
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("Update failed with the following errors: {0}", ex.Message);
                return false;
            }
            return false;
        }
        private static void Main(
            string[] args)
        {
            //var exeName1 = Process.GetCurrentProcess().MainModule.FileName;
            Assembly assembly = Assembly.GetExecutingAssembly();
            FileVersionInfo fvi = FileVersionInfo.GetVersionInfo(assembly.Location);
            Version myVersion = new Version(fvi.ProductVersion);
            MaskerVersion = myVersion.ToString();
            Console.Title = string.Format("Data Masker v{0}",MaskerVersion) ;
            if (!IsAdministrator())
            {
                // Restart and run as admin
                var exeName = Process.GetCurrentProcess().MainModule.FileName;
                ProcessStartInfo startInfo = new ProcessStartInfo(exeName)
                {
                    Verb = "runas",
                    Arguments = "restart"
                };
                Process.Start(startInfo);
                Environment.Exit(1);
            }
            if (!CheckAppConfig())
            {
                Console.WriteLine("Program will exit: Press ENTER to exist..");
                Console.ReadLine();
                System.Environment.Exit(1);
            }
            report.Columns.Add("Table"); report.Columns.Add("Schema"); report.Columns.Add("Column"); report.Columns.Add("Hostname"); report.Columns.Add("DataSourceType") ; report.Columns.Add("TimeStamp"); report.Columns.Add("Operator"); report.Columns.Add("Row count mask"); report.Columns.Add("Row count prd"); report.Columns.Add("Result"); report.Columns.Add("Result Comment");
            //check for update
            if ((bool)allkey.Where(n => n.Key.ToUpper().Equals(AutoUpdate.ToUpper())).Select(n => n.Value).FirstOrDefault())
            {
                CheckUpdate((string)allkey.Where(n => n.Key.ToUpper().Equals(CurrentVersionURL.ToUpper())).Select(n => n.Value).FirstOrDefault(),
                    (string)allkey.Where(n => n.Key.ToUpper().Equals(CurrentInstallerURL.ToUpper())).Select(n => n.Value).FirstOrDefault());
            }
            Run();
        }
        public static void JsonConfig(string json, string excelspreadsheet)
        {
            #region Initialize system variables


            //generate list of words for comments like columns
            List<string> _comment = new List<string> { "Description", "DESCRIPTION", "TEXT", "MEMO", "describing", "Descr", "COMMENT", "comment", "NOTE", "Comment", "REMARK", "remark", "DESC" };
            List<string> _fullName = new List<string> { "full name", "AGENT_NAME", "OFFICER_NAME", "FULL_NAME", "CONTACT_NAME", "MANAGER_NAME" };
            List<string> MaskingRules = new List<string>() { "No Masking required", "Replace Value with fake data","Shuffle", "Flagged" };
            List<string> maskingoutString = new List<string>() { "char", "nchar","string","varchar", "nvarchar", "binary", "varbinary" , "character varying" };
            List<string> ScrableList = new List<string>() { "char", "nchar", "string", "varchar", "nvarchar", "binary", "varbinary", "character varying", "numeric", "decimal", "double", "int","Number"};
            List<TableConfig> tableList = new List<TableConfig>();
            List<string> Vehicles = new List<string> { "Manufacturer", "Vin", "Model", "Type", "Fuel" };
            DataGeneration dataGeneration = new DataGeneration
            {
                locale = "en"
            };
            Config1 config1 = new Config1
            {
                connectionString = ConfigurationManager.AppSettings["ConnectionString"],
                Databasename = ConfigurationManager.AppSettings["DatabaseName"],
                connectionStringPrd = ConfigurationManager.AppSettings["ConnectionStringPrd"],
                Hostname = ConfigurationManager.AppSettings["Hostname"]

            };
            //config1.connectionString2 = "";
            DataSource dataSource = new DataSource
            {
                config = config1,
                type = ConfigurationManager.AppSettings["DataSourceType"]
            };
            #endregion

            #region Create root json objects
            var rootObj = JsonConvert.DeserializeObject<List<RootObject>>(File.ReadAllText(json));
            var oo = ExcelToJson.FromJson(File.ReadAllText(json));
            var query = from root in rootObj
                            //where root.__invalid_name__Masking_Rule.Contains("No masking")
                        group root by root.TableName into newGroup
                        orderby newGroup.Key
                        select newGroup;
            #endregion

            #region build and map column datatype with masked column datatype
            foreach (var nameGroup in query)
            {
                TableConfig table = new TableConfig();
                List<ColumnConfig> colList = new List<ColumnConfig>();
                table.Name = nameGroup.Key;
                foreach (var col in nameGroup)
                {
                    table.PrimaryKeyColumn = col.PKconstraintName.Split(',')[0];
                    table.Schema = col.Schema;
                    if (!col.MaskingRule.ToUpper().Contains("NO MASKING") || col.ColumnName == table.PrimaryKeyColumn)
                    {
                        if (col.DataType.ToUpper().Contains("NUMBER"))
                        {
                            ColumnParameter.Add(new KeyValuePair<string, Dictionary<string, KeyValuePair<string, string>>>(table.Name, new Dictionary<string, KeyValuePair<string, string>>() { { col.ColumnName, new KeyValuePair<string, string>(col.DataType.Split('(').FirstOrDefault(),
                            col.DataType.Split(',').FirstOrDefault().Any(char.IsDigit) ? new string(col.DataType.Split(',').FirstOrDefault().Where(char.IsDigit).ToArray()): "" )
                            } }));
                        }
                        else if (col.DataType.ToUpper().Contains("VARCHAR"))
                        {
                            ColumnParameter.Add(new KeyValuePair<string, Dictionary<string, KeyValuePair<string, string>>>(table.Name, new Dictionary<string, KeyValuePair<string, string>>() { { col.ColumnName, new KeyValuePair<string, string>(col.DataType.Split('(').FirstOrDefault(),
                            col.DataType.Split('(')[1].Any(char.IsDigit) ? new string(  col.DataType.Split('(')[1].Where(char.IsDigit).ToArray()): "" )
                            } }));
                        }
                        else
                            ColumnParameter.Add(new KeyValuePair<string, Dictionary<string, KeyValuePair<string,string>>>(table.Name, new Dictionary<string, KeyValuePair<string,string>>() { { col.ColumnName, new KeyValuePair<string, string>(col.DataType.Split('(').FirstOrDefault(), 
                            col.DataType.Any(char.IsDigit) ? new string(col.DataType.Where(char.IsDigit).ToArray()): "" )
                            } }));
                        //ColumnParameter.Add(table.Name, new KeyValuePair<string, string> ( col.ColumnName, col.DataType.Split('(').FirstOrDefault()));
                    }                  
                    table.TargetSchema = col.TargetSchema;
                    table.RowCount = col.RowCount;
                    bool o = col.RetainNull.ToUpper().Equals("TRUE") ? true : false;
                    bool prview = col.Preview.ToUpper().Equals("FALSE") ? false : true;
                    bool nullString = col.RetainEmptyString.ToUpper().Equals("FALSE") ? false : true;
                    ColumnConfig column = new ColumnConfig
                    {
                        Name = col.ColumnName,
                        RetainNullValues = o,
                        RetainEmptyStringValues = nullString,
                        StringFormatPattern = "",
                        UseValue = col.UseValue.NullIfEmpty(),
                        Preview = prview

                    };
                    var rule = col.MaskingRule;

                    if (col.MaskingRule.ToUpper().Contains("NO MASKING"))
                    {
                        column.Type = DataType.NoMasking;
                        if (col.DataType.ToUpper().Equals("SDO_GEOMETRY") || col.DataType.ToUpper().ToUpper().Contains("GEOMETRY"))
                        {
                            column.Type = DataType.Geometry;
                        }
                        column.Ignore = true;
                        column.Max = col.Max.ToString(); ;
                        column.Min = col.Min.ToString(); ;
                        column.StringFormatPattern = "";
                        column.UseGenderColumn = "";
                    }                   
                    else if (col.MaskingRule.ToUpper().Equals("SHUFFLE"))
                    {
                        column.Type = DataType.Shuffle;
                        column.Max = col.Max.ToString(); ;
                        column.Min = col.Min.ToString(); ;
                        column.StringFormatPattern = "";
                        column.UseGenderColumn = "";
                    }                
                    else if (col.MaskingRule.ToUpper().Contains("MATH"))
                    {
                        if (maskingoutString.Any(n => col.DataType.ToUpper().Contains(n.ToUpper())))
                        {
                            throw new ArgumentException("Math Operation apply only on Numeric Datatype", col.DataType + " on " + col.ColumnName);
                        }
                        column.Type = DataType.math;
                        column.Max = col.Max.ToString(); ;
                        column.Min = col.Min;
                        if (string.IsNullOrEmpty(col.StringFormat))
                        {
                            throw new ArgumentException("Math Operation requires a stringFormat", col.StringFormat + " on " + col.ColumnName);
                        }
                        column.StringFormatPattern = col.StringFormat;
                        column.UseGenderColumn = "";
                        column.Ignore = true;
                        column.Operator = col.RuleReasoning;
                    }
                    else if (!MaskingRules.Any(n => col.MaskingRule.ToUpper().Contains(n.ToUpper())))
                    {
                        switch (ToEnum(col.MaskingRule, DataType.Error))
                        {
                            case DataType.None:
                                column.Type = DataType.None;
                                column.Max = col.Max.ToString(); ;
                                column.Min = col.Min;
                                column.StringFormatPattern = col.StringFormat;
                                column.UseGenderColumn = "";
                                break;
                            case DataType.Vehicle:
                                if (string.IsNullOrEmpty(col.StringFormat))
                                {
                                    throw new ArgumentException("Vehicle type requires a StringFormatPattern value", nameof(col.StringFormat) + " on " + col.ColumnName);

                                }
                                else if (!Vehicles.Any(n=>n == col.StringFormat))
                                {
                                    throw new ArgumentException("Invalid Vehicle StringFormatPattern value: " + col.StringFormat.AddDoubleQuotes(), nameof(col.StringFormat) + " on " + col.ColumnName);

                                }
                                column.Type = DataType.Vehicle;
                                column.Max = col.Max.ToString(); ;
                                column.Min = col.Min;
                                column.StringFormatPattern = col.StringFormat;
                                column.UseGenderColumn = "";
                                break;
                            case DataType.CompanyPersonName:
                                column.Type = DataType.CompanyPersonName;
                                column.Max = col.Max.ToString().Substring(0, col.Max.ToString().IndexOf('.') > 0 ? col.Max.ToString().IndexOf('.') : col.Max.ToString().Length);
                                column.Min = col.Min.ToString().Substring(0, col.Min.ToString().IndexOf('.') > 0 ? col.Min.ToString().IndexOf('.') : col.Min.ToString().Length);

                                column.StringFormatPattern = col.StringFormat;
                                column.UseGenderColumn = "";
                                break;
                            case DataType.MaskingOut:
                                //datatype must be string 
                                if (!maskingoutString.Any(n=>col.DataType.ToUpper().Contains(n.ToUpper())))
                                {
                                    throw new ArgumentException("MaskingOut type apply only on String dataType", col.DataType + " on " + col.ColumnName);
                                }
                                column.Type = DataType.MaskingOut;                             
                                column.Min = col.Min;
                                if (string.IsNullOrEmpty(col.StringFormat))
                                {
                                    throw new ArgumentException("MaskingOut type requires a StringFormatPattern value", nameof(col.StringFormat) + " on " + col.ColumnName);

                                }
                                column.StringFormatPattern = col.StringFormat.Split('(').FirstOrDefault(); //StringFormatpattern = MaskingRight(4) , MaskingLeft(ChunkSize), MaskigMiddle(chunkSize)
                                column.Max = string.Join("", col.StringFormat.Where(char.IsDigit));
                                column.UseGenderColumn = "";
                                break;
                            case DataType.Scramble:
                                //datatype must be string 
                                if (!ScrableList.Any(n => col.DataType.ToUpper().Contains(n.ToUpper())))
                                {
                                    throw new ArgumentException("Scramble type apply only on Strings and Numeric Datatype", col.DataType + " on " + col.ColumnName);
                                }
                                column.Type = DataType.Scramble;
                                column.Max = col.Max.ToString().Substring(0, col.Max.ToString().IndexOf('.') > 0 ? col.Max.ToString().IndexOf('.') : col.Max.ToString().Length);
                                column.Min = col.Min.ToString().Substring(0, col.Min.ToString().IndexOf('.') > 0 ? col.Min.ToString().IndexOf('.') : col.Min.ToString().Length);

                                column.StringFormatPattern = "";
                                column.UseGenderColumn = "";
                                break;
                            case DataType.Company:
                                column.Type = DataType.Company;
                                column.Max = col.Max.ToString().Substring(0, col.Max.ToString().IndexOf('.') > 0 ? col.Max.ToString().IndexOf('.') : col.Max.ToString().Length);
                                column.Min = col.Min.ToString().Substring(0, col.Min.ToString().IndexOf('.') > 0 ? col.Min.ToString().IndexOf('.') : col.Min.ToString().Length);

                                column.StringFormatPattern = "{{COMPANY.COMPANYNAME}} {{COMPANY.COMPANYSUFFIX}}";
                                column.UseGenderColumn = "";
                                break;
                            case DataType.NULL:
                                break;
                            case DataType.TimeSpan:
                                column.Type = DataType.TimeSpan;
                                if (string.IsNullOrEmpty(col.StringFormat))
                                {
                                    throw new ArgumentException("StringFormat type requires a StringFormatPattern value", nameof(col.StringFormat) + " on " + col.ColumnName);
                                }
                                column.Max = col.Max.ToString().Substring(0, col.Max.ToString().IndexOf('.') > 0 ? col.Max.ToString().IndexOf('.') : col.Max.ToString().Length);
                                column.Min = col.Min.ToString().Substring(0, col.Min.ToString().IndexOf('.') > 0 ? col.Min.ToString().IndexOf('.') : col.Min.ToString().Length);
                                column.StringFormatPattern = col.StringFormat;
                                break;
                            case DataType.PostalCode:
                                column.Type = DataType.PostalCode;
                                column.Max = col.Max.ToString(); ;
                                column.Min = col.Min;
                                column.StringFormatPattern = col.StringFormat;
                                column.UseGenderColumn = "";
                                break;
                            case DataType.StringConcat:
                                column.Type = DataType.StringConcat;
                                column.Max = col.Max.ToString().Substring(0, col.Max.ToString().IndexOf('.') > 0 ? col.Max.ToString().IndexOf('.') : col.Max.ToString().Length);
                                column.Min = col.Min.ToString().Substring(0, col.Min.ToString().IndexOf('.') > 0 ? col.Min.ToString().IndexOf('.') : col.Min.ToString().Length);

                                if (string.IsNullOrEmpty(col.StringFormat))
                                {
                                    throw new ArgumentException("StringFormat type requires a StringFormatPattern value", nameof(col.StringFormat) + " on " + col.ColumnName);
                                }
                                column.StringFormatPattern = col.StringFormat;
                                column.UseGenderColumn = "";
                                break;
                            case DataType.Shuffle:
                                column.Type = DataType.Shuffle;
                                column.Max = col.Max.ToString(); ;
                                column.Min = col.Min;
                                column.StringFormatPattern = "";
                                column.UseGenderColumn = "";
                                break;
                            case DataType.Ignore:
                                break;
                            case DataType.Money:
                                column.Type = DataType.Money;
                                column.Max = col.Max.ToString(); ;
                                column.Min = col.Min;
                                column.StringFormatPattern = "{{FINANCE.AMOUNT}}";
                                column.UseGenderColumn = "";
                                break;
                            case DataType.Location:
                                break;
                            case DataType.NoMasking:
                                column.Ignore = true;
                                break;
                            case DataType.math:
                                if (maskingoutString.Any(n => col.DataType.ToUpper().Contains(n.ToUpper())))
                                {
                                    throw new ArgumentException("Math Operation apply only on Numeric Datatype", col.DataType + " on " + col.ColumnName);
                                }
                                column.Type = DataType.math;
                                column.Max = col.Max.ToString().Substring(0, col.Max.ToString().IndexOf('.') > 0 ? col.Max.ToString().IndexOf('.') : col.Max.ToString().Length);
                                column.Min = col.Min.ToString().Substring(0, col.Min.ToString().IndexOf('.') > 0 ? col.Min.ToString().IndexOf('.') : col.Min.ToString().Length);

                                column.StringFormatPattern = col.StringFormat;
                                column.UseGenderColumn = "";
                                column.Ignore = true;
                                column.Operator = col.RuleReasoning;
                                break;
                            case DataType.Shufflegeometry:
                                column.Type = DataType.Shufflegeometry;
                                column.Max = col.Max.ToString(); ;
                                column.Min = col.Min;
                                column.StringFormatPattern = "";
                                column.UseGenderColumn = "";
                                break;
                            case DataType.exception:
                                break;
                            case DataType.Bogus:
                                column.Type = DataType.Bogus;
                                column.Max = col.Max.ToString().Substring(0, col.Max.ToString().IndexOf('.') > 0 ? col.Max.ToString().IndexOf('.') : col.Max.ToString().Length);
                                column.Min = col.Min.ToString().Substring(0, col.Min.ToString().IndexOf('.') > 0 ? col.Min.ToString().IndexOf('.') : col.Min.ToString().Length);

                                if (string.IsNullOrEmpty(col.StringFormat))
                                {
                                    throw new ArgumentException("Bogus type requires a StringFormatPattern value", nameof(col.StringFormat) + " on " + col.ColumnName);
                                    
                                }
                                column.StringFormatPattern = col.StringFormat;
                                column.UseGenderColumn = "";
                                break;
                            case DataType.RandomUsername:
                                column.Type = DataType.RandomUsername;
                                if (string.IsNullOrEmpty(col.Max.ToString()) || string.IsNullOrEmpty(col.Min.ToString()))
                                {
                                    throw new ArgumentException("RandomUsername type requires MinMax value", nameof(col.Min) + nameof(col.Max) + " on " + col.ColumnName);
                                }
                                column.Max = col.Max.ToString().Substring(0, col.Max.ToString().IndexOf('.') > 0 ? col.Max.ToString().IndexOf('.') : col.Max.ToString().Length);
                                column.Min = col.Min.ToString().Substring(0, col.Min.ToString().IndexOf('.') > 0 ? col.Min.ToString().IndexOf('.') : col.Min.ToString().Length);
                                column.StringFormatPattern = "";
                                column.UseGenderColumn = "";
                                break;
                            case DataType.FirstName:
                                column.Type = DataType.FirstName;
                                column.Max = col.Max.ToString(); ;
                                column.Min = col.Min;
                                column.StringFormatPattern = "";
                                column.UseGenderColumn = "";
                                break;
                            case DataType.FullName:
                                column.Type = DataType.FullName;
                                column.Max = col.Max.ToString().Substring(0, col.Max.ToString().IndexOf('.') > 0 ? col.Max.ToString().IndexOf('.') : col.Max.ToString().Length);
                                column.Min = col.Min.ToString().Substring(0, col.Min.ToString().IndexOf('.') > 0 ? col.Min.ToString().IndexOf('.') : col.Min.ToString().Length);

                                column.StringFormatPattern = "";
                                column.UseGenderColumn = "";
                                break;
                            case DataType.LastName:
                                column.Type = DataType.LastName;
                                column.Max = col.Max.ToString().Substring(0, col.Max.ToString().IndexOf('.') > 0 ? col.Max.ToString().IndexOf('.') : col.Max.ToString().Length);
                                column.Min = col.Min.ToString().Substring(0, col.Min.ToString().IndexOf('.') > 0 ? col.Min.ToString().IndexOf('.') : col.Min.ToString().Length);

                                column.StringFormatPattern = "";
                                column.UseGenderColumn = "";
                                break;
                            case DataType.DateOfBirth:
                                column.Type = DataType.DateOfBirth;                    
                                if (string.IsNullOrEmpty(col.Max.ToString()) || string.IsNullOrEmpty(col.Min.ToString()))
                                {
                                    throw new ArgumentException("DateOfBirth type requires MinMax value", nameof(col.Min) + nameof(col.Max) + " on " + col.ColumnName);
                                }
                                if (col.Max.ToString().Equals("-- ::") || col.Min.ToString().Equals("-- ::"))
                                {
                                    column.Ignore = true;
                                }
                                if (col.Max.ToString().Equals(col.Min.ToString()) && !col.Max.ToString().Equals("-- ::"))
                                {
                                    col.Max = DateTime.Now.ToString();
                                }
                                column.Max = col.Max.ToString(); ;
                                column.Min = col.Min;
                                column.StringFormatPattern = "";
                                column.UseGenderColumn = "";
                                column.RetainEmptyStringValues = false;
                                break;
                            case DataType.Date:
                                column.Type = DataType.Date;
                                if (string.IsNullOrEmpty(col.Max.ToString()) || string.IsNullOrEmpty(col.Min.ToString()))
                                {
                                    throw new ArgumentException("Date type requires MinMax value", nameof(col.Min) + nameof(col.Max) + " on " + col.ColumnName);
                                }
                                if (string.IsNullOrEmpty(col.StringFormat.ToString()))
                                {
                                    throw new ArgumentException("Date type requires StringFormat value", nameof(col.StringFormat) +" on " + col.ColumnName);
                                }
                                if (col.Max.ToString().Equals("-- ::") || col.Min.ToString().Equals("-- ::"))
                                {
                                    column.Ignore = true;
                                }
                                //if (col.Max.ToString().Equals(col.Min.ToString()))
                                //{
                                //    col.Max = DateTime.Now.ToString();
                                //}
                                column.Max = col.Max.ToString().Substring(0, col.Max.ToString().IndexOf('.') > 0 ? col.Max.ToString().IndexOf('.') : col.Max.ToString().Length);
                                column.Min = col.Min.ToString().Substring(0, col.Min.ToString().IndexOf('.') > 0 ? col.Min.ToString().IndexOf('.') : col.Min.ToString().Length);
                                column.StringFormatPattern = col.StringFormat;
                                column.UseGenderColumn = "";
                                column.RetainEmptyStringValues = false;
                                break;
                            case DataType.PickRandom:
                                column.Type = DataType.PickRandom;
                                if (string.IsNullOrEmpty(col.StringFormat) || 
                                    string.IsNullOrEmpty(col.Max))
                                {
                                    throw new ArgumentException("PickRandom type requires a StringFormatPattern and Max of a list of random variable seperated with a comma", col.StringFormat + " on" + col.ColumnName);
                                }
                                if (col.StringFormat.Split(',').Any(n => n.Length > Convert.ToInt32(col.Max.ToString().Substring(0, col.Max.ToString().IndexOf('.') > 0 ? col.Max.ToString().IndexOf('.') : col.Max.ToString().Length))))
                                {
                                    throw new ArgumentException("An item in the StringFormatPattern has a length greater than Max", nameof(col.StringFormat) + " on " + col.ColumnName);
                                }
                                column.Max = col.Max.ToString().Substring(0, col.Max.ToString().IndexOf('.') > 0 ? col.Max.ToString().IndexOf('.') : col.Max.ToString().Length);
                                column.Min = col.Min.ToString().Substring(0, col.Min.ToString().IndexOf('.') > 0 ? col.Min.ToString().IndexOf('.') : col.Min.ToString().Length);
                                column.StringFormatPattern = col.StringFormat;
                                column.UseGenderColumn = "";
                                break;
                            case DataType.RandomString2:
                                column.Type = DataType.RandomString2;
                                if (string.IsNullOrEmpty(col.Max.ToString()) || string.IsNullOrEmpty(col.Min.ToString()))
                                {
                                    throw new ArgumentException("RandomString2 type requires a Min and Max value", nameof(col.Min) + nameof(col.Max) + " on " + col.ColumnName);
                                }
                                column.Max = col.Max.ToString().Substring(0, col.Max.ToString().IndexOf('.') > 0 ? col.Max.ToString().IndexOf('.') : col.Max.ToString().Length);
                                column.Min = col.Min.ToString().Substring(0, col.Min.ToString().IndexOf('.') > 0 ? col.Min.ToString().IndexOf('.') : col.Min.ToString().Length);
                                column.StringFormatPattern = col.StringFormat;
                                column.UseGenderColumn = "";
                                break;
                                
                            case DataType.Rant:
                                column.Type = DataType.Rant;
                                if (string.IsNullOrEmpty(col.Max.ToString()))
                                {
                                    throw new ArgumentException("Rant type requires a Max value",  nameof(col.Max) + " on " + col.ColumnName);
                                }
                                column.Max = col.Max.ToString().Substring(0, col.Max.ToString().IndexOf('.') > 0 ? col.Max.ToString().IndexOf('.') : col.Max.ToString().Length);
                                //column.min = col.Min.ToString().Substring(0, col.Min.ToString().IndexOf('.') > 0 ? col.Min.ToString().IndexOf('.') : col.Min.ToString().Length);

                                column.Min = Convert.ToString(1).Substring(0, Convert.ToString(1).IndexOf('.') > 0 ? Convert.ToString(1).IndexOf('.') : Convert.ToString(1).Length); ;
                                column.StringFormatPattern = col.StringFormat;
                                column.UseGenderColumn = "";
                                break;
                            case DataType.State:
                                column.Type = DataType.State;
                                column.Max = col.Max.ToString(); ;
                                column.Min = col.Min;
                                column.StringFormatPattern = "Canada";
                                column.UseGenderColumn = "";
                                break;
                            case DataType.City:
                                column.Type = DataType.City;
                                column.Max = col.Max.ToString(); ;
                                column.Min = col.Min;
                                //column.StringFormatPattern = "";
                                column.StringFormatPattern = "BC";
                                break;
                            case DataType.Lorem:
                                column.Type = DataType.Lorem;
                                if (string.IsNullOrEmpty(col.Max.ToString()))
                                {
                                    throw new ArgumentException("Lorem type requires a Min and Max value", nameof(col.Max) + " on " + col.ColumnName);
                                }
                                column.Max = col.Max.ToString().Substring(0, col.Max.ToString().IndexOf('.') > 0 ? col.Max.ToString().IndexOf('.') : col.Max.ToString().Length);
                                column.Min = col.Min.ToString().Substring(0, col.Min.ToString().IndexOf('.') > 0 ? col.Min.ToString().IndexOf('.') : col.Min.ToString().Length);

                                column.StringFormatPattern = col.StringFormat;
                                column.UseGenderColumn = "";
                                break;
                            case DataType.StringFormat:
                                column.Type = DataType.StringFormat;
                                column.Max = col.Max.ToString().Substring(0, col.Max.ToString().IndexOf('.') > 0 ? col.Max.ToString().IndexOf('.') : col.Max.ToString().Length);
                                column.Min = col.Min.ToString().Substring(0, col.Min.ToString().IndexOf('.') > 0 ? col.Min.ToString().IndexOf('.') : col.Min.ToString().Length);
                                column.StringFormatPattern = col.StringFormat;
                                column.UseGenderColumn = "";
                                break;
                            case DataType.FullAddress:
                                column.Type = DataType.FullAddress;
                                column.Max = col.Max.ToString().Substring(0, col.Max.ToString().IndexOf('.') > 0 ? col.Max.ToString().IndexOf('.') : col.Max.ToString().Length);
                                column.Min = col.Min.ToString().Substring(0, col.Min.ToString().IndexOf('.') > 0 ? col.Min.ToString().IndexOf('.') : col.Min.ToString().Length);

                                column.StringFormatPattern = "Canada";
                                column.UseGenderColumn = "";
                                break;
                            case DataType.StreetAddress:
                                column.Type = DataType.StreetAddress;
                                column.Max = col.Max.ToString().Substring(0, col.Max.ToString().IndexOf('.') > 0 ? col.Max.ToString().IndexOf('.') : col.Max.ToString().Length);
                                column.Min = col.Min.ToString().Substring(0, col.Min.ToString().IndexOf('.') > 0 ? col.Min.ToString().IndexOf('.') : col.Min.ToString().Length);

                                column.StringFormatPattern = col.StringFormat;
                                column.UseGenderColumn = "";
                                break;
                            case DataType.SecondaryAddress:
                                column.Type = DataType.SecondaryAddress;
                                column.Max = col.Max.ToString().Substring(0, col.Max.ToString().IndexOf('.') > 0 ? col.Max.ToString().IndexOf('.') : col.Max.ToString().Length);
                                column.Min = col.Min.ToString().Substring(0, col.Min.ToString().IndexOf('.') > 0 ? col.Min.ToString().IndexOf('.') : col.Min.ToString().Length);

                                column.StringFormatPattern = col.StringFormat;
                                column.UseGenderColumn = "";
                                break;
                            case DataType.PhoneNumber:
                                column.Type = DataType.PhoneNumber;
                                column.Max = col.Max.ToString().Substring(0, col.Max.ToString().IndexOf('.') > 0 ? col.Max.ToString().IndexOf('.') : col.Max.ToString().Length);
                                column.Min = col.Min.ToString().Substring(0, col.Min.ToString().IndexOf('.') > 0 ? col.Min.ToString().IndexOf('.') : col.Min.ToString().Length);

                                if (string.IsNullOrEmpty(col.StringFormat))
                                {
                                    column.StringFormatPattern = "##########";
                                }
                                else
                                    column.StringFormatPattern = col.StringFormat;

                                column.UseGenderColumn = "";
                                break;
                            case DataType.PhoneNumberInt:
                                column.Type = DataType.PhoneNumberInt;
                                column.Max = col.Max.ToString(); ;
                                column.Min = col.Min;
                                if (string.IsNullOrEmpty(col.StringFormat))
                                {
                                    column.StringFormatPattern = "##########";
                                }
                                else
                                    column.StringFormatPattern = col.StringFormat;
                                column.UseGenderColumn = "";
                                break;
                            case DataType.Longitude:
                                column.Type = DataType.Longitude;
                                column.Max = col.Max.ToString(); ;
                                column.Min = col.Min;
                                column.StringFormatPattern = col.StringFormat;
                                column.UseGenderColumn = "";
                                break;
                            case DataType.Latitude:
                                column.Type = DataType.Latitude;
                                column.Max = col.Max.ToString(); ;
                                column.Min = col.Min;
                                column.StringFormatPattern = col.StringFormat;
                                column.UseGenderColumn = "";
                                break;
                            case DataType.RandomSeason:
                                column.Type = DataType.RandomSeason;
                                column.Max = col.Max.ToString(); ;
                                column.Min = col.Min;
                                column.StringFormatPattern = col.StringFormat;
                                column.UseGenderColumn = "";
                                break;
                            case DataType.File:
                                column.Type = DataType.File;
                                column.Max = col.Max.ToString().Substring(0, col.Max.ToString().IndexOf('.') > 0 ? col.Max.ToString().IndexOf('.') : col.Max.ToString().Length);
                                column.Min = col.Min.ToString().Substring(0, col.Min.ToString().IndexOf('.') > 0 ? col.Min.ToString().IndexOf('.') : col.Min.ToString().Length);
                                column.StringFormatPattern = "{{SYSTEM.FILENAME}}";
                                column.UseGenderColumn = "";
                                break;
                            case DataType.Filename:                             
                                column.Type = DataType.File;
                                column.Max = col.Max.ToString().Substring(0, col.Max.ToString().IndexOf('.') > 0 ? col.Max.ToString().IndexOf('.') : col.Max.ToString().Length);
                                column.Min = col.Min.ToString().Substring(0, col.Min.ToString().IndexOf('.') > 0 ? col.Min.ToString().IndexOf('.') : col.Min.ToString().Length);

                                //column.StringFormatPattern = "{{ADDRESS.STREETADDRESS}} {{ADDRESS.CITY}} {{ADDRESS.STATE}}";
                                column.StringFormatPattern = "{{SYSTEM.FILENAME}}";
                                column.UseGenderColumn = "";
                                break;
                            case DataType.Blob:
                                var filename = nameGroup.Where(n => n.ColumnName.Equals("FILE_NAME") || n.ColumnName.Equals("FILENAME")).Select(n => n).FirstOrDefault().ColumnName;
                                column.Type = DataType.Blob;
                                column.Max = col.Max.ToString();
                                column.Min = col.Min.ToString();
                                column.StringFormatPattern = "";
                                if (!string.IsNullOrEmpty(filename))
                                {
                                    column.StringFormatPattern = filename;
                                }
                                column.UseGenderColumn = "";
                                break;
                            case DataType.Clob:
                                column.Type = DataType.Clob;
                                column.Max = col.Max.ToString(); ;
                                column.Min = col.Min.ToString(); ;
                                column.StringFormatPattern = "";
                                column.UseGenderColumn = "";
                                break;
                            case DataType.RandomDec:
                                column.Type = DataType.RandomDec;
                                if (string.IsNullOrEmpty(col.Max.ToString()))
                                {
                                    throw new ArgumentException("RandomDec type requires a Max value",  nameof(col.Max) + " on " + col.ColumnName);
                                }
                                if (string.IsNullOrEmpty(col.Min.ToString()))
                                {
                                    throw new ArgumentException("RandomDec type requires a Max value", nameof(col.Min) + " on " + col.ColumnName);
                                }
                                column.Max = col.Max.ToString(); ;
                                column.Min = col.Min.ToString(); ;
                                column.StringFormatPattern = col.StringFormat;
                                column.UseGenderColumn = "";
                                break;
                            case DataType.Geometry:
                                column.Type = DataType.Geometry;
                                column.Max = col.Max.ToString(); ;
                                column.Min = col.Min.ToString(); ;
                                column.StringFormatPattern = "";
                                column.UseGenderColumn = "";
                                break;
                            case DataType.RandomYear:
                                column.Type = DataType.RandomYear;
                                column.Max = col.Max.ToString().Substring(0, col.Max.ToString().IndexOf('.') > 0 ? col.Max.ToString().IndexOf('.') : col.Max.ToString().Length);
                                column.Min = col.Min.ToString().Substring(0, col.Min.ToString().IndexOf('.') > 0 ? col.Min.ToString().IndexOf('.') : col.Min.ToString().Length);

                                column.StringFormatPattern = col.StringFormat;
                                column.UseGenderColumn = "";
                                break;
                            case DataType.RandomMonth:
                                column.Type = DataType.RandomMonth;
                                column.Max = col.Max.ToString();
                                column.Min = col.Min.ToString();
                                column.StringFormatPattern = col.StringFormat;
                                column.UseGenderColumn = "";
                                break;
                            case DataType.ShufflePolygon:
                                column.Type = DataType.ShufflePolygon;
                                column.Max = col.Max.ToString();
                                column.Min = col.Min.ToString();
                                break;
                            case DataType.RandomInt:
                                column.Type = DataType.RandomInt;
                                if (string.IsNullOrEmpty(col.Max.ToString()))
                                {
                                    throw new ArgumentException("RandomInt type requires a Max value", nameof(col.Max) + " on " + col.ColumnName);
                                }
                                if (string.IsNullOrEmpty(col.Min.ToString()))
                                {
                                    throw new ArgumentException("RandomInt type requires a Max value", nameof(col.Min) + " on " + col.ColumnName);
                                }
                                column.Max = col.Max.ToString().Substring(0, col.Max.ToString().IndexOf('.') > 0 ? col.Max.ToString().IndexOf('.') : col.Max.ToString().Length);
                                column.Min = col.Min.ToString().Substring(0, col.Min.ToString().IndexOf('.') > 0 ? col.Min.ToString().IndexOf('.') : col.Min.ToString().Length);
                                column.StringFormatPattern = col.StringFormat;
                                column.UseGenderColumn = "";
                                break;
                            case DataType.Computed:
                                break;
                            case DataType.RandomHexa:
                                break;
                            default:
                                column.Ignore = true;   
                                break;
                        }
                    }
                    else if (col.ColumnName.ToUpper().Contains("LONGITUDE") && (col.DataType.ToUpper().Contains("NUMERIC") || col.DataType.ToUpper().Contains("DECIMAL")))
                    {
                        column.Type = DataType.Longitude;
                        column.Max = col.Max.ToString();
                        column.Min = col.Min.ToString();
                        column.StringFormatPattern = col.Description;
                        column.Operator = "";
                        column.UseGenderColumn = "";
                    }
                    else if (col.ColumnName.ToUpper().Contains("LATITUDE") && (col.DataType.ToUpper().Contains("NUMERIC") || col.DataType.ToUpper().Contains("DECIMAL")))
                    {
                        column.Type = DataType.Latitude;
                        column.Max = col.Max.ToString();
                        column.Min = col.Min.ToString();
                        column.StringFormatPattern = col.Description;
                        column.Operator = "";
                        column.UseGenderColumn = "";
                    }
                    else if (col.ColumnName.ToUpper().Contains("FIRST_NAME") || col.ColumnName.ToUpper().Contains("FIRSTNAME")  || col.ColumnName.ToUpper().Contains("MIDDLE_NAME"))
                    {
                        column.Type = DataType.FirstName;
                        column.Max = col.Max.ToString(); 
                        column.Min = col.Min.ToString(); 
                        column.StringFormatPattern = "{{NAME.FIRSTNAME}}";
                        column.UseGenderColumn = "";
                    }
                    else if (col.DataType.ToUpper().Equals("BLOB") || col.DataType.ToUpper().Equals("IMAGE"))
                    {
                        var filename = nameGroup.Where(n => n.ColumnName.Equals("FILE_NAME") || n.ColumnName.Equals("FILENAME")).Select(n=>n).FirstOrDefault().ColumnName;
                        column.Type = DataType.Blob;
                        column.Max = col.Max.ToString(); 
                        column.Min = col.Min.ToString();
                       
                        column.StringFormatPattern = col.StringFormat;
                        if (!string.IsNullOrEmpty(filename))
                        {
                            column.StringFormatPattern = filename;
                        }
                        column.UseGenderColumn = "";
                    }
                    else if (col.DataType.ToUpper().Equals("CLOB"))
                    {
                        column.Type = DataType.Clob;
                        column.Max = col.Max.ToString(); ;
                        column.Min = col.Min.ToString(); ;
                        column.StringFormatPattern = col.StringFormat;
                        column.UseGenderColumn = "";
                    }
                    else if (col.ColumnName.ToUpper().Contains("CITY"))
                    {
                        column.Type = DataType.City; 
                        column.Max = col.Max.ToString(); ;
                        column.Min = col.Min.ToString(); ;
                        column.StringFormatPattern = "{{ADDRESS.CITY}}";
                        column.UseGenderColumn = "Canada";
                    }
                    else if (col.ColumnName.ToUpper().Contains("STATE") || col.ColumnName.ToUpper().Contains("PROVINCE"))
                    {
                        column.Type = DataType.State;
                        column.Max = col.Max.ToString(); ;
                        column.Min = col.Min.ToString(); ;
                        column.StringFormatPattern = "{{ADDRESS.CITY}}";
                        column.UseGenderColumn = "Canada";
                    }
                    else if (col.ColumnName.ToUpper().Contains("COUNTRY"))
                    {
                        column.Type = DataType.Bogus;
                        column.Max = col.Max.ToString(); ;
                        column.Min = col.Min.ToString(); ;
                        column.StringFormatPattern = "{{ADDRESS.COUNTRY}}";
                        column.UseGenderColumn = "Canada";
                    }
                    else if (col.DataType.ToUpper().Equals("SDO_GEOMETRY") || col.DataType.ToUpper().ToUpper().Contains("GEOMETRY"))
                    {
                        column.Type = DataType.ShufflePolygon;
                        column.Max = col.Max.ToString(); ;
                        column.Min = col.Min.ToString(); ;
                        column.StringFormatPattern = col.StringFormat;
                        column.UseGenderColumn = "";
                    }
                    else if (col.ColumnName.ToUpper().Contains("SURNAME") || col.ColumnName.ToUpper().Contains("LASTNAME") || col.ColumnName.ToUpper().Contains("LAST_NAME"))
                    {
                        column.Type = DataType.LastName;
                        column.Max = col.Max.ToString(); ;
                        column.Min = col.Min.ToString(); ;
                        column.StringFormatPattern = "{{NAME.LASTNAME}}";
                        column.UseGenderColumn = "";
                    }
                    else if (col.ColumnName.ToUpper().Contains("COMPANY_NAME") || col.ColumnName.ToUpper().Contains("ORGANIZATION_NAME"))
                    {
                        column.Type = DataType.Company;
                        column.Max = col.Max.ToString(); ;
                        column.Min = col.Min.ToString(); ;
                        column.StringFormatPattern = "{{COMPANY.COMPANYNAME}} {{COMPANY.COMPANYSUFFIX}}";
                        column.UseGenderColumn = "";
                    }
                    else if (_comment.Any(n => col.ColumnName.ToUpper().Contains(n)) || col.DataType.ToUpper().Contains("CHAR") || _comment.Any(x => col.Comments.Contains(x)))
                    {
                        var size = col.DataType.ToUpper().Replace("(", " ").Replace(")", " ").Split(' ');
                        if (size.Count() > 1)
                        {
                            var sizze = size[1].ToString();
                            if (!string.IsNullOrEmpty(sizze))
                            {
                                column.Type = DataType.Rant;
                                column.Max = Convert.ToString(sizze);
                                column.Min = Convert.ToString(1); ;
                                column.StringFormatPattern = "DESCRIPTION";
                                column.UseGenderColumn = "";
                            }
                            else
                            {
                                column.Type = DataType.Rant;
                                column.Max = col.Max.ToString(); ;
                                column.Min = Convert.ToString(1) ;
                                column.StringFormatPattern = "DESCRIPTION";
                                column.UseGenderColumn = "";
                            }
                        }
                        else
                        {
                            column.Type = DataType.Rant;
                            column.Max = col.Max.ToString(); ;
                            column.Min = Convert.ToString(1);
                            column.StringFormatPattern = "DESCRIPTION";
                            column.UseGenderColumn = "";
                        }//split varchar(20 byte) and get max number

                    }
                    else if (col.DataType.ToUpper().Contains("DATE"))
                    {
                        column.Type = DataType.DateOfBirth;
                        column.Max = col.Max.ToString(); ;
                        column.Min = col.Min;
                        column.StringFormatPattern = "";
                        column.UseGenderColumn = "";
                    }
                    else if (col.ColumnName.ToUpper().Equals("YEAR"))
                    {
                        column.Type = DataType.RandomYear;
                        column.Max = col.Max.ToString(); ;
                        column.Min = col.Min.ToString(); ;
                        column.StringFormatPattern = "";
                        column.UseGenderColumn = "";
                    }
                    else if (col.ColumnName.ToUpper().Contains("PHONE_NO") || col.ColumnName.ToUpper().Contains("FAX_NO") || col.ColumnName.ToUpper().Contains("CONTRACT_NO") || col.ColumnName.ToUpper().Contains("CELL") || col.ColumnName.ToUpper().Contains("_PHONE") || col.ColumnName.ToUpper().Contains("PHONENUMBER"))
                    {
                        if (col.DataType.ToUpper().Contains("NUMBER"))
                        {
                            column.Type = DataType.PhoneNumberInt;
                            column.Max = col.Max.ToString(); ;
                            column.Min = col.Min.ToString(); ;
                            if (string.IsNullOrEmpty(col.StringFormat))
                            {
                                column.StringFormatPattern = "##########";
                            }
                            else
                                column.StringFormatPattern = col.StringFormat;
                            column.UseGenderColumn = "";
                        }
                        else
                        {
                            column.Type = DataType.PhoneNumber;
                            column.Max = col.Max.ToString(); ;
                            column.Min = col.Min.ToString(); ;
                            if (string.IsNullOrEmpty(col.StringFormat))
                            {
                                column.StringFormatPattern = "##########";
                            }
                            else
                                column.StringFormatPattern = col.StringFormat;
                            column.UseGenderColumn = "";
                        }
                    }
                    else if (col.ColumnName.ToUpper().Contains("EMAIL_ADDRESS"))
                    {
                        column.Type = DataType.Bogus;
                        column.Max = col.Max.ToString(); ;
                        column.Min = col.Min.ToString(); ;
                        column.StringFormatPattern = "{{INTERNET.EMAIL}}";
                        column.UseGenderColumn = "";
                    }
                    else if (col.DataType.ToUpper().Contains("MONEY"))
                    {
                        column.Type = DataType.Money;
                        column.Max = col.Max.ToString();
                        column.Min = col.Min.ToString();
                        column.StringFormatPattern = "{{FINANCE.AMOUNT}}";
                        //column.useGenderColumn = "Gender";
                    }
                    else if (col.ColumnName.ToUpper().Contains("POSTAL_CODE"))
                    {
                        column.Type = DataType.PostalCode;
                        column.Max = col.Max.ToString(); ;
                        column.Min = col.Min.ToString(); ;
                        column.StringFormatPattern = "";
                        column.UseGenderColumn = "";
                    }
                    else if (col.DataType.ToUpper().Equals("CHAR(1 BYTE)"))
                    {
                        var chr = col.DataType.ToUpper().Replace("(", " ").Replace(")", " ").Split(' ');
                        if (chr.Count() > 1)
                        {
                            var charSize = chr[1].ToString();
                            if (!string.IsNullOrEmpty(charSize))
                            {
                                column.Type = DataType.PickRandom;
                                column.Max = Convert.ToString(charSize);
                                column.Min = col.Min.ToString(); ;
                                column.StringFormatPattern = "";
                                column.UseGenderColumn = "";
                            }
                            else
                            {
                                column.Type = DataType.Ignore;
                                column.Max = col.Max.ToString(); ;
                                column.Min = col.Min.ToString(); ;
                                column.Ignore = true;
                                column.StringFormatPattern = "";
                                column.UseGenderColumn = "";
                            }
                        }
                        else
                        {
                            column.Type = DataType.Ignore;
                            column.Max = col.Max.ToString(); ;
                            column.Min = col.Min.ToString(); ;
                            column.Ignore = true;
                            column.StringFormatPattern = "";
                            column.UseGenderColumn = "";
                        }
                    }
                    else if (col.ColumnName.ToUpper().Contains("ADDRESS"))
                    {
                        var size = col.DataType.ToUpper().Replace("(", " ").Replace(")", " ").Split(' ');
                        if (size.Count() > 1)
                        {
                            var sizze = size[1].ToString();
                            if (!string.IsNullOrEmpty(sizze))
                            {
                                if (col.ColumnName.ToUpper().Contains("ADDRESS") && (col.ColumnName.Contains("2") || col.ColumnName.Contains("3")))
                                {
                                    column.Type = DataType.SecondaryAddress;
                                }
                                else if (col.ColumnName.ToUpper().Contains("STREET"))
                                {
                                    column.Type = DataType.StreetAddress;
                                }
                                else
                                    column.Type = DataType.FullAddress;

                                column.Max = Convert.ToString(sizze);
                                column.Min = col.Min.ToString(); ;
                                column.StringFormatPattern = "{{address.fullAddress}}";
                                column.UseGenderColumn = "Canada";
                            }
                            else
                            {
                                column.Type = DataType.FullAddress;
                                column.Max = col.Max.ToString(); ;
                                column.Min = col.Min.ToString(); ;
                                column.StringFormatPattern = "{{address.fullAddress}}";
                                column.UseGenderColumn = "Canada";
                            }
                        }
                        else
                        {
                            column.Type = DataType.FullAddress;
                            column.Max = col.Max.ToString(); ;
                            column.Min = col.Min.ToString(); ;
                            //column.StringFormatPattern = "{{ADDRESS.STREETADDRESS}} {{ADDRESS.CITY}} {{ADDRESS.STATE}}";
                            column.StringFormatPattern = "{{address.fullAddress}}";
                            column.UseGenderColumn = "Canada";
                        }
                    }
                    else if (col.ColumnName.ToUpper().Contains("USERID") || col.ColumnName.ToUpper().Contains("USERNAME"))
                    {
                        column.Type = DataType.RandomUsername;
                        column.Max = col.Max.ToString(); ;
                        column.Min = col.Min.ToString(); ;
                        //column.StringFormatPattern = "{{ADDRESS.STREETADDRESS}} {{ADDRESS.CITY}} {{ADDRESS.STATE}}";
                        column.StringFormatPattern = "";
                        column.UseGenderColumn = "";
                    }
                    else if (col.ColumnName.ToUpper().Contains("FILE_NAME"))
                    {
                        column.Type = DataType.File;
                        column.Max = col.Max.ToString(); ;
                        column.Min = col.Min.ToString(); ;
                        //column.StringFormatPattern = "{{ADDRESS.STREETADDRESS}} {{ADDRESS.CITY}} {{ADDRESS.STATE}}";
                        column.StringFormatPattern = "{{SYSTEM.FILENAME}}";
                        column.UseGenderColumn = "";
                    }
                    else if (_fullName.Any(n => col.ColumnName.ToUpper().Contains(n)) || _fullName.Any(x => col.Comments.Contains(x)))
                    {
                        column.Type = DataType.Bogus;
                        column.Max = col.Max.ToString(); ;
                        column.Min = col.Min.ToString(); ;
                        //column.StringFormatPattern = "{{ADDRESS.STREETADDRESS}} {{ADDRESS.CITY}} {{ADDRESS.STATE}}";
                        column.StringFormatPattern = "{{NAME.FULLNAME}}";
                        column.UseGenderColumn = "";
                    }
                    else if (col.ColumnName.ToUpper().Contains("AMOUNT") || col.ColumnName.ToUpper().Contains("AMT") || col.Comments.Contains("Amount"))
                    {
                        column.Type = DataType.RandomDec;
                        column.Max = col.Max.ToString(); ;
                        column.Min = col.Min;
                        column.StringFormatPattern = "";
                        column.UseGenderColumn = "";
                    }                  
                    else
                    {                       
                        if (col.ColumnName.ToUpper().Equals("NAME")) //set company name
                        {
                            column.Type = DataType.Bogus;
                            //column.type = col.DATA_TYPE;
                            column.StringFormatPattern = "{{COMPANY.COMPANYNAME}}";
                            column.Max = col.Max.ToString(); ;
                            column.Min = col.Min.ToString(); ;
                            column.UseGenderColumn = "";
                        }
                        else if ((col.ColumnName.ToUpper().Contains("AUTHOR") || col.ColumnName.ToUpper().Contains("EDITOR")) && col.DataType.ToUpper().Contains("VARCHAR"))
                        {
                            column.Type = DataType.FullName;
                            column.Max = col.Max.ToString().Substring(0, col.Max.ToString().IndexOf('.') > 0 ? col.Max.ToString().IndexOf('.') : col.Max.ToString().Length);
                            column.Min = col.Min.ToString().Substring(0, col.Min.ToString().IndexOf('.') > 0 ? col.Min.ToString().IndexOf('.') : col.Min.ToString().Length);

                            column.StringFormatPattern = "";
                            column.UseGenderColumn = "";
                        }
                        else if (col.DataType.ToUpper().Contains("NUMBER"))
                        {
                            //var size = col.DataType.ToUpper().Replace("(", " ").Replace(")", " ").Split(' ')[1].Split(',')[0].ToString();

                            column.Type = DataType.RandomInt;
                            column.Max = col.Max.ToString(); ;
                            column.Min = col.Min;
                            column.StringFormatPattern = "";
                            column.UseGenderColumn = "";

                        }
                        else if (col.ColumnName.ToUpper().Equals("TOTAL_AREA")) //set company name
                        {
                            column.Type = DataType.Bogus;
                            //column.type = col.DATA_TYPE;
                            column.StringFormatPattern = "";
                            column.Max = col.Max.ToString(); ;
                            column.Min = col.Min;
                            column.UseGenderColumn = "";
                        }
                        else
                        {
                            count++;
                            collist.Add(new KeyValuePair<string, string>(col.ColumnName, col.TableName));
                            //collist.Add(col.ColumnName.ToUpper(), col.TableName);
                            column.Type = DataType.Ignore;
                            //column.type = col.DATA_TYPE;
                            column.StringFormatPattern = "";
                            column.Max = col.Max.ToString();
                            column.Min = col.Min.ToString();
                            column.UseGenderColumn = "";
                            column.Ignore = true;

                        }
                    }
                    if (!col.ColumnName.Equals(table.PrimaryKeyColumn) && !col.MaskingRule.ToUpper().Contains("FLAGGED"))
                    {
                        colList.Add(column);
                    }
                    
                }

                // if (colList.Count > 0)
                // {
                table.Columns = colList;

                tableList.Add(table);
                //}
                #region check for null type
#pragma warning disable CS0472 // The result of the expression is always the same since a value of this type is never equal to 'null'
                var nullType = table.Columns.Where(x => x.Type == null);
#pragma warning restore CS0472 // The result of the expression is always the same since a value of this type is never equal to 'null'
                if (nullType.Any())
                {
                    int autoNumber1 = 1;
                    foreach (var type in nullType)
                    {
                        Console.WriteLine(autoNumber1++.ToString() + ". "+ $"Column {type.Name} contains invalid masking rule type");
                    }
                    Console.WriteLine("The Program will exit. Hit Enter to exit..");


                    Console.ReadLine();
                    System.Environment.Exit(1);
                }
                #endregion

            }
            #endregion

            #region Initialize Root object
            RootObject1 rootObject1 = new RootObject1
            {
                tables = tableList,

                dataSource = dataSource,
                dataGeneration = dataGeneration
          

            };
            #endregion
            //check for Tables with no primary key or any Identifier and exit if true 
            #region Check for tables without Primary Key if true exit
            var noPrimaryKey = rootObj.Where(n => n.PKconstraintName == null || n.PKconstraintName == string.Empty).GroupBy(n => n.TableName);
            //var cou = noPrimaryKey.Count();
            //primary key applied to relational database not spreadsheet         
            if (noPrimaryKey.Count() != 0 && ConfigurationManager.AppSettings[SourceType] != nameof(DataSourceType.SpreadSheet))
            {
                int autoNumber = 1;
                Console.WriteLine("Required property 'PrimaryKeyColumn' expects a value but got null. Please provide one column identifier for these tables" + "\n" +
                    "See the NullPrimaryKey.txt file in the Output folder.");
                string nullPK = "These tables has no PrimaryKey or Identifier. Provide one column identifier for these tables " + Environment.NewLine + Environment.NewLine;
                foreach (var tables in noPrimaryKey.GroupBy(n => n.Key))
                {
                    nullPK += autoNumber++.ToString() + ". " + tables.Key.ToString() + " PrimaryKey : " + "Null" + Environment.NewLine;

                    //Console.WriteLine(tables.TABLE_NAME);
                }
                File.WriteAllText(@"output\NullPrimaryKey.txt", nullPK);
                Console.WriteLine("The Program will exit. Hit Enter to exit..");



                Console.ReadLine();


                System.Environment.Exit(1);
            }
            #endregion

         

            //jsonpath = @"example-configs\jsconfigTables.json";
            if (!Directory.Exists(@"classification-configs"))
            {
                Directory.CreateDirectory(@"classification-configs");
            }
            var jsonname = Path.GetFileNameWithoutExtension(excelspreadsheet);
            Jsonpath = @"classification-configs\" + jsonname + ".json";
            string jsonresult = JsonConvert.SerializeObject(rootObject1,Formatting.Indented);
            if (!Directory.Exists(@"output\Validation"))
            {
                Directory.CreateDirectory(@"output\Validation");
            }
            var fileTime = DateTime.Now - File.GetLastWriteTime(excelspreadsheet);          
            #region compare original jsonconfig for datatype errors
            if (File.Exists(Jsonpath) && new FileInfo(Jsonpath).Length != 0)
            {
                var fileTimeJS = DateTime.Now - File.GetLastWriteTime(Jsonpath);
                var rootConfig = Config.Load(Jsonpath);
                foreach (var tabitem in rootConfig.Tables)
                {
                    foreach (var colitems in tabitem.Columns)
                    {
                        var newdic = new Dictionary<string, string> { { colitems.Name, colitems.Type.ToString() } };
                        jsconfigTable.Add(new KeyValuePair<TableConfig,ColumnConfig>(tabitem, colitems));

                        //(item.Name, new Dictionary<string, string>() { { items.Name, items.Type.ToString() } });
                    }
                }
                var buildConfig = JsonConvert.DeserializeObject<RootObject1>(jsonresult);
                CompareLogic compareLogic = new CompareLogic();
                var gg = compareLogic.Compare(buildConfig.tables,rootConfig.Tables).Differences;
                foreach (var tabCol in buildConfig.tables)
                {
                    foreach (var col in tabCol.Columns)
                    {
                        copyJsTable.Add(new KeyValuePair<TableConfig, ColumnConfig>(tabCol, col));
                    }
                }
                // var diff = jsconfigTable.Where(x=> x.Key != copyJsTable.Select(n=>n.Key).ToString());
                string mapped = "";
                if (copyJsTable.Count == jsconfigTable.Count)
                {
                    for (int i = 0; i < copyJsTable.Count; i++)
                    {
                        if (!compareLogic.Compare(jsconfigTable[i], copyJsTable[i]).AreEqual)
                        {
                            var diff = compareLogic.Compare(jsconfigTable[i], copyJsTable[i]).Differences;
                            var obj1 = diff.FirstOrDefault().ParentObject1;
                            var obj2 = diff.FirstOrDefault().ParentObject2;
                            if (obj1.GetType() == typeof(TableConfig))
                            {
                                var t1 = (TableConfig)obj1;
                                var t2 = (TableConfig)obj2;
                                var t3 = compareLogic.Compare(t1.Columns, t2.Columns).Differences;
                                var t4 = compareLogic.Compare(t1, t2).Differences;
                                //mapped = t4.FirstOrDefault().PropertyName;
                               mapped = t4.FirstOrDefault().PropertyName + " " + t1.Name +" now mapped with: " + t4.FirstOrDefault().Object1Value;
                            }
                            else
                            {
                                var t1 = (ColumnConfig)obj1;
                                var t2 = (ColumnConfig)obj2;
                                var t4 = compareLogic.Compare(t1, t2).Differences;
                                //mapped = t4.FirstOrDefault().PropertyName;
                                mapped = "Column " +  t1.Name + " of " + t4.FirstOrDefault().PropertyName + " " +  t4.FirstOrDefault().Object1Value +"  now set to " + t4.FirstOrDefault().Object2Value;
                            }


                            //var objDiff = ggg.GetType() == typeof(TableConfig) ? (TableConfig)ggg : (TableConfig)ggg;
                            //var yu = objDiff.Name;
                            //mapped = string.Join(",", jsconfigTable[i].Value.Select(n => n.Value).ToArray());
                            //Console.WriteLine(jsconfigTable[i].Key.ToString() + " " + string.Join(",", copyJsTable[i].Value.ToArray()) + " now mapped with: " + mapped);
                        }
                        //else if (jsconfigTable[i].Value.Where(n => n.Value.Equals(DataType.Ignore.ToString())).Count() != 0)
                        //{
                        //    var xxxx = jsconfigTable[i].Value.Where(n => n.Value.Equals(DataType.Ignore.ToString())).ToDictionary(n => n.Key, n => n.Value);
                        //    //exit
                        //    _allNull.Add(new KeyValuePair<string, Dictionary<string, string>>(string.Join("", jsconfigTable[i].Key.ToArray()).ToString(), xxxx));

                        //}
                     


                    }
                    if (string.IsNullOrEmpty(mapped) || fileTime < new TimeSpan(0,4,0)) //create new jsson if spreadsheet was recently updated and original json has not been modified as previous
                    {
                        //replace original with new json
                        if (!Directory.Exists(@"output\Validation"))
                        {
                            Directory.CreateDirectory(@"output\Validation");
                        }
                        if (!string.IsNullOrEmpty(mapped))
                        {
                            Console.WriteLine("A recent change was made in the Excel SpreadSheet, this will create a new json file in the Classification-Config folder ");
                        }
                        using (var tw = new StreamWriter(Jsonpath, false))
                        {
                            tw.WriteLine(jsonresult.ToString());
                            tw.Close();
                            Console.WriteLine("{0}{1}", "Mapped Json".ToUpper() + Environment.NewLine, jsonresult);
                        }
                        //check map failures and write to file then exit for correction
                        if (count != 0)
                        {
                            string colfailed = count + " columns cannot be mapped to a masking datatype and so will be ignored during masking. Review the " + Jsonpath + " and provide mask datatype for these columns " + Environment.NewLine + Environment.NewLine + string.Join(Environment.NewLine, collist.Select(x => x.Key + " ON TABLE " + x.Value).ToArray());
                            //Console.WriteLine(colfailed);
                            Console.WriteLine(count + " columns cannot be mapped to a masking datatype and so will be ignored during masking" + Environment.NewLine + "{0}", string.Join(Environment.NewLine, collist.Select(n => n.Key + " ON TABLE " + n.Value).ToArray()));
                            Console.WriteLine("Do you wish to continue and ignore masking these columns? [yes/no]");
                            string option = Console.ReadLine();
                            if (option.ToUpper() == "YES" || option.ToUpper() == "Y")
                            {

                            }
                            else
                            {
                                Console.WriteLine("Hit Enter to exist..");
                                Console.ReadLine();
                                File.WriteAllText(@"output\failedColumn.txt", colfailed);
                                System.Environment.Exit(1);
                            }
                        }
                    }
                    else
                        Console.WriteLine(mapped);
                    if (_allNull.Count() != 0)
                    {
                        if (!Directory.Exists(@"output\Validation"))
                        {
                            Directory.CreateDirectory(@"output\Validation");
                        }
                        int notmasked = 1;
                        _colError.Add("Table_name contains column with ignore datatype Column_name :type" + Environment.NewLine + Environment.NewLine);
                        for (int i = 0; i < _allNull.Count; i++)
                        {
                            //_colError.Add("")
                            //Console.WriteLine(string.Join("", _allNull[i].Key.ToArray()) + " contains column with ignore datatype/columns" + " " + string.Join(Environment.NewLine, _allNull[i].Value.Select(n => n.Key + " :" + n.Value).ToArray()));
                            _colError.Add(notmasked++.ToString() + ". " + string.Join("", _allNull[i].Key.ToArray()) + " contains column with ignore datatype" + " " + string.Join("", _allNull[i].Value.Select(n => n.Key + " :" + n.Value).ToArray()) + Environment.NewLine);
                        }
                        string value = string.Join("", _colError.ToArray());
                        Console.Write(Environment.NewLine + "These columns have ignore datatype so will not be masked" + Environment.NewLine);
                        Console.Write(value);
                        Console.Write(Environment.NewLine);

                        Console.WriteLine("Do you want to continue and ignore masking these columns? [yes/no]");
                        string option = Console.ReadLine();
                        if (option == "yes")
                        {
                            JObject o1 = JObject.Parse(File.ReadAllText(Jsonpath));
                            Console.WriteLine(o1);
                            
                            
                            File.WriteAllText(@"output\ColumnNotMasked.txt", value);
                            
                        }
                        else
                        {
                            Console.WriteLine("Hit Enter to exist..");

                            Console.ReadLine();
                            File.WriteAllText(@"output\ColumnNotMasked.txt", value);

                            System.Environment.Exit(1);
                        }
                    }
                }
                else
                {
                    Console.WriteLine("Sequence array in both files are not equal. Check the copyJs.js and JsConfigTables.js");
                    Console.WriteLine("The program with exit....." + "\n"
                            + "Hit Enter to Exit.................");
                    Console.ReadLine();
                    System.Environment.Exit(1);
                }
            }
            else
            {
                //write json file to path
                if (!Directory.Exists(@"output\Validation"))
                {
                    Directory.CreateDirectory(@"output\Validation");
                }

                using (var tw = new StreamWriter(Jsonpath, false))
                {
                    tw.WriteLine(jsonresult.ToString());
                    tw.Close();
                    Console.WriteLine("{0}{1}", "Maped Json".ToUpper() + Environment.NewLine, jsonresult);
                }
                //check map failures and write to file then exit for correction
                if (count != 0)
                {
                    string colfailed = count + " columns cannot be mapped to a masking datatype and so will be ignored during masking. Review the " + Jsonpath +" and provide mask datatype for these columns " + Environment.NewLine + Environment.NewLine + string.Join(Environment.NewLine, collist.Select(x => x.Key + " ON TABLE " + x.Value).ToArray());
                    //Console.WriteLine(colfailed);
                    Console.WriteLine(count + " columns cannot be mapped to a masking datatype and so will be ignored during masking" + Environment.NewLine +"{0}", string.Join(Environment.NewLine, collist.Select(n => n.Key + " ON TABLE " + n.Value).ToArray()));
                    Console.WriteLine("Do you wish to continue and ignore masking these columns? [yes/no]");
                    string option = Console.ReadLine();
                    if (option.ToUpper() == "YES" || option.ToUpper() == "Y")
                    {

                    }
                    else
                    {
                        Console.WriteLine("Hit Enter to exist..");
                        Console.ReadLine();
                        File.WriteAllText(@"output\failedColumn.txt", colfailed);
                        System.Environment.Exit(1);
                    }
                }

                

            }
            #endregion

        }
        public static Config LoadConfig(
            int example)
        {

            if (allkey.Where(n => n.Key.ToUpper().Equals(RunTestJson.ToUpper())).Select(n => n.Value).Select(n => n).ToArray()[0].Equals(true))
            {
                
                if (!Directory.Exists(@"output\Validation"))
                {
                    Directory.CreateDirectory(@"output\Validation");
                }
                File.Create(exceptionPath).Close();
                File.Create(_exceptionpath).Close();
                File.Create(path).Close();
                File.Create(_successfulCommit).Close();
                var config = Config.Load(_testJson);
                //var col = config.Tables.Select(n => n.Columns);
                foreach (var table in config.Tables)
                {
                    table.Columns.Add(new ColumnConfig() { Name = table.PrimaryKeyColumn, Ignore = false });
                    foreach (var col in table.Columns.Where(n=>!n.Ignore))
                    {
                        if (col.Type == DataType.PhoneNumberInt || col.Type == DataType.RandomInt)
                        {
                            ColumnParameter.Add(new KeyValuePair<string, Dictionary<string, KeyValuePair<string, string>>>(table.Name, new Dictionary<string, KeyValuePair<string, string>>() { { col.Name, new KeyValuePair<string, string>("NUMBER",
                            int.TryParse(col.Max,out int o) ? col.Max: "" )
                            } }));
                        }
                        else if (col.Type == DataType.Blob)
                        {
                            ColumnParameter.Add(new KeyValuePair<string, Dictionary<string, KeyValuePair<string, string>>>(table.Name, new Dictionary<string, KeyValuePair<string, string>>() { { col.Name, new KeyValuePair<string, string>("BLOB",
                            col.Max )
                            } }));
                        }
                        else if (col.Type == DataType.Geometry || col.Type == DataType.ShufflePolygon || col.Type == DataType.Shufflegeometry)
                        {
                            ColumnParameter.Add(new KeyValuePair<string, Dictionary<string, KeyValuePair<string, string>>>(table.Name, new Dictionary<string, KeyValuePair<string, string>>() { { col.Name, new KeyValuePair<string, string>("SDO_GEOMETRY",
                            "" )
                            } }));
                        }
                        else if (col.Type == DataType.DateOfBirth)
                        {
                            ColumnParameter.Add(new KeyValuePair<string, Dictionary<string, KeyValuePair<string, string>>>(table.Name, new Dictionary<string, KeyValuePair<string, string>>() { { col.Name, new KeyValuePair<string, string>("DATE",
                            "" )
                            } }));
                        }
                        else if (col.Type == DataType.RandomDec)
                        {
                            ColumnParameter.Add(new KeyValuePair<string, Dictionary<string, KeyValuePair<string, string>>>(table.Name, new Dictionary<string, KeyValuePair<string, string>>() { { col.Name, new KeyValuePair<string, string>("NUMBER",
                            "" )
                            } }));
                        }
                        else if (col.Type == DataType.Clob)
                        {
                            ColumnParameter.Add(new KeyValuePair<string, Dictionary<string, KeyValuePair<string, string>>>(table.Name, new Dictionary<string, KeyValuePair<string, string>>() { { col.Name, new KeyValuePair<string, string>("CLOB",
                            "" )
                            } }));
                        }
                        else
                            ColumnParameter.Add(new KeyValuePair<string, Dictionary<string, KeyValuePair<string, string>>>(table.Name, new Dictionary<string, KeyValuePair<string, string>>() { { col.Name, new KeyValuePair<string, string>("VARCHAR2",
                            col.Max)
                            } }));

                        //ColumnParameter.Add(new KeyValuePair<string, Dictionary<string, KeyValuePair<string, string>>>(name.Name,new Dictionary<string, KeyValuePair<string, string>>() { {col.Name, new KeyValuePair<string, string>(col.Type) } }))
                    }
                  
                }
                return Config.Load(_testJson);
            }
            else
            {
                File.Create(exceptionPath).Close();
                File.Create(_exceptionpath).Close();
                File.Create(path).Close();
                File.Create(_successfulCommit).Close();
                return Config.Load(Jsonpath);
            }

            //return Config.Load($@"\\SFP.IDIR.BCGOV\U130\SOOKEKE$\Masking_sample\APP_TAP_config.json");
        }
        private static string ReadPassword()
        {
            string secured = "";
            ConsoleKeyInfo keyInfo;
           

            do
            {
                keyInfo = Console.ReadKey(true);
                // Backspace Should Not Work
                if (keyInfo.Key != ConsoleKey.Backspace)
                {
                    secured += keyInfo.KeyChar;
                    Console.Write("*");
                }
                else
                {
                    Console.Write("\b");
                }
            }
            while (keyInfo.Key != ConsoleKey.Enter);
          

           
            return secured;
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
        public static void Run()
        {
                  
            try
            {                           
                if (!allkey.Where(n => n.Key.ToUpper().Equals(RunTestJson.ToUpper())).Select(n => n.Value).Select(n => n).ToArray().First().Equals(true))
                {
                    _SpreadSheetPath = ConfigurationManager.AppSettings[ExcelSheetPath];
                    copyjsonPath = ExcelToJson.ToJson(_SpreadSheetPath); //serialize excel to json array
                    JsonConfig(copyjsonPath, _SpreadSheetPath); // map json to object
                }
                
                Console.Title = string.Format("Data Masker v{0}",MaskerVersion);
                Config config = LoadConfig(1);               
                if (Convert.ToString(config.DataSource.Config.connectionString.ToString()).Contains("{0}"))
                {
                    Console.WriteLine("");
                    Console.WriteLine("Type your database user password for non-prod and press enter");
                    config.DataSource.Config.connectionString = string.Format(config.DataSource.Config.connectionString.ToString(), ReadPassword());
                }
                if (Convert.ToString(config.DataSource.Config.connectionStringPrd.ToString()).Contains("{0}"))
                {
                    Console.WriteLine("");
                    Console.WriteLine("Type your database user password for prod and press enter");
                    Console.WriteLine(Environment.NewLine);
                    config.DataSource.Config.connectionStringPrd = string.Format(config.DataSource.Config.connectionStringPrd.ToString(), ReadPassword());
                }  
                _nameDatabase = config.DataSource.Config.Databasename.ToString();
                if (string.IsNullOrEmpty(_nameDatabase)) { throw new ArgumentException("Database name cannot be null, check app.config and specify the database name", _nameDatabase); }
                IDataMasker dataMasker = new DataMasker(new DataGenerator(config.DataGeneration));
                IDataSource dataSource = DataSourceProvider.Provide(config.DataSource.Type, config.DataSource);

                //check to run validation only
                if (allkey.Where(n => n.Key.ToUpper().Equals(RunValidationONLY.ToUpper())).Select(n => n.Value).Select(n => n).ToArray().First().Equals(true))                  
                {
                    Console.WriteLine("Data Masking Validation check has started. Program will exist after validation......................................");
                    Console.Title = string.Format("Data Masking Validation v{0}", MaskerVersion);
                    MaskValidationCheck.Verification(config.DataSource, config, sheetPath, CreateDir, _nameDatabase, exceptionPath, ColumnMapping, "");
                    Console.WriteLine("Validation completed: Press ENTER to exist..");
                    Console.ReadLine();
                    Environment.Exit(1);
                }
                Stopwatch watch = new Stopwatch();
                watch.Start();
                #region Masking operation and Data generation
                foreach (TableConfig tableConfig in config.Tables)
                {
                    //checked if table contains blob column data type and get column that is blob
                    TableParameter = ColumnParameter.Where(n => n.Key == tableConfig.Name).SelectMany(n => n.Value).ToDictionary(n => n.Key, n => n.Value);
                    var isblob = tableConfig.Columns.Where(x => !x.Ignore && x.Type == DataType.Blob);
                    object FileNameWithExtension = null;
                    string[] extcolumn = null;
                    IEnumerable<IDictionary<string, object>> rows = null;
                    //IDictionary<string, object>[] rowsMasked = null;
                   
                    IEnumerable<IDictionary<string, object>> rawData = null;
                    File.AppendAllText(_exceptionpath, "Recorded exception for " + tableConfig.Name + ".........." + Environment.NewLine + Environment.NewLine);
                    if (config.DataSource.Type == DataSourceType.SpreadSheet)
                    {
                        //load spreadsheet to dataTable
                        Console.WriteLine(string.Format("Data Generation for {0} started....", tableConfig.Name));
                        var SheetTable = dataSource.DataTableFromCsv(config.DataSource.Config.connectionString.ToString(), tableConfig);
                        //convert DataTable to object
                        try
                        {
                            rowCount = SheetTable[tableConfig.Name].Rows.Count;
                            //rows = dataSource.CreateObject(SheetTable[config.Tables.IndexOf(tableConfig)]);
                            rows = dataSource.CreateObject(SheetTable[tableConfig.Name]);
                            rawData = dataSource.RawData(null);
                            Console.Title = string.Format("Data Masking v{0}", MaskerVersion);
                            foreach (IDictionary<string, object> row in rows)
                            {
                               
                                dataMasker.Mask(row, tableConfig, dataSource, rowCount, rawData, SheetTable[tableConfig.Name]);
                            }
                        }
                        catch (Exception ex)
                        {

                            File.WriteAllText(_exceptionpath, tableConfig.Name + " is not found in the SpreadSheet Table" + Environment.NewLine + Environment.NewLine);
                            Console.WriteLine(ex.Message);
                        }
                        try
                        {
                            //convert the object to DataTable
                            _dmlTable = dataSource.SpreadSheetTable(rows, tableConfig); MaskTable = _dmlTable;//masked table
                            //var maskedObj = dataSource.CreateObject(_dmlTable); // masked object
                            PrdTable = dataSource.SpreadSheetTable(rawData, tableConfig); //PRD table


                            //convert to DML
                            #region DML Script

                            if (allkey.Where(n => n.Key.ToUpper().Equals(WriteDML.ToUpper())).Select(n => n.Value).Select(n => n).ToArray()[0].Equals(true))
                            {
                                _dmlTable.TableName = tableConfig.Name;
                                CreateDir = Directory.GetCurrentDirectory() + @"\output\" + _nameDatabase + @"\";
                                if (!Directory.Exists(CreateDir))
                                {
                                    Directory.CreateDirectory(CreateDir);
                                }
                                string writePath = CreateDir + @"\" + tableConfig.Name + ".sql";
                                var insertSQL = SqlDML.GenerateInsert(_dmlTable,null,isBinary, extcolumn, null, null, writePath, config, tableConfig);
                                if (allkey.Where(n => n.Key.ToUpper().Equals(MaskTabletoSpreadsheet.ToUpper())).Select(n => n.Value).Select(n => n).ToArray()[0].Equals(true))
                                {
                                    SqlDML.DataTableToExcelSheet(_dmlTable, CreateDir + @"\" + tableConfig.Name + ".xlsx", tableConfig);
                                }
                            }
                            if (allkey.Where(n => n.Key.ToUpper().Equals(RunValidation.ToUpper())).Select(n => n.Value).Select(n => n).ToArray()[0].Equals(true)
                                && PrdTable.Rows != null && MaskTable.Rows != null
                                && allkey.Where(n => n.Key.ToUpper().Equals(MaskedCopyDatabase.ToUpper())).Select(n => n.Value).Select(n => n).ToArray()[0].Equals(false))
                            {
                                Reportvalidation(PrdTable, _dmlTable, config.DataSource, tableConfig);
                            }
                                #endregion

                            
                        }
                        catch (Exception ex)
                        {
                            //string path = Directory.GetCurrentDirectory() + $@"\Output\MaskedExceptions.txt";
                            File.WriteAllText(_exceptionpath, ex.Message + Environment.NewLine + Environment.NewLine);
                            Console.WriteLine(ex.Message);
                        }
                    }
                    else
                    {
                        try
                        {
                            rowCount = int.TryParse(tableConfig.RowCount, out int rc) ? rc : tableConfig.RowCount.Contains("-") && tableConfig.RowCount.Split('-').Count() == 2? (Convert.ToInt32(tableConfig.RowCount.Split('-')[1]) - Convert.ToInt32(tableConfig.RowCount.Split('-').FirstOrDefault())) + 1 : dataSource.GetCount(tableConfig);
                            if (rowCount > 100000 && !string.IsNullOrEmpty(""))
                            {
                                Console.Title = string.Format("Data Generation v{0}", MaskerVersion);
                                if (config.Tables.IndexOf(tableConfig) != 0)
                                {
                                    Console.WriteLine(Environment.NewLine);
                                } 
                                
                                //? Console.WriteLine(Environment.NewLine) : Console.WriteLine("");

                                int fetch = int.TryParse(ConfigurationManager.AppSettings["Fetch"].ToString(), out int f) ? f : 100000;
                                var batch = Math.Ceiling((double)rowCount / (double)fetch);
                                int offset = 0; //int r = 0;
                                for (int j = 0; j < batch; j++)
                                {
                                    Console.WriteLine(string.Format("Data Generation for {0} has started from {1} to {2}....", tableConfig.Name, offset, fetch + offset));
                                    rows = dataSource.GetData(tableConfig, config, rowCount, fetch, offset).ToArray(); //masked
                                    rawData = dataSource.RawData(null); //unmask
                                    Console.Title = string.Format("Data Masking v{0}", MaskerVersion);
                                    Console.WriteLine(string.Format("Data Masking for {0} has started....", tableConfig.Name));


                                    offset = offset + fetch;
                                }
                            }
                            else
                            {
                                Console.Title = string.Format("Data Generation v{0}", MaskerVersion);
                                //Console.WriteLine(Environment.NewLine);
                                if (config.Tables.IndexOf(tableConfig) != 0)
                                {
                                    Console.WriteLine(Environment.NewLine);
                                }
                                Console.WriteLine(string.Format("Data Generation for {0} has started....", tableConfig.Name));
                                rows = dataSource.GetData(tableConfig, config, rowCount, null, null); //masked
                                rawData = dataSource.RawData(null); //unmask
                                Console.Title = string.Format("Data Masking v{0}", MaskerVersion);
                                Console.WriteLine(string.Format("Data Masking for {0} has started....", tableConfig.Name));
                                for (int i = 0; i < rowCount; i++)
                                {
                                    if (isblob.Count() == 1 && rows.ElementAt(i).Select(n => n.Key).ToArray().Where(x => x.Equals(string.Join("", isblob.Select(n => n.StringFormatPattern)))).Count() > 0)
                                    {
                                        isBinary = isblob.Any();
                                        var blobLocation = @"output\" + _nameDatabase + @"\BinaryFiles\" + tableConfig.Name + @"\";
                                        if (!Directory.Exists(blobLocation)){Directory.CreateDirectory(blobLocation);}
                                        FileNameWithExtension = rows.ElementAt(i)[string.Join("", isblob.Select(n => n.StringFormatPattern))];
                                        string ex = FileNameWithExtension.ToString().Substring(FileNameWithExtension.ToString().LastIndexOf('.') + 1);
                                        dataMasker.MaskBLOB(rows.ElementAt(i), tableConfig, dataSource, tableConfig.Columns.Where(n => n.Type == DataType.Shuffle).Any() ? rawData : null, FileNameWithExtension.ToString(), ToEnum(ex, FileTypes.JPEG), blobLocation);
                                    }
                                    else
                                        dataMasker.Mask(rows.ElementAt(i), tableConfig, dataSource, rowCount, tableConfig.Columns.Where(n => n.Type == DataType.Shuffle || n.Type == DataType.Shufflegeometry || n.Type == DataType.ShufflePolygon).Any() ? rawData : null);
                                }
                            }
                            #region Create DML Script
                            _dmlTable = dataSource.SpreadSheetTable(rows, tableConfig); MaskTable = _dmlTable;//masked table      
                            #region preview
                            if (tableConfig.Columns.Where(n => !n.Ignore && n.Preview == true).Any())
                            {
                                List<string> coln = new List<string>
                                {
                                    tableConfig.PrimaryKeyColumn
                                };
                                string[] co = tableConfig.Columns.Where(n => !n.Ignore && n.Preview == true).Select(n => n.Name).Take(6).ToArray();
                                coln.AddRange(co);
                                Console.WriteLine(Environment.NewLine);
                                int.TryParse(ConfigurationManager.AppSettings["PreviewCount"].ToString(), out int previewCount);
                                Console.WriteLine("{0}: Top {1} masked data Preview for table {2}", tableConfig.Name, previewCount, tableConfig.Name);
                                MaskTable.Print(previewCount, coln.ToArray());
                            }
                            #endregion//var maskedObj = dataSource.CreateObject(_dmlTable); // masked object


                            PrdTable = dataSource.SpreadSheetTable(rawData, tableConfig); //PRD table
                            if (allkey.Where(n => n.Key.ToUpper().Equals(WriteDML.ToUpper())).Select(n => n.Value).Select(n => n).ToArray()[0].Equals(true))
                            {
                                Console.WriteLine(Environment.NewLine);
                                Console.WriteLine(string.Format("Generating DML and SpreadSheet for {0}...", tableConfig.Name));
                                _dmlTable.TableName = tableConfig.Name;
                                CreateDir = Directory.GetCurrentDirectory() + @"\output\" + _nameDatabase + @"\";
                                if (!Directory.Exists(CreateDir))
                                {
                                    Directory.CreateDirectory(CreateDir);
                                }
                                string writePath = CreateDir + @"\" + tableConfig.Name + ".sql";
                                var insertSQL = SqlDML.GenerateInsert(_dmlTable, PrdTable, isBinary, extcolumn, null, null, writePath, config, tableConfig);
                                if (!string.IsNullOrEmpty(insertSQL)) { File.AppendAllText(_exceptionpath, insertSQL + Environment.NewLine); }
                                if (allkey.Where(n => n.Key.ToUpper().Equals(MaskTabletoSpreadsheet.ToUpper())).Select(n => n.Value).Select(n => n).ToArray()[0].Equals(true))
                                {
                                    SqlDML.DataTableToExcelSheet(_dmlTable, CreateDir + @"\" + tableConfig.Name + ".xlsx", tableConfig);
                                }
                            }
                            if (allkey.Where(n => n.Key.ToUpper().Equals(RunValidation.ToUpper())).Select(n => n.Value).Select(n => n).ToArray()[0].Equals(true)
                                && PrdTable.Rows != null && MaskTable.Rows != null
                                && allkey.Where(n => n.Key.ToUpper().Equals(MaskedCopyDatabase.ToUpper())).Select(n => n.Value).Select(n => n).ToArray()[0].Equals(false))
                            {
                                Console.WriteLine(string.Format("Running Validation for {0}....", tableConfig.Name));
                                Reportvalidation(PrdTable, _dmlTable, config.DataSource, tableConfig);
                            }
                            #endregion
                            if (allkey.Where(n => n.Key.ToUpper().Equals(MaskedCopyDatabase.ToUpper())).Select(n => n.Value).Select(n => n).ToArray().First().Equals(true))
                            {                               
                                if (tableConfig.Columns.Where(n => !n.Ignore).Any())
                                {
                                    Console.WriteLine("writing table " + tableConfig.Name + " on database " + _nameDatabase + "" + " .....");
                                    isRollback = dataSource.UpdateRows(rows, rowCount, tableConfig, config, TableParameter);
                                }
                            }
                            if (allkey.Where(n => n.Key.ToUpper().Equals(RunValidation.ToUpper())).Select(n => n.Value).Select(n => n).ToArray().First().Equals(true)
                                && allkey.Where(n => n.Key.ToUpper().Equals(MaskedCopyDatabase.ToUpper())).Select(n => n.Value).Select(n => n).ToArray().First().Equals(true)
                                && PrdTable.Rows != null && MaskTable.Rows != null
                                && !isRollback
                                && allkey.Where(n => n.Key.ToUpper().Equals(EmailValidation.ToUpper())).Select(n => n.Value).Select(n => n).ToArray().First().Equals(true))
                            {
                                Console.Title = string.Format("Data Masking Validation v{0}", MaskerVersion);
                                Console.WriteLine(string.Format("Running Validation for {0}....", tableConfig.Name));
                                Reportvalidation(PrdTable, _dmlTable, config.DataSource, tableConfig);
                            }

                        }
                        catch (Exception ex)
                        {
                            File.AppendAllText(exceptionPath, $"Recorded exception on table {tableConfig.Name}: " + ex.Message + Environment.NewLine + Environment.NewLine);
                            Console.WriteLine(ex.ToString());
                        }
                    }
                    isBinary = false;
                }
                #endregion
                //write mapped table and column with type in csv file
                if (!allkey.Where(n => n.Key.ToUpper().Equals(RunTestJson.ToUpper())).Select(n => n.Value).Select(n => n).ToArray().First().Equals(true))
                {
                    var o = OutputSheet(config, copyjsonPath, _nameDatabase);
                }

                if (report.Rows.Count != 0
                    && allkey.Where(n => n.Key.ToUpper().Equals(EmailValidation.ToUpper())).Select(n => n.Value).Select(n => n).ToArray().First().Equals(true))
                {
                    watch.Stop();
                    TimeSpan timeSpan = watch.Elapsed;
                    var timeElapse = string.Format("{0}h {1}m {2}s", timeSpan.Hours, timeSpan.Minutes, timeSpan.Seconds);
                    MaskValidationCheck.Analysis(report, config.DataSource, sheetPath, _nameDatabase, CreateDir, exceptionPath, ColumnMapping, timeElapse);
                }

                #region validate masking 
                //if (allkey.Where(n => n.Key.ToUpper().Equals(RunValidation.ToUpper())).Select(n => n.Value).Select(n => n).ToArray().First().Equals(true)
                //    && allkey.Where(n => n.Key.ToUpper().Equals(MaskedCopyDatabase.ToUpper())).Select(n => n.Value).Select(n => n).ToArray().First().Equals(true)
                //    && allkey.Where(n => n.Key.ToUpper().Equals(EmailValidation.ToUpper())).Select(n => n.Value).Select(n => n).ToArray().First().Equals(true))
                //{
                //    Console.WriteLine("Data Masking Validation has started......................................");
                //    Console.Title = string.Format("Data Masking Validation v{0}",MaskerVersion);
                //    watch.Stop();

                //    TimeSpan timeSpan = watch.Elapsed;
                //    var timeElapse = string.Format("{0}h {1}m {2}s", timeSpan.Hours, timeSpan.Minutes, timeSpan.Seconds);

                //    MaskValidationCheck.Verification(config.DataSource, config, sheetPath, CreateDir, _nameDatabase, exceptionPath, _columnMapping, timeElapse);
                //}
                #endregion
            }
            catch (Exception e)
            {
                File.WriteAllText(_exceptionpath, e.Message + Environment.NewLine + Environment.NewLine);
                Console.WriteLine(e.Message);
                Console.WriteLine("Program will exit: Press ENTER to exist..");
                Console.ReadLine();
            }
          


        }
        public static bool OutputSheet(
            Config config, 
            string json, string _appname)
        {
            DataTable dt = new DataTable();
            dt.Columns.Add("TABLE_NAME");
            dt.Columns.Add("COLUMN_NAME");
            dt.Columns.Add("MASKING RULE");
            dt.Columns.Add("MASKING RULE APPLIED");

            dt.Columns.Add("IGNORE");
            dt.Columns.Add("Format Pattern");
            dt.Columns.Add("Min - Max");
            RootObject rootObject = new RootObject();
            var rootObj = JsonConvert.DeserializeObject<List<RootObject>>(File.ReadAllText(json));
            rootObj.RemoveAll(n=>n.ColumnName == n.PKconstraintName.Split(',')[0]);
            int h = 0;
            int k = 0;
            for (int i = 0; i < config.Tables.Count; i++)
            {


                for (int j = 0; j < config.Tables[i].Columns.Count; j++)
                {



                    var minMax = ToString(config.Tables[i].Columns[j].Min) + " - " + Convert.ToString(config.Tables[i].Columns[j].Max);
                    dt.Rows.Add(config.Tables[i].Name, config.Tables[i].Columns[j].Name, rootObj[j + h].MaskingRule, config.Tables[i].Columns[j].Type, config.Tables[i].Columns[j].Ignore, config.Tables[i].Columns[j].StringFormatPattern, minMax);

                    k++;
                }
                h = k;

            }

            if (dt.Rows != null)
            {
                var csv = WriteTofile(dt, _appname, "_COLUMN_MAPPING");
                var createsheet = ToExcel(csv, _appname, _appname, "_COLUMN_MAPPING");
                if (createsheet == false)
                {
                    
                    Console.WriteLine("cannot create excel file");
                    return false;
                }
            }
            return true;
        }
        private static string ToString(object value)
        {
            if (null == value)
                return "Null";

            try
            {
                return Convert.ToString(value);
            }
            catch (Exception)
            {

                return "Null";
            }

        }
        private static string WriteTofile(DataTable textTable, string directory, string uniquekey)
        {
            StringBuilder fileContent = new StringBuilder();
            //int i = 0;
            if (!Directory.Exists(directory))
            {
                Directory.CreateDirectory(directory);
            }
            if (textTable.Columns.Count == 0)
            {
                return "";
            }
            foreach (var col in textTable.Columns)
            {
                
                fileContent.Append(col.ToString() + ",");
            }

            fileContent.Replace(",", Environment.NewLine, fileContent.Length - 1, 1);

            foreach (DataRow dr in textTable.Rows)
            {
               
                foreach (var column in dr.ItemArray)
                {
                    fileContent.Append("\"" + column.ToString() + "\",");
                }

                fileContent.Replace(",", System.Environment.NewLine, fileContent.Length - 1, 1);
            }

            System.IO.File.WriteAllText(directory + @"\" + directory + uniquekey + ".csv", fileContent.ToString());
            if (File.Exists(directory + @"\" + directory + uniquekey + ".csv"))
            {
                return directory + @"\" + directory + uniquekey + ".csv";
            }
            else
                return "";
            
        }

        private static bool ToExcel(
            string csvFileName, 
            string _appName, 
            string directory, 
            string uniqueKey)
        {
            string worksheetsName = _appName;
            string excelFileName = directory + @"\" + _appName + uniqueKey + ".xlsx";
            if (File.Exists(excelFileName))
            {
                File.Delete(excelFileName);
            }
            bool firstRowIsHeader = true;
            try
            {

                var format = new ExcelTextFormat
                {
                    Delimiter = ',',
                    EOL = "\r\n",
                    // DEFAULT IS "\r\n";

                    TextQualifier = '"'
                };
                System.Text.Encoding.RegisterProvider(System.Text.CodePagesEncodingProvider.Instance);
                using (ExcelPackage package = new ExcelPackage(new FileInfo(excelFileName)))
                {
                    ExcelWorksheet worksheet = package.Workbook.Worksheets.Add(worksheetsName);
                    worksheet.Cells["A1"].LoadFromText(new FileInfo(csvFileName), format, OfficeOpenXml.Table.TableStyles.Medium28, firstRowIsHeader);

                    package.SaveAs(new FileInfo(excelFileName));
                }

            }
            catch (Exception ex)
            {

                Console.WriteLine(ex.Message);
            }
            if (File.Exists(excelFileName))
            {
                ColumnMapping = excelFileName; 
                return true;
            }
            return false;
        }

        private static void GenerateSchema()
        {
            JSchemaGenerator generator = new JSchemaGenerator
            {
                ContractResolver = new CamelCasePropertyNamesContractResolver()
            };
            JSchema schema = generator.Generate(typeof(Config));
            generator.GenerationProviders.Add(new StringEnumGenerationProvider());
            schema.Title = "DataMasker.Config";
            StringWriter writer = new StringWriter();
            JsonTextWriter jsonTextWriter = new JsonTextWriter(writer);
            schema.WriteTo(jsonTextWriter);
            dynamic parsedJson = JsonConvert.DeserializeObject(writer.ToString());
            dynamic prettyString = JsonConvert.SerializeObject(parsedJson, Formatting.Indented);
            StreamWriter fileWriter = new StreamWriter("DataMasker.Config.schema.json");
            fileWriter.WriteLine(schema.Title);
            fileWriter.WriteLine(new string('-', schema.Title.Length));
            fileWriter.WriteLine(prettyString);
            fileWriter.Close();
        }

        public  static bool CheckAppConfig()
        {
            bool flag = false; allkey.Clear();
            //string valid = EmailValidation;
            List<string> allKeys = new List<string>();

            foreach (string key in ConfigurationManager.AppSettings.AllKeys)
            {
                switch (key)
                {
                    case nameof(AppConfig.APP_NAME):
                        allkey.Add(key, ConfigurationManager.AppSettings[key].ToString());
                        break;
                    case nameof(AppConfig.ConnectionString):
                        allkey.Add(key, ConfigurationManager.AppSettings[key].ToString());
                        break;
                    case nameof(AppConfig.CurrentInstallerURL):
                        allkey.Add(key, ConfigurationManager.AppSettings[key].ToString());
                        break;
                    case nameof(AppConfig.CurrentVersionURL):
                        allkey.Add(key, ConfigurationManager.AppSettings[key].ToString());
                        break;
                    case nameof(AppConfig.ConnectionStringPrd):
                        allkey.Add(key, ConfigurationManager.AppSettings[key].ToString());
                        break;
                    case nameof(AppConfig.DatabaseName):
                        allkey.Add(key, ConfigurationManager.AppSettings[key].ToString());
                        break;
                    case nameof(AppConfig.DataSourceType):
                        allkey.Add(key, ConfigurationManager.AppSettings[key].ToString());
                        break;
                    case nameof(AppConfig.ExcelSheetPath):
                        allkey.Add(key, ConfigurationManager.AppSettings[key].ToString());
                        break;
                    case nameof(AppConfig.Hostname):
                        allkey.Add(key, ConfigurationManager.AppSettings[key].ToString());
                        break;
                    case nameof(AppConfig.TestJson):
                        allkey.Add(key, ConfigurationManager.AppSettings[key].ToString());
                        break;
                    case nameof(AppConfig.WriteDML):
                        bool b = ConfigurationManager.AppSettings[key].ToString().ToUpper().Equals("YES") ? true : false;
                        allkey.Add(key, b);
                        break;
                    case nameof(AppConfig.MaskedCopyDatabase):
                        bool o = ConfigurationManager.AppSettings[key].ToString().ToUpper().Equals("YES") ? true : false;
                        allkey.Add(key, o);
                        break;
                    case nameof(AppConfig.RunValidationONLY):
                        bool vo = ConfigurationManager.AppSettings[key].ToString().ToUpper().Equals("YES") ? true : false;
                        allkey.Add(key, vo);
                        break;
                    case nameof(AppConfig.RunValidation):
                        bool v = ConfigurationManager.AppSettings[key].ToString().ToUpper().Equals("YES") ? true : false;
                        allkey.Add(key, v);
                        break;
                    case nameof(AppConfig.MaskTabletoSpreadsheet):
                        bool m = ConfigurationManager.AppSettings[key].ToString().ToUpper().Equals("YES") ? true : false;
                        allkey.Add(key, m);
                        break;
                    case nameof(AppConfig.EmailValidation):
                        bool e = ConfigurationManager.AppSettings[key].ToString().ToUpper().Equals("YES") ? true : false;
                        allkey.Add(key, e);
                        break;             
                    case nameof(AppConfig.RunTestJson):
                        bool tj = ConfigurationManager.AppSettings[key].ToString().ToUpper().Equals("YES") ? true : false;
                        allkey.Add(key, tj);
                        break;
                    case nameof(AppConfig.AutoUpdate):
                        bool auto = ConfigurationManager.AppSettings[key].ToString().ToUpper().Equals("YES") ? true : false;
                        allkey.Add(key, auto);
                        break;
                    default:
                        break;
                }
               
            }
            var missingKey = Enum.GetNames(typeof(AppConfig)).ToList().Except(allkey.Keys.ToList());
            if (allkey.Values.Where(n=>n.Equals(string.Empty)).Count() != 0)
            {
                //var xxx = allkey.Values.Where(n => n.Equals(string.Empty));
                Console.WriteLine("Referencing a null app key value: Mandatory app key value is not set in the App.config" + Environment.NewLine);
                Console.WriteLine(string.Join(Environment.NewLine, allkey.Where(n => n.Value.ToString() == string.Empty).Select(n => n.Key + " : " + n.Value + "Null").ToArray()));
                Console.Title = "Referencing a Null key";
                flag = false;
            }
            else
                flag = true;

            //check email validation and recipient
            if (allkey.Where(n => n.Key.ToUpper().Equals(EmailValidation.ToUpper())).Select(n => n.Value).Select(n => n).ToArray().FirstOrDefault().Equals(true))
            {
                if (string.IsNullOrEmpty(fromEmail)
                    || string.IsNullOrEmpty(Recipients))
                {
                    Console.WriteLine("Sending validation email requires fromEmail AND RecipientEmail address to be set in the app.config");
                    return false;
                }
            }
            if (missingKey.Count() != 0)
            {
                Console.WriteLine("Missing App Key in the App.config file" + Environment.NewLine);
                Console.WriteLine("Keys: " + string.Join(" ", missingKey.ToArray()));
                Console.Title = "Referencing a Null key";
                flag = false;
            }
            return flag;
        }
        public enum AppConfig
        {
            ExcelSheetPath,
            DatabaseName,
            WriteDML,
            MaskTabletoSpreadsheet,
            DataSourceType,
            APP_NAME,
            ConnectionString,
            ConnectionStringPrd,
            MaskedCopyDatabase,
            RunValidation,
            RunValidationONLY,
            Hostname,
            TestJson,
            RunTestJson,
            EmailValidation,
            AutoUpdate,
            CurrentVersionURL,
            CurrentInstallerURL
        }
        private static void UpdateProgress(
            ProgressType progressType,
            int current,
            int? max = null,
            string message = null)
        {
            //if (cliOptions.NoOutput)
            //{
            //    return;
            //}

            max = max ??
                  _progressBars[progressType]
                     .ProgressBar.Max;

            _progressBars[progressType]
               .ProgressBar.Max = max.Value;

            message = message ??
                      _progressBars[progressType]
                         .LastMessage;

            _progressBars[progressType]
               .ProgressBar.Refresh(current, message);
        }
        private static void Reportvalidation(DataTable _prdTable, DataTable _maskedTable, DataSourceConfig dataSourceConfig, TableConfig tableConfig)
        {
            CompareLogic compareLogic = new CompareLogic();
            if (!Directory.Exists($@"output\Validation"))
            {
                Directory.CreateDirectory($@"output\Validation");
            }           
            var _columndatamask = new List<object>();
            var _columndataUnmask = new List<object>();
            //string Hostname = dataSourceConfig.Config.Hostname;
            string schema = tableConfig.Schema;
#pragma warning disable CA1416 // Validate platform compatibility
            string _operator = System.DirectoryServices.AccountManagement.UserPrincipal.Current.DisplayName;
#pragma warning restore CA1416 // Validate platform compatibility

            var result = "";
            var failure = "";

            if (_prdTable.Columns.Count == 0  && tableConfig.Columns.Count() != 0)
            {
                foreach (var col in tableConfig.Columns)
                {
                    _prdTable.Columns.Add(col.Name);
                    
                }
            }
            if (_maskedTable.Columns.Count == 0 && tableConfig.Columns.Count() != 0)
            {
                foreach (var col in tableConfig.Columns)
                {
                    
                    _maskedTable.Columns.Add(col.Name);
                }
            }
            foreach (ColumnConfig dataColumn in tableConfig.Columns)
            {
              
                _columndatamask = new DataView(_maskedTable).ToTable(false, new string[] { dataColumn.Name }).AsEnumerable().Select(n => n[0]).ToList();
                _columndataUnmask = new DataView(_prdTable).ToTable(false, new string[] { dataColumn.Name }).AsEnumerable().Select(n => n[0]).ToList();


                //check for intersect
                List<string> check = new List<string>();
                int rownumber = 0;
                if (_columndatamask.Count == 0)
                {
                    check.Add("PASS");
                }

                for (int i = 0; i < _columndatamask.Count; i++)
                {
                    rownumber = i;


                    if (!_columndatamask[i].IsNullOrDbNull() && !_columndataUnmask[i].IsNullOrDbNull() && !string.IsNullOrWhiteSpace(_columndataUnmask[i].ToString()))
                    {
                        try
                        {
                            if (compareLogic.Compare(_columndatamask[i], _columndataUnmask[i]).AreEqual && dataColumn.Ignore != true)
                            {
                                check.Add("FAIL");
                                                                //match
                            }
                            else
                            {
                                check.Add("PASS");
                            }
                        }
                        catch (IndexOutOfRangeException es)
                        {
                            Console.WriteLine(es.Message);
                        }
                        catch (Exception ex)
                        {

                            Console.WriteLine(ex.Message);
                        }



                    }







                    //unmatch
                }



                if (check.Contains("FAIL"))
                {
                    result = "<font color='red'>FAIL</font>";

                    if (dataColumn.Ignore == true)
                    {
                        failure = "Masking not required";
                        result = "<font color='green'>PASS</font>";
                    }
                    else if (dataColumn.Type == DataType.Shuffle && _columndatamask.Count() == 1)
                    {
                        result = "<b><font color='red'>FAIL</font></b>";
                        failure = "row count must be > 1 for " + DataType.Shuffle.ToString();
                        //result = "<font color='red'>FAIL</font>";
                    }
                    else if (dataColumn.Type == DataType.NoMasking)
                    {
                        result = "<font color='green'>PASS</font>";
                        //result = "<b><font color='blue'>PASS</font></b>";
                        failure = "Masking not required";
                        //result = "<font color='red'>FAIL</font>";
                    }
                    else if (dataColumn.Type == DataType.Shuffle)
                    {
                        result = "<b><font color='blue'>FAIL</font></b>";
                        failure = "Cannot generate a unique shuffle value";
                        //result = "<font color='red'>FAIL</font>";
                    }
                    else if (dataColumn.Type == DataType.exception && check.Contains("PASS"))
                    {
                        failure = "<font color='red'>Applied mask with " + dataColumn.Type.ToString() + "</ font >";
                        result = "<b><font color='blue'>PASS</font></b>";
                    }
                    else if (dataColumn.Type == DataType.City && dataColumn.Ignore == false)
                    {
                        failure = "Same City found but different state or province";
                        result = "<b><font color='blue'>PASS</font></b>";
                    }
                    else
                    {
                        result = "<font color='red'>FAIL</font>";
                        failure = "<b><font color='red'>Found exact match " + dataColumn.Type.ToString() + " </font></b>";
                    }

                    Console.WriteLine(tableConfig.Name + " Failed Validation test on column " + dataColumn.Name + Environment.NewLine);
                    File.AppendAllText(path, tableConfig.Name + " Failed Validation test on column " + dataColumn.Name + Environment.NewLine);

                    report.Rows.Add(tableConfig.Name, tableConfig.Schema, dataColumn.Name, dataSourceConfig.Config.Hostname, dataSourceConfig.Type, DateTime.Now.ToString(), _operator, _columndatamask.Count, _columndataUnmask.Count, result, failure);

                }
                else if (check.Contains("IGNORE"))
                {
                    result = "No Validation";
                    failure = "Column not mask";
                    report.Rows.Add(tableConfig.Name, tableConfig.Schema, dataColumn.Name, dataSourceConfig.Config.Hostname, dataSourceConfig.Type, DateTime.Now.ToString(), _operator, _columndatamask.Count, _columndataUnmask.Count, result, failure);
                }
                else
                {
                    if (_columndatamask.Count == 0)
                    {
                        failure = "No record found";
                    }
                   else if (dataColumn.Ignore == true || dataColumn.Type == DataType.NoMasking)
                    {
                        failure = "Masking not required";
                        result = "<font color='green'>PASS</font>";
                    }
                    else
                        failure = "NULL";
                    result = "<font color='green'>PASS</font>";
                    Console.WriteLine(tableConfig.Name + " Pass Validation test on column " + dataColumn.Name);
                    File.AppendAllText(path, tableConfig.Name + " Pass Validation test on column " + dataColumn.Name + Environment.NewLine);
                    report.Rows.Add(tableConfig.Name, tableConfig.Schema, dataColumn.Name, dataSourceConfig.Config.Hostname, dataSourceConfig.Type, DateTime.Now.ToString(), _operator, _columndatamask.Count, _columndataUnmask.Count, result, failure);


                }

            }
            
            //return report.datar
        }
        private static DataTable DictionariesToDataTable<T>(
        IEnumerable<IDictionary<string, T>> source)
        {
            if (source == null)
            {
                return null;
            }

            var result = new DataTable();
            using (var e = source.GetEnumerator())
            {
                if (!e.MoveNext())
                {
                    return result;
                }

                if (e.Current.Keys.Count == 0)
                {
                    throw new InvalidOperationException();
                }

                var length = e.Current.Keys.Count;

                result.Columns.AddRange(
                    e.Current.Keys.Select(k => new DataColumn(k, typeof(T))).ToArray());

                do
                {
                    if (e.Current.Values.Count != length)
                    {
                        throw new InvalidOperationException();
                    }

                    result.Rows.Add(e.Current.Values);
                }
                while (e.MoveNext());

                return result;
            }
        }
        private static DataTable ConvertToDataTable(IEnumerable<IDictionary<string, object>> dict)
        {
            DataTable dt = new DataTable();

            // Add columns first
            dt.Columns.AddRange(dict.First()
                                       .Select(kvp => new DataColumn() { ColumnName = kvp.Key, DataType = System.Type.GetType("System.String") })
                                       .AsEnumerable()
                                       .ToArray()
                                       );

            // Now add the rows
            dict.SelectMany(Dict => Dict.Select(kvp => new {
                Row = dt.NewRow(),
                Kvp = kvp
            }))
                  .ToList()
                  .ForEach(rowItem => {
                      rowItem.Row[rowItem.Kvp.Key] = rowItem.Kvp.Value;
                      dt.Rows.Add(rowItem.Row);
                  }
                         );
            dt.Dump();
            return dt;
        }
        private static DataTable ToDictionary(IEnumerable<IDictionary<string, object>> list)
        {
            DataTable result = new DataTable();
            if (list.Count() == 0)
                return result;

           

            foreach (IDictionary<string, object> row in list)
            {
                foreach (KeyValuePair<string, object> entry in row)
                {
                    if (!result.Columns.Contains(entry.Key.ToString()))
                    {
                        result.Columns.Add(entry.Key);
                    }
                }
                result.Rows.Add(row.Values.ToArray());
            }

            return result;
        }
    }
    public class DictComparer : IEqualityComparer<Dictionary<string, object>>
    {
        public bool Equals(Dictionary<string, object> x, Dictionary<string, object> y)
        {
            return (x == y) || (x.Count == y.Count && !x.Except(y).Any());
        }

        public int GetHashCode(Dictionary<string, object> x)
        {
            return x.GetHashCode();
        }
    }
    public class OutputCapture : TextWriter, IDisposable
    {
        private TextWriter stdOutWriter;
        public TextWriter Captured { get; private set; }
        public override Encoding Encoding { get { return Encoding.ASCII; } }

        public OutputCapture()
        {
            this.stdOutWriter = Console.Out;
            Console.SetOut(this);
            Captured = new StringWriter();
        }

        override public void Write(string output)
        {
            // Capture the output and also send it to StdOut
            Captured.Write(output);
            stdOutWriter.Write(output);
        }

        override public void WriteLine(string output)
        {
            // Capture the output and also send it to StdOut
            Captured.WriteLine(output);
            stdOutWriter.WriteLine(output);
        }
    }
}
