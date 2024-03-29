﻿using Alba.CsConsoleFormat.Fluent;
//using DocumentFormat.OpenXml.Spreadsheet;
using ExcelDataReader;
using MySql.Data.MySqlClient;
using Npgsql;
using OfficeOpenXml;
using Oracle.ManagedDataAccess.Client;
using SharpCompress.Archives;
using SharpCompress.Archives.Rar;
using SharpCompress.Common;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Mail;
using System.Reflection;
using System.Security.Principal;
using System.Text;

namespace DataClassification
{
    class Getbulkdata
    {
        private static readonly string exceptionPath = Directory.GetCurrentDirectory() + @"\output\" + @"Exception.txt";
        private static readonly Dictionary<string, object> allkey = new Dictionary<string, object>();
        private static string connectionString;
        private static DataSourceType datasource;
        private const string AutoUpdate = "AutoUpdate"; private const string CurrentVersionURL = "CurrentVersionURL"; private const string CurrentInstallerURL = "CurrentInstallerURL";
        private static readonly DateTime dateTime = new DateTime(1990, 1, 1, 0, 0, 0, 0);
        private static readonly DateTime DEFAULT_MIN_DATE = dateTime;
        //private static readonly string _exceptionpath = Directory.GetCurrentDirectory() + ConfigurationManager.AppSettings["_exceptionpath"];
        private static readonly DateTime DEFAULT_MAX_DATE = DateTime.Now;
        private static string fromEmail;
        private static string Cc;
        private static string Recipients;
        private static string appServer;
        private static string TARGETSCHEMA;
        private static string DatabaseName;
        private static string csvOutputfilename;
        private static bool sendEmail;
        private static DataTable data;
        private static DataSet result;
        public static string DownloadMSI { get; private set; }
        public static string InstallMsiPath { get; private set; }
        public static string MaskerVersion { get; private set; }

        //private static DataTable dataTable;
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
        public static bool CheckUpdate(string urlVersion, string urlFile)
        {
            try
            {
                WebClient Client = new WebClient
                {
                    UseDefaultCredentials = true
                };
                var d = Client.DownloadString(urlVersion).Split('/');
                string versionString = d.FirstOrDefault();
               // string content = d.Count() > 1 ? d[1] : "";
                Version latestVersion = new Version(versionString);
                //Console.WriteLine(content);

                //get my own version to compare against latest.
                Assembly assembly = Assembly.GetExecutingAssembly();
                FileVersionInfo fvi = FileVersionInfo.GetVersionInfo(assembly.Location);
                Version myVersion = new Version(fvi.ProductVersion);
                Client.Dispose();
                if (latestVersion > myVersion)
                {
                    //Console.WriteLine(content);
                    Console.WriteLine(string.Format("You've got version {0} of Schema Tool for Windows. Would you like to update to the latest version {1}[Yes/No]?", myVersion, latestVersion));
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
                Clients.DownloadFile(urlPath, path + @"\Schema.rar");
                if (File.Exists(path + @"\Schema.rar"))
                {
                    Clients.Dispose();
                    DownloadMSI = path + @"\Schema.rar";
                    return true;
                }
            }
            catch (Exception)
            {

                throw;
            }

            return false;
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
                if (File.Exists(destination + @"\Schema\Schema.msi"))
                {
                    InstallMsiPath = destination + @"\Schema\Schema.msi";
                    return true;
                }
            }
            catch (Exception ex)
            {

                Console.WriteLine(ex.Message);
            }
            return false;
        }
        public static bool IsAdministrator()
        {
            using (WindowsIdentity identity = WindowsIdentity.GetCurrent())
            {
                WindowsPrincipal principal = new WindowsPrincipal(identity);
                return principal.IsInRole(WindowsBuiltInRole.Administrator);
            }
        }
        public static bool Install(string sMSIPath)
        {
            try
            {
                Console.WriteLine("Installing latest version...");
                string configfile = @"C:\Program Files\IMB\Schema\DBInfoSchema.exe.config";               
                File.Copy(configfile, Path.Combine(Path.GetDirectoryName(configfile)
                    , Path.GetFileName(configfile) + ".bak"),true);
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
        static void Main(string[] args)
        {
            Assembly assembly = Assembly.GetExecutingAssembly();
            FileVersionInfo fvi = FileVersionInfo.GetVersionInfo(assembly.Location);
            Version myVersion = new Version(fvi.ProductVersion);
            MaskerVersion = myVersion.ToString();
            Console.Title = string.Format("Data Classification Generator v{0}", MaskerVersion);
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
            #region set config
            try
            {
                // connectionString = ConfigurationManager.AppSettings["connectionString"];

                if (Convert.ToString(ConfigurationManager.AppSettings["connectionString"].ToString()).Contains("{0}"))
                {
                    //Console.WriteLine("");
                    Console.WriteLine("Type the database user password and press enter...");

                    connectionString = string.Format(ConfigurationManager.AppSettings["connectionString"].ToString(), ReadPassword());
                    Console.WriteLine(Environment.NewLine);
                }
                else
                    connectionString = ConfigurationManager.AppSettings["connectionString"].ToString();
                datasource = ToEnum(ConfigurationManager.AppSettings["DataSourceType"], DataSourceType.None);
                csvOutputfilename = ConfigurationManager.AppSettings["OutputFilename"];
                fromEmail = ConfigurationManager.AppSettings["fromEmail"];
                Recipients = ConfigurationManager.AppSettings["Recipients"];
                Cc = ConfigurationManager.AppSettings["cCEmail"];
                appServer = ConfigurationManager.AppSettings["appServer"];
                TARGETSCHEMA  = ConfigurationManager.AppSettings["TARGETSCHEMA"];
                DatabaseName = ConfigurationManager.AppSettings["DatabaseName"];
                sendEmail = ConfigurationManager.AppSettings["SendEmail"].ToString().ToUpper().Equals("YES") ? true : false;
            }
            catch (Exception con)
            {

                Console.WriteLine("Exception occurs in app.config {0}", con.Message);
                Console.WriteLine("Program will exit: Press ENTER to exist..");
                Console.ReadLine();

                File.WriteAllText(exceptionPath, con.Message + Environment.NewLine + Environment.NewLine);
                System.Environment.Exit(1);
            }
            //check app.config
            if (!CheckAppConfig())
            {
                Console.WriteLine("Program will exit: Press ENTER to exist..");
                Console.ReadLine();
                System.Environment.Exit(1);
            }
            #endregion 
            if ((bool)allkey.Where(n => n.Key.ToUpper().Equals(AutoUpdate.ToUpper())).Select(n => n.Value).FirstOrDefault())
            {
                CheckUpdate((string)allkey.Where(n => n.Key.ToUpper().Equals(CurrentVersionURL.ToUpper())).Select(n => n.Value).FirstOrDefault(),
                    (string)allkey.Where(n => n.Key.ToUpper().Equals(CurrentInstallerURL.ToUpper())).Select(n => n.Value).FirstOrDefault());
            }
            #region schema Gen
            //Console.Title = "Schema Information Generation";
            //var t = ExcelToTable(@"C:\Users\sookeke\Downloads\Litigation Management System - 20190910 Export (3).xlsx");
            if (!Directory.Exists(@"output"))
            {
                Directory.CreateDirectory(@"output");
            }
            File.Create(exceptionPath).Close();
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

            Console.WriteLine("Data Classification has Started....");
            if (datasource == DataSourceType.SpreadSheet)
            {
                data = new DataTable();
                DataColumn[] dc = new DataColumn[] {
                  new DataColumn("TABLE_NAME", typeof(string)),
                   new DataColumn("SCHEMA", typeof(string)),
                    new DataColumn("COLUMN_NAME", typeof(string)),
                      new DataColumn("DATA_TYPE", typeof(string)),
                        new DataColumn("Nullable", typeof(string)),
                          new DataColumn("COLUMN_ID", typeof(string)),
                            new DataColumn("COMMENTS", typeof(string)),
                              new DataColumn("PKconstraintName", typeof(string)),
                new DataColumn("Min", typeof(string)),
                new DataColumn("max", typeof(string)),
                new DataColumn("Retain NULL",typeof(string)),
                new DataColumn("RetainEmptyString",typeof(string)),
                new DataColumn("Public", typeof(string)),
                new DataColumn("Personal", typeof(string)),
                new DataColumn("Sensitive",typeof(string)),
                new DataColumn("Masking Rule", typeof(string)),
                 new DataColumn("Rule set by", typeof(string)),
                new DataColumn("Rule Reasoning",typeof(string)),
                new DataColumn("COMPLETED", typeof(string)),
                 new DataColumn("StringFormat", typeof(string)),
                 new DataColumn("UseValue", typeof(string)),
                  new DataColumn("Preview", typeof(string))
                };
                data.Columns.AddRange(dc);
                Random random = new Random();
                List<string> rantbool = new List<string>() { "Yes", "No" };
                List<string> isNull = new List<string>() { "TRUE" };
                List<string> completed = new List<string>() { "Completed", "Uncompleted" };
                List<string> MaskingRules = new List<string>() { "No Masking required", "Replace Value with fake data", "Shuffle" };
                var tables = ExcelToTable(connectionString);
                foreach (DataTable item in tables)
                {

                    foreach (DataColumn c in item.Columns)
                    {
                        var min = "";
                        var max = "";


                        if (c.DataType == typeof(string))
                        {
                            var o = item.AsEnumerable().Select(al => al.Field<string>(c.ColumnName)).Distinct().ToList().OrderByDescending(s => (s == null ? 0 : s.Length));
                            if (o.FirstOrDefault() == null)
                            {
                                max = "0";
                            }
                            else
                                max = Convert.ToString(o.DefaultIfEmpty("0").FirstOrDefault().Length);
                            if (o.LastOrDefault() == null)
                            {
                                min = "0";
                            }
                            else
                            {

                                min = Convert.ToString(o.DefaultIfEmpty("0").LastOrDefault().Length);
                            }
                        }
                        else if (c.DataType == typeof(DateTime))
                        {
                            var o = item.AsEnumerable().Select(al => al.Field<DateTime?>(c.ColumnName)).Distinct().ToList();
                            min = Convert.ToString(o.Min());
                            max = Convert.ToString(o.Max());
                        }
                        else if (c.DataType == typeof(double))
                        {
                            var o = item.AsEnumerable().Select(al => al.Field<double?>(c.ColumnName)).Distinct().ToList();
                            min = Convert.ToString(o.Min());
                            max = Convert.ToString(o.Max());
                        }
                        else if (c.DataType == typeof(decimal))
                        {
                            var o = item.AsEnumerable().Select(al => al.Field<decimal?>(c.ColumnName)).Distinct().ToList();
                            min = Convert.ToString(o.Min());
                            max = Convert.ToString(o.Max());
                        }
                        else
                        {
                            //c.DataType = typeof(string);
                            var o = item.AsEnumerable().Select(al => al.Field<string>(c.ColumnName)).Distinct().ToList().OrderByDescending(s => (s == null ? 0 : s.Length));
                            if (o.FirstOrDefault() == null)
                            {
                                max = "0";
                            }
                            else
                                max = Convert.ToString(o.DefaultIfEmpty("0").FirstOrDefault().Length);
                            if (o.LastOrDefault() == null)
                            {
                                min = "0";
                            }
                            else
                            {

                                min = Convert.ToString(o.DefaultIfEmpty("0").LastOrDefault().Length);
                            }
                        }
                        data.Rows.Add(
                            item.TableName, "",
                            c.ColumnName,
                            c.DataType.Name, "", "", "",
                            string.Empty,
                            min, max,
                            isNull[random.Next(0, isNull.Count)].ToString(),
                            "FALSE",
                            rantbool[random.Next(0, rantbool.Count)].ToString(),
                            rantbool[random.Next(0, rantbool.Count)].ToString(),
                            rantbool[random.Next(0, rantbool.Count)].ToString(),
                            MaskingRules[random.Next(0, MaskingRules.Count)].ToString(),
                            "BA Name", "",
                            completed[random.Next(0, completed.Count)].ToString(),
                            "",
                            "",
                            ""

                            );

                    }

                }

            }
            else if (datasource == DataSourceType.SqlServer || datasource == DataSourceType.OracleServer)
            {
                data = new DataTable();
                List<string> _comment = new List<string> { "DESCRIPTION", "TEXT", "MEMO", "COMMENT", "COMMENTS", "NOTE", "NOTES", "REMARK", "REMARKS" };
                List<string> _fullName = new List<string> { "OWNER_NAME", "OWNERS_NAME","AGENT_NAME", "OFFICER_NAME", "FULL_NAME", "CONTACT_NAME", "MANAGER_NAME", "NAME" };
                string scriptPath = "";
                scriptPath = @"Tab_ColumnsSQL.sql";
                string scriptPathOrc = @"Tab_ColumnsOracle.sql";
                DataTable getdata = new DataTable();
                data = GetData(connectionString, datasource.Equals(DataSourceType.SqlServer)? scriptPath : scriptPathOrc, datasource);
                DataColumn[] dc = new DataColumn[] {
                new DataColumn("Retain NULL",typeof(string)),
                new DataColumn("RetainEmptyString",typeof(string)),
                new DataColumn("Public", typeof(string)),
                new DataColumn("Personal", typeof(string)),
                new DataColumn("Sensitive",typeof(string)),
                new DataColumn("Masking Rule", typeof(string)),
                 new DataColumn("Rule set by", typeof(string)),
                new DataColumn("Rule Reasoning",typeof(string)),
                new DataColumn("COMPLETED", typeof(string)),
                new DataColumn("StringFormat", typeof(string)),
                 new DataColumn("UseValue", typeof(string)),
                 new DataColumn("Preview", typeof(string))
                };
                //data.Columns.Add("Min");
                // data.Columns.Add("max");
                //data.Columns.Add("Retain NULL"); //add more columns
                data.Columns.AddRange(dc);
                data.Columns.Add("TARGETSCHEMA", typeof(string)).SetOrdinal(3);
                Random random = new Random();
                List<string> rantbool = new List<string>() { "Yes", "No" };
                List<string> isNull = new List<string>() { "TRUE" };
                List<string> completed = new List<string>() { "Completed", "Uncompleted" };
                List<string> MaskingRules = new List<string>() { "No Masking required", "Shuffle", "StringConcat" };
                foreach (DataColumn columns in data.Columns)
                {
                    foreach (DataRow rows in data.Rows)
                    {
                        rows["Retain NULL"] = isNull[random.Next(0, isNull.Count)].ToString(); ;
                        rows["RetainEmptyString"] = "TRUE";
                        rows["COMPLETED"] = completed[random.Next(0, completed.Count)].ToString(); ;
                        rows["StringFormat"] = "";
                        rows["UseValue"] = "";
                        rows["TARGETSCHEMA"] = !string.IsNullOrEmpty(TARGETSCHEMA) ? TARGETSCHEMA.ToUpper() : rows["SCHEMA"].ToString();
                        rows["Preview"] = new List<string>{ "FALSE","TRUE"}[random.Next(0,2)];
                        rows["Public"] = rantbool[random.Next(0, rantbool.Count)].ToString();
                        rows["Personal"] = rantbool[random.Next(0, rantbool.Count)].ToString(); ;
                        rows["Sensitive"] = rantbool[random.Next(0, rantbool.Count)].ToString(); ;
#pragma warning disable CA1416 // Validate platform compatibility
                        rows["Rule set by"] = WindowsIdentity.GetCurrent().Name.Split('\\').Last();
#pragma warning restore CA1416 // Validate platform compatibility
                        if (int.TryParse(rows["Min"].ToString(), out int o) &&
                            int.TryParse(rows["Max"].ToString(), out int m) &&
                            o > m)
                        {
                            rows["Min"] = m;
                            rows["Max"] = o;
                        }
                        if (datasource.Equals(DataSourceType.SqlServer))
                        {
                            if (string.IsNullOrEmpty(rows["PKconstraintName"].ToString()))
                            {
                                rows["PKconstraintName"] = Convert.ToString(rows["PK_ID"]);
                            }
                        }

                        if (rows["column_name"].ToString().ToUpper().Contains("_ID"))
                        {
                            rows["Masking Rule"] = "No Masking required";
                            rows["Rule Reasoning"] = "Column Identifier";
                            rows["Preview"] = "FALSE";
                        }
                        else if (rows["data_type"].ToString().ToUpper().Contains("SDO_GEOMETRY"))
                        {
                            rows["Masking Rule"] = "ShufflePolygon";
                            rows["Rule Reasoning"] = "Column contains Polygon and Geometry";
                        }
                        else if (rows["data_type"].ToString().ToUpper().Contains("BLOB") || rows["data_type"].ToString().ToUpper().Contains("VARBINARY"))
                        {
                            rows["Masking Rule"] = "Blob";
                            rows["Rule Reasoning"] = "Column contains binary objects (image and docs)";
                        }
                        else if ((string.IsNullOrEmpty(rows["Min"].ToString()) && string.IsNullOrEmpty(rows["Max"].ToString())) || (rows["Min"].ToString() == "NULL" && rows["Max"].ToString() == "NULL") || (rows["Min"].ToString() == "1" && rows["Max"].ToString() == "1"))
                        {
                            rows["Masking Rule"] = "No Masking required";
                            rows["Rule Reasoning"] = "No sensitive record found in this column";
                            rows["COMPLETED"] = "Completed";
                            rows["Preview"] = "FALSE";
                        }
                        else if (rows["data_type"].ToString().ToUpper().Contains("DATE") || rows["data_type"].ToString().ToUpper().Contains("TIME"))
                        {
                            rows["Masking Rule"] = "DateOfBirth";
                            rows["RetainEmptyString"] = "FALSE";
                            if (rows["Max"].ToString().Equals(rows["Min"].ToString()) && !rows["Min"].ToString().Equals("-- ::"))
                            {
                                rows["Max"] = DateTime.Now.ToString();
                            }
                        }
                        else if (rows["column_name"].ToString().EndsWith("_DATE"))
                        {
                            rows["Masking Rule"] = "Date";
                            rows["Rule Reasoning"] = "This column is String of DateTime";
                            switch (rows["Max"])
                            {
                                case "8":
                                    rows["StringFormat"] = "yyyyMMdd";
                                    break;
                                case "10":
                                    rows["StringFormat"] = "yyyy-MM-dd";
                                    break;
                                case "6":
                                    rows["StringFormat"] = "yyMMdd";
                                    break;
                                default:
                                    break;
                            }
                            //rows["Min"] = DEFAULT_MIN_DATE.ToShortDateString();
                            //rows["Max"] = DEFAULT_MAX_DATE.ToShortDateString();
                        }
                        else if (rows["column_name"].ToString().EndsWith("_TIME") || rows["data_type"].ToString().ToUpper().Contains("TIME"))
                        {
                            rows["Masking Rule"] = "TimeSpan";
                            switch (rows["Max"])
                            {
                                case "4":
                                    rows["StringFormat"] = "HHmm";
                                    break;
                                case "5":
                                    rows["StringFormat"] = "HH:mm";
                                    break;
                                case "6":
                                    rows["StringFormat"] = "HHmmss";
                                    break;
                                case "8":
                                    rows["StringFormat"] = "HH:mm:ss";
                                    break;
                                case "10":
                                    rows["StringFormat"] = "HH:mm:sstt";
                                    break;
                                case "11":
                                    rows["StringFormat"] = "HH:mm:ss tt";
                                    break;
                                default:
                                    break;
                            }
                        }
                        else if ((rows["data_type"].ToString().ToUpper().Contains("DATE") || rows["data_type"].ToString().ToUpper().Contains("TIME")) && !(DateTime.TryParseExact(rows["Min"].ToString(), formats, CultureInfo.InvariantCulture, DateTimeStyles.None, out DateTime temp) && DateTime.TryParseExact(rows["Max"].ToString(), formats, CultureInfo.InvariantCulture, DateTimeStyles.None, out DateTime temp2)))
                        {
                            rows["Masking Rule"] = "No Masking required";
                            rows["Rule Reasoning"] = "No sensitive record found in this column";
                            rows["COMPLETED"] = "Completed";
                        }
                        else if (rows["column_name"].ToString().Contains("LONGITUDE") && (rows["data_type"].ToString().ToUpper().Contains("NUMERIC") || rows["data_type"].ToString().ToUpper().Contains("DECIMAL")))
                        {
                            rows["Masking Rule"] = "Longitude";
                        }
                        else if (rows["column_name"].ToString().Contains("LATITUDE") && (rows["data_type"].ToString().ToUpper().Contains("NUMERIC") || rows["data_type"].ToString().ToUpper().Contains("DECIMAL")))
                        {
                            rows["Masking Rule"] = "Latitude";
                        }
                        else if (rows["column_name"].ToString().Contains("FIRST_NAME") || rows["column_name"].ToString().Contains("FIRSTNAME") || rows["column_name"].ToString().ToUpper().Contains("MIDDLE_NAME"))
                        {
                            rows["Masking Rule"] = "FirstName";
                        }
                        else if (rows["data_type"].ToString().Equals("BLOB") && rows["data_type"].ToString().ToUpper().Equals("IMAGE"))
                        {
                            rows["Masking Rule"] = "Blob";
                        }
                        else if (rows["data_type"].ToString().Equals("CLOB"))
                        {
                            rows["Masking Rule"] = "Clob";
                        }
                        else if (rows["column_name"].ToString().Equals("CITY") || rows["column_name"].ToString().EndsWith("_CITY"))
                        {
                            rows["Masking Rule"] = "City";
                            rows["StringFormat"] = "{{ADDRESS.CITY}}";
                        }
                        else if (rows["column_name"].ToString().Equals("STATE") || rows["column_name"].ToString().Equals("PROVINCE"))
                        {
                            rows["Masking Rule"] = "State";
                            rows["StringFormat"] = "{{ADDRESS.CITY}}";
                        }
                        else if (rows["column_name"].ToString().Contains("COUNTRY"))
                        {
                            rows["Masking Rule"] = "Bogus";
                            rows["StringFormat"] = "{{ADDRESS.COUNTRY}}";
                        }
                        else if (rows["data_type"].ToString().Equals("SDO_GEOMETRY"))
                        {
                            rows["Masking Rule"] = "ShufflePolygon";
                        }
                        else if (rows["column_name"].ToString().Contains("SURNAME") || rows["column_name"].ToString().Contains("LASTNAME"))
                        {
                            rows["Masking Rule"] = "LastName";
                        }
                        else if (rows["column_name"].ToString().Contains("COMPANY_NAME") || rows["column_name"].ToString().Contains("ORGANIZATION_NAME"))
                        {
                            rows["Masking Rule"] = "Company";
                            rows["StringFormat"] = "{{COMPANY.COMPANYNAME}} {{COMPANY.COMPANYSUFFIX}}";
                        }
                        else if (_comment.Any(n => rows["column_name"].ToString().ToUpper().Contains(n)))
                        {
                            rows["Masking Rule"] = "Rant";
                        }
                        else if (rows["column_name"].ToString().Equals("YEAR") || rows["column_name"].ToString().EndsWith("_YEAR"))
                        {
                            rows["Masking Rule"] = "RandomYear";
                        }
                        else if (rows["column_name"].ToString().Contains("PHONE_NO") || rows["column_name"].ToString().Contains("PHONE_NUMBER") || rows["column_name"].ToString().EndsWith("_PHONE") || rows["column_name"].ToString().Contains("FAX_NO") || (rows["column_name"].ToString().EndsWith("_NUMBER") && rows["Min"].ToString().Equals("10") && rows["Max"].ToString().Equals("10")))
                        {
                            rows["StringFormat"] = "##########";
                            if (rows["data_type"].ToString().ToUpper().Contains("NUMBER"))
                            {
                                rows["Masking Rule"] = "PhoneNumberInt";

                            }
                            else
                            {
                                switch (rows["Max"])
                                {
                                    case "10":
                                        rows["StringFormat"] = "##########";
                                        break;
                                    case "12":
                                        rows["StringFormat"] = "### ###-####";
                                        break;
                                    case "14":
                                        rows["StringFormat"] = "(###) ###-####";
                                        break;
                                    default:
                                        break;
                                }
                                rows["Masking Rule"] = "PhoneNumber";
                            }

                        }
                        else if (rows["column_name"].ToString().Contains("EMAIL_ADDRESS"))
                        {
                            rows["Masking Rule"] = "Bogus";
                            rows["StringFormat"] = "{{INTERNET.EMAIL}}";
                        }
                        else if (rows["data_type"].ToString().Contains("MONEY"))
                        {
                            rows["Masking Rule"] = "Money";
                            rows["StringFormat"] = "{{FINANCE.AMOUNT}}";
                        }
                        else if (rows["column_name"].ToString().Contains("POSTAL_CODE"))
                        {
                            rows["Masking Rule"] = "PostalCode";
                            //rows["StringFormat"] = "{{FINANCE.AMOUNT}}";
                        }
                        else if (rows["column_name"].ToString().Contains("ADDRESS") && rows["column_name"].ToString().Contains("_LINE"))
                        {
                            if (rows["column_name"].ToString().Contains("1"))
                            {
                                rows["Masking Rule"] = "fullAddress";
                            }
                            else if (rows["column_name"].ToString().Contains("2") || rows["column_name"].ToString().Contains("3"))
                            {
                                rows["Masking Rule"] = "SecondaryAddress";
                            }
                            else
                                rows["Masking Rule"] = "No Masking required";
                            //rows["Masking Rule"] = "FullAddress";
                            //rows["StringFormat"] = "{{address.fullAddress}}";
                        }
                        else if (rows["column_name"].ToString().Contains("_ADDRESS"))
                        {
                            if (rows["column_name"].ToString().Contains("1"))
                            {
                                rows["Masking Rule"] = "fullAddress";
                            }
                            else if (rows["column_name"].ToString().Contains("2") || rows["column_name"].ToString().Contains("3"))
                            {
                                rows["Masking Rule"] = "SecondaryAddress";
                            }
                            else if (rows["column_name"].ToString().EndsWith("_ADDRESS"))
                            {
                                rows["Masking Rule"] = "FullAddress";
                                rows["StringFormat"] = "{{address.fullAddress}}";
                            }
                            else
                                rows["Masking Rule"] = MaskingRules[random.Next(0, MaskingRules.Count)].ToString();
                        }
                        else if (rows["column_name"].ToString().Contains("_ADDRESS"))
                        {
                            rows["Masking Rule"] = "FullAddress";
                            rows["StringFormat"] = "{{address.fullAddress}}";
                        }
                        else if (_fullName.Any(n => rows["column_name"].ToString().ToUpper().Contains(n)))
                        {
                            rows["Masking Rule"] = "CompanyPersonName";
                            rows["StringFormat"] = "{{NAME.FULLNAME}}";
                        }
                        else if (rows["column_name"].ToString().Contains("USERID") || rows["column_name"].ToString().Contains("USERNAME"))
                        {
                            rows["Masking Rule"] = "RandomUsername";
                            // rows["StringFormat"] = "{{address.fullAddress}}";
                        }
                        else if (rows["column_name"].ToString().Contains("FILE_NAME") || rows["column_name"].ToString().Contains("FILENAME"))
                        {
                            rows["Masking Rule"] = "File";
                            rows["StringFormat"] = "{{SYSTEM.FILENAME}}";
                        }
                        else if (rows["column_name"].ToString().Contains("_AMOUNT") || rows["column_name"].ToString().Contains("_AMT"))
                        {
                            rows["Masking Rule"] = "RandomDec";
                            //rows["StringFormat"] = "{{SYSTEM.FILENAME}}";
                        }
                        else if (rows["data_type"].ToString().Contains("NUMBER") || rows["data_type"].ToString().Equals("INT"))
                        {
                            if (!IsDouble(rows["Max"].ToString()) && !IsDouble(rows["Min"].ToString()))
                            {
                                var maxNumber = ConvertNumber(rows["Max"].ToString());
                                if (maxNumber > 2147483647)
                                {
                                    rows["Masking Rule"] = "RandomInt64";
                                }
                                else if (maxNumber == 0)
                                {
                                    rows["Masking Rule"] = "No Masking required";
                                }
                                else
                                    rows["Masking Rule"] = "RandomInt"; //630,324,163,796,873,000,000,000,000,000,000,000

                            }
                            else
                                rows["Masking Rule"] = "RandomDec";

                            //rows["StringFormat"] = "{{FINANCE.AMOUNT}}";
                        }
                        else if (rows["table_name"].ToString().ToUpper().Contains("VEHICLE"))
                        {
                            if (rows["column_name"].ToString().ToUpper().Equals("MAKE") || rows["column_name"].ToString().ToUpper().EndsWith("_MAKE"))
                            {
                                rows["Masking Rule"] = "Vehicle";
                                rows["StringFormat"] = "Manufacturer";
                            }
                            else if (rows["column_name"].ToString().ToUpper().Equals("MODEL") || rows["column_name"].ToString().ToUpper().EndsWith("_MODEL"))
                            {
                                rows["Masking Rule"] = "Vehicle";
                                rows["StringFormat"] = "Model";
                            }
                            else if (rows["column_name"].ToString().ToUpper().Contains("_IDENTIFICATION_NUMBER") || rows["column_name"].ToString().ToUpper().EndsWith("_VIN") || rows["column_name"].ToString().ToUpper().Equals("VIN"))
                            {
                                rows["Masking Rule"] = "Vehicle";
                                rows["StringFormat"] = "Vin";
                            }
                            else if (rows["column_name"].ToString().ToUpper().Equals("FUEL") || rows["column_name"].ToString().ToUpper().EndsWith("_FUEL"))
                            {
                                rows["Masking Rule"] = "Vehicle";
                                rows["StringFormat"] = "Fuel";
                            }
                            else if (rows["column_name"].ToString().ToUpper().Contains("_TYPE") || rows["column_name"].ToString().ToUpper().Contains("_STYLE"))
                            {
                                rows["Masking Rule"] = "Vehicle";
                                rows["StringFormat"] = "Type";
                            }
                            else
                                rows["Masking Rule"] = MaskingRules[random.Next(0, MaskingRules.Count)].ToString();
                        }
                        else if (rows["column_name"].ToString().ToUpper().Contains("VEHICLE"))
                        {
                            if (rows["column_name"].ToString().ToUpper().Contains("_MAKE"))
                            {
                                rows["Masking Rule"] = "Vehicle";
                                rows["StringFormat"] = "Manufacturer";
                            }
                            else if (rows["column_name"].ToString().ToUpper().Contains("_MODEL"))
                            {
                                rows["Masking Rule"] = "Vehicle";
                                rows["StringFormat"] = "Model";
                            }
                            else if (rows["column_name"].ToString().ToUpper().Contains("_IDENTIFICATION") || rows["column_name"].ToString().ToUpper().Contains("_REGISTRATION"))
                            {
                                rows["Masking Rule"] = "Vehicle";
                                rows["StringFormat"] = "Vin";
                            }
                            else if (rows["column_name"].ToString().ToUpper().Equals("FUEL") || rows["column_name"].ToString().ToUpper().EndsWith("_FUEL"))
                            {
                                rows["Masking Rule"] = "Vehicle";
                                rows["StringFormat"] = "Fuel";
                            }
                            else if (rows["column_name"].ToString().ToUpper().Equals("TYPE") || rows["column_name"].ToString().ToUpper().EndsWith("_TYPE") || rows["column_name"].ToString().ToUpper().EndsWith("BODY_STYLE"))
                            {
                                rows["Masking Rule"] = "Vehicle";
                                rows["StringFormat"] = "Type";
                            }
                            else
                                rows["Masking Rule"] = MaskingRules[random.Next(0, MaskingRules.Count)].ToString();
                        }                        
                        else if (rows["column_name"].ToString().ToUpper().EndsWith("_NUMBER") && (rows["data_type"].ToString().ToUpper().Contains("VARCHAR") || rows["data_type"].ToString().ToLower().Contains("character")))
                        {
                            for (int i = 0; i < Convert.ToInt32(rows["Max"]); i++)
                            {
                                rows["StringFormat"] += "#";
                            }
                            rows["Masking Rule"] = "StringConcat";
                        }
                        else if (rows["column_name"].ToString().ToUpper().EndsWith("_DIRECTORY"))
                        {
                            rows["Masking Rule"] = "Bogus";
                            rows["StringFormat"] = "{{SYSTEM.DIRECTORYPATH}}";
                        }
                        else if (IsDouble(Convert.ToString(rows["Max"])) && IsDouble(Convert.ToString(rows["Min"])))
                        {
                            rows["Masking Rule"] = "RandomDec";
                        }
                        else
                        {
                            rows["Masking Rule"] = MaskingRules[random.Next(0, MaskingRules.Count)].ToString();
                            if (rows["comments"].ToString().ToLower().Contains("identif"))
                            {
                                rows["Masking Rule"] = "No Masking required";
                            }
                            else if (rows["comments"].ToString().ToLower().Contains("address") && rows["column_name"].ToString().ToLower().Contains("line"))
                            {
                                if (rows["column_name"].ToString().ToLower().Contains("1"))
                                {
                                    rows["Masking Rule"] = "fullAddress";
                                }
                                else
                                    rows["Masking Rule"] = "SecondaryAddress";
                            }
                            else if (rows["Masking Rule"].Equals("StringConcat"))
                            {
                                for (int i = 0; i < Convert.ToInt32(rows["Max"]); i++)
                                {
                                    rows["StringFormat"] += "?";
                                }
                            }
                            else if (rows["Masking Rule"].ToString().ToUpper().Contains("No Masking required".ToUpper()))
                            {
                                rows["Preview"] = "FALSE";
                            }
                        }
                      
                        data.AcceptChanges();
                    }
                }
                if (data.Columns.Contains("PK_ID"))
                {
                    data.Columns.Remove("PK_ID");
                }
                
            }
            else
            {
                //check app.config

                data = new DataTable();
                string scriptPath = "";
                switch (datasource)
                {
                    case DataSourceType.OracleServer:
                        scriptPath = @"Tab_ColumnsOracle.sql";
                        break;
                    case DataSourceType.SqlServer:
                        break;
                    case DataSourceType.PostgresServer:
                        scriptPath = @"Tab_Columns_Postgres.sql";
                        break;
                    case DataSourceType.MySqlServer:
                        scriptPath = @"Tab_ColumnsMySQL.sql";
                        break;
                    case DataSourceType.SpreadSheet:
                        break;
                    case DataSourceType.None:
                        break;
                    default:
                        break;
                        //throw new ArgumentOutOfRangeException(nameof(datasource), datasource, "not implemented");
                }

                //if (datasource == DataSourceType.OracleServer)
                //{
                //    scriptPath = @"Tab_ColumnsOracle.sql";
                //}
                //else if (datasource == DataSourceType.PostgresServer)
                //{
                //    scriptPath = @"Tab_Columns_Postgres.sql";
                //}
                //else if (datasource == DataSourceType.PostgresServer)
                //{
                //    scriptPath = @"Tab_ColumnsMySQL.sql";
                //}


                DataTable getdata = new DataTable();
                data = GetdataTable(connectionString, datasource, scriptPath);
                //ExecuteScript(connectionString, scriptPath);

                var tablename = data.Rows.OfType<DataRow>().Select(dr => dr.Field<string>("table_name")).ToList();
                var columnList = data.Rows.OfType<DataRow>().Select(dr => dr.Field<string>("column_name")).ToList();
                getdata.Merge(data);
                DataColumn[] dc = new DataColumn[] {
                new DataColumn("Min", typeof(string)),
                new DataColumn("Max", typeof(string)),
                new DataColumn("Retain NULL",typeof(string)),
                new DataColumn("RetainEmptyString",typeof(string)),
                new DataColumn("Public", typeof(string)),
                new DataColumn("Personal", typeof(string)),
                new DataColumn("Sensitive",typeof(string)),
                new DataColumn("Masking Rule", typeof(string)),
                 new DataColumn("Rule set by", typeof(string)),
                new DataColumn("Rule Reasoning",typeof(string)),
                new DataColumn("COMPLETED", typeof(string)),
                new DataColumn("StringFormat", typeof(string)),
                 new DataColumn("UseValue", typeof(string)),
                 new DataColumn("Preview", typeof(string))
                };
                //data.Columns.Add("Min");
                // data.Columns.Add("max");
                //data.Columns.Add("Retain NULL"); //add more columns
                data.Columns.AddRange(dc);
                data.Columns.Add("TARGETSCHEMA", typeof(string)).SetOrdinal(3);
                Random random = new Random();
                List<string> rantbool = new List<string>() { "Yes", "No" };
                List<string> isNull = new List<string>() { "TRUE" };
                List<string> completed = new List<string>() { "Completed", "Uncompleted" };
                List<string> MaskingRules = new List<string>() { "No Masking required", "Replace Value with fake data", "Shuffle" };


                foreach (DataColumn columns in data.Columns)
                {
                    foreach (DataRow rows in data.Rows)
                    {
                        rows["Retain NULL"] = isNull[random.Next(0, isNull.Count)].ToString(); ;
                        rows["RetainEmptyString"] = "TRUE";
                        rows["COMPLETED"] = completed[random.Next(0, completed.Count)].ToString(); ;
                        rows["StringFormat"] = "";
                        rows["TARGETSCHEMA"] = string.IsNullOrEmpty(TARGETSCHEMA) ? TARGETSCHEMA : rows["schema"].ToString();
                        rows["UseValue"] = "";
                        rows["Preview"] = "FALSE";
                        rows["Public"] = rantbool[random.Next(0, rantbool.Count)].ToString();
                        rows["Personal"] = rantbool[random.Next(0, rantbool.Count)].ToString(); ;
                        rows["Sensitive"] = rantbool[random.Next(0, rantbool.Count)].ToString(); ;
#pragma warning disable CA1416 // Validate platform compatibility
                        rows["Rule set by"] = WindowsIdentity.GetCurrent().Name.Split('\\').Last(); ;
#pragma warning restore CA1416 // Validate platform compatibility
                        rows["Masking Rule"] = MaskingRules[random.Next(0, MaskingRules.Count)].ToString(); ;                       
                        if (rows["column_name"].ToString().ToUpper().Contains("_ID"))
                        {
                            rows["Masking Rule"] = "No Masking required";
                        }
                        if (rows["data_type"].ToString().ToUpper().Contains("DATE"))
                        {
                            rows["Masking Rule"] = "DateOfBirth";
                        }
                        if (rows["data_type"].ToString().ToUpper().Contains("DATE"))
                        {
                            string tname = rows["table_name"].ToString();
                            string colname = rows["column_name"].ToString();
                            DataTable dtMinMax = new DataTable();
                            dtMinMax = GetMinMax(rows["table_name"].ToString(), rows["schema"].ToString(), rows["column_name"].ToString(), connectionString, datasource);
                            if (dtMinMax != null && dtMinMax.Rows.Count > 0)
                            {
                                var min = dtMinMax.Rows.OfType<DataRow>().Select(dr => dr.Field<DateTime?>("Min")).ToList()[0];
                                var max = dtMinMax.Rows.OfType<DataRow>().Select(dr => dr.Field<DateTime?>("Max")).ToList()[0];
                                rows["Min"] = min;
                                rows["Max"] = max;
                                data.AcceptChanges();
                            }

                        }
                        else if (rows["data_type"].ToString().ToUpper().Contains("NUMBER") || rows["data_type"].ToString().ToUpper().Contains("MONEY") || rows["data_type"].ToString().ToUpper().Contains("DECIMAL"))
                        {
                            string tname = rows["table_name"].ToString();
                            string colname = rows["column_name"].ToString();
                            DataTable dtMinMax = new DataTable();
                            dtMinMax = GetMinMax(rows["table_name"].ToString(), rows["schema"].ToString(), rows["column_name"].ToString(), connectionString, datasource);
                            if (dtMinMax != null && dtMinMax.Rows.Count > 0)
                            {
                                var min = dtMinMax.Rows.OfType<DataRow>().Select(dr => dr.Field<decimal?>("MIN")).ToList()[0];
                                var max = dtMinMax.Rows.OfType<DataRow>().Select(dr => dr.Field<decimal?>("MAX")).ToList()[0];
                                rows["Min"] = min;
                                rows["Max"] = max;
                                data.AcceptChanges();
                            }

                        }
                        else if (rows["data_type"].ToString().ToUpper().Contains("FLOAT"))
                        {
                            string tname = rows["table_name"].ToString();
                            string colname = rows["column_name"].ToString();
                            DataTable dtMinMax = new DataTable();
                            dtMinMax = GetMinMax(rows["table_name"].ToString(), rows["schema"].ToString(), rows["column_name"].ToString(), connectionString, datasource);
                            if (dtMinMax != null && dtMinMax.Rows.Count > 0)
                            {
                                var min = dtMinMax.Rows.OfType<DataRow>().Select(dr => dr.Field<double?>("MIN")).ToList()[0];
                                var max = dtMinMax.Rows.OfType<DataRow>().Select(dr => dr.Field<double?>("MAX")).ToList()[0];
                                rows["Min"] = min;
                                rows["Max"] = max;
                                data.AcceptChanges();
                            }

                        }
                        else if (rows["data_type"].ToString().ToUpper().Contains("BIT"))
                        {
                            string tname = rows["table_name"].ToString();
                            string colname = rows["column_name"].ToString();
                            DataTable dtMinMax = new DataTable();
                            dtMinMax = GetMinMax(rows["table_name"].ToString(), rows["schema"].ToString(), rows["column_name"].ToString(), connectionString, datasource);
                            if (dtMinMax != null && dtMinMax.Rows.Count > 0)
                            {
                                var min = dtMinMax.Rows.OfType<DataRow>().Select(dr => dr.Field<bool?>("MIN")).ToList()[0];
                                var max = dtMinMax.Rows.OfType<DataRow>().Select(dr => dr.Field<bool?>("MAX")).ToList()[0];
                                rows["Min"] = min;
                                rows["Max"] = max;
                                data.AcceptChanges();
                            }

                        }
                        else if (rows["data_type"].ToString().ToUpper().Equals("INT") || rows["data_type"].ToString().ToUpper().Equals("INTEGER"))
                        {
                            string tname = rows["table_name"].ToString();
                            string colname = rows["column_name"].ToString();
                            DataTable dtMinMax = new DataTable();
                            dtMinMax = GetMinMax(rows["table_name"].ToString(), rows["schema"].ToString(), rows["column_name"].ToString(), connectionString, datasource);
                            if (dtMinMax != null && dtMinMax.Rows.Count > 0)
                            {
                                var min = dtMinMax.Rows.OfType<DataRow>().Select(dr => dr.Field<int?>("MIN")).ToList()[0];
                                var max = dtMinMax.Rows.OfType<DataRow>().Select(dr => dr.Field<int?>("MAX")).ToList()[0];
                                rows["Min"] = min;
                                rows["Max"] = max;
                                data.AcceptChanges();
                            }

                        }
                        else if (rows["data_type"].ToString().ToUpper().Contains("VARCHAR2") || rows["data_type"].ToString().ToUpper().Contains("VARCHAR") || rows["data_type"].ToString().ToLower().Contains("character") || rows["data_type"].ToString().ToLower().Contains("text"))
                        {
                            string tname = rows["table_name"].ToString();
                            string colname = rows["column_name"].ToString();
                            DataTable dtMinMax = new DataTable();
                            dtMinMax = GetMinMax(rows["table_name"].ToString(), rows["schema"].ToString(), rows["column_name"].ToString(), connectionString, datasource);
                            if (dtMinMax != null && dtMinMax.Rows.Count > 0)
                            {
                                //var min = dtMinMax.Rows.OfType<DataRow>().Select(dr => dr.Field<decimal?>("MIN")).ToList()[0];
                                var tt = dtMinMax;

                                var MaxNvarchar = dtMinMax.Rows.OfType<DataRow>().Select(dr => dr.Field<object>("MaxNvarchar")).ToList()[0];
                                var MinNvarchar = dtMinMax.Rows.OfType<DataRow>().Select(dr => dr.Field<object>("MinNvarchar")).ToList()[0];
                                //rows["Min"] = min;
                                rows["Max"] = MaxNvarchar;
                                rows["Min"] = MinNvarchar;
                                data.AcceptChanges();
                            }

                        }
                        else
                        {

                        }
                    }
                }
               


            }
            #endregion            //write to excel       
            if (data.Rows != null)
            {
                DataTableToExcel(data, csvOutputfilename, csvOutputfilename);
                data.PrintList(true, 50);

                Colors.WriteLine("Total Columns: ".Red(), "", data.Rows.Count.ToString().Yellow());
                Console.WriteLine("Hit Enter to Continue.....".ToUpper());
                Console.ReadLine();
                if (sendEmail)
                {
                    var sc = string.Join(", ", new DataView(data).ToTable(false, new string[] { "SCHEMA" }).AsEnumerable().Select(n => n[0]).ToList().Distinct().ToArray());
                    var dname = string.Join("", new DataView(data).ToTable(false, new string[] { "DatabaseName" }).AsEnumerable().Select(n => n[0]).ToList().Distinct().ToArray());

                    SendEmail(Recipients, data, csvOutputfilename + @"\" + csvOutputfilename + ".xlsx", appServer, fromEmail, new DataView(data).ToTable(false, new string[] { "DatabaseName" }).AsEnumerable().Select(n => n[0]).ToList().Any() ? dname: csvOutputfilename, Cc, sc, datasource);
                }
                //exportToExcel(data, "APP_GWP");
            }

        }
        public static bool IsDouble(string value)
        {
            if (value.IndexOf(".") >= 0)
            {


                try
                {
                    double.Parse(value);
                    return true;
                }
                catch
                {
                    return false;
                }

            }
            return false;
        }
        private static Int64 ConvertNumber(string value)
        {
            Int64 number; // receives the converted numeric value, if conversion succeeds

            bool result = Int64.TryParse(value, out number);
            if (result)
            {
                // The return value was True, so the conversion was successful
                return number;
            }
            else
            {
                // Make sure the string object is not null, for display purposes
                if (value == null)
                {
                    value = String.Empty;
                }

                // The return value was False, so the conversion failed
                Console.WriteLine("Attempted conversion of '{0}' failed.", value);
               
                
                return 0;
            }
        }
        private static DataTable GetData(string connectionString, string scriptpath, DataSourceType datasource)
        {
            DataTable dataTable = new DataTable();
            if (datasource == DataSourceType.SqlServer)
            {
                using (SqlConnection SqlConnection = new SqlConnection(connectionString))
                {
                    string squery = File.ReadAllText(scriptpath);
                    //System.Collections.Generic.IEnumerable<string> commandStrings = Regex.Split(squery, @"^\s*GO\s*$",
                    //RegexOptions.Multiline | RegexOptions.IgnoreCase);


                    try
                    {
                        SqlConnection.Open();
                    }
                    catch (Exception e)
                    {
                        Console.WriteLine("Exception occurs {0}", e.Message);
                        Console.WriteLine("Program will exit: Press ENTER to exist..");
                        Console.ReadLine();

                        File.WriteAllText(exceptionPath, e.Message + Environment.NewLine + Environment.NewLine);
                        System.Environment.Exit(1);
                    }
                    using (SqlDataAdapter oda = new SqlDataAdapter(squery, SqlConnection))
                    {
                        try
                        {
                            //Fill the data table with select statement's query results:
                            int recordsAffectedSubscriber = 0;
                            oda.SelectCommand.CommandTimeout = 0;
                            recordsAffectedSubscriber = oda.Fill(dataTable);
                        }
                        catch (Exception ex)
                        {

                            File.WriteAllText(exceptionPath, ex.Message + Environment.NewLine + Environment.NewLine);
                        }
                    }

                    return dataTable;

                }
            }
            else if (datasource == DataSourceType.OracleServer)
            {
                using (OracleConnection OrcConnection = new OracleConnection(connectionString))
                {
                    string squery = File.ReadAllText(scriptpath);
                    //System.Collections.Generic.IEnumerable<string> commandStrings = Regex.Split(squery, @"^\s*GO\s*$",
                    //RegexOptions.Multiline | RegexOptions.IgnoreCase);


                    try
                    {
                        OrcConnection.Open();
                    }
                    catch (Exception e)
                    {
                        Console.WriteLine("Exception occurs {0}", e.Message);
                        Console.WriteLine("Program will exit: Press ENTER to exist..");
                        Console.ReadLine();

                        File.WriteAllText(exceptionPath, e.Message + Environment.NewLine + Environment.NewLine);
                        Environment.Exit(1);
                    }
                    using (OracleDataAdapter oda = new OracleDataAdapter(squery, OrcConnection))
                    {
                        try
                        {
                            //Fill the data table with select statement's query results:
                            int recordsAffectedSubscriber = 0;
                            oda.SelectCommand.CommandTimeout = 0;
                            recordsAffectedSubscriber = oda.Fill(dataTable);

                        }
                        catch (Exception ex)
                        {

                            File.WriteAllText(exceptionPath, ex.Message + Environment.NewLine + Environment.NewLine);
                        }
                    }

                    return dataTable;
                }
            }
            return dataTable;
        }
        private static string GetSignature()
        {

            return File.ReadAllText(@"Signature.htm");
        }
        private static void SendEmail(string recipients, DataTable dataTable, string attachment, string appServer, string fromEmail, string database, string Cc, string schema, DataSourceType dataSourceType)
        {
            try
            {
                var rand = new Random();
                var top10 = dataTable.AsEnumerable().OrderBy(n => rand.Next()).Take(5).CopyToDataTable();
                List<string> rowCount = new List<string> { "Total RowCount: " + dataTable.Rows.Count, $"DataSourceType: {datasource.ToString()}" };
                var html = ToHTML(top10, rowCount);
                var sig = GetSignature();
                var r = recipients.Split('.')[0];
                string name = char.ToUpper(r[0]) + r.Remove(0, 1);
                MailMessage mail = new MailMessage()
                {

                    Subject = "Data Classification SpreadSheet for " + database,
                    IsBodyHtml = true,
                    Body = "Hello " + name + ", <br/> " + "<p><b>This message is sent via an inbox that does not receive messages, please do not reply to this email.</b>" + " <br />" + "<b>..................</b>" + "</p> " +

                         "<p> Attached is the data classification spreadsheet of " + database + " for " + schema + " schema. Thank you. </p>" +
                          //"<br />" +
                          html +
                            "<br />" +
                         sig +
                         "<br />" +
                         "</body>" +
                         "</html>",
                    From = new MailAddress(fromEmail, "MOTI Datamasking TRAN:EX")

                };
                SmtpClient SmtpServer = new SmtpClient(appServer);
                //mail.Sender.
                mail.To.Add(recipients);
                if (!string.IsNullOrEmpty(Cc))
                {
                    mail.CC.Add(Cc);
                }
                mail.Attachments.Add(new Attachment(attachment));
                SmtpServer.Port = 25;
               // SmtpServer.Credentials = new System.Net.NetworkCredential();
                SmtpServer.UseDefaultCredentials = true;
                //SmtpServer.EnableSsl = true;
                SmtpServer.Send(mail);
                Console.WriteLine("mail Send");
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.ToString());
                Console.ReadLine();
            }
        }
        private static void DataTableToExcel(DataTable dataTable, string directory, string _tablename)
        {
            dataTable.TableName = _tablename;

            try
            {
                //HttpContext.Current.Response.Clear();
                if (!Directory.Exists(directory))
                {
                    Directory.CreateDirectory(directory);
                }

                var format = new ExcelTextFormat
                {
                    EOL = "\r\n"
                };
                using (ExcelPackage pack = new ExcelPackage())
                {
                    ExcelWorksheet ws = pack.Workbook.Worksheets.Add(dataTable.TableName);
                    ws.Cells["A1"].LoadFromDataTable(dataTable, true, OfficeOpenXml.Table.TableStyles.Medium28);
                    var ms = new System.IO.MemoryStream();
                    pack.SaveAs(new FileInfo(directory + @"\" + directory + ".xlsx"));

                }
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
                Console.WriteLine("Hit enter to exit");
                Console.ReadLine();
            }
        }
        private static DataTableCollection ExcelToTable(string path)
        {
            DataTable dataTable = new DataTable();
            result = new DataSet();
            dataTable.Columns.Clear();
            dataTable.Rows.Clear();
            dataTable.Clear();
            if (true)
            {


                using (FileStream fileStream = new FileStream(path, FileMode.Open, FileAccess.Read))
                {
                    IExcelDataReader excelReader = null;
                    if (Path.GetExtension(path).ToUpper() == ".XLSX")
                    {
                        excelReader = ExcelReaderFactory.CreateOpenXmlReader(fileStream);
                    }
                    else if (Path.GetExtension(path).ToUpper() == ".XLS")
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
                        dataTable = result.Tables[0].AsDataView().ToTable();

                        //foreach (DataTable item in result.Tables)
                        //{
                        //    foreach (var i in item.Columns)
                        //    {

                        //    }

                        //}
                    }
                }
            }

            else
                throw new ArgumentException("Invalid sheet extension", path);

            return result.Tables;
        }
        private static void WriteTofile(DataTable textTable, string directory)
        {
            StringBuilder fileContent = new StringBuilder();
            //int i = 0;
            if (!Directory.Exists(directory))
            {
                Directory.CreateDirectory(directory);
            }
            if (textTable.Columns.Count == 0)
            {
                return;
            }
            foreach (var col in textTable.Columns)
            {
                fileContent.Append(col.ToString() + ",");
            }

            fileContent.Replace(",", System.Environment.NewLine, fileContent.Length - 1, 1);

            foreach (DataRow dr in textTable.Rows)
            {
                foreach (var column in dr.ItemArray)
                {
                    fileContent.Append("\"" + column.ToString() + "\",");
                }

                fileContent.Replace(",", System.Environment.NewLine, fileContent.Length - 1, 1);
            }
            Console.WriteLine(directory);
            Console.ReadLine();
            Console.WriteLine(Directory.GetCurrentDirectory() + @"\" + directory + @"\" + directory + ".csv");
            Console.ReadLine();
            System.IO.File.WriteAllText(directory + @"\" + directory + ".csv", fileContent.ToString());

        }
        private static DataTable GetdataTable(string connectionString1, DataSourceType datasource, string scriptpath)
        {
            DataTable dataTable = new DataTable();
            if (datasource == DataSourceType.OracleServer)
            {
                using (OracleConnection oracleConnection = new OracleConnection(connectionString1))
                {
                    string squery = File.ReadAllText(scriptpath);
                    //System.Collections.Generic.IEnumerable<string> commandStrings = Regex.Split(squery, @"^\s*GO\s*$",
                    //RegexOptions.Multiline | RegexOptions.IgnoreCase);
                    string que = string.Format("BEGIN {0} END;", squery).Replace(Environment.NewLine, "\n");


                    try
                    {
                        oracleConnection.Open();
                        Console.WriteLine("Database Connection established");
                    }
                    catch (Exception e)
                    {
                        Console.WriteLine("Exception occurs {0}", e.Message);
                        Console.WriteLine("Program will exit: Press ENTER to exist..");
                        Console.ReadLine();

                        File.WriteAllText(exceptionPath, e.Message + Environment.NewLine + Environment.NewLine);
                        System.Environment.Exit(1);
                    }
                    using (OracleDataAdapter oda = new OracleDataAdapter(que, oracleConnection))
                    {
                        try
                        {
                            //Fill the data table with select statement's query results:
                            int recordsAffectedSubscriber = 0;

                            recordsAffectedSubscriber = oda.Fill(dataTable);

                        }
                        catch (Exception ex)
                        {

                            File.WriteAllText(exceptionPath, ex.Message + Environment.NewLine + Environment.NewLine);

                        }
                    }

                    return dataTable;

                }
            }
            else if (datasource == DataSourceType.SqlServer)
            {
                using (SqlConnection SqlConnection = new SqlConnection(connectionString1))
                {
                    string squery = File.ReadAllText(scriptpath);
                    //System.Collections.Generic.IEnumerable<string> commandStrings = Regex.Split(squery, @"^\s*GO\s*$",
                    //RegexOptions.Multiline | RegexOptions.IgnoreCase);


                    try
                    {
                        SqlConnection.Open();
                    }
                    catch (Exception e)
                    {
                        Console.WriteLine("Exception occurs {0}", e.Message);
                        Console.WriteLine("Program will exit: Press ENTER to exist..");
                        Console.ReadLine();

                        File.WriteAllText(exceptionPath, e.Message + Environment.NewLine + Environment.NewLine);
                        System.Environment.Exit(1);
                    }
                    using (SqlDataAdapter oda = new SqlDataAdapter(squery, SqlConnection))
                    {
                        try
                        {
                            //Fill the data table with select statement's query results:
                            int recordsAffectedSubscriber = 0;
                            oda.SelectCommand.CommandTimeout = 0;
                            recordsAffectedSubscriber = oda.Fill(dataTable);

                        }
                        catch (Exception ex)
                        {

                            File.WriteAllText(exceptionPath, ex.Message + Environment.NewLine + Environment.NewLine);
                        }
                    }

                    return dataTable;

                }
            }
            else if (datasource == DataSourceType.PostgresServer)
            {
                using (NpgsqlConnection postgreConnection = new NpgsqlConnection(connectionString1))
                {
                    string squery = File.ReadAllText(scriptpath);
                    //System.Collections.Generic.IEnumerable<string> commandStrings = Regex.Split(squery, @"^\s*GO\s*$",
                    //RegexOptions.Multiline | RegexOptions.IgnoreCase);
                    try
                    {
                        postgreConnection.Open();
                    }
                    catch (Exception e)
                    {
                        Console.WriteLine("Exception occurs {0}", e.Message);
                        Console.WriteLine("Program will exit: Press ENTER to exist..");
                        Console.ReadLine();

                        File.WriteAllText(exceptionPath, e.Message + Environment.NewLine + Environment.NewLine);
                        System.Environment.Exit(1);
                    }

                    using (NpgsqlDataAdapter oda = new NpgsqlDataAdapter(squery, postgreConnection))
                    {
                        try
                        {
                            //Fill the data table with select statement's query results:
                            int recordsAffectedSubscriber = 0;

                            recordsAffectedSubscriber = oda.Fill(dataTable);

                        }
                        catch (Exception ex)
                        {
                            File.WriteAllText(exceptionPath, ex.Message + Environment.NewLine + Environment.NewLine);
                        }
                    }

                    return dataTable;

                }
            }
            else if (datasource == DataSourceType.MySqlServer)
            {
                using (MySqlConnection mySQLConnection = new MySqlConnection(connectionString1))
                {
                    string squery = File.ReadAllText(scriptpath);
                    //System.Collections.Generic.IEnumerable<string> commandStrings = Regex.Split(squery, @"^\s*GO\s*$",
                    //RegexOptions.Multiline | RegexOptions.IgnoreCase);

                    try
                    {
                        mySQLConnection.Open();
                    }
                    catch (Exception e)
                    {
                        Console.WriteLine("Exception occurs {0}", e.Message);
                        Console.WriteLine("Program will exit: Press ENTER to exist..");
                        Console.ReadLine();

                        File.WriteAllText(exceptionPath, e.Message + Environment.NewLine + Environment.NewLine);
                        System.Environment.Exit(1);
                    }
                    using (MySqlDataAdapter oda = new MySqlDataAdapter(squery, mySQLConnection))
                    {
                        try
                        {
                            //Fill the data table with select statement's query results:
                            int recordsAffectedSubscriber = 0;

                            recordsAffectedSubscriber = oda.Fill(dataTable);

                        }
                        catch (Exception ex)
                        {

                            File.WriteAllText(exceptionPath, ex.Message + Environment.NewLine + Environment.NewLine);
                        }
                    }

                    return dataTable;

                }
            }
            else
                throw new ArgumentOutOfRangeException(nameof(datasource), datasource, "not implemented");

        }
        public static void ExecuteScript(string _connectionString, string script)
        {
            string squery = File.ReadAllText(script);
            using (StringReader sr = new StringReader(squery))
            {
                var connection = new OracleConnection(_connectionString);
                connection.Open();

                string sqlCommand = "";
                string sqlLine2; 
                byte lineNum = 0; int linExec = 0;
                var sp = squery.Split('/');
                foreach (var sqlLine in squery.Split(new char[] { '/' }, StringSplitOptions.RemoveEmptyEntries))
                {
                    sqlLine2 = sqlLine.Trim().Replace("\r\n", "\n");
                    //var tt = sqlLine.Replace("\r\n", "");
                    var command = new OracleCommand(sqlLine2, connection);
                    command.BindByName = true;
                    Console.WriteLine(sqlLine2);
                    try
                    {
                       int p = command.ExecuteNonQuery();
                        linExec = sqlCommand.Length;
                    }
                    catch (OracleException ex)
                    {
                        connection.Close();
                        Console.WriteLine(sqlLine2);
                        var e2 = new Exception($"{lineNum} - {sqlCommand} <br/> {ex.Message}");
                        throw e2;
                    }
                }

                //while ((sqlLine = sr.ReadLine()) != null)
                //{
                //    sqlLine = sqlLine.Trim(); ++lineNum;

                //    if (sqlLine.Length > 0 && !sqlLine.StartsWith("--"))
                //    {
                //        sqlCommand += (sqlCommand.Length > 0 ? Environment.NewLine : "") + sqlLine;  // Accept multiline SQL

                //        if (sqlLine.StartsWith("/"))
                //        {
                //            if (linExec != 0)
                //            {
                //                sqlCommand = sqlCommand.Substring(linExec, sqlCommand.Length - 1);
                //            }
                //            else
                //                sqlCommand = sqlCommand.Substring(0, sqlCommand.Length - 1);

                //            var command = new OracleCommand(sqlCommand, connection);

                //            try
                //            {
                //                command.ExecuteNonQuery();
                //                linExec = sqlCommand.Length;
                //            }
                //            catch (OracleException ex)
                //            {
                //                connection.Close();
                //                var e2 = new Exception($"{lineNum} - {sqlCommand} <br/> {ex.Message}");
                //                throw e2;
                //            }
                //        }
                //    }
                //}

                connection.Close();

                return;
            }
        }
        private static DataTable GetMinMax(string table, string schema, string column, string connectionString1, DataSourceType DataSource)
        {
            DataTable dataTable = new DataTable();
            //string schema = ConfigurationManager.AppSettings["schema"];
            if (DataSource == DataSourceType.OracleServer)
            {
                using (OracleConnection oracleConnection = new OracleConnection(connectionString1))
                {
                    //string squery = $"Select Min(" { column} ") as Min, Max(" + column + ") as Max, Max(Length(" + column + ")) as MaxNvarchar  from " + table;
                    string squery = $"Select Min({table}.{column}) as Min, Max({table}.{column}) as Max, Min(Length({table}.{column})) as MinNvarChar, Max(Length({table}.{column})) as MaxNvarChar from {schema}.{ table}";                 
                    //System.Collections.Generic.IEnumerable<string> commandStrings = Regex.Split(squery, @"^\s*GO\s*$",
                    //RegexOptions.Multiline | RegexOptions.IgnoreCase);
                    try
                    {
                        oracleConnection.Open();
                    }
                    catch (Exception e)
                    {
                        Console.WriteLine("Exception occurs {0}", e.Message);
                        Console.WriteLine("Program will exit: Press ENTER to exist..");
                        Console.ReadLine();
                        File.WriteAllText(exceptionPath, e.Message + Environment.NewLine + Environment.NewLine);
                        System.Environment.Exit(1);
                    }

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

                            File.AppendAllText(exceptionPath, ex.Message + Environment.NewLine + Environment.NewLine);
                        }
                    }
                    //int FileSize = 0;
                    return dataTable;
                }
            }
            else if (DataSource == DataSourceType.SqlServer)
            {
                using (SqlConnection SqlConnection = new SqlConnection(connectionString1))
                {
                    string squery = $"Select Min([{table}].[{ column }]) as Min, Max([{table}].[{ column }]) as Max, Min(Len([{table}].[{ column }])) as MinNvarchar, Max(Len([{table}].[{ column }])) as MaxNvarchar  from [{schema}].[{table}]";
                    //System.Collections.Generic.IEnumerable<string> commandStrings = Regex.Split(squery, @"^\s*GO\s*$",
                    //RegexOptions.Multiline | RegexOptions.IgnoreCase);
                    try
                    {
                        SqlConnection.Open();
                    }
                    catch (Exception e)
                    {
                        Console.WriteLine("Exception occurs {0}", e.Message);
                        Console.WriteLine("Program will exit: Press ENTER to exist..");
                        Console.ReadLine();
                        File.WriteAllText(exceptionPath, e.Message + Environment.NewLine + Environment.NewLine);
                        System.Environment.Exit(1);
                    }

                    using (SqlDataAdapter oda = new SqlDataAdapter(squery, SqlConnection))
                    {
                        try
                        {
                            //Fill the data table with select statement's query results:
                            int recordsAffectedSubscriber = 0;
                            oda.SelectCommand.CommandTimeout = 0;
                            recordsAffectedSubscriber = oda.Fill(dataTable);

                        }
                        catch (Exception ex)
                        {

                            File.AppendAllText(exceptionPath, ex.Message + Environment.NewLine + Environment.NewLine);
                        }
                    }
                    //int FileSize = 0;
                    return dataTable;
                }
            }
            else if (DataSource == DataSourceType.PostgresServer)
            {
                using (NpgsqlConnection postgresConnection = new NpgsqlConnection(connectionString1))
                {
                    string squery = $"Select Min({column.AddDoubleQuotes()}) as Min,  Max({column.AddDoubleQuotes()}) as Max, '' as MinNvarchar, (select character_maximum_length from information_schema.columns  where table_schema = '{schema}' and column_name = '{column}' and table_name = '{table}') as MaxNvarchar  from {schema.AddDoubleQuotes()}.{table.AddDoubleQuotes()}";
                    //System.Collections.Generic.IEnumerable<string> commandStrings = Regex.Split(squery, @"^\s*GO\s*$",
                    //RegexOptions.Multiline | RegexOptions.IgnoreCase);
                    try
                    {
                        postgresConnection.Open();
                    }
                    catch (Exception e)
                    {

                        Console.WriteLine("Exception occurs {0}", e.Message);
                        Console.WriteLine("Program will exit: Press ENTER to exist..");
                        Console.ReadLine();
                        File.WriteAllText(exceptionPath, e.Message + Environment.NewLine + Environment.NewLine);
                        System.Environment.Exit(1);
                    }

                    using (NpgsqlDataAdapter oda = new NpgsqlDataAdapter(squery, postgresConnection))
                    {
                        try
                        {
                            //Fill the data table with select statement's query results:
                            int recordsAffectedSubscriber = 0;

                            recordsAffectedSubscriber = oda.Fill(dataTable);

                        }
                        catch (Exception ex)
                        {

                            File.AppendAllText(exceptionPath, ex.Message + Environment.NewLine + Environment.NewLine);
                        }
                    }
                    //int FileSize = 0;
                    return dataTable;
                }
            }
            else if (DataSource == DataSourceType.MySqlServer)
            {
                using (MySqlConnection mySqlConnection = new MySqlConnection(connectionString1))
                {
                    //string squery = $"Select Min({column}) as Min, Max({column}) as Max, (select character_maximum_length from information_schema.columns  where table_schema = '{schema}' and column_name = '{column}' and table_name = '{table}') as MaxNvarchar  from " + table;
                    string squery = $"Select Min(`{column}`) as Min, Max(`{column}`) as Max, Min(Length(`{column}`)) as MinNvarchar, Max(Length(`{column}`)) as MaxNvarchar  from `{schema}`.`{table}`"; 

                    //System.Collections.Generic.IEnumerable<string> commandStrings = Regex.Split(squery, @"^\s*GO\s*$",
                    //RegexOptions.Multiline | RegexOptions.IgnoreCase);
                    try
                    {
                        mySqlConnection.Open();
                    }
                    catch (Exception e)
                    {
                        Console.WriteLine("Exception occurs {0}", e.Message);
                        Console.WriteLine("Program will exit: Press ENTER to exist..");
                        Console.ReadLine();
                        File.WriteAllText(exceptionPath, e.Message + Environment.NewLine + Environment.NewLine);
                        System.Environment.Exit(1);
                    }

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

                            File.AppendAllText(exceptionPath, ex.Message + Environment.NewLine + Environment.NewLine);
                        }
                    }
                    //int FileSize = 0;
                    return dataTable;
                }
            }
            else
            {
                throw new ArgumentOutOfRangeException(nameof(datasource), datasource, "not implemented");
            }

        }
        public enum AppConfig
        {
            DatabaseName,
            OutputFilename,
            DataSourceType,
            SqlPath,
            ConnectionString,
            SendEmail,
            appServer,
            fromEmail,
            Recipients,
            AutoUpdate,
            CurrentVersionURL,
            CurrentInstallerURL
        }
        
        public enum DataSourceType
        {
            OracleServer,
            SqlServer,
            PostgresServer,
            MySqlServer,
            SpreadSheet,
            None
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
        public static bool CheckAppConfig()
        {
            bool flag = false;
            List<string> allKeys = new List<string>();
            foreach (string key in ConfigurationManager.AppSettings.AllKeys)
            {
                switch (key)
                {
                    case nameof(AppConfig.DatabaseName):
                        allkey.Add(key, ConfigurationManager.AppSettings[key].ToString());
                        break;
                    case nameof(AppConfig.ConnectionString):
                        allkey.Add(key, ConfigurationManager.AppSettings[key].ToString());
                        break;
                    case nameof(AppConfig.OutputFilename):
                        allkey.Add(key, ConfigurationManager.AppSettings[key].ToString());
                        break;
                    case nameof(AppConfig.DataSourceType):
                        allkey.Add(key, ConfigurationManager.AppSettings[key].ToString());
                        break;
                    case nameof(AppConfig.SqlPath):
                        allkey.Add(key, ConfigurationManager.AppSettings[key].ToString());
                        break;
                    case nameof(AppConfig.fromEmail):
                        allkey.Add(key, ConfigurationManager.AppSettings[key].ToString());
                        break;
                    case nameof(AppConfig.appServer):
                        allkey.Add(key, ConfigurationManager.AppSettings[key].ToString());
                        break;
                    case nameof(AppConfig.CurrentInstallerURL):
                        allkey.Add(key, ConfigurationManager.AppSettings[key].ToString());
                        break;
                    case nameof(AppConfig.CurrentVersionURL):
                        allkey.Add(key, ConfigurationManager.AppSettings[key].ToString());
                        break;
                    case nameof(AppConfig.SendEmail):
                        bool b = ConfigurationManager.AppSettings[key].ToString().ToUpper().Equals("YES") ? true : false;
                        allkey.Add(key, b);
                        break;
                    case nameof(AppConfig.AutoUpdate):
                        bool auto = ConfigurationManager.AppSettings[key].ToString().ToUpper().Equals("YES") ? true : false;
                        allkey.Add(key, auto);
                        break;
                    default:
                        break;
                }


            }
            if (allkey.Values.Where(n => n.Equals(string.Empty)).Count() != 0)
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
            if (allkey.Where(n => n.Key.ToUpper().Equals("SendEmail".ToUpper())).Select(n => n.Value).Select(n => n).ToArray().First().Equals(true))
            {
                if (string.IsNullOrEmpty(fromEmail)
                    || string.IsNullOrEmpty(Recipients) 
                    || string.IsNullOrEmpty(appServer))
                {
                    Console.WriteLine("Sending data classification SpreadSheet via email requires fromEmail, RecipientEmail and appServer address all to be set in the app.config");
                    return false;
                }
            }
            if (allkey.Where(n => n.Key.ToUpper().Equals(AutoUpdate.ToUpper())).Select(n => n.Value).Select(n => n).ToArray().First().Equals(true))
            {
                if (string.IsNullOrEmpty(CurrentVersionURL)
                    || string.IsNullOrEmpty(CurrentInstallerURL))
                {
                    Console.WriteLine("AutoUpdate Requires CurrentVersionURL and CurrentInstallerURL to be set in the app.config");
                    return false;
                }
            }
            return flag;
        }
        public static bool CheckAppConfigSpreadSheet()
        {
            bool flag = false;
            List<string> allKeys = new List<string>();
            foreach (string key in ConfigurationManager.AppSettings.AllKeys)
            {
                switch (key)
                {
                    case nameof(AppConfig.DatabaseName):
                        allkey.Add(key, ConfigurationManager.AppSettings[key].ToString());
                        break;
                    case nameof(AppConfig.ConnectionString):
                        allkey.Add(key, ConfigurationManager.AppSettings[key].ToString());
                        break;
                    case nameof(AppConfig.OutputFilename):
                        allkey.Add(key, ConfigurationManager.AppSettings[key].ToString());
                        break;
                    case nameof(AppConfig.DataSourceType):
                        allkey.Add(key, ConfigurationManager.AppSettings[key].ToString());
                        break;
                    case nameof(AppConfig.fromEmail):
                        allkey.Add(key, ConfigurationManager.AppSettings[key].ToString());
                        break;
                    case nameof(AppConfig.appServer):
                        allkey.Add(key, ConfigurationManager.AppSettings[key].ToString());
                        break; ;
                    case nameof(AppConfig.SendEmail):
                        bool b = ConfigurationManager.AppSettings[key].ToString().ToUpper().Equals("YES") ? true : false;
                        allkey.Add(key, b);
                        break;
                    default:
                        break;
                }


            }
            if (allkey.Values.Where(n => n.Equals(string.Empty)).Count() != 0)
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
            if (allkey.Where(n => n.Key.ToUpper().Equals("SendEmail".ToUpper())).Select(n => n.Value).Select(n => n).ToArray().First().Equals(true))
            {
                if (string.IsNullOrEmpty(fromEmail)
                    || string.IsNullOrEmpty(Recipients)
                    || string.IsNullOrEmpty(appServer))
                {
                    Console.WriteLine("Sending data classification SpreadSheet via email requires fromEmail, RecipientEmail and appServer address to be set in the app.config");
                    return false;
                }
            }
            return flag;
        }
        public static string ToHTML(DataTable dt, IList ts)
        {
            if (dt.Rows.Count == 0) return ""; // enter code here

            StringBuilder builder = new StringBuilder();
            builder.Append("<html>");

            builder.Append("<body>");
            builder.Append("<table border='1px' cellpadding='5' cellspacing='0' ");
            builder.Append("style='border: solid 1px Silver; font-size: x-small;'>");
            builder.Append("<html><style>BODY{font-family: Arial; font-size: 8pt;}H1{font-size: 22px; font-family: 'Segoe UI Light','Segoe UI','Lucida Grande',Verdana,Arial,Helvetica,sans-serif;}H2{font-size: 18px; font-family: 'Segoe UI Light','Segoe UI','Lucida Grande',Verdana,Arial,Helvetica,sans-serif;}H3{font-size: 16px; font-family: 'Segoe UI Light','Segoe UI','Lucida Grande',Verdana,Arial,Helvetica,sans-serif;}TABLE{border: 1px solid black; border-collapse: collapse; font-size: 8pt;}TH{border: 1px solid #969595; background: #dddddd; padding: 5px; color: #000000;}TD{border: 1px solid #969595; padding: 5px; }td.pass{background: #B7EB83;}td.warn{background: #FFF275;}td.fail{background: #FF2626; color: #ffffff;}td.info{background: #85D4FF;}</style><body>");
            builder.Append("<tr align='left' valign='top'>");
            builder.Append(" </ body ></ html > ");
            foreach (DataColumn c in dt.Columns)
            {
                builder.Append("<td align='left' valign='top'><b>");
                builder.Append(System.Web.HttpUtility.HtmlEncode(c.ColumnName));
                builder.Append("</b></td>");
            }
            builder.Append("<br/>");
            builder.Append("<br/>");
            builder.Append("</tr>");
            foreach (DataRow r in dt.Rows)
            {
                builder.Append("<tr align='left' valign='top'>");
                foreach (DataColumn c in dt.Columns)
                {
                    builder.Append("<td align='left' valign='top'>");
                    builder.Append(r[c.ColumnName]);
                    builder.Append("</td>");
                }
                builder.Append("</tr>");
            }
            builder.Append("</table>");
            StringBuilder sb = new StringBuilder();
            builder.Append("<table>");
            foreach (var item in ts)
            {
                builder.AppendFormat("<tr><td>{0}</td></tr>", item);
            }
            builder.Append("</table>");
            builder.Append("</body>");
            builder.Append("</html>");

            return builder.ToString();
        }

    }


}
