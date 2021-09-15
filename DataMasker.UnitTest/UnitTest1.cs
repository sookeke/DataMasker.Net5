using DataMasker;
using DataMasker.Examples;
using DataMasker.Models;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System.Configuration;
using System.IO;

namespace DataMasker.Main.Tests
{
    [TestClass()]
    public class UnitTest1
    {
        [TestMethod()]
        public void CheckAppConfigTest()
        {
            // Program.CheckAppConfig()
            Assert.IsTrue(Program.CheckAppConfig());
        }

        [TestMethod()]
        public void LoadConfigTest()
        {
            Program.CheckAppConfig();
            //var hh = ConfigurationManager.OpenExeConfiguration("App.config");
            var _SpreadSheetPath = ConfigurationManager.AppSettings["ExcelSheetPath"];
            var copyjsonPath = ExcelToJson.ToJson(_SpreadSheetPath);
            Program.JsonConfig(copyjsonPath, _SpreadSheetPath);
            var config = Program.LoadConfig(1);
            Assert.IsTrue(config.Tables.Count > 0);
        }
        [TestMethod]
        public void LoadConfig()
        {
            Program.CheckAppConfig();
            var _SpreadSheetPath = ConfigurationManager.AppSettings["ExcelSheetPath"];
            var copyjsonPath = ExcelToJson.ToJson(_SpreadSheetPath);
            Program.JsonConfig(copyjsonPath, _SpreadSheetPath);
            var config = Program.LoadConfig(1);
            //var type = DataSourceProvider.Provide(config.DataSource.Type, config.DataSource);
            Assert.IsTrue(config != null);
        }
        public void ConvertExcelToJson()
        {
            Program.CheckAppConfig();
            var _SpreadSheetPath = ConfigurationManager.AppSettings["ExcelSheetPath"];
            var copyjsonPath = ExcelToJson.ToJson(_SpreadSheetPath);

            Assert.IsTrue(File.Exists(copyjsonPath));
        }
    }

}

