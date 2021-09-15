using Microsoft.VisualStudio.TestTools.UnitTesting;
using DataMasker.DataSources;
using System;
using System.Collections.Generic;
using System.Text;
using DataMasker.Examples;
using System.Configuration;
using DataMasker.Models;
using DataMasker.Interfaces;
using System.Linq;

namespace DataMasker.DataSources.Tests
{
    [TestClass()]
    public class UnitTest1
    {
        [TestMethod()]
        public void GetCountTest()
        {
            Program.CheckAppConfig();
            var _SpreadSheetPath = ConfigurationManager.AppSettings["ExcelSheetPath"];
            var copyjsonPath = ExcelToJson.ToJson(_SpreadSheetPath);
            Program.JsonConfig(copyjsonPath, _SpreadSheetPath);
            var config = Program.LoadConfig(1);
            IDataSource dataSource = DataSourceProvider.Provide(config.DataSource.Type, config.DataSource);
            var tableConfig = config.Tables.FirstOrDefault();
            var getcount = dataSource.GetCount(tableConfig);
            Assert.IsTrue(getcount > 0);
           // Assert.Fail();
        }
    }
}