using Bogus.DataSets;
using DataMasker.Models;
using DataMasker.DataSources;
using System.Data;
using System.Collections.Generic;

namespace DataMasker.Interfaces
{
    public interface IDataGenerator
    {
        object GetValue(
            ColumnConfig columnConfig,
            object existingValue,
            string tableName,
            Name.Gender? gender);
        object GetValueShuffle(
            ColumnConfig columnConfig, string schema, string table, string column, IDataSource dataSources, IEnumerable<IDictionary<string, object>> dataTable,
            object existingValue,
            Name.Gender? gender);
        object GetBlobValue(ColumnConfig columnConfig, IDataSource dataSource, object existingValue,string FileNameWithExtension, FileTypes FileExtension, string blobLocation, Name.Gender? gender);
        object MathOperation(ColumnConfig columnConfig, object existingValue, object[] source, string operation, int factor);
        object GetAddress(ColumnConfig columnConfig, object existingValue, DataTable dataTable, bool isFulladdress = false);
        object OpenDatabaseAddress(ColumnConfig columnConfig, object existingValue, DataTable dataTable, bool isFulladdress = false);
    }
}
