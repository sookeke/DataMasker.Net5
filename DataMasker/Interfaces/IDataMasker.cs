using System.Collections.Generic;
using System.Data;
using DataMasker.Models;

namespace DataMasker.Interfaces
{
    /// <summary>
    /// IDataMasker
    /// </summary>
    public interface IDataMasker
    {

        /// <summary>
        /// Masks the specified object with new data
        /// </summary>
        /// <param name="obj">The object to mask</param>
        /// <param name="tableConfig">The table configuration.</param>
        /// <returns></returns>
        IDictionary<string, object> Mask(
            IDictionary<string, object> obj,
            TableConfig tableConfig, IDataSource dataSource, int rowCount, IEnumerable<IDictionary<string,object>> data, DataTable _dataTable = null);
        IDictionary<string, object> MaskBLOB(
            IDictionary<string, object> obj,
            TableConfig tableConfig, IDataSource dataSource, IEnumerable<IDictionary<string, object>> data, string FileNameWithExtension, FileTypes fileExtension, string blobLocation);
    }
}
