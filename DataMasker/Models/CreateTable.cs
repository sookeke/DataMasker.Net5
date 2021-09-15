﻿using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DataMasker.Models
{
    /*
    For masking spreadhseet dataSet. 
    Version 1.1.0
    Author: Stanley Okeke
 */

    public static class CreateTable
    {
       public static DataTable SpreadSheetTable(IEnumerable<IDictionary<string, object>> parents)
        {
            var table = new DataTable();

            foreach (var parent in parents)
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
                    object[] columnRows;
                    if (!allEntries.TryGetValue(col.ColumnName, out columnRows))
                        continue;

                    for (int i = 0; i < addedRows.Length; i++)
                        table.Rows[addedRows[i]][col] = columnRows[i];
                }
            }

            return table;
        }
        public static IEnumerable<T> Repeat<T>(this IEnumerable<T> source, int times)
        {
            source = source.ToArray();
            return Enumerable.Range(0, times).SelectMany(_ => source);
        }

    }
   


}
