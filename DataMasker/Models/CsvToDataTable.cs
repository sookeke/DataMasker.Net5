using System;
using System.Collections.Generic;
using System.Data;
using System.Text.RegularExpressions;
using TextFieldParserStandard;

namespace DataMasker.Models
{
    public static class CsvToDataTable
    {
        private static TextFieldParser cvsReader;

        private static DataTable DataTableFromCsv(string csvPath)
        {
            DataTable dataTable = new DataTable();
            dataTable.Columns.Clear();
            dataTable.Rows.Clear();
            dataTable.Clear();

            List<string> allEmails = new List<string>();


            using (cvsReader = new TextFieldParser(csvPath))
            {
                cvsReader.SetDelimiters(new string[] { "," });

                //cvsReader.HasFieldsEnclosedInQuotes = true;
                //read column
                string[] colfield = cvsReader.ReadFields();
                //colfield
                //specila chra string
                string specialChar = @"\|!#$%&/()=?»«@£§€{}.-;'<>,";
                string repclace = @"_";
                repclace.ToCharArray();
                foreach (string column in colfield)
                {
                    foreach (var item in specialChar)
                    {
                        if (column.Contains(item))
                        {
                            column.Replace(item, repclace[0]);

                        }
                    }
                    DataColumn datacolumn = new DataColumn(column);
                    datacolumn.AllowDBNull = true;
                    var dcol = Regex.Replace(datacolumn.ColumnName, @"[^a-zA-Z0-9_.]+", "_");
                    dataTable.Columns.Add(dcol);


                }

                while (!cvsReader.EndOfData)
                {

                    try
                    {
                        string[] fieldData = cvsReader.ReadFields();
                        for (int i = 0; i < fieldData.Length; i++)
                        {
                            if (fieldData[i] == "")
                            {
                                fieldData[i] = null;
                            }


                        }



                        dataTable.Rows.Add(fieldData);
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine(ex.Message);
                        return null;
                    }




                }
            }
            return dataTable;
        }
    }
}
