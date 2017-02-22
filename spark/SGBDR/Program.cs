using System;
using System.Data;
using System.Data.SqlClient;
using System.Diagnostics;
using System.IO;

namespace SGBDR
{
    class Program
    {
        const string CsvFile = @"D:\data.csv";
        const string ConnectionString = "Server=(local);Database=loglight;Trusted_Connection=True";

        static void Main(string[] args)
        {
            using (var conn = new SqlConnection(ConnectionString))
            {
                conn.Open();

                try {
                    var command = new SqlCommand("DELETE TABLE light", conn);
                    var result = command.ExecuteNonQuery();
                } catch (Exception) { }
                try {
                    var command = new SqlCommand("CREATE TABLE light (clientIpAddress VARCHAR(500),domainName VARCHAR(500),remoteUser VARCHAR(500),monthDate VARCHAR(500),request VARCHAR(MAX),httpStatusCode VARCHAR(MAX),bytesSent VARCHAR(MAX),referer VARCHAR(MAX),userAgent VARCHAR(5))", conn);
                    var result = command.ExecuteNonQuery();

                    command = new SqlCommand("CREATE INDEX i1 ON light (domainName)", conn);
                    result = command.ExecuteNonQuery();

                    command = new SqlCommand("CREATE INDEX i2 ON light (monthDate)", conn);
                    result = command.ExecuteNonQuery();
                } catch (Exception) { }

                using (var bulkCopy = new SqlBulkCopy(conn))
                {
                    bulkCopy.DestinationTableName = "light";
                    bulkCopy.BulkCopyTimeout = 0;

                    using (var sr = new StreamReader(CsvFile))
                    {
                        var dataTable = new DataTable();
                        var dataColumns = dataTable.Columns;
                        var columns = sr.ReadLine().Split(';');
                        foreach (var column in columns)
                            dataColumns.Add(column);

                        const int BlockSize = 300000;
                        int i = 0;
                        var sw = Stopwatch.StartNew();
                        LoadCSV(sr, dataTable, BlockSize);
                        while (dataTable.Rows.Count > 0)
                        {
                            Console.Write("-");
                            bulkCopy.WriteToServer(dataTable);
                            i += BlockSize;
                            LoadCSV(sr, dataTable, BlockSize);
                        }
                        sw.Stop();
                        Console.WriteLine("Finished. Elapsed time: " + sw.Elapsed);
                    }
                }
            }

            Console.ReadKey();
        }

        // http://www.codeproject.com/Articles/30705/C-CSV-Import-Export
        private static DataTable LoadCSV(StreamReader sr, DataTable dataTable, int numberOfRows)
        {
            var dataRows = dataTable.Rows;
            dataRows.Clear();

            for (int i = 0; i < numberOfRows; i++)
            {
                var line = sr.ReadLine();
                var backup = line;
                if (line == null)
                    break;
                var index = line.IndexOf('(');
                if (index > 0)
                    line = line.Substring(0, index);
                var tokens = line.Split(';');
                if (tokens.Length == 9)
                {
                    dataRows.Add(tokens);
                }
                else if (tokens.Length > 9)
                {
                }
                else
                {
                    var tokens2 = new string[9];
                    for (int j = 0; j < 9; j++)
                    {
                        if (j < tokens.Length)
                            tokens2[j] = tokens[j];
                    }
                    dataRows.Add(tokens2);
                }
            }
           
            return dataTable;
        }
    }
}
