using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Text.RegularExpressions;

namespace FilesMerger
{
	class Program
	{
		private static Regex regex = new Regex(@"(\d+\.\d+\.\d+\.\d+) (.*) (.*) \[(.*)\] ""(.*)"" (\d+) (.*) ""(.*)"" ""(.*)""", RegexOptions.Compiled);

		static void Main(string[] args)
		{
			using (var sw = new StreamWriter("output.txt"))
			{
				var srcFolder = @"C:\Users\Benoist\Documents\Visual Studio 2013\Projects\OvhLogCollector\OvhLogCollector\bin\Release";
				foreach (var dir in Directory.EnumerateDirectories(srcFolder))
				{
					foreach (var file in Directory.EnumerateFiles(dir, "*.txt"))
					{
						var lines = File.ReadAllLines(file);
						foreach (var line in lines)
						{
							var tokens = regex.Split(line);
							if (tokens.Length != 11)
								continue;
							
							var dateTime = DateTime.ParseExact(tokens[4], "dd/MMM/yyyy:HH:mm:ss zzz", CultureInfo.InvariantCulture);
							tokens[4] = dateTime.ToString("yyyy-MM-dd");

							var csv = string.Join(";", tokens).Trim(';');
							sw.WriteLine(csv);						
						}
					}
				}
			}
		}
	}
}
