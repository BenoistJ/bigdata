using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading;


namespace Analyse
{
	public class Speed1
	{
		public static void Run()
		{
			//var inputs = WholeTextTask();
			//using (var sw = new StreamWriter(@"C:\Users\Benoist\Desktop\Logs\data.csv"))
			//{
			//	sw.WriteLine("clientIpAddress;domainName;remoteUser;dateTime;request;httpStatusCode;bytesSent;referer;userAgent");
			//	foreach (var line in inputs)
			//	{
			//		try
			//		{
			//			line[Utils.IndexOfDate+1] = Utils.GetMonth(line[Utils.IndexOfDate+1]);
			//			line[Utils.IndexOfUrl+1] = GetUrl(line[Utils.IndexOfUrl+1]);

			//			var csv = line.Aggregate((workingSentence, next) => workingSentence + ";" + next).Trim(';');
			//			sw.WriteLine(csv);
			//		}
			//		catch (Exception)
			//		{
			//			Debug.WriteLine("Exception");
			//		}
			//	}
			//}

			ExecuteJob();

			Console.WriteLine("Finished");
			//Console.ReadKey();
		}

		private static string[][] WholeTextTask()
		{
			var inputs = new List<string[]>(2000000);
			foreach (var file in Directory.GetFiles(@"C:\Users\Benoist\Desktop\Logs", "*.log"))
			{
				using (var sr = new StreamReader(file))
				{
					var line = sr.ReadLine();
					while (line != null)
					{
						var tokens = Utils.Parse(line);
						if (tokens != null && tokens.Length > 9)
							inputs.Add(tokens);
						line = sr.ReadLine();
					}
				}
			}
			return inputs.ToArray();
		}

		private static void ExecuteJob()
		{
			var sw = Stopwatch.StartNew();
			var stage = Stopwatch.StartNew();
			var inputs = ExecuteStage1();
			Console.WriteLine("Time for stage 1: {0}s", (int)stage.Elapsed.TotalSeconds);

			// Les 10 pages Web les plus visités
			stage = Stopwatch.StartNew();
			var result1 = ExecuteStage2(inputs);
			Console.WriteLine("Time for stage 2: {0}s", (int)stage.Elapsed.TotalSeconds);

			Console.WriteLine("------ Les 10 pages Web les plus solicités :");
			foreach (var kvp in result1)
				Console.WriteLine("{0}: {1}", kvp.Key, kvp.Value);
			Console.WriteLine();

			// Nombre d'ip unique par mois
			stage = Stopwatch.StartNew();
			var result2 = ExecuteStage3(inputs);
			Console.WriteLine("Time for stage 3: {0}s", (int)stage.Elapsed.TotalSeconds);

			Console.WriteLine("------ Nombre d'adresses IP uniques par mois :");
			foreach (var kvp in result2)
				Console.WriteLine("{0:2}: {1}", kvp.Key, kvp.Value);
			Console.WriteLine();

			Console.WriteLine("Job finished after {0} seconds. input.Count={1}", (int)sw.Elapsed.TotalSeconds, inputs.Length);
		}

		#region Stage 1
		private static string[][] ExecuteStage1()
		{
			//var fileName = "cache1.bin";
			//var data = Utils.FromCache(fileName);
			//if (data != null)
			//	return (string[][])data;

			var inputs = WholeCsvTask();
			//Utils.Cache(fileName, inputs);

			return inputs;
		}

		private static string[][] WholeCsvTask()
		{
			var inputs = new List<string[]>(2000000);
			foreach (var file in Directory.GetFiles(@"C:\Users\Benoist\Desktop\Logs", "*.csv"))
			{
				using (var sr = new StreamReader(file))
				{
					var line = sr.ReadLine();
					while (line != null)
					{
						var tokens = Utils.ParseCsv(line);
						if (tokens != null && tokens.Length > 9)
							inputs.Add(tokens);
						line = sr.ReadLine();
					}
				}
			}
			return inputs.ToArray();
		}
		#endregion

		#region Stage 2
		// Les 10 pages Web les plus visités
		private static KeyValuePair<string, int>[] ExecuteStage2(string[][] data)
		{
			var result = data
			   .Where(tokens => tokens[Utils.IndexOfDomain] == "www.screenpresso.com")
               .GroupBy(tokens => tokens[Utils.IndexOfUrl], (key, values) => new KeyValuePair<string, int>(key, values.Count()))
			   .OrderByDescending(kvp => kvp.Value)
			   .Take(10)
			   .ToArray();

			Thread.Sleep(1000);
			return result;
        }

		private static string GetUrl(string url)
		{
			var index= url.IndexOf('?');
			if (index == -1)
			{
				return url;
			}
			else
			{
				return url.Substring(0, index);
			}
		}
		#endregion

		#region Stage 3
		// Nombre d'ip unique par mois
		private static KeyValuePair<string, int>[] ExecuteStage3(string[][] data)
		{
			var result = data
			   .Where(tokens => tokens[Utils.IndexOfDomain] == "www.screenpresso.com")
			   .GroupBy(tokens => tokens[Utils.IndexOfDate])
			   .Select(lines => new KeyValuePair<string, int>(
				   lines.Key,
				   lines.GroupBy(token => token[Utils.IndexOfIpAddress]).Count()))
			   .OrderByDescending(kvp => kvp.Value)
			   .ToArray();

			Thread.Sleep(1000);
			return result;
		}
		#endregion
	}
}
