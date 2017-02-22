using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Runtime.InteropServices;
using System.Security.Cryptography;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Analyse
{
	public class Speed2
	{
		private static int NumCore = Environment.ProcessorCount;

		public static void Run()
		{
			ExecuteJob();
			Console.ReadKey();
		}

		private static void ExecuteJob()
		{
			var sw = Stopwatch.StartNew();

			var stage = Stopwatch.StartNew();
			var inputs = ExecuteStage1();
			Console.WriteLine("Time for stage 1: {0}s", (int)stage.Elapsed.TotalSeconds);

			stage = Stopwatch.StartNew();
			var result1 = ExecuteStage2(inputs);
			Console.WriteLine("Time for stage 2: {0}s", (int)stage.Elapsed.TotalSeconds);

			stage = Stopwatch.StartNew();
			var result2 = ExecuteStage3(inputs);
			Console.WriteLine("Time for stage 3: {0}s", (int)stage.Elapsed.TotalSeconds);

			Console.WriteLine("------ Les 10 pages Web les plus solicités :");
			foreach (var kvp in result1)
				Console.WriteLine("{0}: {1}", kvp.Key, kvp.Value);
			Console.WriteLine();

			Console.WriteLine("------ Nombre d'adresse IP uniques par mois :");
			foreach (var kvp in result2)
				Console.WriteLine("{0:2}: {1}", kvp.Key, kvp.Value);
			Console.WriteLine();

			Console.WriteLine("Job finished after {0} seconds. input.Count={1}", (int)sw.Elapsed.TotalSeconds, inputs.Length);
		}

		#region Stage 1
		private static string[][] ExecuteStage1()
		{
			var tasks = new Task<string[][]>[NumCore];
			for (int workerId = 0; workerId < NumCore; workerId++)
			{
				tasks[workerId] = Task.Factory.StartNew((workerId2) => ExecuteTask1((int)workerId2), workerId);
            }
			Task.WaitAll(tasks);

			var inputs = new List<string[]>(100000);
			foreach (var task in tasks)
			{
				inputs.AddRange(task.Result);
            }
			return inputs.ToArray();
		}

		private static string[][] ExecuteTask1(int workerId)
		{
			var sw = Stopwatch.StartNew();
			var fileName = string.Format("cache{0}.bin", workerId);
            var data = Utils.FromCache(fileName);
			if (data != null)
				return (string[][])data;

			var inputs = WholeTextTask(workerId);
			Utils.Cache(fileName, inputs);

			Console.WriteLine("WorkerId={0}. ExecuteTask finished after {1} seconds", workerId, (int)sw.Elapsed.TotalSeconds);
			return inputs;
		}

		private static string[][] WholeTextTask(int workerId)
		{
			var inputs = new List<string[]>(2000000);
			foreach (var file in GetFiles(workerId))
			{
				using (var sr = new StreamReader(file))
				{
					var line = sr.ReadLine();
					while (line != null)
					{
						var tokens = ParseLine(line);
						inputs.Add(tokens);
						line = sr.ReadLine();
					}
				}
			}
			return inputs.ToArray();
		}

		private static IEnumerable<string> GetFiles(int workerId)
		{
			var files = Directory.GetFiles("C:\\Logs", "*.log");

			var filesForThisWorker = new List<string>(files.Length % NumCore);
			using (var cipher = MD5.Create())
			{
				foreach (var file in files)
				{
					var bytes = Encoding.ASCII.GetBytes(file);
					var hash = cipher.ComputeHash(bytes);
					var hashVal = BitConverter.ToUInt64(hash, 0);
					var partition = (int)(hashVal % (ulong)NumCore);
					if (partition == workerId)
						filesForThisWorker.Add(file);
				}
			}

			//Console.WriteLine("WorkerId={0}. {1} files to analyze", workerId, filesForThisWorker.Count);
			return filesForThisWorker;
		}

		private static string[] ParseLine(string line)
		{
			var t = line.Split(' ');
			return t;
		}
		#endregion

		#region Stage 2
		private static KeyValuePair<string, int>[] ExecuteStage2(string[][] data)
		{
			var tasks = new Task<KeyValuePair<string, int>[]>[NumCore];
			for (int workerId = 0; workerId < NumCore; workerId++)
			{
				// Shuffle
				var blocks = data.Length / NumCore;
                var subData = new string[blocks][];
				Array.Copy(data, workerId * blocks, subData, 0, blocks);

				// Execute
				tasks[workerId] = Task.Factory.StartNew(
					(context) =>
					{
						var tuple = (Tuple<int, string[][]>)context;
						return ExecuteTask2(tuple.Item1, tuple.Item2);
					},
					Tuple.Create(workerId, subData));
			}
			Task.WaitAll(tasks);

			// Collect
			var inputs = new List<KeyValuePair<string, int>>(100000);
			foreach (var task in tasks)
			{
				inputs.AddRange(task.Result);
			}

			var result = inputs
			   .OrderByDescending(kvp => kvp.Value)
			   .Take(10)
			   .ToArray();

			Thread.Sleep(1000);
			return result;
		}


		private static KeyValuePair<string, int>[] ExecuteTask2(int workerId, string[][] data)
		{
			var result = data
			   .Where(tokens => tokens[Utils.IndexOfDomain] == "www.screenpresso.com")
			   .GroupBy(tokens => GetUrl(tokens[Utils.IndexOfUrl]), (key, values) => new KeyValuePair<string, int>(key, values.Count()))
			   .ToArray();

			Thread.Sleep(1000);
			return result;
		}

		private static string GetUrl(string url)
		{
			var index = url.IndexOf('?');
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
		private static KeyValuePair<string, int>[] ExecuteStage3(string[][] data)
		{
			var result = data
			   .Where(tokens => tokens[Utils.IndexOfDomain] == "www.screenpresso.com")
			   .GroupBy(tokens => Utils.GetMonth(tokens[Utils.IndexOfDate]))
			   .Select(lines => new KeyValuePair<string, int>(
				   lines.Key,
				   lines.GroupBy(token => token[Utils.IndexOfIpAddress]).Count()))
			   .OrderBy(kvp => kvp.Value)
			   .ToArray();

			Thread.Sleep(1000);
			return result;
		}
		#endregion
	}
}
