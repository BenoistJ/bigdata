using System;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.IO.Compression;
using System.Runtime.Serialization.Formatters.Binary;
using System.Text.RegularExpressions;

namespace Analyse
{
	public static class Utils
	{
		public const int IndexOfIpAddress = 0;
		public const int IndexOfDomain = 1;
		public const int IndexOfDate = 3;
		public const int IndexOfUrl = 4;

        private static Regex regex = new Regex(@"(\d+\.\d+\.\d+\.\d+) (.*) (.*) \[(.*)\] ""(.*)"" (\d+) (.*) ""(.*)"" ""(.*)""", RegexOptions.Compiled);

		public static string[] Parse(string line)
		{
			var aa = regex.Split(line);
			return aa;
        }

		public static string[] ParseCsv(string line)
		{
			var aa = line.Split(';');
			return aa;
		}

		public static string GetMonth(string date)
		{
			var dateTime = DateTime.ParseExact(date, "dd/MMM/yyyy:HH:mm:ss zzz", CultureInfo.InvariantCulture);
			return dateTime.ToString("MMMM yyyy");
		}

		public static void Cache(string filePath, object data)
		{
			filePath += ".gz";
			var formatter = new BinaryFormatter();
			using (var fs = File.Create(filePath))
			using (var cs = new GZipStream(fs, CompressionMode.Compress))
			{
				formatter.Serialize(cs, data);
			}
		}

		public static object FromCache(string filePath)
		{
			filePath += ".gz";
            if (!File.Exists(filePath))
			{
				return null;
			}

			var formatter = new BinaryFormatter();
			using (var fs = File.OpenRead(filePath))
			using (var cs = new GZipStream(fs, CompressionMode.Decompress))
			{
				return formatter.Deserialize(cs);
			}
		}
	}
}
