using System;
using System.IO;
using System.Net;
using System.Net.Security;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using System.Threading.Tasks;

namespace OvhLogCollector
{
	class Program
	{
		static void Main(string[] args)
		{
			ServicePointManager.ServerCertificateValidationCallback = delegate(object s, X509Certificate certificate, X509Chain chain, SslPolicyErrors sslPolicyErrors)
				{
					return true; 
				};

			ulong stats = 0;
			ulong www = 0;
		
			var month = 1;
			var year = 2016;
			for (int i = 0; i < 12; i++)
			{
				var folderName = string.Format("https://logs.ovh.net/screenpresso.com/logs-{0}-{1}", month.ToString("00"), year.ToString("0000"));
				Console.WriteLine(folderName);

				var days = new string[31];
				for(int day = 0; day < days.Length; day++)
				{
					var uri = string.Format("{0}/screenpresso.com-{1}-{2}-{3}.log.gz", folderName, (day+1).ToString("00"), month.ToString("00"), year.ToString("0000"));
					try
					{
						var req = (HttpWebRequest)WebRequest.Create(uri);
						req.Accept = "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8";
						req.AutomaticDecompression = DecompressionMethods.Deflate | DecompressionMethods.GZip;
						req.KeepAlive = true;
						req.Timeout = 180 * 1000;
						req.Headers.Add("Accept-Encoding", "gzip, deflate");
						req.Headers.Add("Cookie", "token=eyJhbGciOiJIUzI1NiJ9.eyJleHAiOjE0NjcxMTc5NzIsImRvbSI6InNjcmVlbnByZXNzby5jb20iLCJpcHY0IjoiMTkzLjI1Mi4yMDEuMTEwIiwiaWF0IjoxNDY3MTE0MzcyLCJhdWQiOiJvdmhsb2dzYXBwIn0.N9DRuNC2G6nzaeyipAiCAKcCj9ac3jSXIOPLQ3VkEIU");
							
						var rep = req.GetResponse();
						using (var stream = rep.GetResponseStream())
						using (var sr = new StreamReader(stream))
						{
							var filename = string.Format("{0}{1}{2}.txt", year.ToString("0000"), month.ToString("00"), (day + 1).ToString("00"));
							using (var sw = new StreamWriter(filename))
							{
								var line = sr.ReadLine();
								while (line != null)
								{
									if (line.Contains("stats.screenpresso.com"))
									{
										stats++;
									}
									else
									{
										www++;
										sw.WriteLine(line.Replace("screenpresso", "mydemo"));
									}

									line = sr.ReadLine();
								}
							}
						}

						Console.Write("{0} ", day + 1);
					}
					catch (Exception ex)
					{
						Console.WriteLine(uri + " " + ex.Message);
						System.Threading.Thread.Sleep(4000);
					}
				}

				Console.WriteLine();
				Console.WriteLine("stats={0}, www={1}, ratio={2}", stats, www, stats / (double)www);
				month++;
				if (month > 12)
				{
					month = 1;
					year++;
				}

				Console.WriteLine();
				System.Threading.Thread.Sleep(1000);
			}

			Console.WriteLine("Finished");
			Console.ReadLine();
		}
	}
}
