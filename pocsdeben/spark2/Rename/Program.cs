using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;

namespace ConsoleApplication4
{
	class Program
	{
		static void Main()
		{
			foreach (var filePath in Directory.GetFiles(".", "*.log"))
			{
				var fileName = Path.GetFileNameWithoutExtension(filePath);
				var tokens = fileName.Split('-');

				var newFileName = string.Format("log-{0}_{1}_{2}.log", tokens[3], tokens[2], tokens[1]);
				File.Move(filePath, newFileName);
			}
		}
	}
}
