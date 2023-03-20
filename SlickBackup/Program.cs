using System;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Runtime.Serialization;
using System.Text;
using System.Threading;
using System.Xml.Serialization;

[assembly: InternalsVisibleTo("TestBackup.BackupTestClass")]
[assembly: InternalsVisibleTo("TestBackup")]
[assembly: InternalsVisibleTo("BackupTestClass")]
namespace SlickBackup
{
    public class Program
    {
        private static StreamWriter LogFile = null;

        public class Backup
        {
            public string Title;
            public string Source;
            public string Destination;
            public string IgnoreList;
            public int Reindex = 10;
            public int AutoSave = 600;
        }

        public class BackupConfig
        {
            public Backup[] Backups;
        }


        static void Main(string[] args)
        {
            BackupConfig config = null;
            BackupEngine engine = null;
            Thread scanThread = null;
            bool done = false;
            string logFileName = "SlickBackup_" + DateTime.Now.ToString("yyyy.MM.dd_HH.mm.ss") + ".log";

            LogFile = File.CreateText(logFileName);

            Console.CancelKeyPress += delegate (object? sender, ConsoleCancelEventArgs e)
            {
                e.Cancel = true;

                if (engine != null)
                {
                    engine.SaveCache();
                }

                done = true;
            };

            try
            {
                using (var stream = File.OpenRead("slick.cfg"))
                {
                    XmlSerializer ser = new XmlSerializer(typeof(BackupConfig));
                    config = ser.Deserialize(stream) as BackupConfig;
                }
            }
            catch (Exception ex)
            {
                config = new BackupConfig();
                config.Backups = new Backup[1];
                config.Backups[0] = new Backup() { Title = "Test", Source = "none", Destination = "none" };

                using (var stream = File.OpenWrite("slick.cfg.example"))
                {
                    XmlSerializer ser = new XmlSerializer(typeof(BackupConfig));
                    ser.Serialize(stream, config);
                }

                return;
            }

            scanThread = new Thread(() =>
            {

                try
                {
                    for (int pos = 0; pos < config.Backups.Length; pos++)
                    {
                        var backup = config.Backups[pos];

                        engine = new BackupEngine()
                        {
                            Title = "(" + (pos + 1) + "/" + config.Backups.Length + ") " + backup.Title,
                            SourceFolder = backup.Source,
                            DestinationFolder = backup.Destination,
                            CacheUpdateCounterMax = backup.Reindex,
                            AutoSaveTime = backup.AutoSave
                        };

                        if (backup.IgnoreList != null)
                        {
                            engine.IgnoreList.AddRange(backup.IgnoreList.Replace(";", ",").Split(',').Select(s => s.Trim()));
                        }

                        LogFile.WriteLine("Starting Backup '{0}' on {1}", backup.Title, DateTime.Now.ToString());
                        LogFile.WriteLine("    Source      '{0}'", backup.Source);
                        LogFile.WriteLine("    Destination '{0}'", backup.Destination);
                        LogFile.Flush();

                        engine.Log = LogFunc;
                        engine.Execute();

                        LogFile.WriteLine("Finished '{0}' on {1}", backup.Title, DateTime.Now.ToString());
                        LogFile.WriteLine("-------------------------------------------------------------------------------------");
                        LogFile.Flush();
                    }
                }
                catch (Exception e)
                {
                    Console.WriteLine("EXCEPTION: {0}", e.ToString());
                }
                done = true;
            });

            scanThread.Start();

            while (!done)
            {
                if(engine == null)
                {
                    continue;
                }
                Console.SetCursorPosition(0, 0);
                Console.WriteLine("");
                Console.WriteLine("Title:           {0,-80}", engine.Title);
                Console.WriteLine("State:           {0,-80}", engine.State);
                Console.WriteLine("Progress Size:   {0,-80}", MakeBar(engine.ProgressSize));
                Console.WriteLine("Progress Files:  {0,-80}", MakeBar(engine.ProgressFiles));
                Console.WriteLine("");
                Console.WriteLine("Source:");
                Console.WriteLine("  Path:          {0,-80}", CompressString(engine.SourceIndex.CurrentEntity));
                Console.WriteLine("  Directories:   {0,-80}", engine.SourceIndex.IndexedDirectories);
                Console.WriteLine("  Files:         {0,-80}", engine.SourceIndex.IndexedFiles);
                Console.WriteLine("  Size:          {0,-80}", FormatSize(engine.SourceIndex.IndexedSize));
                Console.WriteLine("");
                Console.WriteLine("Destination:");
                Console.WriteLine("  Path:          {0,-80}", CompressString(engine.DestinationIndex.CurrentEntity));
                Console.WriteLine("  Directories:   {0,-80}", engine.DestinationIndex.IndexedDirectories);
                Console.WriteLine("  Files:         {0,-80}", engine.DestinationIndex.IndexedFiles);
                Console.WriteLine("  Size:          {0,-80}", FormatSize(engine.DestinationIndex.IndexedSize));
                Console.WriteLine("");
                Console.WriteLine("Queue:");
                Console.WriteLine("  Copy:          {0} ({1})          ", engine.FilesToCopy, FormatSize(engine.SizeToCopy));
                Console.WriteLine("  Update:        {0} ({1})          ", engine.FilesToUpdate, FormatSize(engine.SizeToUpdate));
                Console.WriteLine("  Delete:        {0} ({1})          ", engine.FilesToDelete, FormatSize(engine.SizeToDelete));
                Console.WriteLine("");
                Console.WriteLine("Done");
                Console.WriteLine("  Copy:          {0} ({1})          ", engine.FilesCopied, FormatSize(engine.SizeCopied));
                Console.WriteLine("  Update:        {0} ({1})          ", engine.FilesUpdated, FormatSize(engine.SizeUpdated));
                Console.WriteLine("  Delete:        {0} ({1})          ", engine.FilesDeleted, FormatSize(engine.SizeDeleted));
                Console.WriteLine("");
                Console.WriteLine("");

                var msgs = engine.Messages.Skip(Math.Max(0,engine.Messages.Count - 50)).ToArray();
                foreach (var msg in msgs.Where(l => l.StartsWith("[ERROR]") || l.StartsWith("DELETE") || l.StartsWith("[INFO]")).Reverse().Take(20))
                {
                    Console.WriteLine("  {0}", CompressString(msg, 140).PadRight(140));
                }
                Thread.Sleep(100);
                LogFile.Flush();
            }

            foreach (var fail in engine.Messages)
            {
                Console.Error.WriteLine(fail);
            }
            LogFile.Close();
        }

        private static void LogFunc(string line)
        {
            lock (LogFile)
            {
                try
                {
                    LogFile.WriteLine("  {0}", line);
                }
                catch (Exception ex)
                {
                    LogFile.WriteLine("  Exception when adding log entry: ", ex.Message);
                }
            }
        }

        private static string MakeBar(decimal progress, int width = 30)
        {
            //char[] parts = new char[] { ' ', '▏', '▎', '▍', '▌', '▋', '▊', '▉', '█' };
            char[] parts = new char[] { ' ', ' ', ' ', ' ', '▌', '▌', '▌', '▌', '█' };
            byte[] bar = new byte[width];
            int whole_width = (int)Math.Floor(progress * width);
            decimal remainder_width = (progress * width) % 1.0m;

            int part_width = (int)Math.Floor(remainder_width * 8);
            string part_char = "" + parts[part_width];
            
            if ((width - whole_width - 1) < 0)
            {
                part_char = "";
            }

            int emptyChars = width - whole_width - 1;
            string line = "|" + new string('█', whole_width) + part_char + ((emptyChars > 0) ? new string(' ', emptyChars) : "") + "| " + (progress * 100).ToString("0.00") + "%";
            return line;
        }

        private static string CompressString(string str, int maxLength = 80)
        {
            if(str == null)
            {
                return "";
            }

            int maxPart = maxLength / 2 - 1;
            if (str.Length < maxLength)
            {
                return str;
            }
            return str.Substring(0, maxPart) + ".." + str.Substring(str.Length - maxPart, maxPart);
        }

        private static string FormatSize(decimal size)
        {
            string[] units = new[] { "Byte", "KiB", "MiB", "GiB", "TiB" };
            int unit = 0;

            while(size > 1024 && unit < units.Length - 1)
            {
                size /= 1024;
                unit++;
            }

            return size.ToString("0.00") + " " + units[unit];
        }
    }
}
