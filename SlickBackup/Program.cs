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
            StreamWriter logFile = File.CreateText(logFileName);


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

                        logFile.WriteLine("Starting Backup '" + backup.Title + "' on " + DateTime.Now.ToString());
                        logFile.WriteLine("    Source      '" + backup.Source + "'");
                        logFile.WriteLine("    Destination '" + backup.Destination + "'");
                        logFile.Flush();

                        engine.Log = (string line) => { logFile.WriteLine("  " + line); };
                        engine.Execute();

                        logFile.WriteLine("Finished '" + backup.Title + "' on " + DateTime.Now.ToString());
                        logFile.WriteLine("-------------------------------------------------------------------------------------");
                        logFile.Flush();
                    }
                }
                catch (Exception e)
                {
                    Console.WriteLine("EXCEPTION: " + e.ToString());
                }
                finally
                {
                    logFile.Close();
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
                Console.WriteLine("Title:           " + engine.Title.ToString().PadRight(80));
                Console.WriteLine("State:           " + engine.State.ToString().PadRight(80));
                Console.WriteLine("Progress Size:   " + MakeBar(engine.ProgressSize).PadRight(80));
                Console.WriteLine("Progress Files:  " + MakeBar(engine.ProgressFiles).PadRight(80));
                Console.WriteLine("");
                Console.WriteLine("Source:");
                Console.WriteLine("  Path:          " + CompressString(engine.SourceIndex.CurrentEntity).PadRight(80));
                Console.WriteLine("  Directories:   " + engine.SourceIndex.IndexedDirectories.ToString().PadRight(80));
                Console.WriteLine("  Files:         " + engine.SourceIndex.IndexedFiles.ToString().PadRight(80));
                Console.WriteLine("  Size:          " + FormatSize(engine.SourceIndex.IndexedSize).PadRight(80));
                Console.WriteLine("");
                Console.WriteLine("Destination:");
                Console.WriteLine("  Path:          " + CompressString(engine.DestinationIndex.CurrentEntity).PadRight(80));
                Console.WriteLine("  Directories:   " + engine.DestinationIndex.IndexedDirectories.ToString().PadRight(80));
                Console.WriteLine("  Files:         " + engine.DestinationIndex.IndexedFiles.ToString().PadRight(80));
                Console.WriteLine("  Size:          " + FormatSize(engine.DestinationIndex.IndexedSize).PadRight(80));
                Console.WriteLine("");
                Console.WriteLine("Queue:");
                Console.WriteLine("  Copy:          " + (engine.FilesToCopy + " (" + FormatSize(engine.SizeToCopy) + ")").PadRight(80));
                Console.WriteLine("  Update:        " + (engine.FilesToUpdate + " (" + FormatSize(engine.SizeToUpdate) + ")").PadRight(80));
                Console.WriteLine("  Delete:        " + (engine.FilesToDelete + " (" + FormatSize(engine.SizeToDelete) + ")").PadRight(80));
                Console.WriteLine("");
                Console.WriteLine("Done");
                Console.WriteLine("  Copy:          " + (engine.FilesCopied + " (" + FormatSize(engine.SizeCopied) + ")").PadRight(80));
                Console.WriteLine("  Update:        " + (engine.FilesUpdated + " (" + FormatSize(engine.SizeUpdated) + ")").PadRight(80));
                Console.WriteLine("  Delete:        " + (engine.FilesDeleted + " (" + FormatSize(engine.SizeDeleted) + ")").PadRight(80));
                Console.WriteLine("");
                Console.WriteLine("");

                var msgs = engine.Messages.Skip(Math.Max(0,engine.Messages.Count - 50)).ToArray();
                foreach (var msg in msgs.Where(l => l.StartsWith("[ERROR]") || l.StartsWith("DELETE") || l.StartsWith("[INFO]")).Reverse().Take(20))
                {
                    Console.WriteLine("  " + CompressString(msg, 140).PadRight(140));
                }
                Thread.Sleep(100);
                logFile.Flush();
            }

            foreach (var fail in engine.Messages)
            {
                Console.Error.WriteLine(fail);
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
