using Microsoft.VisualStudio.TestTools.UnitTesting;
using SlickBackup;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;

namespace TestBackup
{
    [TestClass]
    public class BackupTestClass
    {
        private Random Rnd = new Random();
        private char[] InvalidChars = Path.GetInvalidFileNameChars();
        private List<char> ValidChars = new();
        private string[] PrefixChars = new string[] { ".", "_", "#", "" };

        [TestInitialize]
        public void TestInit()
        {
            for (int num = 0; num < 255; num++)
            {
                if (!InvalidChars.Contains((char)num))
                {
                    ValidChars.Add((char)num);
                }
            }
        }

        private string GetRandomName(int length)
        {
            return "";
            StringBuilder ret = new();
            ret.Append(PrefixChars[Rnd.Next(0, PrefixChars.Length - 1)]);
            for (int num = 0; num < length; num++)
            {
                ret.Append(ValidChars[Rnd.Next(0, ValidChars.Count - 1)]);
            }
            return ret.ToString();
        }

        [TestMethod]
        public void TestFullCopy()
        {
            TestFullCopy(false);
        }

        [TestMethod]
        public void TestFullCopyRandomized()
        {
            TestFullCopy(true);
        }

        public void TestFullCopy(bool randomized)
        {
            string srcDir = Path.Combine(Path.GetTempPath(), "BackupSource");
            string dstDir = Path.Combine(Path.GetTempPath(), "BackupDestination");

            if (Directory.Exists(srcDir))
            {
                Directory.Delete(srcDir, true);
            }
            if (Directory.Exists(dstDir))
            {
                Directory.Delete(dstDir, true);
            }
            try
            {
                Directory.CreateDirectory(srcDir);
                Directory.CreateDirectory(dstDir);

                FillDirectory(srcDir, 10, 3, 10, "", randomized);

                BackupEngine e = new();
                e.SourceFolder = srcDir;
                e.DestinationFolder = dstDir;

                e.Execute();

                Assert.IsTrue(File.Exists(dstDir + Path.DirectorySeparatorChar + "_dst_cache.sbc"), "Missing cache file in destination folder");
                CompareDirectories(srcDir, dstDir);
            }
            finally
            {
                Directory.Delete(srcDir, true);
                Directory.Delete(dstDir, true);
            }
        }

        [TestMethod]
        public void TestDelete()
        {
            string srcDir = Path.Combine(Path.GetTempPath(), "BackupSource");
            string dstDir = Path.Combine(Path.GetTempPath(), "BackupDestination");

            if (Directory.Exists(srcDir))
            {
                Directory.Delete(srcDir, true);
            }
            if (Directory.Exists(dstDir))
            {
                Directory.Delete(dstDir, true);
            }
            try
            {
                Directory.CreateDirectory(srcDir);
                Directory.CreateDirectory(dstDir);

                FillDirectory(srcDir, 10, 3, 10, "", false);

                BackupEngine e = new();
                e.SourceFolder = srcDir;
                e.DestinationFolder = dstDir;

                e.Execute();

                Assert.IsTrue(File.Exists(dstDir + Path.DirectorySeparatorChar + "_dst_cache.sbc"), "Missing cache file in destination folder");
                CompareDirectories(srcDir, dstDir);

                DeleteRandom(srcDir);

                e.Execute();
                CompareDirectories(srcDir, dstDir);
            }
            finally
            {
                Directory.Delete(srcDir, true);
                Directory.Delete(dstDir, true);
            }
        }

        [TestMethod]
        public void TestUpdate()
        {
            string srcDir = Path.Combine(Path.GetTempPath(), "BackupSource");
            string dstDir = Path.Combine(Path.GetTempPath(), "BackupDestination");

            if (Directory.Exists(srcDir))
            {
                Directory.Delete(srcDir, true);
            }
            if (Directory.Exists(dstDir))
            {
                Directory.Delete(dstDir, true);
            }
            try
            {
                Directory.CreateDirectory(srcDir);
                Directory.CreateDirectory(dstDir);

                FillDirectory(srcDir, 10, 3, 10, "", false);

                BackupEngine e = new();
                e.SourceFolder = srcDir;
                e.DestinationFolder = dstDir;

                e.Execute();

                Assert.IsTrue(File.Exists(dstDir + Path.DirectorySeparatorChar + "_dst_cache.sbc"), "Missing cache file in destination folder");
                CompareDirectories(srcDir, dstDir);

                UpdateRandom(srcDir);

                e = new();
                e.SourceFolder = srcDir;
                e.DestinationFolder = dstDir;
                e.Execute();
                CompareDirectories(srcDir, dstDir);
            }
            finally
            {
                Directory.Delete(srcDir, true);
                Directory.Delete(dstDir, true);
            }
        }

        private void DeleteRandom(string dstDir, double probability = 0.01f)
        {
            foreach (var file in Directory.EnumerateFiles(dstDir))
            {
                if (Rnd.NextDouble() < probability)
                {
                    File.Delete(file);
                }
            }
            foreach (var dir in Directory.EnumerateDirectories(dstDir))
            {
                if (Rnd.NextDouble() < probability)
                {
                    Directory.Delete(dir, true);
                }
                else
                {
                    DeleteRandom(dir);
                }
            }
        }

        private void UpdateRandom(string dstDir, double probability = 0.01f)
        {
            foreach (var file in Directory.EnumerateFiles(dstDir))
            {
                if (Rnd.NextDouble() < probability)
                {
                    var f = File.AppendText(file);
                    f.WriteLine("Update" + Environment.NewLine);
                    f.Flush();
                    f.Close();
                }
            }
            foreach (var dir in Directory.EnumerateDirectories(dstDir))
            {
                UpdateRandom(dir);
            }
        }

        private void CompareDirectories(string srcDir, string dstDir)
        {
            Assert.IsTrue(Directory.Exists(srcDir), "Source folder does not exist");
            Assert.IsTrue(Directory.Exists(dstDir), "Destination folder does not exist");

            var srcFiles = Directory.EnumerateFiles(srcDir).Select(f => new FileInfo(f).Name);
            var dstFiles = Directory.EnumerateFiles(dstDir).Where(s => !s.EndsWith(".sbc")).Select(f => new FileInfo(f).Name);

            Assert.IsTrue(srcFiles.SequenceEqual(dstFiles), "File list not equal");

            var srcDirs = Directory.GetDirectories(srcDir).Select(f => new DirectoryInfo(f).Name);
            var dstDirs = Directory.GetDirectories(dstDir).Select(f => new DirectoryInfo(f).Name);

            Assert.IsTrue(srcDirs.SequenceEqual(dstDirs), "Directory list not equal");

            foreach(string file in srcFiles)
            {
                var srcInfo = new FileInfo(srcDir + Path.DirectorySeparatorChar + file);
                var dstInfo = new FileInfo(dstDir + Path.DirectorySeparatorChar + file);

                Assert.AreEqual(srcInfo.Length, dstInfo.Length, "File lengths have to match");
                Assert.AreEqual(srcInfo.LastWriteTime, dstInfo.LastWriteTime, "LastWriteTime have to match");
                Assert.AreEqual(srcInfo.Attributes, dstInfo.Attributes, "Attributes have to match");
            }

            foreach (string file in srcDirs)
            {
                var srcInfo = new FileInfo(srcDir + Path.DirectorySeparatorChar + file);
                var dstInfo = new FileInfo(dstDir + Path.DirectorySeparatorChar + file);

                CompareDirectories(srcInfo.FullName, dstInfo.FullName);
            }
        }

        private void FillDirectory(string srcDir, int dirCount, int depth, int fileCount, string prefix = "", bool randomize = false)
        {
            int thisDirs = randomize ? Rnd.Next(dirCount) : dirCount;
            int thisFiles = randomize ? Rnd.Next(fileCount) : fileCount;

            if (depth > 0)
            {
                for (int dir = 0; dir < thisDirs; dir++)
                {
                    string dirName = srcDir + Path.DirectorySeparatorChar + "Dir_" + prefix + dir;
                    Directory.CreateDirectory(dirName);
                    FillDirectory(dirName, dirCount, depth - 1, fileCount, prefix + "_", randomize);
                }
            }

            for (int file = 0; file < thisFiles; file++)
            {
                string fileName = srcDir + Path.DirectorySeparatorChar + "File_" + prefix + file;
                var writer = File.CreateText(fileName);

                writer.Write("Content of " + fileName + Environment.NewLine);
                writer.Flush();
                writer.Close();
            }
        }

        [TestMethod]
        public void TestMatchAlgorithm()
        {
            TestMatchAlgorithmRoutine(1000);
        }
        [TestMethod]
        public void TestMatchAlgorithmMulti ()
        {
            for (int num = 0; num < 1000; num++)
            {
                TestMatchAlgorithmRoutine(1000);
            }
        }

        [TestMethod]
        public void TestMatchAlgorithmFolder()
        {
            TestMatchAlgorithmRoutineFolders(20, 2);
        }


        public void TestMatchAlgorithmRoutine(int fileCount = 100)
        {
            List<BackupEngine.TreeNode> srcNodes = new();
            List<BackupEngine.TreeNode> dstNodes = new();
            for (int num = 0; num < fileCount; num++)
            {
                string name = GetRandomName(Rnd.Next(1, 64));
                srcNodes.Add(new BackupEngine.TreeNode() { Name = name + "_Matching_" + num, Type = BackupEngine.eType.File, LastChange = num, Length = num });
                dstNodes.Add(new BackupEngine.TreeNode() { Name = name + "_Matching_" + num, Type = BackupEngine.eType.File, LastChange = num, Length = num });
            }
            for (int num = 0; num < fileCount; num++)
            {
                string name = GetRandomName(Rnd.Next(1, 64));
                srcNodes.Add(new BackupEngine.TreeNode() { Name = name + "UpdateDate1_" + num, Type = BackupEngine.eType.File, LastChange = num + 1, Length = num });
                dstNodes.Add(new BackupEngine.TreeNode() { Name = name + "UpdateDate1_" + num, Type = BackupEngine.eType.File, LastChange = num, Length = num });
            }
            for (int num = 0; num < fileCount; num++)
            {
                string name = GetRandomName(Rnd.Next(1, 64));
                srcNodes.Add(new BackupEngine.TreeNode() { Name = name + "UpdateDate2_" + num, Type = BackupEngine.eType.File, LastChange = num, Length = num });
                dstNodes.Add(new BackupEngine.TreeNode() { Name = name + "UpdateDate2_" + num, Type = BackupEngine.eType.File, LastChange = num + 1, Length = num });
            }
            for (int num = 0; num < fileCount; num++)
            {
                string name = GetRandomName(Rnd.Next(1, 64));
                srcNodes.Add(new BackupEngine.TreeNode() { Name = name + "UpdateLength1_" + num, Type = BackupEngine.eType.File, LastChange = num, Length = num + 1 });
                dstNodes.Add(new BackupEngine.TreeNode() { Name = name + "UpdateLength1_" + num, Type = BackupEngine.eType.File, LastChange = num, Length = num });
            }
            for (int num = 0; num < fileCount; num++)
            {
                string name = GetRandomName(Rnd.Next(1, 64));
                srcNodes.Add(new BackupEngine.TreeNode() { Name = name + "UpdateLength2_" + num, Type = BackupEngine.eType.File, LastChange = num, Length = num });
                dstNodes.Add(new BackupEngine.TreeNode() { Name = name + "UpdateLength2_" + num, Type = BackupEngine.eType.File, LastChange = num, Length = num + 1 });
            }
            for (int num = 0; num < fileCount; num++)
            {
                string name = GetRandomName(Rnd.Next(1, 64));
                srcNodes.Add(new BackupEngine.TreeNode() { Name = name + "SourceOnly_" + num, Type = BackupEngine.eType.File, LastChange = num, Length = num });
            }
            for (int num = 0; num < fileCount; num++)
            {
                string name = GetRandomName(Rnd.Next(1, 64));
                dstNodes.Add(new BackupEngine.TreeNode() { Name = name + "DestinationOnly_" + num, Type = BackupEngine.eType.File, LastChange = num, Length = num });
            }

            BackupEngine e = new();
            BackupEngine.TreeNode srcRoot = new() { Children = srcNodes.ToArray() };
            BackupEngine.TreeNode dstRoot = new() { Children = dstNodes.ToArray() };

            Rnd.Shuffle(srcRoot.Children);
            Rnd.Shuffle(dstRoot.Children);

            e.MatchDirectory("source", srcRoot, "destination", dstRoot);

            Assert.IsTrue(e.FileDeleteQueue.Where(e => !e.Name.Contains("DestinationOly")).Any());
            Assert.AreEqual(fileCount, e.FileDeleteQueue.Count);

            Assert.AreEqual(0, e.FileCopyQueue.Where(e => !e.Key.StartsWith("source\\")).Count());
            Assert.AreEqual(0, e.FileCopyQueue.Where(e => !e.Value.StartsWith("destination\\")).Count());
            Assert.AreEqual(0, e.FileCopyQueue.Where(e => !e.Key.Contains("SourceOnly")).Count());
            Assert.AreEqual(0, e.FileCopyQueue.Where(e => !e.Value.Contains("SourceOnly")).Count());
            Assert.AreEqual(fileCount, e.FileCopyQueue.Count);

            Assert.AreEqual(0, e.FileUpdateQueue.Where(e => !e.Key.StartsWith("source\\")).Count());
            Assert.AreEqual(0, e.FileUpdateQueue.Where(e => !e.Value.StartsWith("destination\\")).Count());
            Assert.AreEqual(0, e.FileUpdateQueue.Where(e => !e.Key.Contains("Update")).Count());
            Assert.AreEqual(0, e.FileUpdateQueue.Where(e => !e.Value.Contains("Update")).Count());
            Assert.AreEqual(fileCount * 4, e.FileUpdateQueue.Count);
        }

        public void TestMatchAlgorithmRoutineFolders(int folderCount = 100, int fileCount = 10)
        {
            int folderCountUpdate = folderCount;
            int folderCountSourceOnly = folderCount + 1;
            int folderCountDestinationOnly = folderCount + 2;

            List<BackupEngine.TreeNode> srcFolders = new();
            List<BackupEngine.TreeNode> dstFolders = new();

            for (int folderNum = 0; folderNum < folderCountUpdate; folderNum++)
            {
                string name = GetRandomName(Rnd.Next(1, 64));

                List<BackupEngine.TreeNode> srcNodes = new();
                List<BackupEngine.TreeNode> dstNodes = new();

                for (int fileNum = 0; fileNum < fileCount; fileNum++)
                {
                    string fileName = GetRandomName(Rnd.Next(1, 64));
                    srcNodes.Add(new BackupEngine.TreeNode() { Name = fileName + "_UpdateFile_" + fileNum, Type = BackupEngine.eType.File, LastChange = folderNum + 1, Length = folderNum });
                    dstNodes.Add(new BackupEngine.TreeNode() { Name = fileName + "_UpdateFile_" + fileNum, Type = BackupEngine.eType.File, LastChange = folderNum, Length = folderNum });
                }

                BackupEngine.TreeNode srcFolder = new BackupEngine.TreeNode() { Name = name + "_MatchingDir_" + folderNum, Type = BackupEngine.eType.Directory, LastChange = folderNum, Length = folderNum };
                BackupEngine.TreeNode dstFolder = new BackupEngine.TreeNode() { Name = name + "_MatchingDir_" + folderNum, Type = BackupEngine.eType.Directory, LastChange = folderNum, Length = folderNum };
                srcFolder.Children = srcNodes.ToArray();
                dstFolder.Children = dstNodes.ToArray();

                Rnd.Shuffle(srcFolder.Children);
                Rnd.Shuffle(dstFolder.Children);

                srcFolders.Add(srcFolder);
                dstFolders.Add(dstFolder);
            }
            for (int folderNum = 0; folderNum < folderCountSourceOnly; folderNum++)
            {
                string name = GetRandomName(Rnd.Next(1, 64));

                List<BackupEngine.TreeNode> srcNodes = new();

                for (int fileNum = 0; fileNum < fileCount; fileNum++)
                {
                    string fileName = GetRandomName(Rnd.Next(1, 64));
                    srcNodes.Add(new BackupEngine.TreeNode() { Name = fileName + "_SourceOnlyFile_" + fileNum, Type = BackupEngine.eType.File, LastChange = folderNum + 1, Length = folderNum });
                }

                BackupEngine.TreeNode srcFolder = new BackupEngine.TreeNode() { Name = name + "_SourceOnlyDir_" + folderNum, Type = BackupEngine.eType.Directory, LastChange = folderNum, Length = folderNum };
                srcFolder.Children = srcNodes.ToArray();

                Rnd.Shuffle(srcFolder.Children);

                srcFolders.Add(srcFolder);
            }
            for (int folderNum = 0; folderNum < folderCountDestinationOnly; folderNum++)
            {
                string name = GetRandomName(Rnd.Next(1, 64));

                List<BackupEngine.TreeNode> dstNodes = new();

                for (int fileNum = 0; fileNum < fileCount; fileNum++)
                {
                    string fileName = GetRandomName(Rnd.Next(1, 64));
                    dstNodes.Add(new BackupEngine.TreeNode() { Name = fileName + "_DestinationOnlyFile_" + fileNum, Type = BackupEngine.eType.File, LastChange = folderNum, Length = folderNum });
                }

                BackupEngine.TreeNode dstFolder = new BackupEngine.TreeNode() { Name = name + "_DestinationOnlyDir_" + folderNum, Type = BackupEngine.eType.Directory, LastChange = folderNum, Length = folderNum };
                dstFolder.Children = dstNodes.ToArray();

                Rnd.Shuffle(dstFolder.Children);

                dstFolders.Add(dstFolder);
            }

            BackupEngine e = new();
            BackupEngine.TreeNode srcRoot = new() { Children = srcFolders.ToArray() };
            BackupEngine.TreeNode dstRoot = new() { Children = dstFolders.ToArray() };

            Rnd.Shuffle(srcRoot.Children);
            Rnd.Shuffle(dstRoot.Children);

            e.MatchDirectory("source", srcRoot, "destination", dstRoot);

            Assert.IsTrue(e.FileDeleteQueue.Where(e => !e.Name.Contains("_DestinationOnlyFile_")).Any());
            Assert.AreEqual(folderCountDestinationOnly, e.FileDeleteQueue.Count);
            Assert.AreEqual(folderCountDestinationOnly * fileCount, e.FilesToDelete);

            Assert.AreEqual(0, e.FileCopyQueue.Where(e => !e.Key.StartsWith("source\\")).Count());
            Assert.AreEqual(0, e.FileCopyQueue.Where(e => !e.Value.StartsWith("destination\\")).Count());
            Assert.AreEqual(folderCountSourceOnly, e.FileCopyQueue.Where(e => !e.Key.Contains("_SourceOnlyFile_")).Count());
            Assert.AreEqual(folderCountSourceOnly, e.FileCopyQueue.Where(e => !e.Value.Contains("_SourceOnlyFile_")).Count());
            Assert.AreEqual(folderCountSourceOnly * fileCount + folderCountSourceOnly, e.FileCopyQueue.Count);
            Assert.AreEqual(folderCountSourceOnly * fileCount + folderCountSourceOnly, e.FilesToCopy);


            Assert.AreEqual(0, e.FileUpdateQueue.Where(e => !e.Key.StartsWith("source\\")).Count());
            Assert.AreEqual(0, e.FileUpdateQueue.Where(e => !e.Value.StartsWith("destination\\")).Count());
            Assert.AreEqual(0, e.FileUpdateQueue.Where(e => !e.Key.Contains("_UpdateFile_")).Count());
            Assert.AreEqual(0, e.FileUpdateQueue.Where(e => !e.Value.Contains("_UpdateFile_")).Count());
            Assert.AreEqual(folderCount * fileCount, e.FileUpdateQueue.Count);
            Assert.AreEqual(folderCount * fileCount, e.FilesToUpdate);

        }

    }
}
