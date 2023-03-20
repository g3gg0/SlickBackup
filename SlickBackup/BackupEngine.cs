using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.Serialization;
using System.Runtime.Serialization.Formatters.Binary;
using System.Security.Cryptography;
using System.Text;
using System.Threading;

using System.Text.Json;
using System.Text.Json.Serialization;
using System.Xml.Serialization;
using System.ComponentModel;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;
using System.Reflection.Metadata.Ecma335;
using System.IO.Compression;

namespace SlickBackup
{
    public class BackupEngine
    {
        public int AutoSaveTime = 10 * 60;
        public int CacheUpdateCounterMax = 10;

        private static readonly DateTime UnixEpoch = new DateTime(1970, 1, 1, 0, 0, 0);
        internal string Title = "";
        internal string SourceFolder = "";
        internal string DestinationFolder = "";

        internal TreeInfo SourceIndex = new();
        internal TreeInfo DestinationIndex = new();

        internal Dictionary<string, string> FileUpdateQueue = new();
        internal Dictionary<string, string> FileCopyQueue = new();
        internal List<TreeNode> FileDeleteQueue = new();

        internal long SizeToCopy = 0;
        internal long SizeToUpdate = 0;
        internal long SizeToDelete = 0;
        internal long SizeDeleted = 0;
        internal long SizeCopied = 0;
        internal long SizeUpdated = 0;
        internal long FilesCopied = 0;
        internal long FilesUpdated = 0;
        internal long FilesDeleted = 0;
        internal long FilesToCopy = 0;
        internal long FilesToUpdate = 0;
        internal long FilesToDelete = 0;

        internal long DirectoriesCopied = 0;

        internal eState State = eState.Init;
        private readonly SHA256 Sha256 = SHA256.Create();

        internal List<string> Messages = new();
        internal List<string> IgnoreList = new();
        private FileStream LockFileHandle;
        internal ParallelOptions ParallelOptions = new();
        internal Action<string> Log;

        public BackupEngine()
        {
            IgnoreList.Add(".sbc$");

            int maxPar = (int)Math.Ceiling((Environment.ProcessorCount * 0.75));
            ParallelOptions.MaxDegreeOfParallelism = maxPar;
        }

        public enum eState
        {
            Init,
            Scan,
            Match,
            Copy,
            Update,
            Delete,
            Done
        }

        public enum eType : int
        {
            Directory = 0,
            File = 1,
            Special = 2
        }

        [Serializable]
        public class TreeInfo
        {
            public TreeNode Root;
            public long IndexedDirectories;
            public long IndexedFiles;
            public long CacheUpdateCounter;
            public decimal IndexedSize;
            internal string CurrentEntity;

            [JsonIgnore]
            internal bool Modified = false;
            [JsonIgnore]
            internal DateTime LastSaveTime;
            [JsonIgnore]
            internal Thread SaveThread;
        }

        [Serializable]
        public class TreeNode
        {
            public string Name;
            public long Length;

            [JsonIgnore]
            public long SizeRecursive
            {
                get
                {
                    if (Type == eType.Directory)
                    {
                        return Children.Sum(c => c.SizeRecursive);
                    }
                    return Length;
                }
            }

            [JsonIgnore]
            public long FilesRecursive
            {
                get
                {
                    if (Type == eType.Directory)
                    {
                        return Children.Sum(c => c.FilesRecursive);
                    }
                    return 1;
                }
            }

            public eType Type;
            public long LastChange;
            public TreeNode[] Children = Array.Empty<TreeNode>();
            [JsonIgnore]
            internal string FullPath = "";

            public override string ToString()
            {
                return Name;
            }

            public TreeNode this[string name]
            {
                get
                {
                    return Children.Where(c => c.Name == name).First();
                }
            }
        }

        public decimal ProgressSize
        {
            get
            {
                decimal total = 0;
                decimal done = 0;
                switch (State)
                {
                    case eState.Init:
                    case eState.Scan:
                    case eState.Match:
                        return 0;
                    case eState.Done:
                        return 1;
                    case eState.Delete:
                        total = SizeToDelete;
                        done = SizeDeleted;
                        break;
                    case eState.Copy:
                        total = SizeToCopy + SizeToUpdate;
                        done = SizeCopied + SizeUpdated;
                        break;
                }

                if (total == 0)
                {
                    return 0;
                }
                return done / total;
            }
        }

        public decimal ProgressFiles
        {
            get
            {
                decimal total = 0;
                decimal done = 0;
                switch (State)
                {
                    case eState.Init:
                    case eState.Scan:
                    case eState.Match:
                        return 0;
                    case eState.Done:
                        return 1;
                    case eState.Delete:
                        total = FilesToDelete;
                        done = FilesDeleted;
                        break;
                    case eState.Copy:
                        total = FilesToCopy + FilesToUpdate;
                        done = FilesCopied + FilesUpdated;
                        break;
                }

                if (total == 0)
                {
                    return 0;
                }
                return done / total;
            }
        }

        internal void MatchFiles()
        {
            State = eState.Match;

            MatchDirectory(SourceIndex.Root.Name, SourceIndex.Root, DestinationIndex.Root.Name, DestinationIndex.Root);
        }

        internal void MatchDirectory(string srcPath, TreeNode src, string dstPath, TreeNode dst)
        {
            SourceIndex.CurrentEntity = srcPath;
            DestinationIndex.CurrentEntity = dstPath;

            var srcList = src.Children.OrderBy(e => e.Name).ToArray();
            var dstList = dst.Children.OrderBy(e => e.Name).ToArray();
            int srcPos = 0;
            int dstPos = 0;

            List<Tuple<TreeNode, TreeNode>> subDirs = new();

            while (srcPos < srcList.Length || dstPos < dstList.Length)
            {
                TreeNode s = null;
                TreeNode d = null;

                if (srcPos < srcList.Length)
                {
                    s = srcList[srcPos];
                }
                if (dstPos < dstList.Length)
                {
                    d = dstList[dstPos];
                }

                int comp = 0;

                if (s == null)
                {
                    comp = 1;
                }
                else if (d == null)
                {
                    comp = -1;
                }
                else
                {
                    comp = s.Name.CompareTo(d.Name);
                }

                if (comp < 0)
                {
                    /* those which are missing in destination have to get copied. Also whose type doesn't match anymore */
                    s.FullPath = Path.Combine(srcPath, s.Name);

                    if (s.Type == eType.Directory)
                    {
                        AddDirectory(src[s.Name], s.FullPath, Path.Combine(dstPath, s.Name));
                    }
                    else
                    {
                        lock (FileCopyQueue)
                        {
                            FileCopyQueue.Add(s.FullPath, Path.Combine(dstPath, s.Name));
                        }
                        FilesToCopy++;
                        SizeToCopy += s.Length;
                    }
                    srcPos++;
                }
                else if (comp > 0)
                {
                    /* those which are missing in source directory can be deleted in destination directory also */
                    d.FullPath = Path.Combine(dstPath, d.Name);
                    string sourcePath = Path.Combine(srcPath, d.Name);

                    if (File.Exists(sourcePath) && !IsIgnored(new FileInfo(sourcePath)))
                    {
                        AddMessage("[ERROR] Consistency check failed. " + d.FullPath + " would get deleted, but still exists in source");
                        //return;
                    }
                    if (Directory.Exists(sourcePath) && !IsIgnored(new DirectoryInfo(sourcePath)))
                    {
                        AddMessage("[ERROR] Consistency check failed. " + d.FullPath + " would get deleted, but still exists in source");
                        //return;
                    }
                    lock (FileDeleteQueue)
                    {
                        FileDeleteQueue.Add(d);
                    }
                    FilesToDelete += d.FilesRecursive;
                    SizeToDelete += d.SizeRecursive;
                    dstPos++;
                }
                else if (s.Type == eType.File && (s.LastChange != d.LastChange || s.Length != d.Length))
                {
                    lock (FileUpdateQueue)
                    {
                        FileUpdateQueue.Add(Path.Combine(srcPath, s.Name), Path.Combine(dstPath, s.Name));
                    }
                    FilesToUpdate++;
                    SizeToUpdate += s.Length;
                    srcPos++;
                    dstPos++;
                }
                else if (s.Type == eType.Directory)
                {
                    /* recurse directories */
                    subDirs.Add(new Tuple<TreeNode, TreeNode>(s, d));
                    srcPos++;
                    dstPos++;
                }
                else
                {
                    srcPos++;
                    dstPos++;
                }
            }

            Parallel.ForEach(subDirs, ParallelOptions, t =>
            {
                MatchDirectory(Path.Combine(srcPath, t.Item1.Name), t.Item1, Path.Combine(dstPath, t.Item2.Name), t.Item2);
            });

#if false
            /* those which are missing in source directory can be deleted in destination directory also */
            var missInSource = dst.Children.Where(predicate: s => !src.Children.Where(d => s.Name == d.Name && s.Type == d.Type).Any());
            foreach (var d in missInSource)
            {
                d.FullPath = Path.Combine(dstPath, d.Name);
                EntitiesToDelete.Add(d);
                SizeToDelete += d.SizeRecursive;
            }

            /* those which are missing in destination have to get copied. Also whose type doesn't match anymore */
            var missInDestination = src.Children.Where(s => !dst.Children.Where(d => s.Name == d.Name).Any() || dst.Children.Where(d => s.Name == d.Name && s.Type != d.Type).Any());
            foreach (var s in missInDestination)
            {
                s.FullPath = Path.Combine(srcPath, s.Name);

                if (s.Type == eType.Directory)
                {
                    AddDirectory(src[s.Name], s.FullPath, Path.Combine(dstPath, s.Name));
                }
                else
                {
                    FilesToCopy.Add(s.FullPath, Path.Combine(dstPath, s.Name));
                    SizeToCopy += s.Length;
                }
            }

            var changed = src.Children.Where(s => s.Type != eType.Directory).Where(s => dst.Children.Where(d => s.Name == d.Name && (s.LastChange != d.LastChange || s.Length != d.Length)).Any());
            foreach (var s in changed)
            {
                FilesToUpdate.Add(Path.Combine(srcPath, s.Name), Path.Combine(dstPath, s.Name));
                SizeToUpdate += s.Length;
            }

            /* recurse source side directories */
            Parallel.ForEach(src.Children.Where(d => d.Type == eType.Directory), new ParallelOptions { MaxDegreeOfParallelism = (int)Math.Ceiling((Environment.ProcessorCount * 0.75)) }, dir =>
            //foreach (var dir in src.Children.Where(d => d.Type == eType.Directory))
            {
                if (!missInDestination.Contains(dir))
                {
                    var dstDir = dst.Children.Where(d => dir.Name == d.Name).First();
                    MatchDirectory(Path.Combine(srcPath, dir.Name), dir, Path.Combine(dstPath, dir.Name), dstDir);
                }
            });
#endif
        }

        private int NodeSort(TreeNode x, TreeNode y)
        {
            return x.Name.CompareTo(y.Name);
        }

        private void AddDirectory(TreeNode srcNode, string src, string dst)
        {
            /* add an dummy entry that hints a directory to be created in the case of empty directories */
            lock (FileCopyQueue)
            {
                FileCopyQueue.Add(src, dst);
                FilesToCopy++;
            }

            foreach (var info in srcNode.Children.Where(c => c.Type != eType.Directory))
            {
                lock (FileCopyQueue)
                {
                    FileCopyQueue.Add(Path.Combine(src, info.Name), Path.Combine(dst, info.Name));
                }
                FilesToCopy++;
                SizeToCopy += info.Length;
            }
            foreach (var info in srcNode.Children.Where(c => c.Type == eType.Directory))
            {
                AddDirectory(info, Path.Combine(src, info.Name), Path.Combine(dst, info.Name));
            }
        }

        private ulong FileChecksum(string filename)
        {
            using FileStream stream = File.Open(filename, FileMode.Open, FileAccess.Read, FileShare.ReadWrite | FileShare.Delete);
            byte[] hash = Sha256.ComputeHash(stream);
            return BitConverter.ToUInt64(hash, 0);
        }

        public void BuildTree()
        {
            State = eState.Scan;

            SourceIndex.Root = new TreeNode() { Name = SourceFolder };
            DestinationIndex.Root = new TreeNode() { Name = DestinationFolder };

            Thread srcThread = new(() =>
            {
                IndexDirectory(SourceIndex, SourceFolder, SourceIndex.Root, true);
            });

            Thread dstThread = new(() =>
            {
                bool indexValid = false;

                indexValid |= LoadCache(DestinationFolder, "_dst_cache.sbc", ref DestinationIndex);
                if (!indexValid)
                {
                    indexValid |= LoadCache(DestinationFolder, "_dst_cache_bak.sbc", ref DestinationIndex);
                }


                if (indexValid)
                {
                    if (DestinationIndex.CacheUpdateCounter >= CacheUpdateCounterMax)
                    {
                        string msg = "Used cache " + DestinationIndex.CacheUpdateCounter + "/" + CacheUpdateCounterMax + " times, reindexing...";

                        DestinationIndex.CurrentEntity = msg;
                        AddMessage("[INFO] " + msg);
                        indexValid = false;
                    }
                    else
                    {
                        AddMessage("  Used cache " + DestinationIndex.CacheUpdateCounter + "/" + CacheUpdateCounterMax + " times. " + (CacheUpdateCounterMax - DestinationIndex.CacheUpdateCounter) + " runs until reindexing.");
                        AddMessage("  File count: " + DestinationIndex.IndexedFiles);
                        AddMessage("  File sizes: " + DestinationIndex.IndexedSize + " (" + FormatSize(DestinationIndex.IndexedSize) + ")");
                    }
                }
                else
                {
                    AddMessage("[INFO] Cache invalid. Indexing destination directory.");
                }

                if (!indexValid)
                {
                    DestinationIndex = new();
                    DestinationIndex.Root = new TreeNode() { Name = DestinationFolder };

                    DateTime start = DateTime.Now;
                    IndexDirectory(DestinationIndex, DestinationFolder, DestinationIndex.Root);
                    DateTime end = DateTime.Now;
                    AddMessage("[INFO] Indexing finished, took " + (end - start).TotalSeconds + " seconds.");

                    DestinationIndex.Modified = true;
                }
                SaveCache();
            });

            srcThread.Name = "Scan source";
            dstThread.Name = "Scan destination";
            srcThread.Start();
            dstThread.Start();

            while (dstThread.IsAlive || srcThread.IsAlive)
            {
                Thread.Sleep(50);
            }
        }

        public static void CopyTo(Stream src, Stream dest)
        {
            byte[] bytes = new byte[4096];

            int cnt;

            while ((cnt = src.Read(bytes, 0, bytes.Length)) != 0)
            {
                dest.Write(bytes, 0, cnt);
            }
        }

        public static byte[] Zip(string str)
        {
            var bytes = Encoding.UTF8.GetBytes(str);

            using (var msi = new MemoryStream(bytes))
            using (var mso = new MemoryStream())
            {
                using (var gs = new GZipStream(mso, CompressionMode.Compress))
                {
                    CopyTo(msi, gs);
                }

                return mso.ToArray();
            }
        }

        public static string Unzip(byte[] bytes)
        {
            using (var msi = new MemoryStream(bytes))
            using (var mso = new MemoryStream())
            {
                using (var gs = new GZipStream(msi, CompressionMode.Decompress))
                {
                    CopyTo(gs, mso);
                }

                return Encoding.UTF8.GetString(mso.ToArray());
            }
        }

        private bool LoadCache(string destinationFolder, string file, ref TreeInfo destinationIndex)
        {
            bool ret = false;
            try
            {
                string cachedIndex = Path.Combine(DestinationFolder, file);

                if(!File.Exists(cachedIndex))
                {
                    return false;
                }

                destinationIndex.CurrentEntity = "Reading '" + cachedIndex + "'";

                string jsonString = null;
                byte[] binary = File.ReadAllBytes(cachedIndex);
                try
                {
                    jsonString = Unzip(binary);
                }
                catch(Exception ex)
                {
                    AddMessage("[INFO] Cache reading failed: " + ex.ToString());
                    jsonString = null;
                }
                if (jsonString == null)
                {
                    jsonString = File.ReadAllText(cachedIndex);
                }
                destinationIndex = JsonSerializer.Deserialize<TreeInfo>(jsonString, new JsonSerializerOptions() { IncludeFields = true });
                destinationIndex.Root.Name = DestinationFolder;
                destinationIndex.CacheUpdateCounter++;
                destinationIndex.CurrentEntity = "";
                destinationIndex.Modified = false;

                destinationIndex.CurrentEntity = "Used cache " + destinationIndex.CacheUpdateCounter + "/" + CacheUpdateCounterMax + " times";
                if (destinationIndex.CacheUpdateCounter == CacheUpdateCounterMax / 2)
                {
                    destinationIndex.CurrentEntity = "Used cache " + destinationIndex.CacheUpdateCounter + "/" + CacheUpdateCounterMax + " times, updating cached sizes";
                    destinationIndex.IndexedSize = destinationIndex.Root.SizeRecursive;
                }

                ret = true;
            }
            catch (Exception ex)
            {
            }

            return ret;
        }
        public void SaveCache()
        {
            string cachedIndex = Path.Combine(DestinationFolder, "_dst_cache");

            DestinationIndex.LastSaveTime = DateTime.Now;

            if (File.Exists(cachedIndex + ".sbc") && !DestinationIndex.Modified)
            {
                return;
            }

            string jsonString = JsonSerializer.Serialize(DestinationIndex, new JsonSerializerOptions() { IncludeFields = true });
            byte[] binary = Zip(jsonString);

            for (int retry = 0; retry < 10; retry++)
            {
                try
                {
                    if (File.Exists(cachedIndex + "_new.sbc"))
                    {
                        File.Delete(cachedIndex + "_new.sbc");
                    }
                    //File.WriteAllText(cachedIndex + "_new.sbc", jsonString);
                    File.WriteAllBytes(cachedIndex + "_new.sbc", binary);

                    if (File.Exists(cachedIndex + "_bak.sbc"))
                    {
                        File.Delete(cachedIndex + "_bak.sbc");
                    }
                    if (File.Exists(cachedIndex + ".sbc"))
                    {
                        File.Move(cachedIndex + ".sbc", cachedIndex + "_bak.sbc");
                    }
                    File.Move(cachedIndex + "_new.sbc", cachedIndex + ".sbc");
                    break;
                }
                catch (Exception ex)
                {
                    Thread.Sleep(1000);
                }
            }

            DestinationIndex.Modified = false;
        }


        private void SaveCacheCheck()
        {
            if ((DateTime.Now - DestinationIndex.LastSaveTime).TotalSeconds > AutoSaveTime)
            {
                lock (DestinationIndex)
                {
                    if (DestinationIndex.SaveThread == null)
                    {
                        DestinationIndex.SaveThread = new Thread(() =>
                        {
                            try
                            {
                                SaveCache();
                            }
                            catch (Exception ex)
                            {
                            }
                            DestinationIndex.SaveThread = null;
                        });

                        try
                        {
                            DestinationIndex.SaveThread.Start();
                        }
                        catch (Exception ex)
                        {
                        }
                    }
                }
            }
        }

        private void TreeSetAttributes(TreeInfo root, string dst, eType fileType, DateTime lastChange, long length)
        {
            DestinationIndex.Modified = true;

            string path = dst.Substring(root.Root.Name.Length);
            var elems = path.Split(Path.DirectorySeparatorChar, StringSplitOptions.RemoveEmptyEntries);

            TreeNode node = root.Root;

            for (int pos = 0; pos < elems.Length; pos++)
            {
                var elem = elems[pos];
                TreeNode next = node.Children.Where(c => c.Name == elem).FirstOrDefault();

                if (next == null)
                {
                    next = new TreeNode()
                    {
                        Name = elem,
                        Type = (pos == elems.Length - 1) ? fileType : eType.Directory
                    };
                    var list = node.Children.ToList();
                    list.Add(next);
                    node.Children = list.ToArray();
                }
                node = next;
            }

            node.LastChange = ToUnixTime(lastChange);
            node.Length = length;

            SaveCacheCheck();
        }

        private long ToUnixTime(DateTime lastChange)
        {
            long unixTimestamp = (long)(lastChange - UnixEpoch).TotalSeconds;
            return unixTimestamp;
        }

        private void TreeDeleteEntry(TreeInfo root, string dst)
        {
            DestinationIndex.Modified = true;

            string path = dst.Substring(root.Root.Name.Length);
            var elems = path.Split(Path.DirectorySeparatorChar, StringSplitOptions.RemoveEmptyEntries);

            TreeNode node = root.Root;

            for (int pos = 0; pos < elems.Length; pos++)
            {
                var elem = elems[pos];

                if (pos == elems.Length - 1)
                {
                    node.Children = node.Children.Where(c => c.Name != elem).ToArray();
                    return;
                }

                TreeNode next = node.Children.Where(c => c.Name == elem).FirstOrDefault();

                if (next == null)
                {
                    return;
                }
                node = next;
            }

            SaveCacheCheck();
        }

        private void DeleteFile(TreeNode ent)
        {
            var fi = new FileInfo(ent.FullPath);

            AddMessage("[VERBOSE] DELETE '" + ent.FullPath + "' (" + FormatSize(fi.Length) + ")");

            if (!fi.FullName.StartsWith(DestinationFolder))
            {
                AddMessage("[ERROR]: Deleting a file that is not within destination folder");
                return;
            }

            bool success = false;

            for (int retry = 0; retry < 5; retry++)
            {
                try
                {
                    if (File.Exists(fi.FullName))
                    {
                        File.Delete(fi.FullName);
                    }
                    success = true;
                    break;
                }
                catch (Exception ex)
                {
                    Thread.Sleep(10);
                }
            }

            if (success)
            {
            }
            else
            {
                AddMessage("[ERROR]: failed to delete");
            }
        }

        private void DeleteDirectory(TreeNode ent)
        {
            var di = new DirectoryInfo(ent.FullPath);
            AddMessage("DELETE Dir: " + di.FullName);

            if (!di.FullName.StartsWith(DestinationFolder))
            {
                AddMessage("[ERROR]: Deleting a directory that is not within destination folder");
                return;
            }

            bool success = false;

            for (int retry = 0; retry < 5; retry++)
            {
                try
                {
                    if (Directory.Exists(di.FullName))
                    {
                        Directory.Delete(di.FullName, true);
                    }
                    success = true;
                    break;
                }
                catch (Exception ex)
                {
                    Thread.Sleep(10);
                }
            }

            if (success)
            {
            }
            else
            {
                AddMessage("[ERROR] failed to delete");
            }
        }

        private bool CopyFile(string src, string dst)
        {
            try
            {
                SourceIndex.CurrentEntity = src;
                DestinationIndex.CurrentEntity = dst;

                var srcInfo = new FileInfo(src);
                var dstInfo = new FileInfo(dst);

                /* first delete the existing file to make help any defect file gets removed before */
                if (File.Exists(dst))
                {
                    File.SetAttributes(dst, FileAttributes.Normal);
                    File.Delete(dst);
                }
                /* also make sure there is no directory with that name */
                if (Directory.Exists(dst))
                {
                    Directory.Delete(dst, true);
                }

                /* make sure destination path exists */
                if (!Directory.Exists(dstInfo.Directory.FullName))
                {
                    string dirs = "";
                    foreach (string dir in dstInfo.Directory.FullName.Split(Path.DirectorySeparatorChar))
                    {
                        dirs += dir + Path.DirectorySeparatorChar;

                        if (!Directory.Exists(dirs))
                        {
                            for (int retry = 0; retry < 10; retry++)
                            {
                                try
                                {
                                    /* if part of that path is a file, delete it */
                                    if(File.Exists(dirs.TrimEnd(Path.DirectorySeparatorChar)))
                                    {
                                        File.Delete(dirs.TrimEnd(Path.DirectorySeparatorChar));
                                    }
                                    if (!Directory.Exists(dirs))
                                    {
                                        Directory.CreateDirectory(dirs);
                                    }
                                    Directory.SetCreationTime(dirs, srcInfo.Directory.CreationTime);
                                    Directory.SetLastWriteTime(dirs, srcInfo.Directory.LastWriteTime);
                                    Directory.SetLastAccessTime(dirs, srcInfo.Directory.LastAccessTime);
                                    break;
                                }
                                catch (Exception e)
                                {
                                    if (retry == 10)
                                    {
                                        AddMessage("[ERROR] COPY/ATTRIBUTES: " + src + " -> " + dst + " -> " + e.Message);
                                    }
                                    Thread.Sleep(10);
                                }
                            }
                        }
                    }
                }

                srcInfo.CopyTo(dst, true);
                //File.Copy(src, dst, true);

                if (!File.Exists(dst))
                {
                    return false;
                }

                for (int retry = 0; retry < 10; retry++)
                {
                    try
                    {
                        /* update file access times. seen to crash */
                        File.SetAttributes(dst, FileAttributes.Normal);
                        File.SetLastWriteTime(dst, srcInfo.LastWriteTime);
                        File.SetCreationTime(dst, srcInfo.CreationTime);
                        File.SetLastAccessTime(dst, srcInfo.LastAccessTime);
                        File.SetAttributes(dst, srcInfo.Attributes);

                        /* update parent directoy access times */
                        Directory.SetCreationTime(dstInfo.Directory.FullName, srcInfo.Directory.CreationTime);
                        Directory.SetLastWriteTime(dstInfo.Directory.FullName, srcInfo.Directory.LastWriteTime);
                        Directory.SetLastAccessTime(dstInfo.Directory.FullName, srcInfo.Directory.LastAccessTime);
                        break;
                    }
                    catch (Exception e)
                    {
                        if (retry == 10)
                        {
                            AddMessage("[ERROR] COPY/ATTRIBUTES: " + src + " -> " + dst + " -> " + e.Message);
                        }
                        Thread.Sleep(10);
                    }
                }

                TreeSetAttributes(DestinationIndex, dst, eType.File, srcInfo.LastWriteTime, srcInfo.Length);
                return true;
            }
            catch (IOException ex)
            {
                /* E_SHARING_VIOLATION */
                if((uint)ex.HResult == 0x80070020)
                {
                    AddMessage("[BUSY] COPY: " + src + " -> " + dst + " -> " + ex.Message);
                }
                else
                {
                    AddMessage("[ERROR] COPY: " + src + " -> " + dst + " -> " + ex.Message);
                }
            }
            catch (Exception ex)
            {
                AddMessage("[ERROR] COPY: " + src + " -> " + dst + " -> " + ex.Message);
            }
            return false;
        }

        private void CopyDirectory(string src, string dst)
        {
            try
            {
                SourceIndex.CurrentEntity = src;
                DestinationIndex.CurrentEntity = dst;

                if (File.Exists(dst))
                {
                    File.SetAttributes(dst, FileAttributes.Normal);
                    File.Delete(dst);
                }
                if (Directory.Exists(dst))
                {
                    Directory.Delete(dst);
                }

                Directory.CreateDirectory(dst);

                foreach (var file in new DirectoryInfo(src).GetFiles())
                {
                    CopyFile(Path.Combine(src, file.Name), Path.Combine(dst, file.Name));
                }
                foreach (var dir in new DirectoryInfo(src).GetDirectories())
                {
                    CopyDirectory(Path.Combine(src, dir.Name), Path.Combine(dst, dir.Name));
                }

                var info = new DirectoryInfo(src);

                Directory.SetLastWriteTime(dst, info.LastWriteTime);
                Directory.SetCreationTime(dst, info.CreationTime);
                Directory.SetLastAccessTime(dst, info.LastAccessTime);

                TreeSetAttributes(DestinationIndex, dst, eType.Directory, info.LastWriteTime, 0);
                DirectoriesCopied++;
            }
            catch (Exception ex)
            {
                AddMessage("[ERROR] COPY: " + src + " -> " + dst + " -> (" + ex.HResult.ToString("X8") + ") " + ex.Message);
            }
        }


        private void IndexDirectory(TreeInfo info, string path, TreeNode node, bool filterActive = false, int level = 0)
        {
            info.CurrentEntity = path;
            info.Modified = true;

            EnumerationOptions opts = new EnumerationOptions
            {
                AttributesToSkip = 0,
                IgnoreInaccessible = false
            };

            List<TreeNode> entries = new List<TreeNode>();
            IEnumerable<FileInfo> filesAll = new DirectoryInfo(path).EnumerateFiles("*", opts);
            IEnumerable<FileInfo> files = filesAll;
            if (filterActive)
            {
                files = filesAll.Where(f => !IsIgnored(f));
            }
            if (level == 0)
            {
                files = files.Where(f => !f.Name.EndsWith(".sbc"));
            }

            foreach (var fileInfo in files)
            {
                TreeNode n = new TreeNode
                {
                    Name = fileInfo.Name,
                    Length = fileInfo.Length,
                    Type = eType.File,
                    LastChange = ToUnixTime(fileInfo.LastWriteTime)
                };

                entries.Add(n);
                info.IndexedFiles++;
                info.IndexedSize += fileInfo.Length;
            }

            IEnumerable<DirectoryInfo> dirsAll = new DirectoryInfo(path).EnumerateDirectories("*", opts);
            IEnumerable<DirectoryInfo> dirs = dirsAll;

            if (filterActive)
            {
                dirs = dirsAll.Where(d => !IsIgnored(d));
            }

            foreach (var dirInfo in dirs)
            {
                TreeNode n = new TreeNode
                {
                    Name = dirInfo.Name,
                    Type = eType.Directory,
                    LastChange = ToUnixTime(dirInfo.LastWriteTime)
                };

                entries.Add(n);
            }

            node.Children = entries.ToArray();

            foreach (TreeNode dirNode in entries.Where(e => e.Type == eType.Directory))
            {
                try
                {
                    IndexDirectory(info, Path.Combine(path, dirNode.Name), dirNode, filterActive, level + 1);
                }
                catch (Exception ex)
                {
                }
            }

            info.IndexedDirectories++;
        }

        private bool IsIgnored(FileInfo f)
        {
            bool ret = IgnoreList.Where(i => f.FullName.Contains(i)).Any();
            ret |= IgnoreList.Where(i => i.StartsWith("^") && i.EndsWith("$")).Any(i => ("^" + f.Name + "$") == i);
            ret |= IgnoreList.Where(i => i.StartsWith("^")).Any(i => ("^" + f.Name) == i);
            ret |= IgnoreList.Where(i => i.EndsWith("$")).Any(i => (f.Name + "$") == i);

            if (ret)
            {
                AddMessage("Ignored: " + f.FullName);
            }

            return ret;
        }


        private bool IsIgnored(DirectoryInfo f)
        {
            bool ret = IgnoreList.Where(i => (f.FullName + Path.DirectorySeparatorChar).Contains(i)).Any();

            if (ret)
            {
                AddMessage("Ignored: " + f.FullName);
            }

            return ret;
        }

        internal void CopyFiles()
        {
            State = eState.Copy;

            var ent = FileCopyQueue.ToArray();
            Parallel.ForEach(ent, ParallelOptions, pair =>
            //foreach (var pair in ent)
            {
                string src = pair.Key;
                string dst = pair.Value;
                long length = 0;

                try
                {
                    var srcInfo = new FileInfo(src);
                    length = srcInfo.Length;
                }
                catch(Exception ex)
                {
                }

                if (Directory.Exists(src))
                {
                    if (File.Exists(dst))
                    {
                        AddMessage("[ERROR] Could not create directory '" + dst + "' as there is already a file with that name.");
                    }
                    else if (!Directory.Exists(dst))
                    {
                        try
                        {
                            Directory.CreateDirectory(dst);
                        }
                        catch (Exception ex)
                        {
                            AddMessage("[ERROR] Could not create directory '" + dst + "': " + ex.Message);
                        }
                    }
                }
                else if (File.Exists(src))
                {
                    AddMessage("[VERBOSE] Copy '" + src + "' (" + FormatSize(length) + ")");
                    if (CopyFile(src, dst))
                    {
                        FilesCopied++;
                        SizeCopied += length;
                        DestinationIndex.IndexedSize += length;
                    }
                }
                else
                {
                    AddMessage("[ERROR] File '" + src + "' was missing during copy. Maybe a temporary file?");
                }

                FileCopyQueue.Remove(src);
            });
        }

        private void AddMessage(string v)
        {
            Log(v);
            
            lock (Messages)
            {
                Messages.Add(v);
            }
        }

        internal void DeleteFiles()
        {
            State = eState.Delete;

            var entities = FileDeleteQueue.ToArray();
            Parallel.ForEach(entities, ParallelOptions, ent =>
            //foreach (TreeNode ent in entities)
            {
                long files = ent.FilesRecursive;
                long size = ent.SizeRecursive;
                if (Directory.Exists(ent.FullPath))
                {
                    DeleteDirectory(ent);
                }
                else if (File.Exists(ent.FullPath))
                {
                    DeleteFile(ent);
                }

                FilesDeleted += files;
                SizeDeleted += size;
                DestinationIndex.IndexedSize -= size;

                TreeDeleteEntry(DestinationIndex, ent.FullPath);

                FileDeleteQueue.Remove(ent);
            });
        }

        internal void UpdateFiles()
        {
            State = eState.Update;

            var files = FileUpdateQueue.ToArray();
            Parallel.ForEach(files, ParallelOptions, pair =>
            //foreach (var pair in files)
            {
                string src = pair.Key;
                string dst = pair.Value;

                try
                {
                    var srcInfo = new FileInfo(src);
                    var dstInfo = new FileInfo(dst);
                    var info = new FileInfo(src);

                    SourceIndex.CurrentEntity = src;
                    DestinationIndex.CurrentEntity = dst;

                    AddMessage("[VERBOSE] UPDATE '" + src + "' (" + FormatSize(srcInfo.Length) + ")");

                    if (srcInfo.Length != dstInfo.Length)
                    {
                        CopyFile(src, dst);
                    }
                    else if (FilesDiffer(src, dst))
                    {
                        CopyFile(src, dst);
                    }
                    else
                    {
                        /* just an attribute update */
                File.SetAttributes(dst, FileAttributes.Normal);
                        File.SetLastWriteTime(dst, info.LastWriteTime);
                        File.SetCreationTime(dst, info.CreationTime);
                        File.SetLastAccessTime(dst, info.LastAccessTime);
                    }

                    FilesUpdated++;
                    SizeUpdated += srcInfo.Length;
                    DestinationIndex.IndexedSize -= dstInfo.Length;
                    DestinationIndex.IndexedSize += srcInfo.Length;

                    File.SetAttributes(dst, info.Attributes);

                    TreeSetAttributes(DestinationIndex, dst, eType.File, info.LastWriteTime, info.Length);
                }
                catch (Exception ex)
                {
                    AddMessage("[ERROR] UPDATE: " + src + " -> " + dst + " -> " + ex.Message);
                }

                FileUpdateQueue.Remove(src);
            });
        }

        private bool FilesDiffer(string src, string dst)
        {
            try
            {
                if (FileChecksum(src) != FileChecksum(dst))
                {
                    return true;
                }
            }
            catch (Exception ex)
            {
                AddMessage("[ERROR] HASH: " + src + " -> " + dst + " -> " + ex.Message);
            }
            return false;
        }


        private static string FormatSize(decimal size)
        {
            string[] units = new[] { "Byte", "KiB", "MiB", "GiB", "TiB" };
            int unit = 0;

            while (size > 1024 && unit < units.Length - 1)
            {
                size /= 1024;
                unit++;
            }

            return size.ToString("0.00") + " " + units[unit];
        }
        internal void Finish()
        {
            try
            {
                List<string> messages = new();

                AddMessage("Title:               " + Title);
                AddMessage("  Source size:       " + (SourceIndex.IndexedFiles + " (" + FormatSize(SourceIndex.IndexedSize) + ")"));
                AddMessage("  Destination size:  " + (DestinationIndex.IndexedFiles + " (" + FormatSize(DestinationIndex.IndexedSize) + ")"));
                AddMessage("  Copied:            " + (FilesCopied + " (" + FormatSize(SizeCopied) + ")"));
                AddMessage("  Deleted:           " + (FilesDeleted + " (" + FormatSize(SizeDeleted) + ")"));
            }
            catch (Exception ex)
            {
            }
            State = eState.Done;
        }

        internal void Execute()
        {
            if (!LockBackupFolder())
            {
                return;
            }
            try
            {
                BuildTree();
                MatchFiles();
                DeleteFiles();
                CopyFiles();
                UpdateFiles();
            }
            catch (Exception e)
            {
            }
            finally
            {
                UnlockBackupFolder();
                Finish();
                SaveCache();
            }
        }

        private void UnlockBackupFolder()
        {
            LockFileHandle.Close();
            try
            {
                File.Delete(LockFileHandle.Name);
            }
            catch (Exception ex)
            {
            }
        }

        private bool LockBackupFolder()
        {
            string lockFile = Path.Combine(DestinationFolder, "_dst_lock.sbc");

            if (File.Exists(lockFile))
            {
                try
                {
                    File.Delete(lockFile);
                }
                catch (Exception ex)
                {
                    return false;
                }
            }

            try
            {
                LockFileHandle = File.Open(lockFile, FileMode.CreateNew, FileAccess.ReadWrite, FileShare.None);
                return true;
            }
            catch (Exception ex)
            {
            }
            
            return false;
        }
    }
}
