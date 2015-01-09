using System;
using System.Collections.Generic;
using System.Text;

namespace RaptorDB
{
    public class Global
    {
        /// <summary>
        /// Store bitmap as int offsets then switch over to bitarray
        /// </summary>
        public static int BitmapOffsetSwitchOverCount = 10;
        /// <summary>
        /// True = Save to other views in process , False = background save to other views
        /// </summary>
        public static bool BackgroundSaveToOtherViews = true;
        /// <summary>
        /// Default maximum string key size for indexes
        /// </summary>
        public static byte DefaultStringKeySize = 60;
        /// <summary>
        /// Free bitmap index memory on save 
        /// </summary>
        public static bool FreeBitmapMemoryOnSave = false;
        /// <summary>
        /// Number of items in each index page (default = 10000) [Expert only, do not change]
        /// </summary>
        public static ushort PageItemCount = 10000;
        /// <summary>
        /// KeyStore save to disk timer
        /// </summary>
        public static int SaveIndexToDiskTimerSeconds = 60;
        /// <summary>
        /// Flush the StorageFile stream immediately
        /// </summary>
        public static bool FlushStorageFileImmediately = false;
        /// <summary>
        /// Save doc as binary json
        /// </summary>
        public static bool SaveAsBinaryJSON = true;
        /// <summary>
        /// Remove completed tasks timer
        /// </summary>
        public static int TaskCleanupTimerSeconds = 3;
        /// <summary>
        /// Save to other views timer seconds if enabled 
        /// </summary>
        public static int BackgroundSaveViewTimer = 1;
        /// <summary>
        /// How many items to process in a background view save event
        /// </summary>
        public static int BackgroundViewSaveBatchSize = 1000000;
        ///// <summary>
        ///// Check the restore folder for new backup files to restore
        ///// </summary>
        //public static int RestoreTimerSeconds = 10; // TODO : implement this
        /// <summary>
        /// Timer for full text indexing of original documents (default = 15 sec)
        /// </summary>
        public static int FullTextTimerSeconds = 15;
        /// <summary>
        /// How many documents to full text index in a batch
        /// </summary>
        public static int BackgroundFullTextIndexBatchSize = 10000;
        /// <summary>
        /// Free memory checking timer (default = 60 sec)
        /// </summary>
        public static int FreeMemoryTimerSeconds = 60;
        /// <summary>
        /// Memory usage limit for internal caching (default = 100 Mb) [using GC.GetTotalMemory()]
        /// </summary>
        public static long MemoryLimit = 100;
        /// <summary>
        /// Backup cron schedule (default = "0 * * * *" [every hour])  
        /// </summary>
        public static string BackupCronSchedule = "0 * * * *";
        /// <summary>
        /// Require primary view to be defined for save, false = key/value store (default = true)
        /// </summary>
        public static bool RequirePrimaryView = true;
        /// <summary>
        /// Maximum documents in each package for replication
        /// </summary>
        public static int PackageSizeItemCountLimit = 10000;
        /// <summary>
        /// Process inbox timer (default = 60 sec)
        /// </summary>
        public static int ProcessInboxTimerSeconds = 10;
        /// <summary>
        /// Split the data storage files in MegaBytes (default 0 = off) [500 = 500mb]
        /// <para> - You can set and unset this value anytime and it will operate from that point on.</para>
        /// <para> - If you unset (0) the value previous split files will remain and all the data will go to the last file.</para>
        /// </summary>
        public static ushort SplitStorageFilesMegaBytes = 0;
    }
}
