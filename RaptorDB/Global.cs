using System;
using System.Collections.Generic;
using System.Text;

namespace RaptorDB
{
    internal class Global
    {
        /// <summary>
        /// Store bitmap as int offsets then switch over to bitarray
        /// </summary>
        public static int BitmapOffsetSwitchOverCount = 10;
        /// <summary>
        /// True = Save to other views in process , False = background save to other views
        /// </summary>
        public static bool BackgroundSaveToOtherViews = true;

        public static byte DefaultStringKeySize = 60;

        public static bool FreeBitmapMemoryOnSave = false;

        public static ushort PageItemCount = 10000;
        /// <summary>
        /// KeyStore save to disk timer
        /// </summary>
        public static int SaveIndexToDiskTimerSeconds = 60;
        /// <summary>
        /// Flush the StorageFile stream immediatley
        /// </summary>
        public static bool FlushStorageFileImmetiatley = false;
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
        /// <summary>
        /// Check the restore folder for new backup files to restore
        /// </summary>
        public static int RestoreTimerSeconds = 10;
        /// <summary>
        /// Timer for full text indexing of original documents (default = 15 sec)
        /// </summary>
        public static int FullTextTimerSeconds = 15;
        /// <summary>
        /// How many documents to full text index in a batch
        /// </summary>
        public static int BackgroundFullIndexSize = 10000;
        /// <summary>
        /// Free memory checking timer (default = 60 sec)
        /// </summary>
        public static int FreeMemoryTimerSeconds = 60;
        /// <summary>
        /// Memory usage limit for internal caching (default = 100 Mb) [using GC.GetTotalMemory()]
        /// </summary>
        public static long MemoryLimit = 100;
    }
}
