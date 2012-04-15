using System;
using System.Collections.Generic;
using System.Text;

namespace RaptorDB
{
    internal class Global
    {
        public static bool FreeMemoryOnCommit = false;

        public static int BitmapOffsetSwitchOverCount = 10;

        public static int NodeDepthCheckingCount = 30;

        public static bool SyncSaves = true; 

        public static byte DefaultStringKeySize = 60; 

        public static bool FreeBitmapMemoryOnSave = false;
        
        public static ushort PageItemCount = 10000;
        
        public static int SaveTimerSeconds = 60;
        
        public static bool FlushStorageFileImmetiatley = false;

        public static bool SaveAsBinaryJSON = true;

        public static int TaskCleanupTimerSeconds = 30;

        //public static bool BackgroundIndexing = true;
    }
}
