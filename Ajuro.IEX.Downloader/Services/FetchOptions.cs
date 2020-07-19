using System;
using System.Collections.Generic;
using System.Text;

namespace Ajuro.IEX.Downloader.Services
{

    public class FetchOptions
    {
        public bool FromFile = false; // Just use the file fo fetch data
        public bool Save = false; // Do save
        public bool SaveOnly = false; // And only save
        public bool Overwrite = false; // And only save
        public bool SaveReportsToDb = false; // And only save
    }
}
