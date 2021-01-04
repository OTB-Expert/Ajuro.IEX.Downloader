﻿using Ajuro.IEX.Downloader.Models;
using Ajuro.Net.Processor.Models;
using Ajuro.Net.Stock.Repositories;
using Ajuro.Net.Types.Stock;
using Ajuro.Net.Types.Stock.Models;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http.Internal;

namespace Ajuro.IEX.Downloader.Services
{
    public enum Step {
        DownloadIntraday, // will download from IEX Cloud resulting in one record per SymbolId and per Day like: CODE-yyyyMMdd
        AggregdateDays, // will get all the records of a symbol, order them and merge them into one record
        AggregateSymbols
    }
    public interface IDownloaderService
    {


        void SetOptions(DownloaderOptions options);

        // If you need this, you're doing the wrong thing! 
        // DownloaderOptions GetOptions();


        #region FRAGMENTS
        Task<IEnumerable<Sample>> CreateFragmentsFromFiles(BaseSelector selector, ResultSelector resultSelector);
        Task<IEnumerable<Sample>> CreateFragmentsFromDb(BaseSelector selector, ResultSelector resultSelector);
        
        #endregion
        
        #region CACHING
        
        Task<List<Tick>> GetAllHistoricalFromDb(BaseSelector selector, bool overwrite, bool saveToFile = true, bool saveToDb = false, bool overWrite = false);
        Task<Dictionary<int, object[][]>> GetAllHistoricalFromFiles(BaseSelector selector, bool overwrite, bool saveToFile = true, bool saveToDb = false, bool overWrite = false);
        string GetResultFromFile(string folder, string fileName);

        #endregion

        #region REPORTING
        Task<IEnumerable<FileResourceGroup>> ListFiles(BaseSelector selector, ReportingOptions reportingOptions);
        Task<List<DownloadIntradayReport>> GetFileRecordsByReportingOptions(BaseSelector selector, ReportingOptions reportingOptions);
        IQueryable<Daily> GetDailyRecordsByReportingOptions(BaseSelector selector, ReportingOptions reportingOptions);
        IQueryable<Tick> GetIntradayRecordsByReportingOptions(BaseSelector selector, ReportingOptions reportingOptions);
        Task<List<DownloadIntradayReport>> CountFiles(BaseSelector selector, ReportingOptions reportingOptions);
        Task<List<DownloadIntradayReport>> ListFiles_WithContent_PerCode_OnTheGivenMonth(BaseSelector selector, ReportingOptions reportingOptions);
        Task<List<DownloadIntradayReport>> CountFiles_PerCode_OnTheGivenMonth(BaseSelector selector, ReportingOptions reportingOptions);
        Task<List<DownloadIntradayReport>> CountFiles_AndCountIntradays_PerCode_OnTheGivenMonth(BaseSelector selector, ReportingOptions reportingOptions, bool overWrite);
        
        #endregion

        #region COLLECT DATA
        
        Task<List<DownloadReport>> Download(BaseSelector selector, DownloadOptions options);
        Task<IEnumerable<GraphModel>> DownloadIntraday(BaseSelector selector, DateTime date);
        Task<int> FetchToday(BaseSelector selector);

        Task<List<StockReport>> QuickPull(BaseSelector selector, bool isControl = false);

        Task<List<StockReport>> Pool(BaseSelector selector, bool isControl = false);

        Task<Tick> UpsertTicks(BaseSelector selector, Symbol symbol, DateTime date, object[][] ticksArray);
        Task<StockReport> Pool(BaseSelector selector, Symbol symbol, DateTime date);
        
        
        Task<List<StockReport>> FetchLast(BaseSelector selector, Symbol symbol, int i);

        Task<StockReport> FetchDate(BaseSelector selector, Symbol symbol, DateTime date, bool saveOnly = false, bool fromFile = false);

        Task<string> FetchString(BaseSelector selector, DownloadOptions options);
        
        Task<List<StockResult>> GetLasts(BaseSelector selector, int ticksCount);
        Task<IEnumerable<IEnumerable<object[][]>>> CollectIntraday(BaseSelector selector, DownloadOptions options);
        
        #endregion
        
        #region PROCESING
        
        Task<object[][]> ProcessString(BaseSelector selector, DownloadOptions options, string dataString = null);
        Task<bool> SaveResult(BaseSelector selector, Result existentResult, long startTime, string key, object content, bool replaceIfExists, string backupFolder, SaveResultsOptions saveResultsOptions);

        #endregion

    }

    public class DownloadOptions
    {
        public bool IfDbMissingSave { get; set; }
        public bool UpdateDbIfExists { get; set; }
        public Download_Options Step_01_Download_Options { get; set; }
        public Join_Options Step_02_Join_Options { get; set; }
        public Aggregate_Options Step_03_Aggregate_Options { get; set; }
        public SelectorOptions SelectorOptions { get; set; }
        public DateTime[] Dates { get; set; }
        public int[] SymbolIds { get; set; }
        public bool BuildDictionary { get;  set; }
        public bool IfFileMissingSave_DailySymbolHistoryFolder { get;  set; }
        public bool FromFileIfExists { get;  set; }
        public bool ReplaceFileIfExists { get;  set; }
        public bool SaveOnly { get;  set; }
        public bool SkipReportingToDb { get;  set; }
        public bool FromDbIfExists { get; set; }

        public DownloadOptions()
        {
            Step_01_Download_Options = new Download_Options();
            Step_02_Join_Options = new Join_Options();
            Step_03_Aggregate_Options = new Aggregate_Options();
            SelectorOptions = new SelectorOptions();
        }
    }

    public class SelectorOptions
    {
        public int FromDateOffset { get; set; }
        public int ToDateOffset { get; set; }
        public long FromDateSeconds { get; set; }
        public long ToDateSeconds { get; set; }
    }

    public class Download_Options
    {
        public bool Skip_This_Step { get; set; }
    }

    public class Aggregate_Options
    {
        public bool Skip_This_Step { get; set; }
    }

    public class Join_Options
    {
        public bool Skip_This_Step { get; set; }
    }

    public class DownloaderOptions
    {
        public string IEX_Token { get; set; }
        public string DailyGraphsFolder { get; set; }
        public string DailySymbolHistoryFolder { get; set; }
        public string SymbolHistoryFolder { get; set; }
        public string LargeResultsFolder { get; set; }

        /// <summary>
        /// Assuming a release to server, we need to keep the server clean. If you really need the files into production either generate them on local and upload them or set this flag to true.
        /// </summary>
        public bool ReleaseConfiguration_Allow_WriteFiles { get; set; } // Do not create local overwrites for this option.


        /// <summary>
        /// Assumming this is a local enviroment where is ok to save files.
        /// </summary>
        public bool DebugConfiguration_Allow_WriteFiles { get; set; }

        /// <summary>
        /// Assuming a release to production, on time write keep this false and set ReleaseConfiguration_Allow_WriteFiles true.
        /// </summary>
        public bool ReleaseConfiguration_Allow_OverwriteFiles { get; set; }

        /// <summary> 
        /// Protect the files you donțt want to regenerate (might be a verry long running opperation).
        /// </summary>
        public bool DebugConfiguration_Allow_OverwriteFiles { get; set; }

        /// <summary>
        /// In case of an exception while writing to DB, the content will be saved into LargeResultsFolder. Ensure content is saved in case of SQL execution timeout.
        /// </summary>
        public bool FallbackToFiles { get; set; }        
    }

    public class FileResourceGroup
    {
        public string Category { get; set; }
        public string Folder { get; set; }
        public string Path { get; set; }
        public string Name { get; internal set; }
        public long Length { get; internal set; }
        public long LastWrite { get; internal set; }
    }

    public class SaveResultsOptions
    {
        public bool SaveToFile { get; set; }
        public bool SaveToDb { get; set; }
    }
}
