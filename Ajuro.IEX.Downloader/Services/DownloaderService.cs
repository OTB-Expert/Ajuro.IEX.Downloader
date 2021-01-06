using Ajuro.IEX.Downloader.Models;
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
        
        Task<IEnumerable<FileResourceGroup>> ListFiles(BaseSelector selector);
        List<DownloadIntradayReport> GetFileRecordsByReportingOptions(ReportingOptions reportingOptions);
        IQueryable<Daily> GetDailyRecordsByReportingOptions(ReportingOptions reportingOptions);
        IQueryable<Tick> GetIntradayRecordsByReportingOptions(ReportingOptions reportingOptions);
        Task<List<DownloadIntradayReport>> BuildDownloadSummary(BaseSelector selector, ReportingOptions reportingOptions, bool overWrite);
        
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
        public DownloaderOptions()
        {
 #if Linux
            DailySymbolHistoryFolder = @"/home/florin/PRO/Test3/Data/Historical/DailySymbolHistory";
            SymbolHistoryFolder = @"/home/florin/PRO/Test3/Data/Historical/SymbolHistory";
            DailyGraphsFolder = @"/home/florin/PRO/Test3/Data/Historical/DailyGraphs";
            LargeResultsFolder = @"/home/florin/PRO/Test3/Data/Historical/LargeResults";
#else
            DailySymbolHistoryFolder = @"C:\PRO\EasyStockData\DailySymbolHistory";
            SymbolHistoryFolder = @"C:\PRO\EasyStockData\SymbolHistory";
            DailyGraphsFolder = @"C:\PRO\EasyStockData\DailyGraphs";
            LargeResultsFolder = @"C:\PRO\EasyStockData\LargeResults";
#endif
        }
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

    public class DownloaderService : IDownloaderService
    {
        private readonly IResultRepository _resultRepository;
        private readonly IUserRepository _userRepository;
        private readonly IDailyRepository _dailyRepository;
        private readonly ITickRepository _tickRepository;
        private readonly ISymbolRepository _symbolRepository;
        private readonly IEndpointRepository _endpointRepository;
        private readonly IAlertRepository _alertRepository;
        // private readonly ILogger<DownloaderService> _logger;
        private readonly ILogRepository _logRepository;

        public DownloaderService
            (
                IUserRepository userRepository,
                ISymbolRepository symbolRepository,
                IAlertRepository alertRepository,
                IDailyRepository dailyRepository,
                ITickRepository tickRepository,
                IEndpointRepository endpointRepository,
                ILogRepository logRepository,
                IResultRepository resultRepository
                // ILogger<DownloaderService> logger
            )
        {
            _userRepository = userRepository;
            _dailyRepository = dailyRepository;
            _symbolRepository = symbolRepository;
            _alertRepository = alertRepository;
            _endpointRepository = endpointRepository;
            _tickRepository = tickRepository;
            // _logger = logger;
            _resultRepository = resultRepository;
            _logRepository = logRepository;
        }

        #region PROCESSING
        
        private DownloaderOptions downloaderOptions { get; set; }

        public DownloaderOptions GetOptions()
        {
            return this.downloaderOptions;
        }

        public void SetOptions(DownloaderOptions downloaderOptions)
        {
            this.downloaderOptions = downloaderOptions;
        }
        
        /// <summary>
        /// Merge intraday records into one foreach symbol
        /// </summary>
        public async Task<object[][]> ProcessString(BaseSelector selector, DownloadOptions options, string dataString = null)
        {
            var date = options.Dates[0];
            var symbolId = options.SymbolIds[0];
            var symbolCode = Static.SymbolCodeFromId[symbolId];
            var fileName = string.Empty;
            fileName = downloaderOptions.DailySymbolHistoryFolder + "\\" + "Intraday_" + symbolCode + "_" + date.ToString("yyyyMMdd") + ".json";

            if (!string.IsNullOrEmpty(dataString)) { 
                // No need to collect
            }
            else if (date > DateTime.MinValue)
            {
                if (File.Exists(fileName))
                {
                    dataString = File.ReadAllText(fileName);
                }
            }
            else
            {
                fileName = downloaderOptions.SymbolHistoryFolder + "\\" + symbolCode + ".json";

                if (!options.ReplaceFileIfExists && File.Exists(fileName))
                {
                    dataString = File.ReadAllText(fileName);
                    var items = (object[][])JsonConvert.DeserializeObject<object[][]>(dataString);
                    return items;
                }

                var strings = new List<string>();

                List<Sample> samples = new List<Sample>();
                string[] FilePaths = null;

                FilePaths = Directory.GetFiles(downloaderOptions.DailySymbolHistoryFolder, "Intraday_" + symbolCode + "_*.json");
                FilePaths = FilePaths.OrderBy(p => p).ToArray();

                var filesCount = FilePaths.Count();
                int fileIndex = 0;
                for (int f = 0; f < FilePaths.Length; f++)
                {
                    var filePath = FilePaths[f];
                    fileIndex++;
                    string content = File.ReadAllText(filePath);
                    if (content.Length == 2)
                    {
                        continue;
                    }
                    if (content.Length < 500)
                    {
                        continue;
                    }
                    strings.Add(content.Substring(1, content.Length - 2));
                }
                dataString = "[" + string.Join(",", strings) + "]";
            }

            try
            {
                var values = new List<IexItem>();
                var emptyTicks = 0;
                try
                {
                    values = (List<IexItem>)JsonConvert.DeserializeObject<List<IexItem>>(dataString);
                    emptyTicks = values.Count;
                    values = values.Where(p => p.average.HasValue == true).ToList();
                    emptyTicks -= values.Count();
                }
                catch (Exception e)
                {
                    /*
                    logEntryBreakdown.EndTime = DateTimeOffset.UtcNow.ToUnixTimeSeconds();
                    await _logRepository.AddAsync(new Log()
                    {
                        StartTime = logEntryBreakdown.StartTime,
                        EndTime = logEntryBreakdown.EndTime,
                        Tag = logEntryBreakdown,
                        TagString = JsonConvert.SerializeObject(logEntryBreakdown),
                        Source = CommandSource.Scheduler,
                        Method = logEntryBreakdown.Indicator
                    });*/
                    /*
                    new Report
                    {
                        SymbolId = symbol.SymbolId,
                        Samples = 0,
                        Message = (data.Length > 30 ? data.Substring(0, 30) + "..." : data),
                        Date = date,
    #if DEBUG
                        Url = url,
    #endif
                        Updated = DateTime.UtcNow,
                        Alerts = _alertRepository.All().Where(p => p.Symbol.SymbolId == symbol.SymbolId && p.IsEnabled == true).Count(),
                        Last = -1
                    };*/
                }

                // _logger.LogInformation("Loaded count for " + symbol.Name + " : " + values.Count);
                if (values.Count == 0)
                {
                    if (emptyTicks > 0)
                    {
                        // Is empty but we take a note that we already captured it.
                        // symbol.Updated = DateTimeOffset.UtcNow.ToUnixTimeSeconds();
                        // await _symbolRepository.UpdateAsync(symbol);
                    }
                    else
                    {
                        // symbol.Active = false;
                        // symbol.Updated = DateTimeOffset.UtcNow.ToUnixTimeSeconds();
                        // await _symbolRepository.UpdateAsync(symbol);
                    }
                    new StockReport
                    {
                        SymbolId = symbolId,
                        Code = symbolCode,
                        Samples = 0,
                        Message = "No data!",
                        Date = date,
#if DEBUG
                        // Url = url,
#endif
                        Updated = DateTime.UtcNow,
                        Alerts = _alertRepository.All().Where(p => p.Symbol.SymbolId == symbolId && p.IsEnabled == true).Count(),
                        Last = -1
                    };
                }
                var ticksArray = values.Where(p => p.marketAverage.HasValue).Where(p => p.marketAverage.Value != -1).Select(p => new object[] { (Int64)(p.date.AddSeconds(ToSeconds(p.minute)).Subtract(new DateTime(1970, 1, 1))).TotalMilliseconds, p.marketAverage.Value }).ToArray();
                if (options.IfFileMissingSave_DailySymbolHistoryFolder && !File.Exists(fileName))
                {
                    File.WriteAllText(fileName, JsonConvert.SerializeObject(ticksArray));
                }
                return ticksArray;
            }
            catch (Exception ex)
            {
                new Info(selector, -1, ex, string.Empty);
            }
            return null;
        }

        public int ToSeconds(string minutes)
        {
            if (minutes.IndexOf(':') > 0)
            {
                return int.Parse(minutes.Split(':')[0]) * 60 * 60 + int.Parse(minutes.Split(':')[1]) * 60;
            }
            return 0;
        }
         public string GetResultFromFile(string backupFolder, string key)
        {
            if(File.Exists(backupFolder + "\\" + key + ".json"))
            {
                return File.ReadAllText(backupFolder + "\\" + key + ".json");
            }
            return null;
        }
        public async Task<bool> SaveResult(BaseSelector selector, Result existentResult, long startTime, string key, object content, bool replaceIfExists, string backupFolder, SaveResultsOptions saveResultsOptions)
        {
            if (existentResult == null)
            {
                existentResult = new Result(selector)
                {
                    Key = key,
                    TagString = JsonConvert.SerializeObject(content),
                    EndTime = DateTimeOffset.UtcNow.ToUnixTimeSeconds(),
                    StartTime = startTime,
                    // User = currentUser
                };
                try
                {
                    if (saveResultsOptions.SaveToFile)
                    {
                        File.WriteAllText(backupFolder + "\\" + key + ".json", existentResult.TagString);
                    }
                    if(saveResultsOptions.SaveToDb)
                    {
                        try
                        {
                            await _resultRepository.AddAsync(existentResult);
                        }
                        catch (Exception ex)
                        {
                            new Info(selector, -1, ex, "Duplicate result");
                        }
                    }
                }
                catch (Exception ex)
                {
                    new Info(selector, -1, ex, string.Empty);
                    File.WriteAllText(backupFolder + "\\" + key + ".json", existentResult.TagString);
                }
            }
            else
            {
                if (replaceIfExists)
                {
                    existentResult.TagString = JsonConvert.SerializeObject(content);
                    existentResult.EndTime = DateTimeOffset.UtcNow.ToUnixTimeSeconds();
                    existentResult.StartTime = startTime;
                    try
                    {
                        if (saveResultsOptions.SaveToFile)
                        {
                            File.WriteAllText(backupFolder + "\\" + key + ".json", JsonConvert.SerializeObject(content));
                        }
                        if(saveResultsOptions.SaveToDb)
                        {
                            await _resultRepository.UpdateAsync(existentResult);
                        }
                    }
                    catch (Exception ex)
                    {
                        new Info(selector, -1, ex, string.Empty);
                        File.WriteAllText(backupFolder + "\\" + key + ".json", JsonConvert.SerializeObject(content));
                    }
                }
            }
            return true;
        }

       
        
        #endregion
        
        #region COLLECT DATA
        
        public async Task<IEnumerable<GraphModel>> DownloadIntraday(BaseSelector selector, DateTime date)
        {
            if (Static.SymbolsDictionary.Count() == 0)
            {
                return null;
            }
            bool saveToday = false;
            if (date == DateTime.MinValue)
            {
                saveToday = true;
                date = DateTime.UtcNow.Date;
            }

            new Info(selector, (date == DateTime.Today.AddDays(-1) ? "Save: ":"Memo: ") + date.ToString());

            var resultKey = "DailyGraphsSP500";
            var startTime = DateTimeOffset.UtcNow.ToUnixTimeSeconds();
            var symbIds = new int[] { };

            // var date = DateTime.UtcNow.AddDays(-1);
            var overWrite = false;
            var result = _resultRepository.GetAllByKey(resultKey + "_" + date.ToString("yyyyMMdd")).FirstOrDefault();
            if (result != null && !overWrite)
            {
                // Try from DB  
                return (List<GraphModel>)JsonConvert.DeserializeObject<List<GraphModel>>(result.TagString);
            }
            else
            {
                // Try from file
                if(downloaderOptions == null)
                {
                    // Is from worker
                    downloaderOptions = new DownloaderOptions()
                    {
                        DailyGraphsFolder = ""
                    };
                }
                var stringContent = GetResultFromFile(downloaderOptions.DailyGraphsFolder, resultKey + "_" + date.ToString("yyyyMMdd"));
                if (!string.IsNullOrEmpty(stringContent))
                {
                    var items = (List<GraphModel>)JsonConvert.DeserializeObject<List<GraphModel>>(stringContent);
                    return items.Where(p => symbIds.Length == 0 || symbIds.Contains(p.SymbolId));
                }
            }

            var DailyGraphsSP500 = new List<GraphModel>();
            if (Static.SymbolCodes == null || Static.SymbolCodes.Length == 0)
            {
                return DailyGraphsSP500;
            }
            var triggerSymbolIds = Static.Triggers.Select(p => p.SymbolId).Where(symbolId => Static.SymbolCodeFromId.ContainsKey(symbolId) && _userRepository.GetSP500().Any(s => s == Static.SymbolCodeFromId[symbolId])).Distinct();
            var triggerSymbolIdsCount = triggerSymbolIds.Count();
            var count = triggerSymbolIds.Count();
            var left = count;
            var uncachedSymbolIds = triggerSymbolIds.Where(symbolId => !Static.SymbolsIntraday.ContainsKey(symbolId));
            var uncachedSymbolIdsCount = uncachedSymbolIds.Count();
            var downloadedSymbolTicks = _tickRepository.All().Where(p => uncachedSymbolIds.Contains(p.SymbolId) && p.Seconds == Static.MidnightSecondsFromSeconds(Static.SecondsFromDateTime(date)));
            var downloadedSymbolTicksCount = downloadedSymbolTicks.Count();
            foreach (var symbolId in triggerSymbolIds)
            {
                left--;
                try
                {
                    var fromDb = downloadedSymbolTicks.Any(s => s.SymbolId == symbolId);
                    if (fromDb)
                    {
                        Static.SymbolsIntraday.Add(symbolId, JsonConvert.DeserializeObject<object[][]>(downloadedSymbolTicks.FirstOrDefault(s => s.SymbolId == symbolId).Serialized));
                    }
                    new Info(selector, symbolId, "Intraday [" + left + " / " + count + "] " + Static.SymbolCodeFromId[symbolId] + "... " + (Static.SymbolsIntraday.ContainsKey(symbolId) ? " From memory!" : "") + (fromDb ? " From bulk DB!" : ""));
                    DailyGraphsSP500.Add(
            // result = Static.Triggers.Select(p => 
            new GraphModel()
            {
                SymbolId = symbolId,
                Symbol = Static.SymbolCodeFromId[symbolId],
                Values = Static.SymbolsIntraday.ContainsKey(symbolId) ? Static.SymbolsIntraday[symbolId] : (await CollectIntraday(selector, new DownloadOptions()
                {
                    SymbolIds = new int[]
            {
            symbolId
                },
                    Dates = new DateTime[] {
            date
                },
                    FromDbIfExists = true,
                    FromFileIfExists = true, // If there is no DB entry use the file
                    IfDbMissingSave = true,
#if DEBUG
                    IfFileMissingSave_DailySymbolHistoryFolder = true, // Only save files on local
#else
                    IfFileMissingSave_DailySymbolHistoryFolder = false, // = date == DateTime.Today.AddDays(-1), // Only save yesturday
#endif
                    UpdateDbIfExists = false,
                    ReplaceFileIfExists = false,
                    BuildDictionary = true
                })).FirstOrDefault().FirstOrDefault(),
            });
                }
                catch (Exception ex)
                {
                    new Info(selector, symbolId, ex, "Exception fetching intraday for symbol " + Static.SymbolCodeFromId[symbolId] + ": " + ex.Message + (ex.InnerException != null ? ", " + ex.InnerException.Message + (ex.InnerException.InnerException != null ? ", " + ex.InnerException.InnerException.Message : "") : ""));
                }
            }
            await SaveResult(selector, result, startTime, resultKey + "_" + date.ToString("yyyyMMdd"), DailyGraphsSP500, true, downloaderOptions.DailyGraphsFolder, new SaveResultsOptions
            {
                SaveToDb = true,
                SaveToFile = true
            });
            return DailyGraphsSP500;
        }
        
        public async Task<IEnumerable<IEnumerable<object[][]>>> CollectIntraday(BaseSelector selector, DownloadOptions options)
        {
            var result = new List<List<object[][]>>();
            foreach (DateTime date in options.Dates)
            {
                var dayItems = new List<object[][]>();
                var seconds = Static.SecondsFromDateTime(date);
                foreach (int symbolId in options.SymbolIds)
                {
                    object[][] values = null;
                    if (options.FromDbIfExists)
                    {
                        var dbEntry = _tickRepository.All().FirstOrDefault(p=>p.SymbolId == symbolId && p.Seconds == Static.MidnightSecondsFromSeconds(seconds));
                        if(dbEntry != null)
                        {
                            new Info(selector, symbolId, "  From DB... " + Static.SymbolCodeFromId[dbEntry.SymbolId]);
                            values = JsonConvert.DeserializeObject<object[][]>(dbEntry.Serialized);
                        }
                    }
                    if (values == null)
                    {
                        var dataString = await FetchString(selector, new DownloadOptions()
                        {
                            SymbolIds = new int[] { symbolId },
                            Dates = new DateTime[] { date },
                            FromFileIfExists = false,
                            IfFileMissingSave_DailySymbolHistoryFolder = false
                        });
                        values = await ProcessString(selector, options, dataString);
                    }
                    // var items = (List<IexItem>)JsonConvert.DeserializeObject<List<IexItem>>(dataString);
                    // var values = items.Where(p => p.marketAverage.HasValue).Where(p => p.marketAverage.Value != -1).Select(p => new object[] { (Int64)(p.date.AddSeconds(ToSeconds(p.minute)).Subtract(new DateTime(1970, 1, 1))).TotalMilliseconds, p.marketAverage.Value }).ToArray();
                    if (options.IfDbMissingSave)
                    {
                       var record = _tickRepository.GetByDayAndSymbolId(symbolId, seconds).FirstOrDefault();
                        if(record == null)
                        {
                            record = new Tick()
                            {
                                SymbolId = symbolId,
                                Samples = values.Count(),
                                Seconds = Static.MidnightSecondsFromSeconds(seconds),
                                Serialized = JsonConvert.SerializeObject(values),
                                Date = date,
                                Symbol = Static.SymbolCodeFromId[symbolId]
                            };
                            await _tickRepository.AddAsync(record);
                        }
                        else if (options.UpdateDbIfExists)
                        {

                        }
                    }
                    if (options.BuildDictionary)
                    {
                        if (Static.SymbolsIntraday.ContainsKey(symbolId))
                        {
                            Static.SymbolsIntraday[symbolId] = values;
                        }
                        else
                        {
                            Static.SymbolsIntraday.Add(symbolId, values);
                        }
                    }
                    dayItems.Add(values);
                }
                result.Add(dayItems);
            }
            return result;
        }
        public async Task<StockReport> PoolSymbolOnDate(BaseSelector selector, Symbol symbol, DateTime date)
        {
            var data = await PoolSymbolTicksOnDate(selector, symbol, date, false);
            return data;
        }

        public async Task<List<StockReport>> Pool(BaseSelector selector, bool isControl = false)
        {
            if (!isControl)
            {
                return null;
            }
#if DEBUG
            if (!isControl)
            {
                return null;
            }
#endif

            var symbols = _symbolRepository.GetAllActive().AsEnumerable().ToList();
            List<StockReport> allReports = new List<StockReport>();
            var startDate = DateTime.UtcNow;
            foreach (var symbol in symbols)
            {
                // Fetch will also alter
                var reports = await FetchLast(selector, symbol);
                allReports.AddRange(reports);
                System.Threading.Thread.Sleep(500);
            }
            var endDate = DateTime.UtcNow;

            var endpoint = new StockEndpoint()
            {
#if DEBUG
                Description = isControl ? "IMPORTANT Stock Debug POOL CTRL" : "IMPORTANT Debug POOL CRON took " + (endDate - startDate).TotalSeconds + "seconds!",
#else
                Description = isControl ? "IMPORTANT Stock PROD POOL CTRL" : "IMPORTANT PROD POOL CRON took " + (endDate - startDate).TotalSeconds + "seconds!",
#endif
                Url = "https://localhost:5000/api/symbol/{symbolId:1}/collect/{date:2020-03-26}",
                Updated = DateTimeOffset.UtcNow.ToUnixTimeSeconds(),
                Name = "news",
                Action = JsonConvert.SerializeObject(allReports)
            };
            await _endpointRepository.AddAsync(endpoint);
            return allReports.ToList();
        }

        public async Task<List<StockReport>> QuickPull(BaseSelector selector, bool isControl = false)
        {
#if DEBUG
            // return null;
#endif
            var ints = _alertRepository.GetAllWithUsersAndSymbols().Where(p => p.IsEnabled).Select(p => p.Symbol.SymbolId).ToList();
            var symbols = _symbolRepository.GetAllActive().Where(p => ints.Contains(p.SymbolId)).AsEnumerable().ToList();
            List<StockReport> allReports = new List<StockReport>();
            foreach (var symbol in symbols)
            {
                // Fetch will also alter
                var reports = await FetchLast(selector, symbol);
                allReports.AddRange(reports);
                System.Threading.Thread.Sleep(500);
            }

            var endpoint = new StockEndpoint()
            {
#if DEBUG
                Description = isControl ? "IMPORTANT Stock Debug POOL CTRL" : "IMPORTANT Debug POOL CRON",
#else
                Description = isControl ? "IMPORTANT Stock PROD POOL CTRL" : "IMPORTANT PROD POOL CRON",
#endif
                Url = "https://localhost:5000/api/symbol/{symbolId:1}/collect/{date:2020-03-26}",
                Updated = DateTimeOffset.UtcNow.ToUnixTimeSeconds(),
                Name = "news",
                Action = JsonConvert.SerializeObject(allReports)
            };
            await _endpointRepository.AddAsync(endpoint);
            return allReports.ToList();
        }

        public async Task<StockReport> Pool(BaseSelector selector, Symbol symbol, DateTime date)
        {
            return await PoolSymbolOnDate(selector, symbol, date);
        }

        public async Task<List<StockResult>> GetLasts(BaseSelector selector, int ticksCount)
        {
            var symbols = _symbolRepository.GetAllActive();
            foreach (var symbol in symbols)
            {
                await FetchLast(selector, symbol, 0);
            }
            var dates = new List<DateTime>() { };
            for (var i = ticksCount / 54 - 1; i >= 0; i--)
            {
                dates.Add(DateTime.UtcNow.Date.AddDays(-i));
            }
            return await GetBulk(selector, dates);
        }
        
        public async Task<StockReport> FetchDate(BaseSelector selector, Symbol symbol, DateTime date, bool save = false, bool fromFile = false)
        {
            var stringData = await FetchString(selector, new DownloadOptions(){
                SymbolIds = new int[] { symbol.SymbolId },
                Dates = new DateTime[] { date },
                FromFileIfExists = true
            });
            var tickArray = await ProcessString(selector, new DownloadOptions()
            {
                SymbolIds = new int[] { symbol.SymbolId },
                Dates = new DateTime[] { date },
                FromFileIfExists = true
            }, stringData);
            var data = await PoolSymbolTicksOnDate(selector, symbol, date, true, save, fromFile);
            if (data != null && data.TickIdId > 0)
            {
                // await _tickRepository.GetByIdAsync(data.TickIdId);
            }
            return data;
        }

        private async Task<StockReport> PoolSymbolTicksOnDate(BaseSelector selector, Symbol symbol, DateTime date, bool IsImportant = false, bool saveOnly = false, bool fromFile = false)
        {

            var logEntryBreakdown = new LogEntryBreakdown("PoolSymbolTicksOnDate");
            // Console.WriteLine(data);
            StockEndpoint endpoint = null;
            bool tickExists = false;
            try
            {
                if (date > DateTime.MinValue)
                {
                    var stringData = await FetchString(selector, new DownloadOptions()
                    {
                        SymbolIds = new int[] { symbol.SymbolId },
                        Dates = new DateTime[] { date },
                        FromFileIfExists = true
                    });
                }
                var ticksArray = await ProcessString(selector, new DownloadOptions()
                {
                    SymbolIds = new int[] { symbol.SymbolId },
                    Dates = new DateTime[] { date },
                    FromFileIfExists = true
                });
                var seconds = (new DateTimeOffset(date.Date)).ToUnixTimeSeconds();

                var existentTick = await UpsertTicks(selector, symbol, date, ticksArray);

                var symbolLastValue = (long)(symbol.Value * 100);
                symbol.Value = (double)ticksArray.LastOrDefault()[1];
                symbol.DayStart = (double)ticksArray.FirstOrDefault()[1];
                symbol.DayEnd = (double)ticksArray.LastOrDefault()[1];
                symbol.DayPercentage = symbol.DayStart == 0 || symbol.DayEnd == 0 ? 0 : symbol.DayStart < symbol.DayEnd ? (100 - (symbol.DayStart * 100 / symbol.DayEnd)) : (100 - (symbol.DayEnd * 100 / symbol.DayStart)) * -1;
                symbol.Samples = ticksArray.Length;
                symbol.Updated = DateTimeOffset.UtcNow.ToUnixTimeSeconds();
                await _symbolRepository.UpdateAsync(symbol);

                // Send email alerts
                var alerts = _alertRepository.All().ToList();
                foreach (var alert in alerts)
                {
                    // await EvaluateAndSendAlert(symbol, alert, symbolLastValue);
                }
                /*
                endpoint = new Endpoint()
                {
#if DEBUG
                    Description = "Debug " + (IsImportant ? "IMP" : "") + $": {ticksArray.Length} ticks for symbol {symbol.SymbolId}",
#else
                        Description = "Prod " + (IsImportant ? "IMP":"") + $": {ticksArray.Length} ticks for symbol {symbol.SymbolId}",
#endif
                    Url = url,
                    Updated = DateTimeOffset.UtcNow.ToUnixTimeSeconds(),
                    Name = "GetSymbolTicksOnDate",
                    Action = (data.Length > 500 ? data.Substring(0, 500) + "..." : data)
                };
                if (IsImportant) await _endpointRepository.AddAsync(endpoint);*/

                logEntryBreakdown.EndTime = DateTimeOffset.UtcNow.ToUnixTimeSeconds();
                logEntryBreakdown.Messages.Add(symbol.Code);
                await _logRepository.AddAsync(new Log()
                {
                    StartTime = logEntryBreakdown.StartTime,
                    EndTime = logEntryBreakdown.EndTime,
                    Tag = logEntryBreakdown,
                    TagString = JsonConvert.SerializeObject(logEntryBreakdown),
                    Source = CommandSource.Startup,
                    Method = logEntryBreakdown.Indicator
                });
                return new StockReport
                {
                    SymbolId = symbol.SymbolId,
                    TickIdId = existentTick.TickId,
                    Code = symbol.Code,
                    Samples = ticksArray.Length,
                    Message = tickExists ? "OK-U!" : "OK-A!",
                    Date = date,
#if DEBUG
                    Url = symbol.Code,
#endif
                    Updated = DateTime.UtcNow,
                    Alerts = alerts.Count(),
                    Last = (decimal)ticksArray.LastOrDefault()[1]
                };
                /*}
                else
                {
                    var tick = new Tick()
                    {
                        Date = date,
                                    Seconds = (new DateTimeOffset(DateTime.Today.Date)).ToUnixTimeSeconds(),
                        Serialized = JsonConvert.SerializeObject(ticksArray),
                        Symbol = symbol.Name,
                        SymbolId = symbol.SymbolId,
                    };
                    var s = await _tickRepository.AddAsync(tick);
                    return new Report
                    {
                        SymbolId = symbol.SymbolId,
                        TickIdId = tick.TickId,
                        Code = symbol.Code,
                        Samples = ticksArray.Length,
                        Message = "OK-A!",
                        Updated = DateTime.UtcNow,
                        Alerts = _alertRepository.All().Where(p => p.Symbol.SymbolId == symbol.SymbolId && p.IsEnabled == true).Count(),
                        Last = (decimal)values.LastOrDefault().average
                    };
                }*/
            }
            catch (Exception ex)
            {
                new Info(selector, -1, ex, string.Empty);
                // _logger.LogError(ex, "StockService Exception");
                logEntryBreakdown.Messages.Add(ex.Message + (ex.InnerException != null ? ". " + ex.InnerException.Message : ""));
                logEntryBreakdown.Messages.Add(symbol.Code);
                logEntryBreakdown.EndTime = DateTimeOffset.UtcNow.ToUnixTimeSeconds();
                await _logRepository.AddAsync(new Log()
                {
                    StartTime = logEntryBreakdown.StartTime,
                    EndTime = logEntryBreakdown.EndTime,
                    Tag = logEntryBreakdown,
                    TagString = JsonConvert.SerializeObject(logEntryBreakdown),
                    Source = CommandSource.Startup,
                    Method = logEntryBreakdown.Indicator
                });

                return new StockReport
                {
                    SymbolId = symbol.SymbolId,
                    Samples = 0,
                    Message = ex.Message,
                    Updated = DateTime.UtcNow,
                    Alerts = 0,
                    Last = -1
                };
            }

            logEntryBreakdown.EndTime = DateTimeOffset.UtcNow.ToUnixTimeSeconds();
            await _logRepository.AddAsync(new Log()
            {
                StartTime = logEntryBreakdown.StartTime,
                EndTime = logEntryBreakdown.EndTime,
                Tag = logEntryBreakdown,
                TagString = JsonConvert.SerializeObject(logEntryBreakdown),
                Source = CommandSource.Startup,
                Method = logEntryBreakdown.Indicator
            });
            return null;
        }

        public async Task<List<DownloadReport>> Download(BaseSelector selector, DownloadOptions options)
        {/*
            options.SelectorOptions.FromDateSeconds > 0 ? options.SelectorOptions.FromDateSeconds - options.SelectorOptions.FromDateOffset * 86400
            var dates = new List<DateTime>() { };
            for (var i = days - 1; i >= 0; i--)
            {
                dates.Add(DateTime.UtcNow.Date.AddDays(-i));
            }*/
            return null;
        }
        
        /// <summary>
        /// Use this link: https://localhost:5000/stock/collect/4
        /// </summary>
        /// <param name="symbol"></param>
        /// <param name="days"></param>
        /// <returns></returns>
        public async Task<List<StockReport>> FetchLast(BaseSelector selector, Symbol symbol, int days = 0)
        {
            var dates = new List<DateTime>() { };
            for (var i = days; i >= 0; i--)
            {
                dates.Add(DateTime.UtcNow.Date.AddDays(-i));
            }

            List<StockReport> reports = new List<StockReport>();
            foreach (var date in dates)
            {
                var report = await PoolSymbolTicksOnDate(selector, symbol, date, false);
                reports.Add(report);
            }
            return reports;
        }
        
        public async Task<List<StockResult>> GetLastDays(BaseSelector selector, int days)
        {
            var dates = new List<DateTime>() { };
            for (var i = days - 1; i >= 0; i--)
            {
                dates.Add(DateTime.UtcNow.Date.AddDays(-i));
            }
            return await GetBulk(selector, dates);
        }

        async Task<List<StockResult>> GetBulk(BaseSelector selector, List<DateTime> dates)
        {   
            var response = new List<StockResult>();
            var symbols = new List<string>() { "MSFT", "UBER", "BA", "LLOY", "TSLA", "DOM", "KO", "AMD", "NVDA", "EXPN", "LSE", "TSCO", "ULVR", "ABBV", "EL", "PYPL", "GSK", "MSTF", "CCL", "ISF", "IUSA" };
            foreach (var symbol in symbols)
            {
                StockResult result = new StockResult()
                {
                    Symbol = symbol,
                    Data = ""
                };
                foreach (var date in dates)
                {
                    var serialized = _tickRepository.All().FirstOrDefault(p => p.Date == date && p.Symbol == symbol)?.Serialized;
                    if (!string.IsNullOrEmpty(serialized) && serialized.Length > 2)
                    {
                        result.Data += "," + serialized.Substring(1, serialized.Length - 2);
                    }
                }
                if (!string.IsNullOrEmpty(result.Data.ToString()))
                {
                    result.Data = "[" + result.Data.ToString().Substring(1) + "]";
                }
                else
                {
                    result.Data = "[]";
                }
                response.Add(result);
            }
            return response;
        }

        public async Task<string> FetchString(BaseSelector selector, DownloadOptions options)
        {
            var symbolId = options.SymbolIds[0];
            var date = options.Dates[0];
            var symbolCode = Static.SymbolCodeFromId[symbolId];

            if (string.IsNullOrEmpty(symbolCode))
            {
                return null;
            }

            LogEntryBreakdown logEntryBreakdown = new LogEntryBreakdown("PoolSymbolTicksOnDate " + symbolCode + " - " + date.ToShortDateString());
            var fileName = downloaderOptions.DailySymbolHistoryFolder + "\\Intraday_" + symbolCode + "_" + date.ToString("yyyyMMdd") + ".json";
            var url = "https://cloud.iexapis.com/stable/stock/" + symbolCode + "/chart/date/" + date.ToString("yyyyMMdd") + "?token=" + downloaderOptions.IEX_Token;
            var loggingUrl = "https://cloud.iexapis.com/stable/stock/" + symbolCode + "/chart/date/" + date.ToString("yyyyMMdd");
            string dataString = string.Empty;
            if (string.IsNullOrEmpty(dataString) && options.FromFileIfExists && File.Exists(fileName))
            {
                new Info(selector, symbolId, "  From file... " + fileName);
                dataString = File.ReadAllText(fileName);
            }
            if(string.IsNullOrEmpty(dataString))
            {
                new Info(selector, symbolId, "  From IEX.Cloud... <a target='_blank' href='"+ url + "'>" + loggingUrl + "</a>");
                dataString = await GetJsonStream(url);
            }

            if (dataString == "Unknown symbol" || dataString == "Forbidden" || dataString == "You have exceeded your allotted message quota. Please enable pay-as-you-go to regain access")
            {
                return null;
            }
            if (options.IfFileMissingSave_DailySymbolHistoryFolder && !File.Exists(fileName))
            {
                try
                {
                    File.WriteAllText(fileName, dataString);
                }
                catch (Exception es) { }
            }
            return dataString;
        }

        public async Task<int> FetchToday(BaseSelector selector)
        {
            var symbols = _symbolRepository.GetAllActive();
            foreach (var symbol in symbols)
            {
                await FetchLast(selector, symbol, 0);
            }
            return 0;
        }
        
        public async Task<Tick> UpsertTicks(BaseSelector selector, Symbol symbol, DateTime date, object[][] ticksArray)
        {
            long seconds = 0;
            if (date > DateTime.MinValue)
            {
                seconds = (new DateTimeOffset(date.Date)).ToUnixTimeSeconds();
            }

            var existentTick = _tickRepository.All().FirstOrDefault(p => p.Seconds == seconds && p.SymbolId == symbol.SymbolId);
            // if (date.DayOfYear != DateTime.UtcNow.DayOfYear)
            // {
            if (existentTick != null)
            {
                // tickExists = true;
                existentTick.Serialized = JsonConvert.SerializeObject(ticksArray);
                existentTick.Samples = ticksArray.Length;
                existentTick.Date = date;
                existentTick.Seconds = seconds;
                // existentTick.SymbolId = symbol.SymbolId;
                await _tickRepository.UpdateAsync(existentTick);
            }
            else
            {
                existentTick = new Tick()
                {
                    Date = date,
                    Seconds = seconds,
                    Serialized = JsonConvert.SerializeObject(ticksArray),
                    Samples = ticksArray.Length,
                    Symbol = symbol.Code,
                    SymbolId = symbol.SymbolId,
                };
                await _tickRepository.AddAsync(existentTick);
            }
            return existentTick;
        }


        public async Task<string> GetJsonStream(string url)
        {
            HttpClient client = new HttpClient();
            HttpResponseMessage response = await client.GetAsync(url);
            string content = await response.Content.ReadAsStringAsync();
            return content;
        }
        
        #endregion
        
        #region CACHING
        
        public async Task<List<Tick>> GetAllHistoricalFromDb(BaseSelector selector, bool overwite, bool saveToFile = true, bool saveToDb = false, bool overWrite = false)
        {
            var startTime = DateTimeOffset.UtcNow.ToUnixTimeSeconds();

            var resultKey = "AllHistoricalDictionary";

            if (File.Exists(downloaderOptions.LargeResultsFolder + "\\" + resultKey + ".json"))
            {
                var fileContent = File.ReadAllText(downloaderOptions.LargeResultsFolder + "\\" + resultKey + ".json");
                return (List<Tick>)JsonConvert.DeserializeObject<List<Tick>>(fileContent);
            }
            var result = _resultRepository.GetAllByKey(resultKey).FirstOrDefault();
            if (result != null && !overWrite)
            {
                return (List<Tick>)JsonConvert.DeserializeObject<List<Tick>>(result.TagString);
            }

            var symbols = _symbolRepository.GetAllActive().ToList();
            var aggregatedTicks = new List<Tick>();
            for (int symi = 0; symi < symbols.Count; symi++)
            {
                if (!Static.SP500.Contains(symbols[symi].Code))
                {
                    continue;
                }

                var aggregatedTick = _tickRepository.All().FirstOrDefault(p => p.Seconds == 0 && p.SymbolId == symbols[symi].SymbolId);
                if (aggregatedTick == null)
                {
                    var tickArray = await ProcessString(selector, new DownloadOptions(){
                        SymbolIds = new int[] { symbols[symi].SymbolId },
                        Dates = new DateTime[] { DateTime.MinValue },
                        FromFileIfExists = true,
                        ReplaceFileIfExists = overwite,
                        SaveOnly = true
                    });

                    aggregatedTick = await UpsertTicks(selector, symbols[symi], DateTime.MinValue, tickArray);
                }
                aggregatedTicks.Add(aggregatedTick);

                if (saveToFile)
                {
                    var success = await SaveResult(selector, result, startTime, resultKey, aggregatedTicks, false, downloaderOptions.LargeResultsFolder, new SaveResultsOptions()
                    {
                        SaveToDb = true,
                        SaveToFile = true
                    });
                }
            }
            return aggregatedTicks;
        }

        public async Task<Dictionary<int, object[][]>> GetAllHistoricalFromFiles(BaseSelector selector, bool overwite, bool saveToFile = true, bool saveToDb = false, bool overWrite = false)
        {
            var startTime = DateTimeOffset.UtcNow.ToUnixTimeSeconds();
            overWrite = false;

            var resultKey = "AllHistoricalDictionary";
            Result result = null;
            if (!overWrite && File.Exists(downloaderOptions.LargeResultsFolder + "\\" + resultKey + ".json"))
            {
                var fileContent = File.ReadAllText(downloaderOptions.LargeResultsFolder + "\\" + resultKey + ".json");
                result = (Result)JsonConvert.DeserializeObject<Result>(fileContent);
                return (Dictionary<int, object[][]>)result.Tag;
            }

            Dictionary<int, object[][]> priceDictionary = new Dictionary<int, object[][]>();
            var symbols = _symbolRepository.GetAllActive().ToList();
            for (int symi = 0; symi < symbols.Count; symi++)
            {
                if (!Static.SP500.Contains(symbols[symi].Code))
                {
                    continue;
                }
                if(symbols[symi].Code == "KO")
                {

                }

                var tickArray = await ProcessString(selector, new DownloadOptions()
                {
                    SymbolIds = new int[] { symbols[symi].SymbolId },
                    Dates = new DateTime[] { DateTime.MinValue },
                    FromFileIfExists = true,
                    SaveOnly = false,
                    ReplaceFileIfExists = overwite
                    });
                try
                {
                    priceDictionary.Add(symbols[symi].SymbolId, tickArray);
                }
                catch (Exception ex)
                {
                    new Info(selector, -1, ex, string.Empty);
                }
            }
            if (saveToFile)
            {
                var success = await SaveResult(selector, result, startTime, resultKey, priceDictionary, false, downloaderOptions.LargeResultsFolder, new SaveResultsOptions()
                {
                    SaveToDb = false,
                    SaveToFile = true
                });
            }
            return (Dictionary<int, object[][]>)result.Tag;
        }
        
        #endregion
        
        #region REPORTING
        
        

        public async Task<List<DownloadIntradayReport>> BuildDownloadSummary(BaseSelector selector, ReportingOptions reportingOptions, bool overWrite)
        {
            var resultKey = "HistoricalFilesSummary" + (reportingOptions.StartingAt > DateTime.MinValue ? "_" + reportingOptions.StartingAt.ToShortDateString() : string.Empty);
            
            // Try to recover it from disk
            List<DownloadIntradayReport> reports = new List<DownloadIntradayReport>();
            if (File.Exists(downloaderOptions.LargeResultsFolder + "\\" + resultKey + ".json"))
            {
                var fileContent = File.ReadAllText(downloaderOptions.LargeResultsFolder + "\\" + resultKey + ".json");
                return (List<DownloadIntradayReport>)JsonConvert.DeserializeObject<List<DownloadIntradayReport>>(fileContent);
            }
            
            // Try to recover it from DB
            var result = _resultRepository.GetAllByKey(resultKey).FirstOrDefault();
            if (result != null && !overWrite)
            {
                return (List<DownloadIntradayReport>)JsonConvert.DeserializeObject<List<DownloadIntradayReport>>(result.TagString);
            }
            
            // Preparing for action
            var startTime = DateTimeOffset.UtcNow.ToUnixTimeSeconds();
            string[] FilePaths;
            FilePaths = Directory.GetFiles(downloaderOptions.DailySymbolHistoryFolder, "Intraday_*_*.json");
            FilePaths = FilePaths.OrderBy(p => p).ToArray();

            int simbsLeft = Static.SP500.Count();
            foreach (var code in Static.SP500)
            {
                simbsLeft--;
                var symbolId = 0;
                var symbol = await _symbolRepository.GetByCodeAsync(code);
                if (symbol == null)
                {
                    continue;
                }
                symbolId = symbol.SymbolId;
                var report = new DownloadIntradayReport()
                {
                    SymbolId = symbolId,
                    Code = code,
                    Details = new List<IntradayDetail>()
                };
                var codePaths = FilePaths.Where(p => p.IndexOf("_" + code + "_") > 0);
                int left = codePaths.Count();

                DateTime from = DateTime.MaxValue;
                DateTime to = DateTime.MinValue;
                foreach (var codePath in codePaths)
                {
                    left--;
                    if (simbsLeft > 438)
                    {
                        //continue;
                    }
                    var dateString = codePath.Substring(codePath.LastIndexOf("_") + 1);
                    dateString = dateString.Substring(0, dateString.LastIndexOf("."));
                    dateString = dateString.Substring(6, 2) + "-" + dateString.Substring(4, 2) + "-" + dateString.Substring(0, 4);
                    DateTime date = DateTime.Parse(dateString);
                    if (date < from)
                    {
                        from = date;
                    }
                    if (to < date)
                    {
                        to = date;
                    }

                    var dataString = File.ReadAllText(codePath);
                    var count = 0;
                    var itemsLength = 0;
                    try
                    {
                        var items = (List<IexItem>)JsonConvert.DeserializeObject<List<IexItem>>(dataString);
                        if (items.Count() > 0)
                        {
                            count = items.Where(p => p.marketAverage.HasValue && p.marketAverage != 0).Count();
                        }
                        itemsLength = items.Count();
                    }
                    catch (Exception ex)
                    {
                        new Info(selector, -1, ex, string.Empty);
                        itemsLength = -1;
                    }
                    if (left == 0 || left % 50 == 0)
                    {
                        Console.WriteLine(simbsLeft + " " + code + ": " + left);
                    }
                    report.Details.Add(new IntradayDetail()
                    {
                        Seconds = (new DateTimeOffset(date)).ToUnixTimeSeconds(),
                        Samples = count,
                        Total = itemsLength,
                        Count = 1,

                    });
                }

                // Aggregate daily results
                report.From = from;
                report.To = to;
                report.Count = codePaths.Count();
                reports.Add(report); ;
            }
            
            // Saving action results
            var success = await SaveResult(selector, result, startTime, resultKey, reports, overWrite, downloaderOptions.SymbolHistoryFolder, new SaveResultsOptions
            {
                SaveToDb = true,
                SaveToFile = true
            });
            return reports;
        }
        public async Task<IEnumerable<FileResourceGroup>> ListFiles(BaseSelector selector)
        {
            List<FileResourceGroup> result = new List<FileResourceGroup>();

            string[] dailySymbolHistoryFiles;
            dailySymbolHistoryFiles = new string[] { };// Directory.GetFiles(downloaderOptions.DailySymbolHistoryFolder);
            dailySymbolHistoryFiles = dailySymbolHistoryFiles.OrderBy(p => p).ToArray();

            string[] dailyGraphsFiles;
            dailyGraphsFiles = Directory.GetFiles(downloaderOptions.DailyGraphsFolder);
            dailyGraphsFiles = dailyGraphsFiles.OrderBy(p => p).ToArray();

            string[] symbolHistoryFiles;
            symbolHistoryFiles = Directory.GetFiles(downloaderOptions.SymbolHistoryFolder);
            symbolHistoryFiles = symbolHistoryFiles.OrderBy(p => p).ToArray();

            string[] largeResultsFiles;
            largeResultsFiles = Directory.GetFiles(downloaderOptions.LargeResultsFolder);
            largeResultsFiles = largeResultsFiles.OrderBy(p => p).ToArray();

            foreach (var path in dailySymbolHistoryFiles)
            {
                var fileInfo = new FileInfo(path);
                result.Add(new FileResourceGroup
                {
                    Category = "DailySymbolHistory",
                    // Folder = downloaderOptions.DailySymbolHistoryFolder,
                    Name = fileInfo.Name,
                    Length = fileInfo.Length,
                    LastWrite = Static.SecondsFromDateTime(fileInfo.LastWriteTimeUtc)
                });
            }

            foreach (var path in dailyGraphsFiles)
            {
                var fileInfo = new FileInfo(path);
                result.Add(new FileResourceGroup
                {
                    Category = "DailySymbolHistory",
                    // Folder = downloaderOptions.DailySymbolHistoryFolder,
                    Name = fileInfo.Name,
                    Length = fileInfo.Length,
                    LastWrite = Static.SecondsFromDateTime(fileInfo.LastWriteTimeUtc)
                });
            }

            foreach (var path in symbolHistoryFiles.Take(50))
            {
                var fileInfo = new FileInfo(path);
                result.Add(new FileResourceGroup
                {
                    Category = "DailySymbolHistory",
                    // Folder = downloaderOptions.DailySymbolHistoryFolder,
                    Name = fileInfo.Name,
                    Length = fileInfo.Length,
                    LastWrite = Static.SecondsFromDateTime(fileInfo.LastWriteTimeUtc)
                });
            }

            foreach (var path in largeResultsFiles)
            {
                var fileInfo = new FileInfo(path);
                result.Add(new FileResourceGroup
                {
                    Category = "DailySymbolHistory",
                    // Folder = downloaderOptions.DailySymbolHistoryFolder,
                    Name = fileInfo.Name,
                    Length = fileInfo.Length,
                    LastWrite = Static.SecondsFromDateTime(fileInfo.LastWriteTimeUtc)
                });
            }
            return result;
        }

        public List<DownloadIntradayReport> GetFileRecordsByReportingOptions(ReportingOptions reportingOptions)
        {
            string[] FilePaths;
            FilePaths = Directory.GetFiles(downloaderOptions.DailySymbolHistoryFolder, "Intraday_*_*.json");
            FilePaths = FilePaths.OrderBy(p => p).ToArray();
            return null;
        }

        public IQueryable<Daily> GetDailyRecordsByReportingOptions(ReportingOptions reportingOptions)
        {
            var items = _dailyRepository.All().Where(p =>
                (string.IsNullOrEmpty(reportingOptions.Code) || p.Symbol.Code == reportingOptions.Code) &&
                (reportingOptions.SymbolId == 0 || p.Symbol.SymbolId == reportingOptions.SymbolId)
            );
            if (reportingOptions.StartingAt > DateTime.MinValue)
            {
                items = items.Where(p=>p.Date > reportingOptions.StartingAt);
            }
            if (reportingOptions.Take > 0)
            {
                items = items.Take(reportingOptions.Take);
            }
            return items;
        }

        public IQueryable<Tick> GetIntradayRecordsByReportingOptions(ReportingOptions reportingOptions)
        {
            var items = _tickRepository.All().Where(p =>
                (string.IsNullOrEmpty(reportingOptions.Code) || p.Symbol == reportingOptions.Code) &&
                (reportingOptions.SymbolId == 0 || p.SymbolId == reportingOptions.SymbolId)
            );
            if (reportingOptions.StartingAt > DateTime.MinValue)
            {
                items = items.Where(p=>p.Date > reportingOptions.StartingAt);
            }
            if (reportingOptions.Take > 0)
            {
                items = items.Take(reportingOptions.Take);
            }
            return items;
        }
        
        #endregion REPORTING
        
        #region FRAGMENTS
        


        public async Task<string> CreateAndSaveSegments(BaseSelector selector, ResultSelector resultSelector)
        {
            var TagString = resultSelector.TagString;
            if (string.IsNullOrEmpty(TagString))
            {
                StringBuilder sb = new StringBuilder();
                var samples = await CreateFragmentsFromDb(selector, resultSelector);
                foreach (var sample in samples)
                {
                    sb.AppendLine(sample.ToString());
                }
                TagString = sb.ToString();
            }

            resultSelector.TagString = null;
            var key = $"Fragments_id_{ resultSelector.SymbolId }_len_{ resultSelector.Length }_len_{ resultSelector.Lost }_len_{ resultSelector.Margin }_len_{ resultSelector.Save }";
            var existentResult = _resultRepository.GetAllByKey(key).FirstOrDefault();
            if (existentResult != null)
            {
                if (resultSelector.Replace)
                {
                    existentResult.TagString = TagString;
                    var s = _resultRepository.UpdateAsync(existentResult).Result;
                }
            }
            else
            {
                await _resultRepository.AddAsync(new Result(selector)
                {
                    TagString = TagString,
                    Key = key,
                    User = null
                });
            }
            return resultSelector.TagString;
        }

        // Skip this for now.
        public async Task<IEnumerable<Sample>> CreateFragmentsFromFiles(BaseSelector selector, ResultSelector resultSelector)
        {
            bool running = true;
            List<Sample> samples = new List<Sample>();
            string[] FilePaths = null;

            var symbol = await _symbolRepository.GetByIdAsync(resultSelector.SymbolId);
            FilePaths = Directory.GetFiles(downloaderOptions.SymbolHistoryFolder, resultSelector.SymbolId > 0 ? symbol.Code + ".json" : "*.json");
            FilePaths = FilePaths.OrderBy(p => p).ToArray();

            var filesCount = FilePaths.Count();
            int fileIndex = 0;
            for (int f = 0; f < FilePaths.Length; f++)
            {
                if (!running)
                {
                    f--;
                    Thread.Sleep(1000);
                    continue;
                }
                var filePath = FilePaths[f];
                fileIndex++;
                string content = File.ReadAllText(filePath);
                var data = (List<TickArray>)JsonConvert.DeserializeObject<List<TickArray>>(content);
                if (data.Count < resultSelector.Margin + resultSelector.Length)
                {
                    continue;
                }
                samples.AddRange(CreateFragments(selector, resultSelector, symbol, data));
            }
            return samples;
        }


        // Skip this for now.
        public async Task<IEnumerable<Sample>> CreateFragmentsFromDb(BaseSelector selector, ResultSelector resultSelector)
        {
            resultSelector.SymbolId = 5;
            resultSelector.Take = 100;
            if (resultSelector.Margin == 0) resultSelector.Margin = 3;


            bool running = true;
            List<Sample> samples = new List<Sample>();
            var symbol = await _symbolRepository.GetByIdAsync(resultSelector.SymbolId);
            var symbolRecords = _tickRepository.All().Where(p=>(symbol == null || p.SymbolId == symbol.SymbolId) && p.Samples > 1000);
            foreach(var s in symbolRecords)
            {
                var data = (List<TickArray>)JsonConvert.DeserializeObject<List<TickArray>>(s.Serialized);
                if (data.Count < resultSelector.Margin + resultSelector.Length)
                {
                    continue;
                }
                samples.AddRange(CreateFragments(selector, resultSelector, symbol, data));
            }
            return samples;
        }

        private IEnumerable<Sample> CreateFragments(BaseSelector selector, ResultSelector resultSelector, Symbol symbol, List<TickArray> data)
        {
            List<Sample> samples = new List<Sample>();
            int anchor = 0;
            int low = 0;
            double trigger = 0;
            Sample lastSample = null;
            for (int i = 0; i < data.Count; i++)
            {
                if(samples.Count() >= resultSelector.Take)
                {
                    break;
                }

                if (lastSample != null)
                {
                    if (data[i].V < lastSample.Min.V)
                    {
                        lastSample.Min = data[i];
                    }
                }
                if (i == 0 || data[i].V > data[anchor].V)
                {
                    // Price going up, update anchor
                    anchor = i;
                    trigger = data[i].V - data[i].V / 100.0 * resultSelector.Lost;
                    lastSample = null;
                    continue;
                }
                if (data[i].V <= trigger)
                {
                    // This is it!
                    var values = data.Skip(i - resultSelector.Margin - resultSelector.Length + 1).Take(resultSelector.Length + resultSelector.Margin).ToList();
                    lastSample = new Sample
                    {
                        Code = symbol.Code,
                        Date = _userRepository.ReadableTimespan(values[0].T),
                        Pick = data[anchor],
                        Entry = values[0],
                        Margin = values[resultSelector.Length],
                        Lost = values[values.Count - 1],
                        Min = data[i],
                        Values = values.Select(p => p.V)
                    };
                    anchor = i;
                    trigger = data[i].V - data[i].V / 100.0 * resultSelector.Lost;
                    low = i;
                    if (values[0].V == values[1].V && values[1].V == values[2].V)
                    {
                        continue;
                    }
                    samples.Add(lastSample);
                    continue;
                }

            }
            return samples;
        }


        // Skip this for now.
        public async Task<IEnumerable<Sample>> CreateFragmentsFromIntraday(BaseSelector selector, ResultSelector resultSelector)
        {
            int FrameSize = 15;
            decimal PercentChange = 2;
            int BufferSize = 2;
            bool running = true;
            List<Sample> samples = new List<Sample>();
            string[] FilePaths = null;

            FilePaths = Directory.GetFiles(downloaderOptions.DailySymbolHistoryFolder, "Intraday_*_*.json");
            FilePaths = FilePaths.OrderBy(p => p).ToArray();

            var filesCount = FilePaths.Count();
            int fileIndex = 0;
            for (int f = 0; f < FilePaths.Length; f++)
            {
                if (!running)
                {
                    f--;
                    Thread.Sleep(1000);
                    continue;
                }
                var filePath = FilePaths[f];
                fileIndex++;
                var fileName = filePath.Substring(filePath.LastIndexOf("\\") + 1);
                var symbolCode = fileName.Substring(fileName.IndexOf("_") + 1);
                symbolCode = symbolCode.Substring(0, symbolCode.IndexOf("_"));
                var fileDate = fileName.Substring(fileName.LastIndexOf("_") + 1);
                var dateString = fileName.Substring(fileName.LastIndexOf("_") + 1);
                dateString = dateString.Substring(6, 2) + "-" + dateString.Substring(4, 2) + "-" + dateString.Substring(0, 4);
                /*
                var fileName = filePath.Substring(filePath.LastIndexOf("\\") + 1);
                LabelStatus.SetText("[" + fileIndex + "/" + filesCount + "] " + fileName);
                var symbolCode = fileName.Substring(0, fileName.IndexOf("-"));
                var dateString = fileName.Substring(fileName.LastIndexOf("-") + 1);
                dateString = dateString.Substring(0, 10);
                */
                string content = File.ReadAllText(filePath);
                var data = (List<IexItem>)JsonConvert.DeserializeObject<List<IexItem>>(content);
                if (data.Count < FrameSize + BufferSize)
                {
                    continue;
                }
                decimal anchor = 0;
                string anchorMinute = data[0].minute;
                decimal price = 0;
                double percent = 0;
                for (int i = 0; i < data.Count; i++)
                {
                    if (!data[i].marketAverage.HasValue || data[i].marketAverage.Value <= 0)
                    {
                        continue;
                    }
                    if (i < BufferSize + FrameSize)
                    {
                        continue; // Insufficient frame size
                    }

                    if (anchor < data[i].marketAverage.Value)
                    {
                        anchor = data[i].marketAverage.Value;
                        anchorMinute = data[0].minute;
                        price = anchor;
                        continue;
                    }

                    if (price > data[i].marketAverage.Value)
                    {
                        price = data[i].marketAverage.Value;
                    }
                    if (anchor / 100 * price <= PercentChange)
                    {
                        if (!data[i - FrameSize].marketAverage.HasValue || !data[i - BufferSize - FrameSize].marketAverage.HasValue)
                        {
                            continue;
                        }

                        // This is it!
                        var values = data.Skip(i - BufferSize - FrameSize).Take(BufferSize + FrameSize).Select(p => p.marketAverage.HasValue ? p.marketAverage.Value : 0);
                        if (values.Any(p => p <= 0))
                        {
                            continue; // Missing values in frame
                        }/*
                        var sample = new Sample
                        {
                            Code = symbolCode,
                            Date = fileDate,
                            Anchor = anchor,
                            From = new DateTimeOffset(DateTime.Parse(dateString + " " + anchorMinute)).ToUnixTimeSeconds(),
                            Timestamp = new DateTimeOffset(DateTime.Parse(dateString + " " + data[i].minute)).ToUnixTimeSeconds(),
                            FramePercent = anchor / 100 * data[i - FrameSize].marketAverage.Value,
                            BreakPercent = anchor / 100 * data[i - BufferSize - FrameSize].marketAverage.Value,
                            // Values = data.Skip(i - BufferSize - FrameSize).Take(BufferSize + FrameSize).Select(p => p.marketAverage.HasValue ? p.marketAverage.Value : 0)
                        };
                        anchor = price;
                        sample.Distance = sample.Timestamp - sample.From;
                        if (take == 0 || samples.Count < take)
                        {
                            samples.Add(sample);
                        }
                        */

                        if (resultSelector.Take > 0 && samples.Count == resultSelector.Take && !resultSelector.SaveFile)
                        {
                            return samples;
                        }
                    }

                }
            }
            return samples;
        }
        
        #endregion
        
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
