﻿using Ajuro.IEX.Downloader.Models;
using Ajuro.Net.Stock.Repositories;
using Ajuro.Net.Types.Stock.Models;
using All.Ajuro.Net.Types.Stock.Models;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Ajuro.IEX.Downloader.Services
{
    public enum Step { 
        DownloadIntraday, // will download from IEX Cloud resulting in one record per SymbolId and per Day like: CODE-yyyyMMdd
        AggredateDays, // will get all the records of a symbol, order them and merge them into one record
        AggregateSymbols 
    }
    public interface IDownloaderService
    {
        


        void SetOptions(DownloaderOptions options);
        string[] GetSP500();
        Task<List<DownloadIntradayReport>> BuildDownloadSummary(bool overWrite);
        Task<bool> SaveResult(Result existentResult, long startTime, string key, object content, bool replaceIfExists, string backupFolder);


        Task<IEnumerable<Sample>> CreateFragments(ResultSelector resultSelector, int take, bool saveFile = false);

        Task<List<StockReport>> FetchLast(Symbol symbol, int i);

        Task<StockReport> FetchDate(Symbol symbol, DateTime date, bool saveOnly = false, bool fromFile = false);

        Task<string> FetchString(Symbol symbol, DateTime date, FetchOptions fetchOptions);

        Task<object[][]> ProcessString(Symbol symbol, DateTime date, FetchOptions fetchOptions);

        Task<List<StockResult>> GetLasts(int ticksCount);

        Task<int> FetchToday();

        Task<List<StockReport>> QuickPull(bool isControl = false);

        Task<List<StockReport>> Pool(bool isControl = false);

        Task<Tick> UpsertTicks(Symbol symbol, DateTime date, object[][] ticksArray);
        Task<StockReport> Pool(Symbol symbol, DateTime date);

        Task<IEnumerable<TickArray>> GetHistorical(int symbolId, int days, bool fromFiles = false);
        Task<List<Tick>> GetAllHistoricalFromDb(bool overwrite, bool saveToFile = true, bool saveToDb = false, bool overWrite = false);
        Task<Dictionary<int, object[][]>> GetAllHistoricalFromFiles(bool overwrite, bool saveToFile = true, bool saveToDb = false, bool overWrite = false);
    }

    public class DownloaderOptions
    {
        public string IEX_Token { get; set; }
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
        private readonly IAlpacaAccountRepository _alpacaAccountRepository;
        private readonly IAlpacaOrderRepository _alpacaOrderRepository;
        private readonly IAlpacaPositionRepository _alpacaPositionRepository;
        private readonly IDailyRepository _dailyRepository;
        private readonly ITickRepository _tickRepository;
        private readonly INewsRepository _newsRepository;
        private readonly ISymbolRepository _symbolRepository;
        private readonly IPositionRepository _positionRepository;
        private readonly ICountryRepository _countryRepository;
        private readonly ICurrencyRepository _currencyRepository;
        private readonly IEndpointRepository _endpointRepository;
        private readonly IAlertRepository _alertRepository;
        // private readonly ILogger<DownloaderService> _logger;
        private readonly ISubscriptionRepository _subscriptionRepository;
        private readonly ILogRepository _logRepository;

        public DownloaderService
            (
                IUserRepository userRepository,
                IAlpacaAccountRepository alpacaAccountRepository,
                IAlpacaOrderRepository alpacaOrderRepository,
                IAlpacaPositionRepository alpacaPositionRepository,
                ICountryRepository countryRepository,
                ICurrencyRepository currencyRepository,
                ISymbolRepository symbolRepository,
                IAlertRepository alertRepository,
                IPositionRepository positionRepository,
                IDailyRepository dailyRepository,
                ITickRepository tickRepository,
                INewsRepository newsRepository,
                IEndpointRepository endpointRepository,
                ISubscriptionRepository subscriptionRepository,
                ILogRepository logRepository,
                IResultRepository resultRepository
                // ILogger<DownloaderService> logger
            )
        {
            _alpacaAccountRepository = alpacaAccountRepository;
            _alpacaOrderRepository = alpacaOrderRepository;
            _alpacaPositionRepository = alpacaPositionRepository;
            _userRepository = userRepository;
            _dailyRepository = dailyRepository;
            _countryRepository = countryRepository;
            _currencyRepository = currencyRepository;
            _positionRepository = positionRepository;
            _symbolRepository = symbolRepository;
            _alertRepository = alertRepository;
            _endpointRepository = endpointRepository;
            _tickRepository = tickRepository;
            _newsRepository = newsRepository;
            // _logger = logger;
            _subscriptionRepository = subscriptionRepository;
            _resultRepository = resultRepository;
            _logRepository = logRepository;
        }

        public async Task<List<Tick>> GetAllHistoricalFromDb(bool overwite, bool saveToFile = true, bool saveToDb = false, bool overWrite = false)
        {
            var startTime = DateTimeOffset.UtcNow.ToUnixTimeSeconds();

            var resultKey = "AllHistoricalDictionary";

            if (File.Exists(downloaderOptions.LargeResultsFolder + "\\" + resultKey + ".json"))
            {
                var fileContent = File.ReadAllText(downloaderOptions.LargeResultsFolder + "\\" + resultKey + ".json");
                return (List<Tick>)JsonConvert.DeserializeObject<List<Tick>>(fileContent);
            }
            var result = _resultRepository.GetByKey(resultKey).FirstOrDefault();
            if (result != null && !overWrite)
            {
                return (List<Tick>)JsonConvert.DeserializeObject<List<Tick>>(result.TagString);
            }

            var symbols = _symbolRepository.GetAllActive().ToList();
            var aggregatedTicks = new List<Tick>();
            for (int symi = 0; symi < symbols.Count; symi++)
            {
                if (!GetSP500().Contains(symbols[symi].Code))
                {
                    continue;
                }

                var aggregatedTick = _tickRepository.All().FirstOrDefault(p => p.Seconds == 0 && p.SymbolId == symbols[symi].SymbolId);
                if (aggregatedTick == null)
                {
                    var tickArray = await ProcessString(symbols[symi], DateTime.MinValue, new FetchOptions()
                    {
                        FromFile = true,
                        SaveOnly = true,
                        Overwrite = overwite
                    });

                    aggregatedTick = await UpsertTicks(symbols[symi], DateTime.MinValue, tickArray);
                }
                aggregatedTicks.Add(aggregatedTick);

                if (saveToFile)
                {
                    var success = await SaveResult(result, startTime, resultKey, aggregatedTicks, false, downloaderOptions.LargeResultsFolder);
                }
            }
            return aggregatedTicks;
        }

        public async Task<Dictionary<int, object[][]>> GetAllHistoricalFromFiles(bool overwite, bool saveToFile = true, bool saveToDb = false, bool overWrite = false)
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
                if (!GetSP500().Contains(symbols[symi].Code))
                {
                    continue;
                }
                if(symbols[symi].Code == "KO")
                {

                }

                var tickArray = await ProcessString(symbols[symi], DateTime.MinValue, new FetchOptions()
                {
                    FromFile = true,
                    SaveOnly = false,
                    Overwrite = overwite
                });
                try
                {
                    priceDictionary.Add(symbols[symi].SymbolId, tickArray);
                }
                catch (Exception ex)
                {

                }
            }
            if (saveToFile)
            {
                var success = await SaveResult(result, startTime, resultKey, priceDictionary, false, downloaderOptions.LargeResultsFolder);
            }
            return (Dictionary<int, object[][]>)result.Tag;
        }

        private DownloaderOptions downloaderOptions { get; set; }

        public void SetOptions(DownloaderOptions downloaderOptions)
        {
            this.downloaderOptions = downloaderOptions;
        }

        /// <summary>
        /// Merge intraday records into one foreach symbol
        /// </summary>
        public async Task<object[][]> ProcessString(Symbol symbol, DateTime date, FetchOptions fetchOptions)
        {
            var dataString = string.Empty;
            var fileName = string.Empty;
#if DEBUG
            if (date > DateTime.MinValue)
            {
                fileName = downloaderOptions.DailySymbolHistoryFolder + "\\" + "Intraday_" + symbol.Code + "_" + date.ToString("yyyyMMdd") + ".json";
                dataString = File.ReadAllText(fileName);
            }
            else
            {
                fileName = downloaderOptions.SymbolHistoryFolder + "\\" + symbol.Code + ".json";

                if (!fetchOptions.Overwrite && File.Exists(fileName))
                {
                    dataString = File.ReadAllText(fileName);
                    var items = (object[][])JsonConvert.DeserializeObject<object[][]>(dataString);
                    return items;
                }

                var strings = new List<string>();

                List<Sample> samples = new List<Sample>();
                string[] FilePaths = null;

                FilePaths = Directory.GetFiles(downloaderOptions.DailySymbolHistoryFolder, "Intraday_" + symbol.Code + "_*.json");
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
#else
            dataString = await FetchString(symbol, date, fetchOptions);
#endif
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
                        symbol.Updated = DateTimeOffset.UtcNow.ToUnixTimeSeconds();
                        await _symbolRepository.UpdateAsync(symbol);
                    }
                    else
                    {
                        // symbol.Active = false;
                        // symbol.Updated = DateTimeOffset.UtcNow.ToUnixTimeSeconds();
                        // await _symbolRepository.UpdateAsync(symbol);
                    }
                    new StockReport
                    {
                        SymbolId = symbol.SymbolId,
                        Code = symbol.Code,
                        Samples = 0,
                        Message = "No data!",
                        Date = date,
#if DEBUG
                        // Url = url,
#endif
                        Updated = DateTime.UtcNow,
                        Alerts = _alertRepository.All().Where(p => p.Symbol.SymbolId == symbol.SymbolId && p.IsEnabled == true).Count(),
                        Last = -1
                    };
                }
                var ticksArray = values.Where(p => p.marketAverage.HasValue).Where(p => p.marketAverage.Value != -1).Select(p => new object[] { (Int64)(p.date.AddSeconds(ToSeconds(p.minute)).Subtract(new DateTime(1970, 1, 1))).TotalMilliseconds, p.marketAverage.Value }).ToArray();
                if (date == DateTime.MinValue)
                {
                    File.WriteAllText(fileName, JsonConvert.SerializeObject(ticksArray));
                }
                return ticksArray;
            }
            catch (Exception ex)
            {
            }
            return null;
        }

        public async Task<StockReport> FetchDate(Symbol symbol, DateTime date, bool save = false, bool fromFile = false)
        {
            var stringData = await FetchString(symbol, date, new FetchOptions()
            {
                FromFile = true
            });
            var tickArray = await ProcessString(symbol, date, new FetchOptions()
            {
                FromFile = true
            });
            var data = await PoolSymbolTicksOnDate(symbol, date, true, save, fromFile);
            if (data != null && data.TickIdId > 0)
            {
                // await _tickRepository.GetByIdAsync(data.TickIdId);
            }
            return data;
        }

        private async Task<StockReport> PoolSymbolTicksOnDate(Symbol symbol, DateTime date, bool IsImportant = false, bool saveOnly = false, bool fromFile = false)
        {

            var logEntryBreakdown = new LogEntryBreakdown("PoolSymbolTicksOnDate");
            // Console.WriteLine(data);
            StockEndpoint endpoint = null;
            bool tickExists = false;
            try
            {
                if (date > DateTime.MinValue)
                {
                    var stringData = await FetchString(symbol, date, new FetchOptions()
                    {
                        FromFile = true,
                        SaveReportsToDb = true
                    });
                }
                var ticksArray = await ProcessString(symbol, date, new FetchOptions()
                {
                    FromFile = true
                });
                var seconds = (new DateTimeOffset(date.Date)).ToUnixTimeSeconds();

                var existentTick = await UpsertTicks(symbol, date, ticksArray);

                var symbolLastValue = (long)(symbol.Value * 100);
                symbol.Value = (float)ticksArray.LastOrDefault()[1];
                symbol.DayStart = (float)ticksArray.FirstOrDefault()[1];
                symbol.DayEnd = (float)ticksArray.LastOrDefault()[1];
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
                    Source = CommandSource.Scheduler,
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
                    Source = CommandSource.Scheduler,
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
                Source = CommandSource.Scheduler,
                Method = logEntryBreakdown.Indicator
            });
            return null;
        }


        public async Task<List<DownloadIntradayReport>> BuildDownloadSummary(bool overWrite)
        {
            var resultKey = "HistoricalFilesSummary";
            List<DownloadIntradayReport> reports = new List<DownloadIntradayReport>();
            if (File.Exists(downloaderOptions.LargeResultsFolder + "\\" + resultKey + ".json"))
            {
                var fileContent = File.ReadAllText(downloaderOptions.LargeResultsFolder + "\\" + resultKey + ".json");
                return (List<DownloadIntradayReport>)JsonConvert.DeserializeObject<List<DownloadIntradayReport>>(fileContent);
            }
            var result = _resultRepository.GetByKey(resultKey).FirstOrDefault();
            if (result != null && !overWrite)
            {
                return (List<DownloadIntradayReport>)JsonConvert.DeserializeObject<List<DownloadIntradayReport>>(result.TagString);
            }
            var startTime = DateTimeOffset.UtcNow.ToUnixTimeSeconds();
            string[] FilePaths;
            FilePaths = Directory.GetFiles(downloaderOptions.DailySymbolHistoryFolder, "Intraday_*_*.json");
            FilePaths = FilePaths.OrderBy(p => p).ToArray();

            int simbsLeft = SP500.Count();
            foreach (var code in SP500)
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

                report.From = from;
                report.To = to;
                report.Count = codePaths.Count();
                reports.Add(report); ;
            }
            var success = await SaveResult(result, startTime, resultKey, reports, overWrite, downloaderOptions.SymbolHistoryFolder);
            return reports;
        }
        public string[] GetSP500()
        {
            return SP500;
        }


        public string[] SP500 = new string[] { "NVR", "BKNG", "ISRG", "ALGN", "PAYC", "GOOG", "GOOGL", "FLT", "AMZN", "BA", "DXCM", "MSCI", "AZO", "MTD", "CTAS", "ZBRA", "MLM", "AVB", "ABMD", "UNH", "NOW", "LRCX", "ESS", "SIVB", "CMG", "SPG", "RCL", "MA", "BLK", "ADP", "SHW", "TFX", "URI", "SNA", "ALLE", "AAPL", "TDG", "RE", "SYK", "ORLY", "LIN", "BRK.B", "HCA", "CDW", "ADSK", "UNP", "EQIX", "AVGO", "IPGP", "ZBH", "CME", "ECL", "SWKS", "ARE", "TIF", "PXD", "KSU", "ADBE", "CI", "LMT", "BIIB", "ANSS", "FIS", "MSI", "WAT", "ANET", "GWW", "TT", "NOC", "TRV", "UHS", "WYNN", "KLAC", "CAT", "CB", "NVDA", "TMO", "SPGI", "V", "HD", "CXO", "DVA", "GPN", "PAYX", "ADS", "ANTM", "PSA", "STZ", "MAA", "PVH", "DE", "HON", "LW", "APH", "MTB", "ACN", "LHX", "PNC", "NFLX", "VAR", "OXY", "CMI", "INTU", "FRT", "AMT", "GD", "BXP", "MDT", "SRE", "ROK", "MMM", "AMGN", "JPM", "EQR", "COO", "RL", "OKE", "APD", "VLO", "AVY", "RTX", "HES", "CVX", "FANG", "AIZ", "FB", "DHR", "IT", "ALB", "DLR", "MSFT", "PSX", "VFC", "WM", "EOG", "CINF", "ITW", "SWK", "ULTA", "DOV", "EL", "HII", "KEYS", "XOM", "EMR", "MCD", "AAP", "COP", "FISV", "HAS", "LNC", "PRU", "DTE", "ODFL", "MCO", "ALL", "CBRE", "EMN", "ROP", "MPC", "ROST", "LYV", "VMC", "UPS", "ETN", "FMC", "SBUX", "EW", "IEX", "UAL", "WAB", "GL", "STE", "SLG", "GRMN", "C", "AXP", "EXPE", "IBM", "OTIS", "FTV", "MCHP", "JBHT", "MAR", "GS", "APA", "PGR", "RSG", "CCL", "FDX", "SYY", "UDR", "OMC", "DHI", "WELL", "TXN", "NCLH", "PPG", "KMX", "NTAP", "AME", "MMC", "WLTW", "NSC", "KSS", "NTRS", "BR", "LEN", "CMA", "BBY", "L", "IR", "HSIC", "QCOM", "ETR", "MU", "NEE", "WDC", "AMP", "VNO", "WRB", "TFC", "CRM", "TJX", "PFG", "HIG", "XYL", "DFS", "PLD", "HFC", "BWA", "USB", "LOW", "PG", "QRVO", "PCAR", "FFIV", "AJG", "CTSH", "TEL", "ADI", "AIV", "AON", "APTV", "FRC", "TXT", "INFO", "XRAY", "IFF", "MCK", "O", "VTR", "CPRT", "RJF", "AMAT", "GPC", "RHI", "BF.B", "ICE", "HSY", "FLS", "NDAQ", "WHR", "GLW", "PKG", "TROW", "EXR", "DVN", "AAL", "COF", "PEP", "BK", "CVS", "EIX", "JKHY", "DAL", "PHM", "DISCA", "STT", "VRSK", "SLB", "CL", "JCI", "CERN", "HWM", "IDXX", "ABT", "AEE", "PEAK", "TGT", "FBHS", "KMB", "PNW", "PEG", "ALXN", "YUM", "DISCK", "J", "GM", "WFC", "AOS", "NKE", "BSX", "MET", "PYPL", "ADM", "EXC", "LEG", "PKI", "ZION", "INTC", "PWR", "WRK", "HAL", "CE", "BAC", "TTWO", "MNST", "COST", "JWN", "SEE", "NRG", "NOV", "DOW", "MDLZ", "AFL", "IP", "SYF", "MO", "KO", "DD", "AIG", "TWTR", "WBA", "EA", "SCHW", "DIS", "KHC", "ORCL", "WY", "NBL", "AEP", "BEN", "UNM", "MRO", "DISH", "DUK", "VRSN", "WU", "CSX", "KEY", "ES", "REG", "FTI", "HBI", "CF", "CSCO", "T", "FAST", "TMUS", "HST", "ETFC", "CCI", "HPQ", "NWL", "LYB", "FE", "K", "KIM", "FTNT", "CARR", "TPR", "AWK", "HLT", "IVZ", "TSN", "MS", "NUE", "MAS", "LB", "ABBV", "CMCSA", "PBCT", "CFG", "CAH", "FITB", "XRX", "F", "PNR", "CHRW", "IPG", "CNC", "PM", "MRK", "MXIM", "VIAC", "UAA", "DRE", "FCX", "CMS", "EVRG", "IRM", "MHK", "HPE", "LVS", "BKR", "JNJ", "BAX", "LNT", "UA", "XLNX", "SNPS", "VZ", "HBAN", "ED", "SO", "KMI", "ZTS", "CTVA", "AMD", "AES", "NI", "BMY", "COG", "EFX", "HOG", "WMB", "RF", "DXC", "ALK", "HRB", "PRGO", "AMCR", "STX", "WST", "FOXA", "ATO", "EXPD", "FOX", "NLSN", "WEC", "MOS", "COTY", "GIS", "XEL", "CTL", "JNPR", "CNP", "NWS", "GPS", "NWSA", "GE", "CDNS", "CAG", "HOLX", "TAP", "LDOS", "MKC", "RMD", "HRL", "PPL", "MGM", "PFE", "MYL", "NLOK", "LUV", "ROL", "LKQ", "DG", "TSCO", "INCY", "DLTR", "ATVI", "DRI", "ABC", "CHD", "FLIR", "EBAY", "WMT", "CPB", "KR", "D", "A", "CTXS", "GILD", "LH", "NEM", "PH", "BLL", "LLY", "BDX", "REGN", "IQV", "SJM", "HUM", "DGX", "AKAM", "CBOE", "SBAC", "ILMN", "MKTX", "DPZ", "CLX", "VRTX", "CHTR" };

        public async Task<bool> SaveResult(Result existentResult, long startTime, string key, object content, bool replaceIfExists, string backupFolder)
        {

            if (existentResult == null)
            {
                existentResult = new Result()
                {
                    Key = key,
                    TagString = JsonConvert.SerializeObject(content),
                    EndTime = DateTimeOffset.UtcNow.ToUnixTimeSeconds(),
                    StartTime = startTime,
                    // User = currentUser
                };
                try
                {
                    await _resultRepository.AddAsync(existentResult);
                }
                catch (Exception ex)
                {
                    File.WriteAllText(backupFolder + "\\" + key + ".json", JsonConvert.SerializeObject(content));
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
                        await _resultRepository.UpdateAsync(existentResult);
                    }
                    catch (Exception ex)
                    {
                        File.WriteAllText(backupFolder + "\\" + key + ".json", JsonConvert.SerializeObject(content));
                    }
                }
            }
            return true;
        }

        /// <summary>
        /// Use this link: https://localhost:5000/stock/collect/4
        /// </summary>
        /// <param name="symbol"></param>
        /// <param name="days"></param>
        /// <returns></returns>
        public async Task<List<StockReport>> FetchLast(Symbol symbol, int days = 0)
        {
            var dates = new List<DateTime>() { };
            for (var i = days; i >= 0; i--)
            {
                dates.Add(DateTime.UtcNow.Date.AddDays(-i));
            }

            List<StockReport> reports = new List<StockReport>();
            foreach (var date in dates)
            {
                var report = await PoolSymbolTicksOnDate(symbol, date, false);
                reports.Add(report);
            }
            return reports;
        }

        public async Task<StockReport> PoolSymbolOnDate(Symbol symbol, DateTime date)
        {
            var data = await PoolSymbolTicksOnDate(symbol, date, false);
            return data;
        }

        public async Task<List<StockReport>> Pool(bool isControl = false)
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
                var reports = await FetchLast(symbol);
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

        public async Task<List<StockReport>> QuickPull(bool isControl = false)
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
                var reports = await FetchLast(symbol);
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

        public async Task<StockReport> Pool(Symbol symbol, DateTime date)
        {
            return await PoolSymbolOnDate(symbol, date);
        }

        public async Task<List<StockResult>> GetLasts(int ticksCount)
        {
            var symbols = _symbolRepository.GetAllActive();
            foreach (var symbol in symbols)
            {
                await FetchLast(symbol, 0);
            }
            var dates = new List<DateTime>() { };
            for (var i = ticksCount / 54 - 1; i >= 0; i--)
            {
                dates.Add(DateTime.UtcNow.Date.AddDays(-i));
            }
            return await GetBulk(dates);
        }

        public async Task<List<StockResult>> GetLastDays(int days)
        {
            var dates = new List<DateTime>() { };
            for (var i = days - 1; i >= 0; i--)
            {
                dates.Add(DateTime.UtcNow.Date.AddDays(-i));
            }
            return await GetBulk(dates);
        }

        async Task<List<StockResult>> GetBulk(List<DateTime> dates)
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

        public async Task<string> FetchString(Symbol symbol, DateTime date, FetchOptions fetchOptions)
        {

            if (string.IsNullOrEmpty(symbol.Code))
            {
                return null;
            }

            LogEntryBreakdown logEntryBreakdown = new LogEntryBreakdown("PoolSymbolTicksOnDate " + symbol.Code + " - " + date.ToShortDateString());

            var url = "https://cloud.iexapis.com/stable/stock/" + symbol.Code + "/chart/date/" + date.ToString("yyyyMMdd") + "?" + downloaderOptions.IEX_Token;
            var loggingUrl = "https://cloud.iexapis.com/stable/stock/" + symbol.Code + "/chart/date/" + date.ToString("yyyyMMdd");
            if (false)
            {
                // _logger.LogError(DateTime.UtcNow.ToLongTimeString() + " >> " + url);
            }

            // _logger.LogInformation("Loading...\n" + url);
            var data = string.Empty;
#if DEBUG
            var fileName = downloaderOptions.DailySymbolHistoryFolder + "\\Intraday_" + symbol.Code + "_" + date.ToString("yyyyMMdd") + ".json";
            if (!File.Exists(fileName))
            {
                try
                {
                    File.WriteAllText(fileName, "");
                    data = GetJsonStream(url).Result;
                    File.WriteAllText(fileName, data);
                }
                catch (Exception es) { }
            }
            else
            {
            }
            if (fetchOptions.SaveOnly)
            {
                return null;
            }
            else
            {
                StockEndpoint endpoint = null;
                StockReport report = null;

                endpoint = new StockEndpoint()
                {
#if DEBUG
                    Description = "Debug: ",
#else
                        Description = "Prod: ",
#endif
                    Url = url,
                    Updated = DateTimeOffset.UtcNow.ToUnixTimeSeconds(),
                    Name = "GetSymbolTicksOnDate",
                    Action = (data.Length > 500 ? data.Substring(0, 500) + "..." : data)
                };

                if (data == "Unknown symbol")
                {
                    // Console.WriteLine(symbol.Name + " : " + data);
                    report = new StockReport
                    {
                        SymbolId = symbol.SymbolId,
                        Code = symbol.Code,
                        Samples = 0,
                        Message = "Unknown symbol",
                        Date = date,
#if DEBUG
                        Url = url,
#endif
                        Updated = DateTime.UtcNow,
                        Alerts = _alertRepository.All().Where(p => p.Symbol.SymbolId == symbol.SymbolId && p.IsEnabled == true).Count(),
                        Last = -1
                    };
                }
                if (data == "Forbidden")
                {
                    report = new StockReport
                    {
                        SymbolId = symbol.SymbolId,
                        Code = symbol.Code,
                        Samples = 0,
                        Message = "Forbidden",
                        Date = date,
#if DEBUG
                        Url = url,
#endif
                        Updated = DateTime.UtcNow,
                        Alerts = _alertRepository.All().Where(p => p.Symbol.SymbolId == symbol.SymbolId && p.IsEnabled == true).Count(),
                        Last = -1
                    };
                }
                if (data == "You have exceeded your allotted message quota. Please enable pay-as-you-go to regain access")
                {
                    report = new StockReport
                    {
                        SymbolId = symbol.SymbolId,
                        Code = symbol.Code,
                        Samples = 0,
                        Message = "Quota exceeded",
                        Date = date,
#if DEBUG
                        Url = url,
#endif
                        Updated = DateTime.UtcNow,
                        Alerts = _alertRepository.All().Where(p => p.Symbol.SymbolId == symbol.SymbolId && p.IsEnabled == true).Count(),
                        Last = -1
                    };
                }
                if (fetchOptions.SaveReportsToDb) await _endpointRepository.AddAsync(endpoint);
                if (report != null)
                {
                    // await _reportRepository.AddAsync(endpoint);
                }
            }
            data = File.ReadAllText(fileName);
#else
            data = GetJsonStream(url).Result;
#endif
            return data;
        }

        public async Task<IEnumerable<TickArray>> GetHistorical(int symbolId, int days, bool fromFiles = false)
        {
            var values = new List<TickArray>();
            if (fromFiles)
            {
                var symbol = await _symbolRepository.GetByIdAsync(symbolId);
                var FilePaths = System.IO.Directory.GetFiles(downloaderOptions.DailySymbolHistoryFolder, "Intraday_" + symbol.Code + "_*.json");
                FilePaths = FilePaths.OrderBy(p => p).ToArray();

                foreach (var filePath in FilePaths)
                {
                    var content = System.IO.File.ReadAllText(filePath);
                    var items = (List<IexItem>)JsonConvert.DeserializeObject<List<IexItem>>(content);
                    items.Select(p => p.average);
                }
            }
            else
            {
                var seconds = new DateTimeOffset(DateTime.UtcNow.AddDays(-days).Date).ToUnixTimeMilliseconds();
                return _tickRepository.AllBySymbol(symbolId);
            }
            return values;
        }

        public async Task<int> FetchToday()
        {
            var symbols = _symbolRepository.GetAllActive();
            foreach (var symbol in symbols)
            {
                await FetchLast(symbol, 0);
            }
            return 0;
        }


        // Skip this for now.
        public async Task<IEnumerable<Sample>> CreateFragments(ResultSelector resultSelector, int take, bool saveFile = false)
        {
            int FrameSize = 15;
            decimal PercentChange = 2;
            int BufferSize = 2;
            bool running = true;
            List<Sample> samples = new List<Sample>();
            string[] FilePaths = null;

            FilePaths = Directory.GetFiles(downloaderOptions.DailySymbolHistoryFolder, "Intraday_*_*.json");
            FilePaths = FilePaths.OrderBy(p=>p).ToArray();

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
                float percent = 0;
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
                        }
                        var sample = new Sample
                        {
                            Code = symbolCode,
                            Date = fileDate,
                            Anchor = anchor,
                            From = new DateTimeOffset(DateTime.Parse(dateString + " " + anchorMinute)).ToUnixTimeSeconds(),
                            Timestamp = new DateTimeOffset(DateTime.Parse(dateString + " " + data[i].minute)).ToUnixTimeSeconds(),
                            FramePercent = anchor / 100 * data[i - FrameSize].marketAverage.Value,
                            BreakPercent = anchor / 100 * data[i - BufferSize - FrameSize].marketAverage.Value,
                            Values = data.Skip(i - BufferSize - FrameSize).Take(BufferSize + FrameSize).Select(p => p.marketAverage.HasValue ? p.marketAverage.Value : 0)
                        };
                        anchor = price;
                        sample.Distance = sample.Timestamp - sample.From;

                        if (take == 0 || samples.Count < take)
                        {
                            samples.Add(sample);
                        }

                        if (take > 0 && samples.Count == take && !saveFile)
                        {
                            return samples;
                        }
                    }

                }
            }
            return samples;
        }
        public async Task<Tick> UpsertTicks(Symbol symbol, DateTime date, object[][] ticksArray)
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

        public int ToSeconds(string minutes)
        {
            if (minutes.IndexOf(':') > 0)
            {
                return int.Parse(minutes.Split(':')[0]) * 60 * 60 + int.Parse(minutes.Split(':')[1]) * 60;
            }
            return 0;
        }
    }
}