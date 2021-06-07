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
using System.Security.Permissions;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http.Internal;
using Microsoft.AspNetCore.Mvc;

namespace Ajuro.IEX.Downloader.Services
{
    public partial class DownloaderService : IDownloaderService
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
        public async Task<object[][]> ProcessString(BaseSelector selector, DownloadOptions options,
            string dataString = null)
        {
            var date = options.Dates[0];
            var symbolId = options.SymbolIds[0];
            var symbolCode = Static.SymbolCodeFromId[symbolId];
            var destinationFile = string.Empty;
            var sourceFilesPattern = "Intraday_" + symbolCode + "_" + date.ToString("yyyyMM") + "*.json";

            if (!string.IsNullOrEmpty(dataString))
            {
                // No need to collect
            }
            else
            {
                destinationFile = Path.Join(downloaderOptions.MonthlyParsedFiles, symbolCode + ".json");

                if (!options.Step_01_Download_Options.Replace_File_If_Exists && File.Exists(destinationFile))
                {
                    dataString = File.ReadAllText(destinationFile);
                    var items = (object[][]) JsonConvert.DeserializeObject<object[][]>(dataString);
                    return items;
                }

                var strings = new List<string>();

                List<Sample> samples = new List<Sample>();
                string[] sourceFiles = null;

                sourceFiles =
                    Directory.GetFiles(downloaderOptions.DailySymbolHistoryFolder,
                        sourceFilesPattern); // was for all year here
                sourceFiles = sourceFiles.OrderBy(p => p).ToArray();

                var filesCount = sourceFiles.Count();
                int fileIndex = 0;
                for (int f = 0; f < sourceFiles.Length; f++)
                {
                    var filePath = sourceFiles[f];
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
                    values = (List<IexItem>) JsonConvert.DeserializeObject<List<IexItem>>(dataString);
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
                        Alerts = _alertRepository.All().Where(p => p.Symbol.SymbolId == symbolId && p.IsEnabled == true)
                            .Count(),
                        Last = -1
                    };
                }

                var ticksArray = values.Where(p => p.marketAverage.HasValue).Where(p => p.marketAverage.Value != -1)
                    .Select(p => new object[]
                    {
                        (Int64) (p.date.AddSeconds(ToSeconds(p.minute)).Subtract(new DateTime(1970, 1, 1)))
                        .TotalMilliseconds,
                        p.marketAverage.Value
                    }).ToArray();
                if (options.Step_01_Download_Options.Save_File_If_Missing_And_Nonempty && !File.Exists(destinationFile))
                {
                    Static.WriteAllText(destinationFile, JsonConvert.SerializeObject(ticksArray));
                }

                return ticksArray;
            }
            catch (Exception ex)
            {
                new Info(selector, -1, ex, string.Empty);
            }

            return null;
        }

        /// <summary>
        /// Merge intraday records into one foreach symbol
        /// </summary>
        public async Task<List<object[][]>> NestedProcessString(BaseSelector selector,
            ReportingOptions reportingOptions)
        {
            List<DownloadIntradayReport> reports = new List<DownloadIntradayReport>();
            // Digest query object
            if (reportingOptions.FromDate == null)
            {
                return null;
            }

            // Make sure we use the first day in a month
            var date = new DateTime(reportingOptions.FromDate.Year, reportingOptions.FromDate.Month, 1);


            // Preparing for action
            var startTime = DateTimeOffset.UtcNow.ToUnixTimeSeconds();
            string[] sourceFiles;
            var fileDateFilter = date == DateTime.MinValue ? "*" : date.ToString("yyyyMM") + "*";
            var codeFilter = string.IsNullOrEmpty(reportingOptions.Code) ? "*" : reportingOptions.Code;
            sourceFiles = Directory.GetFiles(downloaderOptions.DailySymbolHistoryFolder,
                $"Intraday_{codeFilter}_{fileDateFilter}.json");
            sourceFiles = sourceFiles.OrderBy(p => p).ToArray();

            int simbsLeft = reportingOptions.Codes.Count();
            var tickArrays = new List<object[][]>();
            foreach (var code in reportingOptions.Codes)
            {
                if (!string.IsNullOrEmpty(reportingOptions.Code))
                {
                    if (reportingOptions.Code != code)
                    {
                        continue;
                        ;
                    }
                }

                simbsLeft--;
                var symbolId = 0;

                Symbol symbol = null;
                try
                {
                    symbol = Static.SymbolsDictionary.FirstOrDefault(p => p.Value.Code == code).Value;
                }
                catch (Exception ex)
                {
                    // Triggerally unhandled
                }

                if (symbol == null)
                {
                    continue;
                }

                symbolId = symbol.SymbolId;


                var resultKey = "Parsed_" + symbol.Code + "" +
                                (date > DateTime.MinValue ? "_" + date.ToString("yyyyMMdd") : string.Empty);

                var report = new DownloadIntradayReport()
                {
                    SymbolId = symbolId,
                    Code = code,
                    Details = new List<IntradayDetail>()
                };

                // Try to recover it from disk
                var destinationName = Path.Join(downloaderOptions.MonthlyParsedFiles, resultKey + ".json");
                if (!reportingOptions.SkipMonthlySummaryCaching && File.Exists(destinationName))
                {
                    var fileContent = File.ReadAllText(destinationName);
                    var items = (object[][]) JsonConvert.DeserializeObject<object[][]>(fileContent);

                    report.Details.Add(new IntradayDetail()
                    {
                        Seconds = (new DateTimeOffset(date)).ToUnixTimeSeconds(),
                        Samples = 0, // count RF_MONTHLY_SUMMARIES
                        Total = items.Length,
                        Count = 1,
                    });

                    report.From = date;
                    report.To = date;
                    report.Count = 0;
                    reports.Add(report);
                    continue;
                }

                var codePaths = sourceFiles.Where(p => p.IndexOf("_" + code + "_") > 0);
                int left = codePaths.Count();

                DateTime from = DateTime.MaxValue;
                DateTime to = DateTime.MinValue;
                var dataString = string.Empty;
                var strings = new List<string>();
                foreach (var filePath in codePaths)
                {
                    left--;
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

                try
                {
                    var values = new List<IexItem>();
                    var emptyTicks = 0;
                    try
                    {
                        values = (List<IexItem>) JsonConvert.DeserializeObject<List<IexItem>>(dataString);
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
                            Code = symbol.Code,
                            Samples = 0,
                            Message = "No data!",
                            Date = date,
#if DEBUG
                            // Url = url,
#endif
                            Updated = DateTime.UtcNow,
                            Alerts = _alertRepository.All()
                                .Where(p => p.Symbol.SymbolId == symbolId && p.IsEnabled == true).Count(),
                            Last = -1
                        };
                    }

                    var ticksArray = values
                        .Where(p => p.marketAverage.HasValue)
                        .Where(p => p.marketAverage.Value != -1)
                        .Select(p =>
                            new object[]
                            {
                                (Int64) (p.date.AddSeconds(ToSeconds(p.minute)).Subtract(new DateTime(1970, 1, 1)))
                                .TotalMilliseconds / 10000,
                                p.marketAverage.Value
                            }).ToArray();


                    if (!reportingOptions.Skip_Replacing_Monthly_SummaryCaching)
                    {
                        var content = JsonConvert.SerializeObject(ticksArray);
                        new Info(selector, symbol.SymbolId, destinationName + " Size: " + content.Length);
                        Static.WriteAllText(destinationName, content);
                    }

                    tickArrays.Add(ticksArray);
                }
                catch (Exception ex)
                {
                    new Info(selector, -1, ex, string.Empty);
                }

                if (!string.IsNullOrEmpty(reportingOptions.Code))
                {
                    // Should return after processing the specified code
                    return null;
                }
            }

            return tickArrays;
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
            var path = Path.Join(backupFolder, key + ".json");
            if (File.Exists(path))
            {
                return File.ReadAllText(path);
            }

            return null;
        }

        public async Task<bool> SaveResult(BaseSelector selector, Result existentResult, long startTime, string key,
            object content, bool replaceIfExists, string backupFolder, SaveResultsOptions saveResultsOptions)
        {
            var path = Path.Join(backupFolder, key + ".json");
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
                        Static.WriteAllText(path, existentResult.TagString);
                    }

                    if (saveResultsOptions.SaveToDb)
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
                    Static.WriteAllText(path, existentResult.TagString);
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
                            Static.WriteAllText(path, JsonConvert.SerializeObject(content));
                        }

                        if (saveResultsOptions.SaveToDb)
                        {
                            await _resultRepository.UpdateAsync(existentResult);
                        }
                    }
                    catch (Exception ex)
                    {
                        new Info(selector, -1, ex, string.Empty);
                        Static.WriteAllText(path, JsonConvert.SerializeObject(content));
                    }
                }
            }

            return true;
        }

        #endregion

        #region COLLECT DATA

        public async Task<int> UpdateTodayFromIex(BaseSelector selector)
        {
            if (Static.IsOffHours(DateTime.UtcNow))
            {
                return -1;
            }

            // Check if a pull is needed
            var lastTesla = _tickRepository.All().FirstOrDefault(p =>
                p.SymbolId == Static.SymbolIdFromCode["TSLA"]
                && p.Date == DateTime.Today.AddDays(-1)
            );
            if (lastTesla != null)
            {
                var ticks = JsonConvert.DeserializeObject<object[][]>(lastTesla.Serialized);
                var lastTick = Static.DateTimeFromTickSeconds((long) ticks.Last()[0]);
                var varSecondsAgo = (DateTimeOffset.Now.ToUnixTimeMilliseconds() / 10000 / 10) - (long) ticks.Last()[0];
                var varSecondsAgo2 = DateTime.UtcNow - lastTick;
                if (varSecondsAgo < 360)
                {
                    return -2;
                }
            }

            var reportingOptions = new ReportingOptions
            {
                FromDate = DateTime.Today,
                Take = 1,
                Codes = new string[] { },
                ProcessType = ProcessType.Step_0_LoadToday_IntoDb,
                IsAllCodes = true,
            };

            if (downloaderOptions == null)
            {
                downloaderOptions = new DownloaderOptions();
            }

            await BulkProcess(selector, reportingOptions, ActionRange.AllForDay);
            return 0;
        }

        public async Task<IEnumerable<GraphModel>> DownloadIntraday(BaseSelector selector, DateTime date)
        {
            var eventId = Static.EventStarted(selector, KnownEndpoints.DownloadIntraday);
            new Info(selector, "DownloadIntraday...");
            
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

            new Info(selector, (date == DateTime.Today.AddDays(-1) ? "Save: " : "Memo: ") + date.ToString());

            var resultKey = "DailyGraphsSP500";
            var startTime = DateTimeOffset.UtcNow.ToUnixTimeSeconds();
            var symbIds = new int[] { };

            // var date = DateTime.UtcNow.AddDays(-1);
            var overWrite = false;
            var result = _resultRepository.GetAllByKey(resultKey + "_" + date.ToString("yyyyMMdd")).FirstOrDefault();
            if (result != null && !overWrite)
            {
                // Try from DB  
                return (List<GraphModel>) JsonConvert.DeserializeObject<List<GraphModel>>(result.TagString);
            }
            else
            {
                // Try from file
                if (downloaderOptions == null)
                {
                    // Is from worker
                    downloaderOptions = new DownloaderOptions()
                    {
                        DailyGraphsFolder = ""
                    };
                }

                var stringContent = GetResultFromFile(downloaderOptions.DailyGraphsFolder,
                    resultKey + "_" + date.ToString("yyyyMMdd"));
                if (!string.IsNullOrEmpty(stringContent))
                {
                    var items = (List<GraphModel>) JsonConvert.DeserializeObject<List<GraphModel>>(stringContent);
                    return items.Where(p => symbIds.Length == 0 || symbIds.Contains(p.SymbolId));
                }
            }

            var DailyGraphsSP500 = new List<GraphModel>();
            if (Static.SymbolCodes == null || Static.SymbolCodes.Length == 0)
            {
                return DailyGraphsSP500;
            }

            var triggerSymbolIds = Static.Triggers.Select(p => p.SymbolId).Where(symbolId =>
                Static.SymbolCodeFromId.ContainsKey(symbolId) &&
                Static.SP500.Any(s => s == Static.SymbolCodeFromId[symbolId])).Distinct();
            var triggerSymbolIdsCount = triggerSymbolIds.Count();
            var count = triggerSymbolIds.Count();
            var left = count;
            var uncachedSymbolIds = triggerSymbolIds.Where(symbolId => !Static.SymbolsIntraday.ContainsKey(symbolId));
            var uncachedSymbolIdsCount = uncachedSymbolIds.Count();
            var downloadedSymbolTicks = _tickRepository.All().Where(p =>
                uncachedSymbolIds.Contains(p.SymbolId) &&
                p.Seconds == Static.MidnightSecondsFromSeconds(Static.SecondsFromDateTime(date)));
            var downloadedSymbolTicksCount = downloadedSymbolTicks.Count();
            foreach (var symbolId in Static.SymbolIDs)
            {
                left--;
                try
                {
                    var fromDb = downloadedSymbolTicks.Any(s => s.SymbolId == symbolId);
                    if (fromDb)
                    {
                        Static.SymbolsIntraday.Add(symbolId,
                            JsonConvert.DeserializeObject<object[][]>(downloadedSymbolTicks
                                .FirstOrDefault(s => s.SymbolId == symbolId).Serialized));
                    }

                    new Info(selector, symbolId,
                        "Intraday [" + left + " / " + count + "] " + Static.SymbolCodeFromId[symbolId] + "... " +
                        (Static.SymbolsIntraday.ContainsKey(symbolId) ? " From memory!" : "") +
                        (fromDb ? " From bulk DB!" : ""));
                    DailyGraphsSP500.Add(
                        // result = Static.Triggers.Select(p => 
                        new GraphModel()
                        {
                            SymbolId = symbolId,
                            Symbol = Static.SymbolCodeFromId[symbolId],
                            Values = Static.SymbolsIntraday.ContainsKey(symbolId)
                                ? Static.SymbolsIntraday[symbolId]
                                : (await CollectIntraday(selector, new DownloadOptions()
                                {
                                    SymbolIds = new int[]
                                    {
                                        symbolId
                                    },
                                    Dates = new DateTime[]
                                    {
                                        date
                                    },
                                    FromDbIfExists = true,
                                    IfDbMissingSave = true,
#if DEBUG
                                    Step_01_Download_Options = new Download_Options()
                                    {
                                        Save_File_If_Missing_And_Nonempty = true
                                    }, // Only save files on local
#else
                    // IfFileMissingSave_DailySymbolHistoryFolder = false, // = date == DateTime.Today.AddDays(-1), // Only save yesturday
#endif
                                    UpdateDbIfExists = false,
                                    BuildDictionary = true
                                })).FirstOrDefault().FirstOrDefault(),
                        });
                }
                catch (Exception ex)
                {
                    new Info(selector, symbolId, ex,
                        "Exception fetching intraday for symbol " + Static.SymbolCodeFromId[symbolId] + ": " +
                        ex.Message + (ex.InnerException != null
                            ? ", " + ex.InnerException.Message + (ex.InnerException.InnerException != null
                                ? ", " + ex.InnerException.InnerException.Message
                                : "")
                            : ""));
                }
            }

            await SaveResult(selector, result, startTime, resultKey + "_" + date.ToString("yyyyMMdd"), DailyGraphsSP500,
                true, downloaderOptions.DailyGraphsFolder, new SaveResultsOptions
                {
                    SaveToDb = true,
                    SaveToFile = true
                });
            
            Static.EventEnded(eventId);
            return DailyGraphsSP500;
        }

        public async Task<IEnumerable<IEnumerable<object[][]>>> CollectIntraday(BaseSelector selector,
            DownloadOptions options)
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
                        var dbEntry = _tickRepository.All().FirstOrDefault(p =>
                            p.IsMonthly == false &&
                            p.SymbolId == symbolId
                            && p.Seconds == Static.MidnightSecondsFromSeconds(seconds));
                        if (dbEntry != null)
                        {
                            new Info(selector, symbolId, "  From DB... " + Static.SymbolCodeFromId[dbEntry.SymbolId]);
                            values = JsonConvert.DeserializeObject<object[][]>(dbEntry.Serialized);
                        }
                    }

                    if (values == null)
                    {
                        var dataString = await DownloadCodeForDay(selector, new DownloadOptions()
                        {
                            SymbolIds = new int[] {symbolId},
                            Dates = new DateTime[] {date},
                            Step_01_Download_Options = new Download_Options()
                            {
                                Skip_Checking_For_File = true,
#if DEBUG
                                Save_File_If_Missing_And_Nonempty =
                                    true // IfFileMissingSave_DailySymbolHistoryFolder = false
#endif
                            }
                        });
                        values = await ProcessString(selector, options, dataString);
                    }

                    // var items = (List<IexItem>)JsonConvert.DeserializeObject<List<IexItem>>(dataString);
                    // var values = items.Where(p => p.marketAverage.HasValue).Where(p => p.marketAverage.Value != -1).Select(p => new object[] { (Int64)(p.date.AddSeconds(ToSeconds(p.minute)).Subtract(new DateTime(1970, 1, 1))).TotalMilliseconds, p.marketAverage.Value }).ToArray();
                    if (options.IfDbMissingSave)
                    {
                        var record = _tickRepository.GetByDayAndSymbolId(symbolId, seconds).FirstOrDefault();
                        if (record == null)
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
                Description = isControl
                    ? "IMPORTANT Stock Debug POOL CTRL"
                    : "IMPORTANT Debug POOL CRON took " + (endDate - startDate).TotalSeconds + "seconds!",
#else
                Description =
 isControl ? "IMPORTANT Stock PROD POOL CTRL" : "IMPORTANT PROD POOL CRON took " + (endDate - startDate).TotalSeconds + "seconds!",
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
            var ints = _alertRepository.GetAllWithUsersAndSymbols().Where(p => p.IsEnabled)
                .Select(p => p.Symbol.SymbolId).ToList();
            var symbols = _symbolRepository.GetAllActive().Where(p => ints.Contains(p.SymbolId)).AsEnumerable()
                .ToList();
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

        public async Task<StockReport> FetchDate(BaseSelector selector, Symbol symbol, DateTime date, bool save = false,
            bool fromFile = false)
        {
            var stringData = await DownloadCodeForDay(selector, new DownloadOptions()
            {
                SymbolIds = new int[] {symbol.SymbolId},
                Dates = new DateTime[] {date},
            });
            var tickArray = await ProcessString(selector, new DownloadOptions()
            {
                SymbolIds = new int[] {symbol.SymbolId},
                Dates = new DateTime[] {date},
            }, stringData);
            var data = await PoolSymbolTicksOnDate(selector, symbol, date, true, save, fromFile);
            if (data != null && data.TickIdId > 0)
            {
                // await _tickRepository.GetByIdAsync(data.TickIdId);
            }

            return data;
        }

        private async Task<StockReport> PoolSymbolTicksOnDate(BaseSelector selector, Symbol symbol, DateTime date,
            bool IsImportant = false, bool saveOnly = false, bool fromFile = false)
        {

            var logEntryBreakdown = new LogEntryBreakdown("PoolSymbolTicksOnDate");
            // new Info(null, data);
            StockEndpoint endpoint = null;
            bool tickExists = false;
            try
            {
                if (date > DateTime.MinValue)
                {
                    var stringData = await DownloadCodeForDay(selector, new DownloadOptions()
                    {
                        SymbolIds = new int[] {symbol.SymbolId},
                        Dates = new DateTime[] {date},
                    });
                }

                var ticksArray = await ProcessString(selector, new DownloadOptions()
                {
                    SymbolIds = new int[] {symbol.SymbolId},
                    Dates = new DateTime[] {date},
                });

                if (ticksArray.Count() == 0)
                {
                    new Info(null, "NO tickes................ WHY ?");
                    new Info(selector, symbol.SymbolId, "Why there is no tick?");

                    return new StockReport
                    {
                        SymbolId = symbol.SymbolId,
                        Samples = 0,
                        Message = "Why there is no tick?",
                        Updated = DateTime.UtcNow,
                        Alerts = 0,
                        Last = -1
                    };
                }

                var seconds = (new DateTimeOffset(date.Date)).ToUnixTimeSeconds();

                var existentTick = await UpsertTicks(selector, symbol, date, ticksArray);

                var symbolLastValue = (long) (symbol.Value * 100);
                symbol.Value = (double) ticksArray.LastOrDefault()[1];
                symbol.DayStart = (double) ticksArray.FirstOrDefault()[1];
                symbol.DayEnd = (double) ticksArray.LastOrDefault()[1];
                symbol.DayPercentage = symbol.DayStart == 0 || symbol.DayEnd == 0 ? 0 :
                    symbol.DayStart < symbol.DayEnd ? (100 - (symbol.DayStart * 100 / symbol.DayEnd)) :
                    (100 - (symbol.DayEnd * 100 / symbol.DayStart)) * -1;
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
                    Last = (double) ticksArray.LastOrDefault()[1]
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
                logEntryBreakdown.Messages.Add(ex.Message +
                                               (ex.InnerException != null ? ". " + ex.InnerException.Message : ""));
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

        public async Task<IEnumerable<Object>> BulkProcess(BaseSelector selector, ReportingOptions reportingOptions,
            ActionRange action)
        {
            var results = new List<object>();

            // What codes will be processed
            if (!string.IsNullOrEmpty(reportingOptions.Code))
            {
                reportingOptions.Codes = new[] {reportingOptions.Code}; // Most likely you only need one code at a time
            }
            else
            {
                if (reportingOptions.Codes == null || reportingOptions.Codes.Length == 0 ||
                    (reportingOptions.Codes.Length == 1 && reportingOptions.Codes[0] == "all"))
                {
                    reportingOptions.IsAllCodes = true;
                    reportingOptions.Codes = Static.SP500; // Process all codes if no specific code provided
                }
                else
                {
                    if (reportingOptions.Codes[0] == "all")
                    {
                        reportingOptions.Codes = reportingOptions.Codes.Skip(1).ToArray();
                    }

                    if (reportingOptions.Codes.Length == 1)
                    {
                        var index = Array.IndexOf(Static.SP500, reportingOptions.Codes[0]);
                        reportingOptions.Codes = Static.SP500.Skip(index > 2 ? index - 2 : 0).Take(5).ToArray();
                    }
                }
            }

            List<DateTime> dates = Static.CreateDatesRange(reportingOptions); // use it from reportingOptions.Dates

            var messages = new List<string>();
            var allCount = reportingOptions.Codes.Count() * reportingOptions.Dates.Count();
            var codesCount = reportingOptions.Codes.Count();
            var datesCount = reportingOptions.Dates.Count();
            var allLeft = reportingOptions.Codes.Count() * reportingOptions.Dates.Count();
            var codesLeft = reportingOptions.Codes.Count();
            var datesLeft = reportingOptions.Dates.Count();
            bool allCOdesAreProcessedAtOnce_skipNeeded = false; // Skip from running for each symbol

            foreach (var code in reportingOptions.Codes)
            {
                if (code == "DEMO")
                {
                    continue;
                }

                if (!Static.SymbolIdFromCode.ContainsKey(code))
                {

                    continue;
                }

                datesLeft = datesCount;
                if (allCOdesAreProcessedAtOnce_skipNeeded)
                {
                    break;
                }

                foreach (var rdate in reportingOptions.Dates)
                {
                    if (!reportingOptions.IsMonthly &&
                        (rdate.DayOfWeek == DayOfWeek.Saturday || rdate.DayOfWeek == DayOfWeek.Sunday))
                    {
                        continue; // skip weekends
                    }

                    if (reportingOptions.ProcessType == ProcessType.Step_1_DownloadFromIex ||
                        reportingOptions.ProcessType == ProcessType.Step_0_LoadToday_IntoDb)
                    {
                        var result = await DownloadCodeForDay(selector, new DownloadOptions()
                        {
                            SymbolIds = new[] {Static.SymbolIdFromCode[code]},
                            Dates = new[] {rdate},
                            ProcessType = reportingOptions.ProcessType,
                            SaveOnly = true,
                            Step_01_Download_Options = new Download_Options()
                            {
                                Skip_This_Step = false,
                                Save_File_If_Missing_And_Nonempty =
                                    reportingOptions.ProcessType != ProcessType.Step_0_LoadToday_IntoDb,
                                Skip_Loading_If_File_Exists =
                                    reportingOptions.ProcessType == ProcessType.Step_1_DownloadFromIex,
                                Skip_Checking_For_File = true,
                                Skip_Logging = false,
                                Replace_File_If_Exists =
                                    reportingOptions.ProcessType != ProcessType.Step_0_LoadToday_IntoDb &&
                                    reportingOptions.ReplaceDestinationIfExists,
                                Save_As_Daily_Tick = reportingOptions.ProcessType == ProcessType.Step_0_LoadToday_IntoDb
                            },
                            Step_02_Join_Options = new Join_Options()
                            {
                                Skip_This_Step = true
                            },
                            Step_03_Aggregate_Options = new Aggregate_Options()
                            {
                                Skip_This_Step = true
                            }
                        });
                        results.Add(result);
                    }

                    if (reportingOptions.ProcessType == ProcessType.Step_2_ProcessFiles)
                    {
                        var result = await NestedProcessString(selector, new ReportingOptions()
                            {
                                FromDate = rdate,
                                IsAllCodes = reportingOptions.IsAllCodes,
                                Codes = reportingOptions.Codes,
                                Source = reportingOptions.Source,
                                SkipMonthlySummaryCaching = reportingOptions.ReplaceDestinationIfExists,
                                SkipDailySummaryCaching = reportingOptions.ReplaceDestinationIfExists,
                                ReplaceDestinationIfExists = reportingOptions.ReplaceDestinationIfExists,
                                Skip_RF_PER_CODE_SUMMARY_Caching = reportingOptions.Skip_RF_PER_CODE_SUMMARY_Caching
                            }
                            /*,
                            new DownloadOptions()
                        {
                            SymbolIds = new[] {Static.SymbolIdFromCode[code]},
                            Dates = new[] {rdate},
                            SaveOnly = true,
                            Step_01_Download_Options = new Download_Options()
                            {
                                Skip_This_Step = true
                            },
                            Step_02_Join_Options = new Join_Options()
                            {
                                Skip_This_Step = false,
                                Save_File_If_Missing_And_Nonempty = true,
                                Skip_Loading_If_File_Exists = true,
                                Skip_Checking_For_File = true,
                                Skip_Logging = false,
                                Replace_File_If_Exists = reportingOptions.ReplaceDestinationIfExists
                            },
                            Step_03_Aggregate_Options = new Aggregate_Options()
                            {
                                Skip_This_Step = true
                            }
                        }*/);

                        results.Add(result);
                        allCOdesAreProcessedAtOnce_skipNeeded =
                            true; // This action is per all symbols,so no need to run it for each of them
                    }

                    if (reportingOptions.ProcessType == ProcessType.RF_MONTHLY_SUMMARIES)
                    {
                        if (rdate.Day == 1)
                        {
                            var result = await RF_MONTHLY_SUMMARIES_From_CountHistoricalFiles(selector,
                                new ReportingOptions()
                                {
                                    FromDate = rdate,
                                    Dates = new List<DateTime>() {rdate},
                                    Source = reportingOptions.Source,
                                    SkipMonthlySummaryCaching = reportingOptions.SkipMonthlySummaryCaching,
                                    // Replace_File_If_Exists = reportingOptions.ReplaceDestinationIfExists
                                });
                            // results.Add( new { Counts = result, Id = reportingOptions.Dates.IndexOf(rdate), From = rdate});
                            results = result.Cast<object>().ToList();
                            allCOdesAreProcessedAtOnce_skipNeeded =
                                true; // This action is per all symbols,so no need to run it for each of them
                            break; // Only run once. No muliple months
                        }

                        allCOdesAreProcessedAtOnce_skipNeeded =
                            true; // This action is per all symbols,so no need to run it for each of them
                    }

                    if (reportingOptions.ProcessType == ProcessType.RF_UPLOAD_MONTHLY)
                    {
                        if (rdate.Day == 1 || (reportingOptions.Dates.Count == 1 &&
                                               reportingOptions.ReplaceDestinationIfExists))
                        {
                            var result = await RF_UPLOAD_MONTHLY_From_ProcesedFiles(selector, new ReportingOptions()
                            {
                                FromDate = rdate,
                                Take = reportingOptions.Take,
                                Codes = reportingOptions.Codes,
                                Dates = new List<DateTime>() {rdate},
                                Source = reportingOptions.Source,
                                ReplaceDestinationIfExists = reportingOptions.ReplaceDestinationIfExists,
                                SkipMonthlySummaryCaching = reportingOptions.SkipMonthlySummaryCaching,
                                // Replace_File_If_Exists = reportingOptions.ReplaceDestinationIfExists
                            });
                            // results.Add( new { Counts = result, Id = reportingOptions.Dates.IndexOf(rdate), From = rdate});
                            results = result.Cast<object>().ToList();
                            allCOdesAreProcessedAtOnce_skipNeeded =
                                true; // This action is per all symbols,so no need to run it for each of them
                            // break; // Only run once. No muliple months
                        }

                        allCOdesAreProcessedAtOnce_skipNeeded =
                            true; // This action is per all symbols,so no need to run it for each of them
                    }

                    if (false && reportingOptions.ProcessType == ProcessType.RF_INTRADAY_FILES_LIST)
                    {
                        var result = await RF_MONTHLY_SUMMARIES_From_CountHistoricalFiles(selector,
                            new ReportingOptions()
                            {
                                FromDate = rdate,
                                Source = reportingOptions.Source,
                                SkipDailySummaryCaching = reportingOptions.SkipDailySummaryCaching
                            });
                        results.Add(new {Counts = result, Id = reportingOptions.Dates.IndexOf(rdate), From = rdate});
                        allCOdesAreProcessedAtOnce_skipNeeded =
                            true; // This action is per all symbols,so no need to run it for each of them
                    }

                    if (reportingOptions.ProcessType == ProcessType.RF_PER_CODE_SUMMARY ||
                        reportingOptions.ProcessType == ProcessType.RF_DB_CODE_SUMMARY)
                    {
                        // Takes tens of minutes per month for all symbols
                        var result = await RF_PER_CODE_SUMMARY_CountIntradays_PerCode_OnTheGivenMonth(selector,
                            new ReportingOptions()
                            {
                                FromDate = rdate,
                                IsAllCodes = reportingOptions.IsAllCodes,
                                Codes = reportingOptions.Codes,
                                Source = reportingOptions.Source,
                                SkipDailySummaryCaching = reportingOptions.ReplaceDestinationIfExists,
                                ReplaceDestinationIfExists = reportingOptions.ReplaceDestinationIfExists,
                                Skip_RF_PER_CODE_SUMMARY_Caching = reportingOptions.Skip_RF_PER_CODE_SUMMARY_Caching
                            });
                        results = result.Cast<object>().ToList();
                        allCOdesAreProcessedAtOnce_skipNeeded =
                            true; // This action is per all symbols,so no need to run it for each of them
                        break; // Only run once. No muliple months
                    }

                    if (reportingOptions.ProcessType == ProcessType.RF_FILE_CONTENT)
                    {
                        // Takes tens of minutes per month for all symbols
                        var result = await RF_PER_CODE_SUMMARY_CountIntradays_PerCode_OnTheGivenMonth(selector,
                            new ReportingOptions()
                            {
                                FromDate = rdate,
                                Codes = reportingOptions.Codes,
                                Source = reportingOptions.Source,
                                SkipDailySummaryCaching = reportingOptions.ReplaceDestinationIfExists,
                                ReplaceDestinationIfExists = reportingOptions.ReplaceDestinationIfExists,
                                Skip_RF_PER_CODE_SUMMARY_Caching = reportingOptions.Skip_RF_PER_CODE_SUMMARY_Caching
                            });
                        results.Add(new {Counts = result, Id = reportingOptions.Dates.IndexOf(rdate), From = rdate});
                        allCOdesAreProcessedAtOnce_skipNeeded =
                            true; // This action is per all symbols,so no need to run it for each of them
                        break; // Only run once. No muliple months
                    }

                    if (reportingOptions.ProcessType == ProcessType.RF_INTRADAY_FILES_LIST)
                    {
                        if (reportingOptions.Dates.Count > 1 && reportingOptions.Codes.Length > 1)
                        {
                            return new List<object> {"Please restrict to one date or to one code"};
                        }

                        // Takes tens of minutes per month for all symbols
                        var result = await ListFiles_WithContent_PerCode_OnTheGivenMonth(selector,
                            new ReportingOptions()
                            {
                                FromDate = rdate,
                                Code = code,
                                Source = reportingOptions.Source,
                                ReplaceDestinationIfExists = reportingOptions.ReplaceDestinationIfExists,
                                Skip_RF_PER_CODE_SUMMARY_Caching = reportingOptions.Skip_RF_PER_CODE_SUMMARY_Caching
                            });
                        results = result.Cast<object>().ToList();
                        allCOdesAreProcessedAtOnce_skipNeeded =
                            true; // This action is per all symbols,so no need to run it for each of them
                        break; // Only run once. No muliple months
                    }

                    if (reportingOptions.ProcessType == ProcessType.RF_INTRADAY_TICKS_LIST)
                    {
                        if (reportingOptions.Dates.Count > 1 && reportingOptions.Codes.Length > 1)
                        {
                            return new List<object> {"Please restrict to one date or to one code"};
                        }

                        // Takes tens of minutes per month for all symbols
                        var result = await ListTicks_WithContent_PerCode_OnTheGivenMonth(selector,
                            new ReportingOptions()
                            {
                                FromDate = rdate,
                                Code = code,
                                Source = reportingOptions.Source,
                                ReplaceDestinationIfExists = reportingOptions.ReplaceDestinationIfExists,
                                Skip_RF_PER_CODE_SUMMARY_Caching = reportingOptions.Skip_RF_PER_CODE_SUMMARY_Caching
                            });
                        results = result.Cast<object>().ToList();
                        allCOdesAreProcessedAtOnce_skipNeeded =
                            true; // This action is per all symbols,so no need to run it for each of them
                        break; // Only run once. No muliple months
                    }

                    if (reportingOptions.ProcessType == ProcessType.RF_MONTHLY_FILES_LIST)
                    {
                        var startOfMonth = new DateTime(reportingOptions.FromDate.Year, reportingOptions.FromDate.Month,
                            1);
                        var result = await ListParsedMonthlyFiles_WithContent_PerCode(selector, new ReportingOptions()
                        {
                            FromDate = startOfMonth,
                            Code = code,
                            Source = reportingOptions.Source,
                            ReplaceDestinationIfExists = reportingOptions.ReplaceDestinationIfExists,
                            Skip_RF_PER_CODE_SUMMARY_Caching = reportingOptions.Skip_RF_PER_CODE_SUMMARY_Caching
                        });
                        results = result.Cast<object>().ToList();
                        allCOdesAreProcessedAtOnce_skipNeeded =
                            true; // This action is per all symbols,so no need to run it for each of them
                        break; // Only run once. No multiple months
                    }

                    if (reportingOptions.ProcessType == ProcessType.RF_MONTHLY_TICKS_LIST)
                    {
                        var startOfMonth = new DateTime(reportingOptions.FromDate.Year, reportingOptions.FromDate.Month,
                            1);
                        var result = await ListTicks_WithContent_PerCode_OnTheGivenMonth(selector,
                            new ReportingOptions()
                            {
                                FromDate = startOfMonth,
                                Code = code,
                                IsMonthly = true,
                                Source = reportingOptions.Source,
                                ReplaceDestinationIfExists = reportingOptions.ReplaceDestinationIfExists,
                                Skip_RF_PER_CODE_SUMMARY_Caching = reportingOptions.Skip_RF_PER_CODE_SUMMARY_Caching
                            });
                        results = result.Cast<object>().ToList();
                        allCOdesAreProcessedAtOnce_skipNeeded =
                            true; // This action is per all symbols,so no need to run it for each of them
                        break; // Only run once. No multiple months
                    }

                    allLeft--;
                    datesLeft--;
                }

                codesLeft--;
            }

            return results;
        }

        public async Task<List<DownloadReport>> Download(BaseSelector selector, DownloadOptions options)
        {
            /*
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
            var symbols = new List<string>()
            {
                "MSFT", "UBER", "BA", "LLOY", "TSLA", "DOM", "KO", "AMD", "NVDA", "EXPN", "LSE", "TSCO", "ULVR", "ABBV",
                "EL", "PYPL", "GSK", "MSTF", "CCL", "ISF", "IUSA"
            };
            foreach (var symbol in symbols)
            {
                StockResult result = new StockResult()
                {
                    Symbol = symbol,
                    Data = ""
                };
                foreach (var date in dates)
                {
                    var serialized = _tickRepository.All().FirstOrDefault(p =>
                        p.IsMonthly == false &&
                        p.Date == date &&
                        p.Symbol == symbol)?.Serialized;
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

        public enum DataSource
        {
            Unknown,
            FromServer,
            FromFile,
            FromDB,
        }

        // Wil not overwrite if file exists.
        public async Task<string> DownloadCodeForDay(BaseSelector selector, DownloadOptions options)
        {
            if (Static.SymbolCodeFromId.Count == 0)
            {
                return null;
            }

            var symbolId = options.SymbolIds[0];
            var date = options.Dates[0];

            if (date.DayOfWeek == DayOfWeek.Saturday || date.DayOfWeek == DayOfWeek.Sunday)
            {
                return null; // skip weekends
            }

            if (!Static.SymbolCodeFromId.ContainsKey(symbolId))
            {
                return null; // skip missing symbols
            }

            var symbolCode = Static.SymbolCodeFromId[symbolId];

            if (string.IsNullOrEmpty(symbolCode))
            {
                return null;
            }

            DataSource dataSource = DataSource.Unknown;

            LogEntryBreakdown logEntryBreakdown =
                new LogEntryBreakdown("PoolSymbolTicksOnDate " + symbolCode + " - " + date.ToShortDateString());
            var fileName = Path.Join(downloaderOptions.DailySymbolHistoryFolder,
                "Intraday_" + symbolCode + "_" + date.ToString("yyyyMMdd") + ".json");
            var url = "https://cloud.iexapis.com/stable/stock/" + symbolCode + "/chart/date/" +
                      date.ToString("yyyyMMdd") + "?token=" + downloaderOptions.IEX_Token;
            var loggingUrl = "https://cloud.iexapis.com/stable/stock/" + symbolCode + "/chart/date/" +
                             date.ToString("yyyyMMdd");
            string dataString = string.Empty;
            if (string.IsNullOrEmpty(dataString) && File.Exists(fileName) &&
                options.ProcessType != ProcessType.Step_0_LoadToday_IntoDb)
            {
                if (options.Step_01_Download_Options.Skip_Loading_If_File_Exists)
                {
                    if (!options.Step_01_Download_Options.Skip_Logging)
                        new Info(selector, symbolId,
                            "File exists: " + "Intraday_" + symbolCode + "_" + date.ToString("yyyyMMdd") + ".json");
                    return "1";
                }

                new Info(selector, symbolId, "  From file... " + fileName);
                dataString = File.ReadAllText(fileName);
                dataSource = DataSource.FromFile;
            }

            if (string.IsNullOrEmpty(dataString))
            {
                dataString = await Static.GetJsonStream(url);
                if (!options.Step_01_Download_Options.Skip_Logging)
                {
                    new Info(selector, symbolId,
                        "  From IEX.Cloud... <a target='_blank' href='" + url + "'>" + loggingUrl + "</a> Code: " +
                        symbolCode + ", Date: " + date.ToString("yyyy-MM-dd") + ", Size: " + dataString.Length);
                }

                dataSource = DataSource.FromServer;
            }

            if (dataString == "Unknown symbol" || dataString == "Forbidden" || dataString ==
                "You have exceeded your CollectLastOrderUpdatesallotted message quota. Please enable pay-as-you-go to regain access"
            )
            {
                return null;
            }

            if (dataSource != DataSource.FromFile && options.Step_01_Download_Options.Save_File_If_Missing_And_Nonempty)
            {
                // if (!string.IsNullOrEmpty(dataString) && dataString.Length > 2 && !File.Exists(fileName))
                {
                    try
                    {
                        Static.WriteAllText(fileName, dataString);
                    }
                    catch (Exception es)
                    {
                    }
                }
            }

            if (dataSource != DataSource.FromFile && options.Step_01_Download_Options.Save_As_Daily_Tick)
            {
                var values = await ProcessString(selector, options, dataString);

                var record = _tickRepository.All().FirstOrDefault(p =>
                    p.Date == DateTime.Today.Date && p.SymbolId == symbolId && p.IsMonthly == false);
                if (record == null)
                {
                    record = new Tick()
                    {
                        SymbolId = symbolId,
                        Samples = values.Count(),
                        Seconds = Static.MidnightSecondsFromSeconds(Static.SecondsFromDateTime(DateTime.Today.Date)),
                        Serialized = JsonConvert.SerializeObject(values),
                        Date = DateTime.Today.Date,
                        Symbol = Static.SymbolCodeFromId[symbolId]
                    };
                    await _tickRepository.AddAsync(record);
                    Static.Ticks.Add(record);
                }
                else
                {
                    record.Serialized = JsonConvert.SerializeObject(values);
                    record.Samples = values.Count();
                    await _tickRepository.UpdateAsync(record);
                    var tick = Static.Ticks.FirstOrDefault(p =>
                        p.SymbolId == symbolId && p.Date == DateTime.Today.Date);
                    // LONG PRICE * 1000
                    record.Ticks = values.Select(p => new long[] {(long) p[0], Convert.ToInt64(((decimal) p[1]) * 100)});
                    // record.Ticks = values.Select(p => new decimal[] {(decimal) p[0], (decimal) p[1]});
                    if (tick == null)
                    {
                        Static.Ticks.Add(record);
                    }
                    else
                    {
                        tick.Ticks = record.Ticks;
                        tick.Samples = values.Count();
                    }
                }
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

            var existentTick = _tickRepository.All().FirstOrDefault(p =>
                p.IsMonthly == false &&
                p.Seconds == seconds &&
                p.SymbolId == symbol.SymbolId);
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


        #endregion

        #region CACHING

        public async Task<List<Tick>> GetAllHistoricalFromDb(BaseSelector selector, bool overwite,
            bool saveToFile = true, bool saveToDb = false, bool overWrite = false)
        {
            var startTime = DateTimeOffset.UtcNow.ToUnixTimeSeconds();

            var resultKey = "AllHistoricalDictionary";

            var path = Path.Join(downloaderOptions.LargeResultsFolder, resultKey + ".json");
            if (File.Exists(path))
            {
                var fileContent = File.ReadAllText(path);
                return (List<Tick>) JsonConvert.DeserializeObject<List<Tick>>(fileContent);
            }

            var result = _resultRepository.GetAllByKey(resultKey).FirstOrDefault();
            if (result != null && !overWrite)
            {
                return (List<Tick>) JsonConvert.DeserializeObject<List<Tick>>(result.TagString);
            }

            var symbols = _symbolRepository.GetAllActive().ToList();
            var aggregatedTicks = new List<Tick>();
            for (int symi = 0; symi < symbols.Count; symi++)
            {
                if (!Static.SP500.Contains(symbols[symi].Code))
                {
                    continue;
                }

                var aggregatedTick = _tickRepository.All().FirstOrDefault(p =>
                    p.IsMonthly == false &&
                    p.Seconds == 0 &&
                    p.SymbolId == symbols[symi].SymbolId);
                if (aggregatedTick == null)
                {
                    var tickArray = await ProcessString(selector, new DownloadOptions()
                    {
                        SymbolIds = new int[] {symbols[symi].SymbolId},
                        Dates = new DateTime[] {DateTime.MinValue},
                        Step_01_Download_Options = new Download_Options()
                        {
                            Replace_File_If_Exists = overwite
                        },
                        SaveOnly = true
                    });

                    aggregatedTick = await UpsertTicks(selector, symbols[symi], DateTime.MinValue, tickArray);
                }

                aggregatedTicks.Add(aggregatedTick);

                if (saveToFile)
                {
                    var success = await SaveResult(selector, result, startTime, resultKey, aggregatedTicks, false,
                        downloaderOptions.LargeResultsFolder, new SaveResultsOptions()
                        {
                            SaveToDb = true,
                            SaveToFile = true
                        });
                }
            }

            return aggregatedTicks;
        }

        public async Task<Dictionary<int, object[][]>> GetAllHistoricalFromFiles(BaseSelector selector, bool overwite,
            bool saveToFile = true, bool saveToDb = false, bool overWrite = false)
        {
            var startTime = DateTimeOffset.UtcNow.ToUnixTimeSeconds();
            overWrite = false;

            var resultKey = "AllHistoricalDictionary";
            Result result = null;
            var path = Path.Join(downloaderOptions.LargeResultsFolder, resultKey + ".json");
            if (!overWrite && File.Exists(path))
            {
                var fileContent = File.ReadAllText(path);
                result = (Result) JsonConvert.DeserializeObject<Result>(fileContent);
                return (Dictionary<int, object[][]>) result.Tag;
            }

            Dictionary<int, object[][]> priceDictionary = new Dictionary<int, object[][]>();
            var symbols = _symbolRepository.GetAllActive().ToList();
            for (int symi = 0; symi < symbols.Count; symi++)
            {
                if (!Static.SP500.Contains(symbols[symi].Code))
                {
                    continue;
                }

                if (symbols[symi].Code == "KO")
                {

                }

                var tickArray = await ProcessString(selector, new DownloadOptions()
                {
                    SymbolIds = new int[] {symbols[symi].SymbolId},
                    Dates = new DateTime[] {DateTime.MinValue},
                    SaveOnly = false,
                    Step_01_Download_Options = new Download_Options()
                    {
                        Replace_File_If_Exists = overwite
                    },
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
                var success = await SaveResult(selector, result, startTime, resultKey, priceDictionary, false,
                    downloaderOptions.LargeResultsFolder, new SaveResultsOptions()
                    {
                        SaveToDb = false,
                        SaveToFile = true
                    });
            }

            return (Dictionary<int, object[][]>) result.Tag;
        }

        #endregion

        #region REPORTING

        public async Task<IEnumerable<DownloadIntradayReport>> ListParsedMonthlyFiles_WithContent_PerCode(
            BaseSelector selector, ReportingOptions reportingOptions)
        {
            List<DownloadIntradayReport> reports = new List<DownloadIntradayReport>();

            // Digest query object
            if (string.IsNullOrEmpty(reportingOptions.Code))
            {
                return null;
            }

            if (reportingOptions.FromDate == null)
            {
                return null;
            }

            string[] filePaths;
            var symbolCode = reportingOptions.Code;
            var fileDateFilter = reportingOptions.FromDate == DateTime.MinValue
                ? "*"
                : reportingOptions.FromDate.ToString("yyyyMM") + "*";
            filePaths = Directory.GetFiles(downloaderOptions.MonthlyParsedFiles, $"Parsed_{symbolCode}_*.json");
            filePaths = filePaths.OrderBy(p => p).ToArray();
            int i = 0;
            foreach (var filePath in filePaths)
            {
                var fileContent = File.ReadAllText(filePath);
                // var items = (List<TickArray>)JsonConvert.DeserializeObject<List<TickArray>>(fileContent);

                var dateString = filePath.Substring(filePath.LastIndexOf("_") + 1);
                dateString = dateString.Substring(0, dateString.LastIndexOf("."));
                dateString = dateString.Substring(6, 2) + "-" + dateString.Substring(4, 2) + "-" +
                             dateString.Substring(0, 4);
                DateTime date = DateTime.Parse(dateString);

                var report = new DownloadIntradayReport()
                {
                    intradayFile = i++,
                    Code = symbolCode,
                    From = date,
                    Count = fileContent.Split("],[").Count(),
                    Counts = fileContent.Length < 100 ? fileContent : fileContent.Substring(0, 100)
                };
                reports.Add(report);
            }

            return reports;
        }

        // RF_INTRADAY_FILES_LIST
        public async Task<IEnumerable<DownloadIntradayReport>> ListFiles_WithContent_PerCode_OnTheGivenMonth(
            BaseSelector selector, ReportingOptions reportingOptions)
        {
            List<DownloadIntradayReport> reports = new List<DownloadIntradayReport>();

            // Digest query object
            if (string.IsNullOrEmpty(reportingOptions.Code))
            {
                return null;
            }

            if (reportingOptions.FromDate == null)
            {
                return null;
            }

            string[] filePaths;
            var symbolCode = reportingOptions.Code;
            var fileDateFilter = reportingOptions.FromDate == DateTime.MinValue
                ? "*"
                : reportingOptions.FromDate.ToString("yyyyMM") + "*";
            filePaths = Directory.GetFiles(downloaderOptions.DailySymbolHistoryFolder,
                $"Intraday_{symbolCode}_{fileDateFilter}.json");
            filePaths = filePaths.OrderBy(p => p).ToArray();
            int i = 0;
            foreach (var filePath in filePaths)
            {
                var fileContent = File.ReadAllText(filePath);
                var items = (List<IexItem>) JsonConvert.DeserializeObject<List<IexItem>>(fileContent);

                var dateString = filePath.Substring(filePath.LastIndexOf("_") + 1);
                dateString = dateString.Substring(0, dateString.LastIndexOf("."));
                dateString = dateString.Substring(6, 2) + "-" + dateString.Substring(4, 2) + "-" +
                             dateString.Substring(0, 4);
                DateTime date = DateTime.Parse(dateString);

                var report = new DownloadIntradayReport()
                {
                    intradayFile = i++,
                    Code = symbolCode,
                    From = date,
                    Count = items.Count,
                    Counts = items
                };
                reports.Add(report);
            }

            return reports;
        }

        // RF_INTRADAY_TICKS_LIST
        public async Task<IEnumerable<DownloadIntradayReport>> ListTicks_WithContent_PerCode_OnTheGivenMonth(
            BaseSelector selector, ReportingOptions reportingOptions)
        {
            List<DownloadIntradayReport> reports = new List<DownloadIntradayReport>();
            // Digest query object
            if (string.IsNullOrEmpty(reportingOptions.Code))
            {
                return null;
            }

            if (reportingOptions.FromDate == null)
            {
                return null;
            }

            reportingOptions.SymbolId = Static.SymbolIdFromCode[reportingOptions.Code];
            var symbolTick = _tickRepository.All()
                .FirstOrDefault(p =>
                    p.IsMonthly == reportingOptions.IsMonthly &&
                    p.Date == reportingOptions.FromDate &&
                    p.SymbolId == reportingOptions.SymbolId);
            if (symbolTick != null)
            {
                var ticks = JsonConvert.DeserializeObject<List<decimal[]>>(symbolTick.Serialized);
                var tickDays = ticks.GroupBy(p => (int) (p[0] * 10 / Static.DaySeconds)).Select(p =>
                    new
                    {
                        Seconds = p.Key * Static.DaySeconds,
                        Values = p.Select(k => k[1])
                    }).ToList();
                var i = 1;
                reports = tickDays.Select(p => new DownloadIntradayReport()
                {
                    intradayFile = i++,
                    Code = reportingOptions.Code,
                    From = Static.DateTimeFromSeconds(p.Seconds),
                    Count = p.Values.Count(),
                    Counts = p.Values
                }).ToList();
            }

            return reports;
        }


        public async Task<IEnumerable<DownloadIntradayReport>> RF_UPLOAD_MONTHLY_From_ProcesedFiles(
            BaseSelector selector,
            ReportingOptions reportingOptions)
        {
            List<DownloadIntradayReport> reports = new List<DownloadIntradayReport>();
            var startOfMonth = new DateTime(reportingOptions.FromDate.Year, reportingOptions.FromDate.Month, 1);

            // Step 2 - mandatory - Collecting the generated file paths in one go. Better than checking for each existence.
            var sourceFiles = Directory.GetFiles(downloaderOptions.MonthlyParsedFiles,
                $"Parsed_*_{startOfMonth.ToString("yyyyMM")}01.json");
            Array.Sort(sourceFiles);
            DateTime uploadFrom = reportingOptions.FromDate;
            if (reportingOptions.Take > 365)
            {
                reportingOptions.Take = 365;
            }

            DateTime uploadTo = reportingOptions.FromDate.AddDays(reportingOptions.Take);
            if (reportingOptions.Take == 30)
            {
                uploadTo = reportingOptions.FromDate.AddMonths(1);
            }

            int simbsLeft = Static.SP500.Count();

            var pageSize = 10;
            var addedOrUpdated = 0;
            var missingCodes = reportingOptions.Codes == null || reportingOptions.Codes.Length < 1 ? Static.SP500.ToList() : reportingOptions.Codes.ToList();
            for (int i = 0; i < missingCodes.Count; i += pageSize) // each 10 codes
            {
                var missingCodesPartition = missingCodes.Skip(i).Take(pageSize).ToArray();
                var missingIdsPartition = Static.SymbolIdFromCode.Where(p => missingCodesPartition.Contains(p.Key))
                    .Select(p => p.Value).ToArray();
                var existentTicks = _tickRepository.All().Where(p =>
                    p.IsMonthly == true &&
                    p.Date == startOfMonth &&
                    missingIdsPartition.Contains(p.SymbolId)
                ).ToList();

                foreach (var code in missingCodesPartition)
                {
                    simbsLeft--;
                    var symbolId = 0;

                    Symbol symbol = null;
                    try
                    {
                        symbol = Static.SymbolsDictionary.FirstOrDefault(p => p.Value.Code == code).Value;
                    }
                    catch (Exception ex)
                    {
                        // Triggerally unhandled
                    }

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
                    var codePaths = sourceFiles.Where(p => p.IndexOf("_" + code + "_") > 0);
                    int left = codePaths.Count();

                    DateTime from = DateTime.MaxValue;
                    DateTime to = DateTime.MinValue;
                    foreach (var codePath in codePaths)
                    {
                        left--;
                        var dateString = codePath.Substring(codePath.LastIndexOf("_") + 1);
                        dateString = dateString.Substring(0, dateString.LastIndexOf("."));
                        dateString = dateString.Substring(6, 2) + "-" + dateString.Substring(4, 2) + "-" +
                                     dateString.Substring(0, 4);
                        DateTime fileDate = DateTime.Parse(dateString);

                        if (uploadFrom < fileDate || fileDate > uploadTo)
                        {
                            break;
                        }

                        if (fileDate < from)
                        {
                            from = fileDate;
                        }

                        if (to < fileDate)
                        {
                            to = fileDate;
                        }

                        var count = 0;
                        var itemsLength = 1;
                        if (left == 0 || left % 50 == 0)
                        {
                            // new Info(null, simbsLeft + " " + code + ": " + left);
                        }

                        var existentTick =
                            existentTicks.FirstOrDefault(p =>
                                p.Date == fileDate &&
                                p.SymbolId == symbol.SymbolId &&
                                p.IsMonthly == true
                            );
                        var exists = existentTick != null;

                        new Info(selector, symbolId,
                            $@"Date: {fileDate.ToString("yyyy-MM-dd")}, Progress: S:{simbsLeft}/{Static.SP500.Length} F:{left}/{codePaths.Count()}, TickId: {existentTick?.TickId}, Code: {symbol.Code} ");
                        if (exists)
                        {
                            if (reportingOptions.ReplaceDestinationIfExists)
                            {
                                var serializedTicksArray = File.ReadAllText(codePath);
                                // tickExists = true;
                                existentTick.Serialized = serializedTicksArray;
                                existentTick.Samples = serializedTicksArray.Split("],[").Length;
                                existentTick.Updated = Static.Now();
                                // existentTick.SymbolId = symbol.SymbolId;
                                await _tickRepository.UpdateAsync(existentTick);
                            }
                            else
                            {
                                continue;
                            }
                        }
                        else
                        {
                            var serializedTicksArray = File.ReadAllText(codePath);
                            existentTick = new Tick()
                            {
                                Date = startOfMonth,
                                Serialized = serializedTicksArray,
                                Samples = serializedTicksArray.Split("],[").Length,
                                Symbol = symbol.Code,
                                Seconds = Static.SecondsFromDateTime(from),
                                SymbolId = symbol.SymbolId,
                                Created = Static.Now(),
                                IsMonthly = true
                            };
                            await _tickRepository.AddAsync(existentTick);
                        }

                        new Info(selector, symbolId,
                            $@"  Uploaded {(exists ? " existent " : " new ")}... Code: {symbol.Code} Day: {fileDate.ToString("yyyyMMdd")} Samples: {existentTick.Samples}");
                    }
                }
            }

            return reports;
        }

        // No need for this
        public async Task<IEnumerable<DownloadIntradayReport>> RF_MONTHLY_SUMMARIES_From_CountHistoricalFiles(
            BaseSelector selector,
            ReportingOptions reportingOptions)
        {
            List<DownloadIntradayReport> reports = new List<DownloadIntradayReport>();
            if (!reportingOptions.SkipMonthlySummaryCaching)
            {
                // Regenerate counting
                // Step 1 - optional - Generating the files. Prefer to have it in 2 steps with 2 file readings than using it from return
                await RF_MONTHLY_SUMMARIES_CountFiles_PerCode_OnTheGivenMonth(selector, reportingOptions);
            }

            // Step 2 - mandatory - Collecting the generated file paths in one go. Better than checking for each existence.
            var filePaths = Directory.GetFiles(downloaderOptions.LargeResultsFolder, $"CountHistoricalFiles_*.json");
            var selectedFilePath =
                filePaths.FirstOrDefault(p => p.Contains(reportingOptions.FromDate.ToString("yyyyMMdd")));
            int index = 0;
            filePaths = filePaths.OrderBy(p => p).ToArray();
            if (!string.IsNullOrEmpty(selectedFilePath))
            {
                index = Array.IndexOf(filePaths, selectedFilePath);
            }

            filePaths = filePaths.Skip(index > 0 ? index - 1 : 0).Take(3).ToArray();

            int i = 0;

            foreach (var filePath in filePaths)
            {
                var dateString = filePath.Substring(filePath.LastIndexOf("_") + 1);
                dateString = dateString.Substring(0, dateString.LastIndexOf("."));
                dateString = dateString.Substring(6, 2) + "-" + dateString.Substring(4, 2) + "-" +
                             dateString.Substring(0, 4);
                DateTime date = DateTime.Parse(dateString);
                string code = "none";

                var count = new List<CodeCount> {new CodeCount {Code = code, Count = 1}};
                // if (!reportingOptions.AvoidReadingFilesContent)
                {
                    var fileContent = File.ReadAllText(filePath);
                    var content =
                        (List<DownloadIntradayReport>) JsonConvert.DeserializeObject<List<DownloadIntradayReport>>(
                            fileContent);
                    count = content.Select(p => new CodeCount {Code = p.Code, Count = p.Count}).ToList();
                }


                var report = new DownloadIntradayReport()
                {
                    MonthlySummaryId = i++,
                    From = date,
                    To = date.AddMonths(1),
                    Counts = count
                };
                reports.Add(report);
            }

            return reports;
        }


        // Preparing files for RF_INTRADAY_FILES_LIST
        public async Task<List<DownloadIntradayReport>> RF_MONTHLY_SUMMARIES_CountFiles_PerCode_OnTheGivenMonth(
            BaseSelector selector, ReportingOptions reportingOptions)
        {
            List<DownloadIntradayReport> reports = new List<DownloadIntradayReport>();
            // Digest query object
            if (reportingOptions.FromDate == null)
            {
                return null;
            }

            // Make sure we use the first day in a month
            var date = new DateTime(reportingOptions.FromDate.Year, reportingOptions.FromDate.Month, 1);

            var resultKey = "CountHistoricalFiles" +
                            (date > DateTime.MinValue ? "_" + date.ToString("yyyyMMdd") : string.Empty);

            // Try to recover it from disk
            var path = Path.Join(downloaderOptions.LargeResultsFolder, resultKey + ".json");
            if (!reportingOptions.SkipMonthlySummaryCaching && File.Exists(path))
            {
                var fileContent = File.ReadAllText(path);
                return (List<DownloadIntradayReport>) JsonConvert.DeserializeObject<List<DownloadIntradayReport>>(
                    fileContent);
            }

            // Preparing for action
            var startTime = DateTimeOffset.UtcNow.ToUnixTimeSeconds();
            string[] sourceFiles;
            var fileDateFilter = date == DateTime.MinValue ? "*" : date.ToString("yyyyMM") + "*";
            sourceFiles = Directory.GetFiles(downloaderOptions.DailySymbolHistoryFolder,
                $"Intraday_*_{fileDateFilter}.json");
            sourceFiles = sourceFiles.OrderBy(p => p).ToArray();

            int simbsLeft = Static.SP500.Count();
            foreach (var code in Static.SP500)
            {
                simbsLeft--;
                var symbolId = 0;

                Symbol symbol = null;
                try
                {
                    symbol = Static.SymbolsDictionary.FirstOrDefault(p => p.Value.Code == code).Value;
                }
                catch (Exception ex)
                {
                    // Triggerally unhandled
                }

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
                var codePaths = sourceFiles.Where(p => p.IndexOf("_" + code + "_") > 0);
                int left = codePaths.Count();

                DateTime from = DateTime.MaxValue;
                DateTime to = DateTime.MinValue;
                foreach (var codePath in codePaths)
                {
                    left--;
                    var dateString = codePath.Substring(codePath.LastIndexOf("_") + 1);
                    dateString = dateString.Substring(0, dateString.LastIndexOf("."));
                    dateString = dateString.Substring(6, 2) + "-" + dateString.Substring(4, 2) + "-" +
                                 dateString.Substring(0, 4);
                    DateTime fileDate = DateTime.Parse(dateString);
                    if (fileDate < from)
                    {
                        from = fileDate;
                    }

                    if (to < fileDate)
                    {
                        to = fileDate;
                    }

                    var count = 0;
                    var itemsLength = 1;
                    if (left == 0 || left % 50 == 0)
                    {
                        // Cnew Info(null, simbsLeft + " " + code + ": " + left);
                    }

                    report.Details.Add(new IntradayDetail()
                    {
                        Seconds = (new DateTimeOffset(fileDate)).ToUnixTimeSeconds(),
                        Samples = 0, // count RF_MONTHLY_SUMMARIES
                        Total = itemsLength,
                        Count = 1,
                    });
                }

                // Aggregate daily results
                report.From = from;
                report.To = to;
                report.Count = codePaths.Count();
                reports.Add(report);
            }

            if (!reportingOptions.Skip_Replacing_Monthly_SummaryCaching)
            {
                Static.WriteAllText(path, JsonConvert.SerializeObject(reports));
            }

            return reports;
        }


        // RF_PER_CODE_SUMMARY
        public async Task<IEnumerable<DownloadIntradayReport>>
            RF_PER_CODE_SUMMARY_CountIntradays_PerCode_OnTheGivenMonth(BaseSelector selector,
                ReportingOptions reportingOptions)
        {
            List<DownloadIntradayReport> reports = new List<DownloadIntradayReport>();

            // Make sure we use the first day in a month
            var date = new DateTime(reportingOptions.FromDate.Year, reportingOptions.FromDate.Month, 1);

            var resultKey = (reportingOptions.Source == DataSourceType.Files
                                ? "HistoricalFilesSummary"
                                : "HistoricalDbSummary")
                            // + (reportingOptions.Codes.Count() > 0 && reportingOptions.Codes.Count() != Static.SP500.Length ? "_" + string.Join("_", reportingOptions.Codes) : "") 
                            + (date > DateTime.MinValue ? "_" + date.ToString("yyyyMMdd") : string.Empty);
            var destinationFile = Path.Join(downloaderOptions.CountsFolder, resultKey + ".json");
            // Try to recover it from disk
            if (reportingOptions.Source != DataSourceType.Db &&
                !reportingOptions
                    .ReplaceDestinationIfExists && // Trying to rewrite a selected range of symbols will not succeed but will return the evaluated symbols instead of their caching
                !reportingOptions.SkipDailySummaryCaching &&
                File.Exists(destinationFile))
            {
                var fileContent = File.ReadAllText(destinationFile);
                var data =
                    (List<DownloadIntradayReport>) JsonConvert.DeserializeObject<List<DownloadIntradayReport>>(
                        fileContent);
                if (reportingOptions.IsAllCodes)
                {
                    return data;
                }

                return data.Where(p => reportingOptions.Codes.Contains(p.Code)).OrderBy(p => p.Code);
            }

            // Try to recover it from DB
            var result = _resultRepository.GetAllByKey(resultKey).FirstOrDefault(); // Keep autside for updates
            if (reportingOptions.Source != DataSourceType.Db && !reportingOptions.ReplaceDestinationIfExists &&
                !reportingOptions.SkipDailySummaryCaching && !reportingOptions.ReplaceDestinationIfExists)
            {
                if (result != null)
                {
                    var data = (List<DownloadIntradayReport>) JsonConvert
                        .DeserializeObject<List<DownloadIntradayReport>>(
                            result.TagString);
                    if (reportingOptions.IsAllCodes)
                    {
                        return data;
                    }

                    return data.Where(p => reportingOptions.Codes.Contains(p.Code));
                }
            }

            // Preparing for action
            var startTime = DateTimeOffset.UtcNow.ToUnixTimeSeconds();


            string[] sourceFiles;
            var symbolCode = (reportingOptions.Codes.Count() == 1 ? reportingOptions.Codes[0] : "*");
            var fileDateFilter = reportingOptions.FromDate == DateTime.MinValue
                ? "*"
                : reportingOptions.FromDate.ToString("yyyyMM") + "*";
            sourceFiles = Directory.GetFiles(downloaderOptions.DailySymbolHistoryFolder,
                $"Intraday_{symbolCode}_{fileDateFilter}.json");
            if (reportingOptions.Codes.Count() > 1)
            {
                sourceFiles = sourceFiles.Where(p =>
                    reportingOptions.Codes.Contains(p.Substring(p.IndexOf('_') + 1,
                        p.LastIndexOf('_') - p.IndexOf('_') - 1))).ToArray();
            }

            sourceFiles = sourceFiles.OrderBy(p => p).ToArray();

            int simbsLeft = Static.SP500.Count();
            foreach (var code in Static.SP500)
            {
                if (
                    // reportingOptions.Codes.Length > 0 && 
                    (reportingOptions.Codes.Length > 0 &&
                     !reportingOptions.Codes.Contains(code)) || // Is not in selected range is a range was selected
                    (!string.IsNullOrEmpty(reportingOptions.Code) &&
                     code != reportingOptions.Code) // Is not the chosen code is any is chosen
                )
                {
                    continue;
                }

                if (reportingOptions.SymbolId > 0 && Static.SymbolIdFromCode[code] != reportingOptions.SymbolId)
                {
                    continue;
                }

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

                if (reportingOptions.Source == DataSourceType.Files)
                {
                    CountFromFiles(selector, sourceFiles, symbol, date, simbsLeft, report);
                }

                if (reportingOptions.Source == DataSourceType.Db)
                {
                    CountFromDb(selector, sourceFiles, symbol, date, simbsLeft, report);
                }

                reports.Add(report);
            }

            // Saving action results
            if (reportingOptions.IsAllCodes)
            {
                var success = await SaveResult(selector, result, startTime, resultKey, reports,
                    reportingOptions.ReplaceDestinationIfExists, downloaderOptions.CountsFolder, new SaveResultsOptions
                    {
                        SaveToDb = true,
                        SaveToFile = true
                    });
            }

            return reports;
        }

        private void CountFromFiles(BaseSelector selector, string[] filePaths, Symbol symbol, DateTime date,
            int simbsLeft, DownloadIntradayReport report)
        {
            var codePaths = filePaths.Where(p => p.IndexOf("_" + symbol.Code + "_") > 0);
            int left = codePaths.Count();

            DateTime from = DateTime.MaxValue;
            DateTime to = DateTime.MinValue;
            foreach (var codePath in codePaths)
            {
                left--;
                var dateString = codePath.Substring(codePath.LastIndexOf("_") + 1);
                dateString = dateString.Substring(0, dateString.LastIndexOf("."));
                dateString = dateString.Substring(6, 2) + "-" + dateString.Substring(4, 2) + "-" +
                             dateString.Substring(0, 4);
                DateTime fileDate = DateTime.Parse(dateString);
                if (fileDate < from)
                {
                    from = fileDate;
                }

                if (to < fileDate)
                {
                    to = fileDate;
                }

                var dataString = File.ReadAllText(codePath);



                var count = 0;
                var itemsLength = 0;
                try
                {
                    var items = (List<IexItem>) JsonConvert.DeserializeObject<List<IexItem>>(dataString);
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
                    new Info(null,
                        $" File Left: {date.ToString("yyyy-MM-dd")},  Left: {simbsLeft}, Code: {symbol.Code}, Samples: {count} ");
                }

                report.Details.Add(new IntradayDetail()
                {
                    Seconds = (new DateTimeOffset(date)).ToUnixTimeSeconds(),
                    Samples = count, // Ticks count from intraday files
                    Total = itemsLength,
                    Count = 1,
                });
            }

            // Aggregate daily results
            report.From = from;
            report.To = to;
            report.Count = codePaths.Count();
        }

        private void CountFromDb(BaseSelector selector, string[] filePaths, Symbol symbol, DateTime date, int simbsLeft,
            DownloadIntradayReport report)
        {
            var endDate = new DateTime(date.Month == 12 ? date.Year + 1 : date.Year,
                date.Month == 12 ? 1 : date.Month + 1, 1);
            var codePaths = _tickRepository.All()
                .Where(p => p.SymbolId == symbol.SymbolId && p.Date >= date && p.Date < endDate);
            int left = codePaths.Count();

            DateTime from = DateTime.MaxValue;
            DateTime to = DateTime.MinValue;
            foreach (var codePath in codePaths)
            {
                left--;
                var fileDate = codePath.Date;
                if (fileDate < from)
                {
                    from = fileDate;
                }

                if (to < fileDate)
                {
                    to = fileDate;
                }

                var dataString = codePath.Serialized;



                var count = 0;
                var itemsLength = 0;
                try
                {
                    if (!string.IsNullOrEmpty(dataString))
                    {
                        itemsLength = dataString.Split("],[").Length;
                    }
                }
                catch (Exception ex)
                {
                    new Info(selector, -1, ex, string.Empty);
                    itemsLength = -1;
                }

                count = itemsLength;

                if (left == 0 || left % 50 == 0)
                {
                    new Info(null,
                        $" DB Left: {date.ToString("yyyy-MM-dd")},  Left: {simbsLeft}, Code: {symbol.Code}, Samples: {count} ");
                }

                report.Details.Add(new IntradayDetail()
                {
                    Seconds = (new DateTimeOffset(date)).ToUnixTimeSeconds(),
                    Samples = count, // Ticks count from intraday files
                    Total = itemsLength,
                    Count = 1,
                });
            }

            // Aggregate daily results
            report.From = from;
            report.To = to;
            report.Count = codePaths.Count();
        }

        public async Task<IEnumerable<FileResourceGroup>> ListFiles(BaseSelector selector,
            ReportingOptions reportingOptions)
        {
            List<FileResourceGroup> result = new List<FileResourceGroup>();

            string[] dailySymbolHistoryFiles;
            dailySymbolHistoryFiles =
                new string[] { }; // Directory.GetFiles(downloaderOptions.DailySymbolHistoryFolder);
            dailySymbolHistoryFiles = dailySymbolHistoryFiles.OrderBy(p => p).ToArray();

            string[] dailyGraphsFiles;
            dailyGraphsFiles = Directory.GetFiles(downloaderOptions.DailyGraphsFolder);
            dailyGraphsFiles = dailyGraphsFiles.OrderBy(p => p).ToArray();

            string[] countFiles;
            countFiles = Directory.GetFiles(downloaderOptions.CountsFolder);
            countFiles = countFiles.OrderBy(p => p).ToArray();

            string[] monthlyParsedFiles;
            monthlyParsedFiles = Directory.GetFiles(downloaderOptions.MonthlyParsedFiles);
            monthlyParsedFiles = monthlyParsedFiles.OrderBy(p => p).ToArray();

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

            foreach (var path in countFiles.Take(50))
            {
                var fileInfo = new FileInfo(path);
                result.Add(new FileResourceGroup
                {
                    Category = "CountFiles",
                    // Folder = downloaderOptions.DailySymbolHistoryFolder,
                    Name = fileInfo.Name,
                    Length = fileInfo.Length,
                    LastWrite = Static.SecondsFromDateTime(fileInfo.LastWriteTimeUtc)
                });
            }

            foreach (var path in monthlyParsedFiles.Take(50))
            {
                var fileInfo = new FileInfo(path);
                result.Add(new FileResourceGroup
                {
                    Category = "MonthlyParsedFiles",
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

        public async Task<IEnumerable<DownloadIntradayReport>> GetFileRecordsByReportingOptions(BaseSelector selector,
            ReportingOptions reportingOptions)
        {
            List<DownloadIntradayReport> reports = new List<DownloadIntradayReport>();
            string[] FilePaths;
            var symbolCode = string.IsNullOrEmpty(reportingOptions.Code) ? "*" : reportingOptions.Code;
            var fileDateFilter = reportingOptions.FromDate == DateTime.MinValue
                ? "*"
                : reportingOptions.FromDate.ToString("yyyyMM") + "*";
            FilePaths = Directory.GetFiles(downloaderOptions.DailySymbolHistoryFolder,
                $"Intraday_{symbolCode}_{fileDateFilter}.json");
            FilePaths = FilePaths.OrderBy(p => p).ToArray();

            int simbsLeft = Static.SP500.Count();
            foreach (var code in Static.SP500)
            {
                if (!string.IsNullOrEmpty(reportingOptions.Code) && code != reportingOptions.Code)
                {
                    continue;
                }

                if (reportingOptions.SymbolId > 0 && Static.SymbolIdFromCode[code] != reportingOptions.SymbolId)
                {
                    continue;
                }

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
                    dateString = dateString.Substring(6, 2) + "-" + dateString.Substring(4, 2) + "-" +
                                 dateString.Substring(0, 4);
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
                        var items = (List<IexItem>) JsonConvert.DeserializeObject<List<IexItem>>(dataString);
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
                        new Info(null, simbsLeft + " " + code + ": " + left);
                    }

                    report.Details.Add(new IntradayDetail()
                    {
                        Seconds = (new DateTimeOffset(date)).ToUnixTimeSeconds(),
                        Samples = count, // Ticks count from intraday files
                        Total = itemsLength,
                        Count = 1,

                    });
                }

                // Aggregate daily results
                report.From = from;
                report.To = to;
                report.Count = codePaths.Count();
                reports.Add(report);
                ;
            }

            return reports;
        }

        public IQueryable<Daily> GetDailyRecordsByReportingOptions(BaseSelector selector,
            ReportingOptions reportingOptions)
        {
            var items = _dailyRepository.All().Where(p =>
                (string.IsNullOrEmpty(reportingOptions.Code) || p.Symbol.Code == reportingOptions.Code) &&
                (reportingOptions.SymbolId == 0 || p.Symbol.SymbolId == reportingOptions.SymbolId)
            );
            if (reportingOptions.FromDate > DateTime.MinValue)
            {
                items = items.Where(p => p.Date > reportingOptions.FromDate);
            }

            if (reportingOptions.Take > 0)
            {
                items = items.Take(reportingOptions.Take);
            }

            return items;
        }

        public IQueryable<Tick> GetIntradayRecordsByReportingOptions(BaseSelector selector,
            ReportingOptions reportingOptions)
        {
            var items = _tickRepository.All().Where(p =>
                (string.IsNullOrEmpty(reportingOptions.Code) || p.Symbol == reportingOptions.Code) &&
                (reportingOptions.SymbolId == 0 || p.SymbolId == reportingOptions.SymbolId)
            );
            if (reportingOptions.FromDate > DateTime.MinValue)
            {
                items = items.Where(p => p.Date > reportingOptions.FromDate);
            }

            if (reportingOptions.Take > 0)
            {
                items = items.Take(reportingOptions.Take);
            }

            return items;
        }

        #endregion REPORTING
        
        #region INTERVAL PREVIEW

        public IQueryable<Tick> GetIntervalPreview(BaseSelector selector, Static.LastIntervalType interval, int[] symbolIds = null)
        {
            var seconds = 0;
            switch (interval)
            {
                case Static.LastIntervalType.Year:
                    seconds = Static.IntervalTypeYear_Interval;
                    break;
                case Static.LastIntervalType.Month:
                    seconds = Static.IntervalTypeMonth_Interval;
                    break;
                case Static.LastIntervalType.Week:
                    seconds = Static.IntervalTypeWeek_Interval;
                    break;
                case Static.LastIntervalType.Day:
                    seconds = Static.IntervalTypeDay_Interval;
                    break;
            }
            var existentTicks = _tickRepository.TicksBySymbolIdAndByInterval(interval, symbolIds);
            return existentTicks;
        }

        public async Task<IEnumerable<string>> CreateIntervalPreviews(Static.LastIntervalType interval)
        {
            var symbolsCount = Static.SymbolIDs.Count();
            int symbolPos = 0;
            
            List<Tick> allDayTicks = interval != Static.LastIntervalType.Day ? null : _tickRepository.TicksBySymbolIdAndByInterval(Static.LastIntervalType.Day).ToList();
            List<Tick> allMonthTicks = interval == Static.LastIntervalType.Day ? null : _tickRepository.TicksBySymbolIdAndByInterval(Static.LastIntervalType.Day).ToList();
            List<Tick> allYearTicks = interval != Static.LastIntervalType.Year ? null : _tickRepository.TicksBySymbolIdAndByInterval(Static.LastIntervalType.Year).ToList();
            
            foreach (var symbolId in Static.SymbolIDs)
            {
                symbolPos++;
                var existentTickDay = allDayTicks?.FirstOrDefault(p => p.SymbolId == symbolId);
                var existentTickMonth = allMonthTicks?.FirstOrDefault(p => p.SymbolId == symbolId);
                var existentTickYear = allYearTicks?.FirstOrDefault(p => p.SymbolId == symbolId);

                if (interval == Static.LastIntervalType.Day)
                {
                    var tick = await CreateOrUpdateTodayIntradayInterval(existentTickDay, symbolId);
                    continue;
                }

                var oneYearSecond = Static.SecondsFromDateTime(DateTime.Today.AddMonths(-1));
                var oneMonthSecond = Static.SecondsFromDateTime(DateTime.Today.AddYears(-1));
                
                var lastMonthSecond = Static.SecondsFromDateTime(DateTime.Today.AddMonths(-1));
                var lastYearSecond = Static.SecondsFromDateTime(DateTime.Today.AddYears(-1));

                if (existentTickMonth != null && existentTickMonth.Serialized.Length > 0)
                {
                    var lii = existentTickMonth.Serialized.LastIndexOf('[');
                    if (lii > -1)
                    {
                        var lastSeconds = existentTickMonth.Serialized.Substring(lii+1);
                        if (lastSeconds.Length > 0)
                        {
                            lastSeconds = lastSeconds.Substring(0, lastSeconds.IndexOf(','));
                            long lastSecond = 0;
                            if(long.TryParse(lastSeconds, out lastSecond))
                            {
                                lastMonthSecond = lastSecond;
                            }
                        }
                    }
                }
                if (existentTickYear != null && existentTickYear.Serialized.Length > 0)
                {
                    var lii = existentTickYear.Serialized.LastIndexOf('[');
                    if (lii > -1)
                    {
                        var lastSeconds = existentTickYear.Serialized.Substring(lii+1);
                        if (lastSeconds.Length > 0)
                        {
                            lastSeconds = lastSeconds.Substring(0, lastSeconds.IndexOf(','));
                            long lastSecond = 0;
                            if(long.TryParse(lastSeconds, out lastSecond))
                            {
                                lastYearSecond = lastSecond;
                            }
                        }
                    }
                }

                var fromSeconds = lastMonthSecond;
                if (lastYearSecond > 0 && lastYearSecond < fromSeconds)
                {
                    fromSeconds = lastYearSecond;
                }
                var fromDate = Static.StartOfMonthFromSeconds(fromSeconds);
                var monthlyTicks = _tickRepository.GetAllMonthlyBySymbolId(symbolId, fromDate).OrderBy(p=>p.Date).ToList();

                List<TickArray> allMonthlyTicks = new List<TickArray>();
                if(interval == Static.LastIntervalType.Year){ // All last year
                    foreach (var tick in monthlyTicks)
                    {   //   "[[157795740,328.53],[157795752,328.383],[157795758,328.818],[157795764,329.145],[157795770,329.005],[157795776,328.8],[157795782,328.219],[157795788,327.886],[157795794,328.199],[157795800,328.592],[157795812,329.24],[157795818,329.632],[157795824,329.869],[157795830,329.824],[157795836,329.891],[157795866,330.181],[157795872,330.524],[157795878,330.608],[157795884,331.092],[157795890,331.133],[157795896,330.899],[157795902,330.995],[157795908,331.054],[157795914,331.07],[157795920,331.053],[157"
                        var tickMonth = (IEnumerable<TickArray>)JsonConvert.DeserializeObject<IEnumerable<TickArray>>(tick.Serialized);
                        allMonthlyTicks.AddRange(tickMonth);
                    }
                }
                if(interval == Static.LastIntervalType.Month){ // Only last 30 days
                    foreach (var tick in monthlyTicks)
                    {   //   "[[157795740,328.53],[157795752,328.383],[157795758,328.818],[157795764,329.145],[157795770,329.005],[157795776,328.8],[157795782,328.219],[157795788,327.886],[157795794,328.199],[157795800,328.592],[157795812,329.24],[157795818,329.632],[157795824,329.869],[157795830,329.824],[157795836,329.891],[157795866,330.181],[157795872,330.524],[157795878,330.608],[157795884,331.092],[157795890,331.133],[157795896,330.899],[157795902,330.995],[157795908,331.054],[157795914,331.07],[157795920,331.053],[157"
                        var tickMonth = (IEnumerable<TickArray>)JsonConvert.DeserializeObject<IEnumerable<TickArray>>(tick.Serialized);
                        allMonthlyTicks.AddRange(tickMonth);
                    }
                }

                if (interval == Static.LastIntervalType.Year)
                {
                    new Info(new BaseSelector(),
                        $"BuildLastInterval... [{symbolPos}/{symbolsCount}] for symbolId:{symbolId} at every:{Static.IntervalTypeYear_Interval} seconds.");
                    await BuildLastInterval(symbolId, existentTickMonth, existentTickYear, lastMonthSecond, lastYearSecond, allMonthlyTicks, Static.LastIntervalType.Year);
                }
                
                // Month interval will also be updated on year, because we already have the data we need.
                new Info(new BaseSelector(), $"BuildLastInterval... [{ symbolPos }/{ symbolsCount }] for symbolId:{ symbolId } at every:{ Static.IntervalTypeMonth_Interval } seconds.");
                await BuildLastInterval(symbolId, existentTickMonth, existentTickYear, lastMonthSecond, lastYearSecond,  allMonthlyTicks, Static.LastIntervalType.Month);
            }

            return null;
        }

        private async Task<Tick> CreateOrUpdateTodayIntradayInterval(Tick existentTick, int symbolId)
        {
            List<TickArray> inputTicks = new List<TickArray>();
            inputTicks = Static.SymbolsDictionary[symbolId].TicksCache;
            var serialized = JsonConvert.SerializeObject(inputTicks);
                    
            if (existentTick != null)
            {
                existentTick.Updated = Static.Now();
                existentTick.Serialized = serialized;
                existentTick.Samples = inputTicks.Count();
                existentTick.Seconds = Static.SecondsFromDateTime(DateTime.Today.Date);
                await _tickRepository.UpdateAsync(existentTick);
                return existentTick;
            }
            else
            {
                var newTick = new Tick()
                {
                    Created = Static.SecondsFromDateTime(DateTime.Today.Date),
                    Updated = Static.Now(),
                    Date = DateTime.Today.Date,
                    IsMonthly = false,
                    Serialized = serialized,
                    Interval = Static.IntervalTypeDay_Interval,
                    SymbolId = symbolId,
                    Symbol = Static.SymbolCodeFromId[symbolId],
                    Samples = inputTicks.Count(),
                    Seconds = Static.SecondsFromDateTime(DateTime.Today.Date),
                    // Ticks = outputTicks,
                };
                await _tickRepository.AddAsync(newTick);
                return newTick;
            }
        }

        private async Task BuildLastInterval(int symbolId, Tick destinationTickYear, Tick destinationTickMonth, long lastMonthSecond, long lastYearSecond, List<TickArray> inputTicks, Static.LastIntervalType interval)
        {
            if (interval == Static.LastIntervalType.Day)
            {
                return; // Intra-day ticks of the current day will be saved as they are.
            }
            try
            {
                List<TickArray> outputYearTicks = new List<TickArray>();
                List<TickArray> outputMonthTicks = new List<TickArray>();
                
                var oneYearAgoDate = DateTime.Today.AddYears(-1);
                var oneMonthAgoDate = DateTime.Today.AddMonths(-1);
                
                var oneYearAgoSeconds = Static.SecondsFromDateTime(oneYearAgoDate);
                var oneMonthAgoSeconds = Static.SecondsFromDateTime(oneMonthAgoDate);
                
                var fromSecond_Year = lastYearSecond > 0 ? lastYearSecond : oneYearAgoSeconds;
                var fromSecond_Month = lastMonthSecond > 0 ? lastMonthSecond : oneMonthAgoSeconds;

                var yesterdayDate = DateTime.Today.AddDays(0);
                var yesterdaySeconds = Static.SecondsFromDateTime(yesterdayDate);
                var endingAt = yesterdaySeconds / 10; // Both year and month
                
                // Aggregate data for YEAR

                if (interval == Static.LastIntervalType.Year)
                {
                    var timeFrameYearTicks = inputTicks
                        .Where(tick => tick.T >= fromSecond_Year / 10 && tick.T < yesterdaySeconds / 10).ToList(); // Ticks left to aggregate

                    var startingLeftYearAt = fromSecond_Year / 10;
                    var nextLeftYearAt = fromSecond_Year / 10;
                    endingAt = yesterdaySeconds / 10; // Both year and month
                    var intervalForAllYear = Static.IntervalTypeYear_Interval / 10; // Seconds in a hour / 10

                    int yearTickPos = 0;
                    int yearIntervalsCount = (int) (endingAt - startingLeftYearAt) / intervalForAllYear;
                    while (startingLeftYearAt < endingAt)
                    {
                        yearTickPos++;
                        if (yearTickPos % 1000 == 0)
                        {
                            new Info(new BaseSelector(),
                                $"BuildLastInterval s [{yearTickPos}/{yearIntervalsCount}] for symbolId:{symbolId} at interval: {interval}.");
                        }

                        nextLeftYearAt = startingLeftYearAt + intervalForAllYear;
                        // var dayOffset = ((startingAt % 8640) / 60) % 1;
                        // if (dayOffset > 10 && dayOffset < 100)
                        {
                            // 1621209600 = 17 May 2021
                            // 1621296000 = 18 May 2021
                            // => 86400 seconds in a day
                            var intervalTicks =
                                timeFrameYearTicks.Where(tick => tick.T >= startingLeftYearAt && tick.T < nextLeftYearAt).ToList();
                            if (intervalTicks.Any())
                            {
                                outputYearTicks.Add(new TickArray()
                                {
                                    T = startingLeftYearAt,
                                    V = intervalTicks.Sum(p => p.V) / intervalTicks.Count()
                                });
                            }
                        }

                        startingLeftYearAt = nextLeftYearAt;
                    }
                }
                
                // Aggregate data for month
                
                    var timeFrameMonthTicks = inputTicks
                        .Where(tick => tick.T >= fromSecond_Month / 10 && tick.T < yesterdaySeconds / 10).ToList(); // Ticks left to aggregate

                    var startingLeftMonthAt = fromSecond_Month / 10;
                    var nextLeftMonthAt = fromSecond_Month / 10;
                    endingAt = yesterdaySeconds / 10; // Both year and month
                    var intervalForAllMonth = Static.IntervalTypeMonth_Interval / 10; // Seconds in a hour / 10

                    int monthTickPos = 0;
                    int monthIntervalsCount = (int) (endingAt - startingLeftMonthAt) / intervalForAllMonth;
                    while (startingLeftMonthAt < endingAt)
                    {
                        monthTickPos++;
                        if (monthTickPos % 1000 == 0)
                        {
                            new Info(new BaseSelector(),
                                $"BuildLastInterval s [{monthTickPos}/{monthIntervalsCount}] for symbolId:{symbolId} at interval: {interval}.");
                        }

                        nextLeftMonthAt = startingLeftMonthAt + intervalForAllMonth;
                        // var dayOffset = ((startingAt % 8640) / 60) % 1;
                        // if (dayOffset > 10 && dayOffset < 100)
                        {
                            // 1621209600 = 17 May 2021
                            // 1621296000 = 18 May 2021
                            // => 86400 seconds in a day
                            var intervalTicks =
                                timeFrameMonthTicks.Where(tick => tick.T >= startingLeftMonthAt && tick.T < nextLeftMonthAt).ToList();
                            if (intervalTicks.Any())
                            {
                                outputMonthTicks.Add(new TickArray()
                                {
                                    T = startingLeftMonthAt,
                                    V = intervalTicks.Sum(p => p.V) / intervalTicks.Count()
                                });
                            }
                        }

                        startingLeftMonthAt = nextLeftMonthAt;
                    }

                // Save Data

                if (interval == Static.LastIntervalType.Year)
                {

                    if (destinationTickYear != null)
                    {
                        var existentTick = destinationTickYear;
                        var partialSerializedYear = JsonConvert.SerializeObject(outputYearTicks);
                        var serializedYear = existentTick.Serialized.Length > 10 ? Static.ConcatSerialized(existentTick.Serialized, partialSerializedYear, true) : partialSerializedYear;
                        
                        existentTick.Updated = Static.Now();
                        existentTick.Serialized = serializedYear;
                        existentTick.Samples = outputYearTicks.Count();
                        existentTick.Seconds = oneYearAgoSeconds;
                        await _tickRepository.UpdateAsync(existentTick);
                    }
                    else
                    {
                        var serializedYear = JsonConvert.SerializeObject(outputYearTicks);
                        destinationTickYear = new Tick()
                        {
                            Created = oneYearAgoSeconds,
                            Updated = Static.Now(),
                            Date = oneYearAgoDate,
                            IsMonthly = false,
                            Serialized = serializedYear,
                            Interval = Static.IntervalTypeYear_Interval,
                            SymbolId = symbolId,
                            Symbol = Static.SymbolCodeFromId[symbolId],
                            Samples = outputYearTicks.Count(),
                            Seconds = oneYearAgoSeconds,
                            // Ticks = outputTicks,
                        };
                        await _tickRepository.AddAsync(destinationTickYear);
                    }
                }
                

                if (destinationTickMonth != null)
                {
                    var existentTick = destinationTickYear;
                    var partialSerializedMonth = JsonConvert.SerializeObject(outputMonthTicks); 
                    var serializedMonth = existentTick.Serialized.Length > 10 ? Static.ConcatSerialized(existentTick.Serialized, partialSerializedMonth, true) : partialSerializedMonth;

                    existentTick.Updated = Static.Now();
                    existentTick.Serialized = serializedMonth;
                    existentTick.Samples = outputMonthTicks.Count();
                    existentTick.Seconds = oneYearAgoSeconds;
                    await _tickRepository.UpdateAsync(existentTick);
                }
                else
                {
                    var serializedMonth = JsonConvert.SerializeObject(outputMonthTicks); 
                    destinationTickMonth = new Tick()
                    {
                        Created = oneYearAgoSeconds,
                        Updated = Static.Now(),
                        Date = oneYearAgoDate,
                        IsMonthly = false,
                        Serialized = serializedMonth,
                        Interval = Static.IntervalTypeMonth_Interval,
                        SymbolId = symbolId,
                        Symbol = Static.SymbolCodeFromId[symbolId],
                        Samples = outputMonthTicks.Count(),
                        Seconds = oneYearAgoSeconds,
                        // Ticks = outputTicks,
                    };
                    await _tickRepository.AddAsync(destinationTickMonth);
                }
            }
            catch (Exception ex)
            {
                new Info(new BaseSelector(), $"BuildLastInterval... exception for symbolId:{symbolId} for interval: {interval} message: {ex.Message}");
            }
        }
        
        #endregion

        public async Task<IEnumerable<Step1_Coverage_For_Day>> MissingIntradaysForMonth(DateTime date,
            bool downloadMissing = false, IEnumerable<Step1_Coverage_For_Day> knownItems = null)
        {
            var missingItems = new List<Step1_Coverage_For_Day>();

            {
                var startOfMonth = new DateTime(date.Year, date.Month, 1);
                var sourceFiles = Directory.GetFiles(downloaderOptions.DailySymbolHistoryFolder,
                    $"Intraday_*_{date.ToString("yyyyMM")}*.json");

                for (var i = 0; i <= 31; i++)
                {
                    var dateToAdd = startOfMonth.AddDays(i);
                    if (dateToAdd >= DateTime.Today)
                    {
                        break;
                    }

                    if (dateToAdd.Month != startOfMonth.Month)
                    {
                        break;
                    }

                    if (Static.IsOpenDay(dateToAdd) && (knownItems == null || knownItems.Count() == 0 ||
                                                        knownItems.Any(p => p.Date == dateToAdd)))
                    {
                        var dayList = new Step1_Coverage_For_Day(dateToAdd);
                        foreach (var code in Static.SymbolCodes)
                        {
                            if (code == "DEMO")
                            {
                                continue;
                            }

                            var path = Path.Combine(downloaderOptions.DailySymbolHistoryFolder,
                                $"Intraday_{code}_{dateToAdd.ToString("yyyyMMdd")}.json");
                            if (!sourceFiles.Contains(path))
                            {
                                dayList.Codes.Add(code);
                            }
                        }

                        if (dayList.Codes.Any())
                        {
                            missingItems.Add(dayList);
                        }
                    }
                }
            }


            if (downloadMissing)
            {
                foreach (var day in missingItems)
                {
                    var selector = new BaseSelector(CommandSource.Endpoint);
                    ReportingOptions reportingOptions = new ReportingOptions()
                    {
                        Source = DataSourceType.Db,
                        FromDate = day.Date,
                        Take = 1,
                        ReplaceDestinationIfExists = true,
                        GoBackward = false,
                        ProcessType = ProcessType.Step_1_DownloadFromIex,
                        Codes = day.Codes.ToArray()
                    };
                    var results = await BulkProcess(selector, reportingOptions, ActionRange.AllForDay);
                }
            }

            return missingItems;
        }
        
        public async Task<IEnumerable<Step2_Coverage_For_Code>> MissingProcessedMonths(DateTime date,
            bool processMissing = false, IEnumerable<Step2_Coverage_For_Code> knownItems = null)
        {
            var startOfMonth = new DateTime(date.Year, date.Month, 1);
            var missingItems = new List<Step2_Coverage_For_Code>();
            {
                var sourceFiles = Directory.GetFiles(downloaderOptions.DailySymbolHistoryFolder,
                    $"Intraday_*_{date.ToString("yyyyMM")}*.json");
                var destinationFiles = Directory.GetFiles(downloaderOptions.MonthlyParsedFiles,
                    "Parsed_*_" + date.ToString("yyyyMM") + "01.json");
                
                var codesCount = Static.SymbolCodes.Length;
                int c = 0;
                foreach (var code in Static.SymbolCodes)
                {
                    c++;
                    if(c%20==0) Console.WriteLine($"Collecting {c} from {codesCount}");
                    if (code == "DEMO")
                    {
                        continue;
                    }
                    if(knownItems != null && knownItems.Count() > 0 && !knownItems.Any(p => p.Code == code))
                    {
                        // Do not reevaluate completed codes.
                        continue;
                    }

                    var destinationFile = destinationFiles.FirstOrDefault(p => p.Contains($"Parsed_{code}_"));
                    var destinationFileContent = "[]";
                    var monthItems = new object[0][];
                    if (destinationFile != null)
                    {
                        // Parse file not created for this month
                        destinationFileContent = File.ReadAllText(destinationFile);
                        monthItems = (object[][]) JsonConvert.DeserializeObject<object[][]>(destinationFileContent);
                    }

                    var dayList = new Step2_Coverage_For_Code(code);
                    for (var i = 0; i <= 31; i++)
                    {
                        var dateToAdd = startOfMonth.AddDays(i);
                        if (dateToAdd >= DateTime.Today)
                        {
                            break;
                        }

                        if (dateToAdd.Month != startOfMonth.Month)
                        {
                            break;
                        }

                        if (Static.IsOpenDay(dateToAdd))
                        {
                            var startOfDaySconds = Static.SecondsFromDateTime(dateToAdd);
                            var dayItems = monthItems.Where(p => (long)p[0]*10 >= startOfDaySconds && (long)p[0]*10 < startOfDaySconds + Static.DaySeconds ).ToList();
                            if (dayItems.Count() < 10)
                            {
                                var sourceFile = sourceFiles.FirstOrDefault(p => p.Contains($"Intraday_{code}_{dateToAdd.ToString("yyyyMMdd")}.json"));
                                if (sourceFile != null)
                                {
                                    dayList.Dates.Add(dateToAdd);
                                }
                                else
                                {
                                    
                                }
                            }
                        }
                    }

                    if (dayList.Dates.Any())
                    {
                        missingItems.Add(dayList);
                    }
                }
            }
            
            if (processMissing)
            {
                var codesCount = missingItems.Count;
                int i = 0;
                foreach (var code in missingItems)
                {
                    i++;
                    Console.WriteLine($"Processing {i} from {codesCount}");
                    var selector = new BaseSelector(CommandSource.Endpoint);
                    ReportingOptions reportingOptions = new ReportingOptions()
                    {
                        Source = DataSourceType.Files,
                        FromDate = startOfMonth,
                        Take = 1,
                        IsAllCodes = false,
                        Code = code.Code,
                        SkipMonthlySummaryCaching = true,
                        IsMonthly = true,
                        SkipDailySummaryCaching = true,
                        ReplaceDestinationIfExists = true,
                        Skip_RF_PER_CODE_SUMMARY_Caching = true,
                        ProcessType = ProcessType.Step_2_ProcessFiles
                    };
                    var results = await BulkProcess(selector, reportingOptions, ActionRange.AllForDay);
                }
            }
            return missingItems;
        }
        
        
        public async Task<IEnumerable<Step1_Coverage_For_Day>> UploadProcessedMonths(DateTime date, bool uploadMissing = false, IEnumerable<Step1_Coverage_For_Day> knownItems = null)
        {
            var missingItems = new List<Step1_Coverage_For_Day>();
            {
                var startOfMonth = new DateTime(date.Year, date.Month, 1);
                var contents = new List<KeyValuePair<string, object[][]>>() { };

                var f = 0;
                var codesCount = Static.SymbolCodes.Length;
                var dayList = new Step1_Coverage_For_Day(startOfMonth);
                foreach (var code in Static.SymbolCodes)
                {
                    f++;
                    Console.WriteLine($"Reading {f} from {codesCount}");
                    var path = Path.Combine(downloaderOptions.MonthlyParsedFiles, $"Parsed_{code}_{startOfMonth.ToString("yyyyMM")}01.json");
                    var fileContent = File.ReadAllText(path);
                    var items = (object[][]) JsonConvert.DeserializeObject<object[][]>(fileContent);
                    // contents.Add(new KeyValuePair<string, object[][]>(code, items));
                    
                    // var items = (List<IexItem>) JsonConvert.DeserializeObject<List<IexItem>>(fileContent);
                    var serialized = _tickRepository.All().FirstOrDefault(p =>
                        p.IsMonthly == true &
                        p.SymbolId == Static.SymbolIdFromCode[code] &&
                        p.Date == startOfMonth
                    );
                    if (serialized == null || fileContent != serialized.Serialized)
                    {
                        dayList.Codes.Add(code);
                    }
                }
                
                if (dayList.Codes.Any())
                {
                    missingItems.Add(dayList);
                }
                /*
                for (var i = 0; i <= 31; i++)
                {
                    Console.WriteLine($"Checking {i} from {DateTime.DaysInMonth(startOfMonth.Year, startOfMonth.Month)}");
                    var dateToAdd = startOfMonth.AddDays(i);
                    if (dateToAdd >= DateTime.Today)
                    {
                        break;
                    }

                    if (dateToAdd.Month != startOfMonth.Month)
                    {
                        break;
                    }

                    if (Static.IsOpenDay(dateToAdd) && (knownItems == null || knownItems.Count() == 0 || knownItems.Any(p => p.Date == dateToAdd)))
                    {
                        var dayList = new Step1_Coverage_For_Day(dateToAdd);
                        foreach (var code in Static.SymbolCodes)
                        {
                            if (code == "DEMO")
                            {
                                continue;
                            }

                            var path = Path.Combine(downloaderOptions.DailySymbolHistoryFolder, $"Intraday_{code}_{dateToAdd.ToString("yyyyMMdd")}.json");
                            var fileContent = File.ReadAllText(path);
                            // var items = (List<IexItem>) JsonConvert.DeserializeObject<List<IexItem>>(fileContent);
                            var serialized = _tickRepository.All().FirstOrDefault(p =>
                                p.IsMonthly == true &
                                p.SymbolId == Static.SymbolIdFromCode[code] &&
                                p.Date == startOfMonth
                            );
                            if (serialized == null || fileContent != serialized.Serialized)
                            {
                                dayList.Codes.Add(code);
                            }
                        }

                        if (dayList.Codes.Any())
                        {
                            missingItems.Add(dayList);
                        }
                    }
                }*/
            }


            if (uploadMissing)
            {
                foreach (var day in missingItems)
                {
                    var selector = new BaseSelector(CommandSource.Endpoint);
                    ReportingOptions reportingOptions = new ReportingOptions()
                    {
                        Source = DataSourceType.Files,
                        FromDate = day.Date,
                        Take = 1,
                        ReplaceDestinationIfExists = true,
                        GoBackward = false,
                        ProcessType = ProcessType.RF_UPLOAD_MONTHLY,
                        Codes = day.Codes.ToArray()
                    };
                    var results = await BulkProcess(selector, reportingOptions, ActionRange.AllForDay);
                }
            }

            return missingItems;
        }
    }


    public class Step1_Coverage_For_Day
    {
        public Step1_Coverage_For_Day(){}

        public Step1_Coverage_For_Day(DateTime date)
        {
            this.Date = date;
        }
        public DateTime Date { get; set; }
        public List<string> Codes { get; set; } = new List<string>();
    }

    public class Step2_Coverage_For_Code
    {
        public Step2_Coverage_For_Code(){}

        public Step2_Coverage_For_Code(string code)
        {
            this.Code = code;
        }

        public List<DateTime> Dates { get; set; } = new List<DateTime>();
        public string Code { get; set; }
    }

    public class CodeCount
    {
        public string Code { get; set; }
        public int Count { get; set; }
    }
}
