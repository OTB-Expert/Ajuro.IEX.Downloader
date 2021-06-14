using Ajuro.IEX.Downloader.Models;
using Ajuro.Net.Processor.Models;
using Ajuro.Net.Stock.Repositories;
using Ajuro.Net.Types.Stock;
using Ajuro.Net.Types.Stock.Models;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
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
    public partial class IntervalService : IIntervalService
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

        public IntervalService
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


        #region INTERVAL PREVIEW

        public async Task<Tick> UploadInterval(UploadIntervalModel model)
        {
            return await UpsertInterval(model.SymbolId, model.Interval, null, model.Ticks, true);
        }

        public IQueryable<Tick> GetIntervalPreview(BaseSelector selector, Static.LastIntervalType interval,
            int[] symbolIds = null)
        {
            int seconds = Static.IntervalToSeconds(interval);

            var existentTicks = _tickRepository.TicksBySymbolIdsAndByInterval(interval, symbolIds);
            return existentTicks;
        }

        public async Task<IEnumerable<string>> CreateIntervalPreviews(Static.LastIntervalType interval)
        {
            var symbolsCount = Static.SymbolIDs.Count();
            int symbolPos = 0;

            int symbolsPageSize = 10;
            for (int s = 0; s < Static.SymbolsDictionary.Count; s += symbolsPageSize)
            {
                var symbolIds = Static.SymbolsDictionary.Skip(s).Take(symbolsPageSize).Select(p => p.Key).ToArray();

                List<Tick> allDayTicks = interval != Static.LastIntervalType.Day
                    ? null
                    : _tickRepository.TicksBySymbolIdsAndByInterval(Static.LastIntervalType.Day, symbolIds).ToList();
                List<Tick> allMonthTicks = interval == Static.LastIntervalType.Day
                    ? null
                    : _tickRepository.TicksBySymbolIdsAndByInterval(Static.LastIntervalType.Month, symbolIds).ToList();
                List<Tick> allYearTicks = interval != Static.LastIntervalType.Year
                    ? null
                    : _tickRepository.TicksBySymbolIdsAndByInterval(Static.LastIntervalType.Year, symbolIds).ToList();

                foreach (var symbolId in symbolIds)
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
                            var lastSeconds = existentTickMonth.Serialized.Substring(lii + 1);
                            if (lastSeconds.Length > 0 && lastSeconds.IndexOf(',') > 0)
                            {
                                lastSeconds = lastSeconds.Substring(0, lastSeconds.IndexOf(','));
                                long lastSecond = 0;
                                if (long.TryParse(lastSeconds, out lastSecond))
                                {
                                    lastMonthSecond = lastSecond * 10;
                                }
                            }
                            else
                            {
                                new Info(new BaseSelector(),
                                    $"BuildLastInterval... Month is empty: {existentTickMonth.Symbol} ");
                            }
                        }
                    }

                    if (existentTickYear != null && existentTickYear.Serialized.Length > 0)
                    {
                        var lii = existentTickYear.Serialized.LastIndexOf('[');
                        if (lii > -1)
                        {
                            var lastSeconds = existentTickYear.Serialized.Substring(lii + 1);
                            if (lastSeconds.Length > 0 && lastSeconds.IndexOf(',') > 0)
                            {
                                lastSeconds = lastSeconds.Substring(0, lastSeconds.IndexOf(','));
                                long lastSecond = 0;
                                if (long.TryParse(lastSeconds, out lastSecond))
                                {
                                    lastYearSecond = lastSecond * 10;
                                }
                            }
                            else
                            {
                                new Info(new BaseSelector(),
                                    $"BuildLastInterval... Year is empty: {existentTickYear.Symbol} ");
                            }
                        }
                    }

                    var fromSeconds = lastMonthSecond;
                    if (interval == Static.LastIntervalType.Year && lastYearSecond > 0 && lastYearSecond < fromSeconds)
                    {
                        fromSeconds = lastYearSecond;
                    }

                    if (lastMonthSecond > 0 && lastMonthSecond < fromSeconds)
                    {
                        fromSeconds = lastMonthSecond;
                    }

                    if (Static.DateTimeFromSeconds(fromSeconds) >= DateTime.Today.Date.AddDays(-1))
                    {
                        new Info(new BaseSelector(),
                            $"BuildLastInterval... SKIP [{symbolPos}/{symbolsCount}] for symbolId:{symbolId} at every:{Static.IntervalTypeYear_Interval} seconds.");

                        continue;
                    }

                    var fromDate = Static.StartOfMonthFromSeconds(fromSeconds);
                    var monthlyTicks = _tickRepository.GetAllMonthlyBySymbolId(symbolId, fromDate).OrderBy(p => p.Date)
                        .ToList();
                    if (monthlyTicks.Count == 0)
                    {
                        new Info(new BaseSelector(),
                            $"BuildLastInterval... NO NEW DATA [{symbolPos}/{symbolsCount}] for symbolId:{symbolId} at every:{Static.IntervalTypeYear_Interval} seconds.");

                        continue;
                    }

                    List<TickArray> allMonthlyTicks = new List<TickArray>();
                    if (interval == Static.LastIntervalType.Year)
                    {
                        // All last year
                        foreach (var tick in monthlyTicks)
                        {
                            //   "[[157795740,328.53],[157795752,328.383],[157795758,328.818],[157795764,329.145],[157795770,329.005],[157795776,328.8],[157795782,328.219],[157795788,327.886],[157795794,328.199],[157795800,328.592],[157795812,329.24],[157795818,329.632],[157795824,329.869],[157795830,329.824],[157795836,329.891],[157795866,330.181],[157795872,330.524],[157795878,330.608],[157795884,331.092],[157795890,331.133],[157795896,330.899],[157795902,330.995],[157795908,331.054],[157795914,331.07],[157795920,331.053],[157"
                            var tickMonth =
                                (IEnumerable<TickArray>) JsonConvert.DeserializeObject<IEnumerable<TickArray>>(
                                    tick.Serialized);
                            allMonthlyTicks.AddRange(tickMonth);
                        }
                    }

                    if (interval == Static.LastIntervalType.Month)
                    {
                        // Only last 30 days
                        foreach (var tick in monthlyTicks)
                        {
                            //   "[[157795740,328.53],[157795752,328.383],[157795758,328.818],[157795764,329.145],[157795770,329.005],[157795776,328.8],[157795782,328.219],[157795788,327.886],[157795794,328.199],[157795800,328.592],[157795812,329.24],[157795818,329.632],[157795824,329.869],[157795830,329.824],[157795836,329.891],[157795866,330.181],[157795872,330.524],[157795878,330.608],[157795884,331.092],[157795890,331.133],[157795896,330.899],[157795902,330.995],[157795908,331.054],[157795914,331.07],[157795920,331.053],[157"
                            var tickMonth =
                                (IEnumerable<TickArray>) JsonConvert.DeserializeObject<IEnumerable<TickArray>>(
                                    tick.Serialized);
                            allMonthlyTicks.AddRange(tickMonth);
                        }
                    }

                    if (allMonthlyTicks.Count == 0)
                    {
                        new Info(new BaseSelector(),
                            $"BuildLastInterval... NO NEW TICKS [{symbolPos}/{symbolsCount}] for symbolId:{symbolId} at every:{Static.IntervalTypeYear_Interval} seconds.");

                        continue;
                    }

                    if (interval == Static.LastIntervalType.Year)
                    {
                        new Info(new BaseSelector(),
                            $"BuildLastInterval... [{symbolPos}/{symbolsCount}] for symbolId:{symbolId} at every:{Static.IntervalTypeYear_Interval} seconds.");
                        await BuildLastInterval(symbolId, existentTickYear, existentTickMonth, lastMonthSecond,
                            lastYearSecond, allMonthlyTicks, Static.LastIntervalType.Year);
                    }

                    // Month interval will also be updated on year, because we already have the data we need.
                    new Info(new BaseSelector(),
                        $"BuildLastInterval... [{symbolPos}/{symbolsCount}] for symbolId:{symbolId} at every:{Static.IntervalTypeMonth_Interval} seconds.");
                    await BuildLastInterval(symbolId, existentTickYear, existentTickMonth, lastMonthSecond,
                        lastYearSecond, allMonthlyTicks,
                        Static.LastIntervalType
                            .Month); // Existent month has 19 April 2021 09:30:00 = 161882460 to 162083232 = 12 May 2021 15:12:00
                }
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

        private async Task BuildLastInterval(int symbolId, Tick destinationTickYear, Tick destinationTickMonth,
            long lastMonthSecond, long lastYearSecond, List<TickArray> inputTicks, Static.LastIntervalType interval)
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
                var fromSecond_Month = lastMonthSecond > 0 ? lastMonthSecond : oneMonthAgoSeconds; // 12-05-2021

                var yesterdayDate = DateTime.Today.AddDays(0);
                var yesterdaySeconds = Static.SecondsFromDateTime(DateTime.Today.AddDays(0));
                var endingAt = yesterdaySeconds / 10; // Both year and month

                // Aggregate data for YEAR

                if (interval == Static.LastIntervalType.Year)
                {
                    var timeFrameYearTicks = inputTicks
                        .Where(tick => tick.T >= fromSecond_Year / 10 && tick.T < yesterdaySeconds / 10)
                        .ToList(); // Ticks left to aggregate

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
                                timeFrameYearTicks
                                    .Where(tick => tick.T >= startingLeftYearAt && tick.T < nextLeftYearAt).ToList();
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
                    .Where(tick => tick.T >= fromSecond_Month / 10 && tick.T < yesterdaySeconds / 10)
                    .ToList(); // Ticks left to aggregate
                // 12-05-2021 - 07-06-2021 == 13 May 2021 09:30:00 = 162089820 to 1623081540 = 7 June 2021 15:59:00
                var startingLeftMonthAt = fromSecond_Month / 10; // 12 May 2021 
                var nextLeftMonthAt = fromSecond_Month / 10;
                endingAt = yesterdaySeconds / 10; // Both year and month
                var intervalForAllMonth = Static.IntervalTypeMonth_Interval / 10; // Seconds in a hour / 10

                int monthTickPos = 0;
                int monthIntervalsCount = (int) (endingAt - startingLeftMonthAt) / intervalForAllMonth;
                while (startingLeftMonthAt < endingAt)
                {
                    // 7 June 2021 23:00:00 = 1623106800 to 1623106800 = 7 June 2021 23:00:00
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
                            timeFrameMonthTicks.Where(tick => tick.T >= startingLeftMonthAt && tick.T < nextLeftMonthAt)
                                .ToList();
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
                    await UpsertInterval(symbolId, Static.LastIntervalType.Year, destinationTickYear, outputYearTicks, false);
                }


                if (destinationTickMonth != null) // Existent month (destinationTickMonth) has 19 April 2021 09:30:00 = 161882460 to 162083232 = 12 May 2021 15:12:00
                {
                    var existentTick = destinationTickMonth;
                    var existentTicks = (IEnumerable<TickArray>) JsonConvert.DeserializeObject<IEnumerable<TickArray>>(
                        destinationTickMonth.Serialized);
                    var monthTicks =
                        existentTicks.Where(p => p.T > oneMonthAgoSeconds / 10)
                            .ToList(); // From 10 May 2021 09:30:00 to 7 June 2021 15:54:00 to 12 May 2021 15:12:00 (8 may was Saturday)
                    monthTicks.AddRange(outputMonthTicks); // Add from 13 May 2021 09:30:00
                    outputMonthTicks = monthTicks;
                }

                await UpsertInterval(symbolId, Static.LastIntervalType.Month, destinationTickMonth, outputMonthTicks, false);
               
            }
            catch (Exception ex)
            {
                new Info(new BaseSelector(),
                    $"BuildLastInterval... exception for symbolId:{symbolId} for interval: {interval} message: {ex.Message}");
            }
        }

        private async Task<Tick> UpsertInterval(int symbolId, Static.LastIntervalType interval, Tick destinationTick, List<TickArray> ticks, bool checkIfExists = true)
        {
            var startingDate = Static.IntervalToStartingDate(interval);
            var startingSecond = Static.SecondsFromDateTime(startingDate);

            if (destinationTick == null && checkIfExists)
            {
                destinationTick = _tickRepository.TicksBySymbolIdAndByInterval(interval, symbolId);
            }
            
            if (destinationTick != null)
            {
                var partialSerializedYear = JsonConvert.SerializeObject(ticks);
                var serializedYear = destinationTick.Serialized.Length > 10
                    ? Static.ConcatSerialized(destinationTick.Serialized, partialSerializedYear, true)
                    : partialSerializedYear;
                destinationTick.Updated = Static.Now();
                destinationTick.Serialized = serializedYear;
                destinationTick.Samples = ticks.Count();
                destinationTick.Seconds = startingSecond;
                await _tickRepository.UpdateAsync(destinationTick);
            }
            else
            {
                var serializedYear = JsonConvert.SerializeObject(ticks);
                destinationTick = new Tick()
                {
                    Created = Static.Now(),
                    Date = startingDate,
                    IsMonthly = false,
                    Serialized = serializedYear,
                    Interval = Static.IntervalToSeconds(interval),
                    SymbolId = symbolId,
                    Symbol = Static.SymbolCodeFromId[symbolId],
                    Samples = ticks.Count(),
                    Seconds = startingSecond,
                };
                await _tickRepository.AddAsync(destinationTick);
            }

            return destinationTick;
        }

        #endregion

    }

    public class UploadIntervalModel
    {
        public string Serialized { get; set; }
        public string Code { get; set; }
        public int SymbolId { get; set; }
        public List<TickArray> Ticks { get; set; }
        public Static.LastIntervalType Interval { get; set; }
    }
}