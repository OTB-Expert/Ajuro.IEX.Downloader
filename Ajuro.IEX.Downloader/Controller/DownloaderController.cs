using System.Threading.Tasks;
using Microsoft.Extensions.Configuration;
using System.Collections.Generic;
using System.Linq;
using System;
using Ajuro.IEX.Downloader.Services;
using Newtonsoft.Json;
using Ajuro.IEX.Downloader.Models;
using Ajuro.Net.Types.Stock.Models;
using Ajuro.Net.Stock.Repositories;
using Ajuro.Net.Types.Stock;
using Microsoft.AspNetCore.Mvc;

namespace Ajuro.Security.Controllers.v3
{
  [Produces("application/json")]
  [Route("api/[controller]")]
  public partial class DownloaderController : Controller
  {
    private static IConfiguration Configuration { get; set; }

    private readonly IUserRepository _userRepository;

    private readonly ISymbolRepository _symbolRepository;

    // private readonly IAlertRepository _alertRepository;
    private readonly ITickRepository _tickRepository;

    // private readonly IDailyRepository _dailyRepository;
    private readonly IDownloaderService _downloaderService;

    // private readonly ILogRepository _logRepository;
    private readonly IResultRepository _resultRepository;

    // private string PageToken { get; set; }

    public DownloaderController(
      IUserRepository userRepository,
      ISymbolRepository symbolRepository,
      // IAlertRepository alertRepository,
      ITickRepository stockRepository,
      // IDailyRepository dailyRepository,
      IDownloaderService downloaderService,
      // ILogRepository logRepository,
      IResultRepository resultRepository
    )
    {
      _userRepository = userRepository;
      _symbolRepository = symbolRepository;
      // _alertRepository = alertRepository;
      _tickRepository = stockRepository;
      // _dailyRepository = dailyRepository;
      _downloaderService = downloaderService;
      // _logRepository = logRepository;
      _resultRepository = resultRepository;

      _downloaderService.SetOptions(new DownloaderOptions()
      {
        ReleaseConfiguration_Allow_WriteFiles = false,
        DebugConfiguration_Allow_WriteFiles = false,
        ReleaseConfiguration_Allow_OverwriteFiles = false,
        DebugConfiguration_Allow_OverwriteFiles = false,
#if DEBUG
        FallbackToFiles = true
#endif
      });
    }

    #region FRAGMENTS

    [HttpGet("fragments/{symbolId}/{length}/{lost}/{from}/{to}/{save}/{margin}/{skip}/{take}")]
    public async Task<IEnumerable<Sample>> GetFragmentsGet(string from, string to, int symbolId, int length, int lost,
      double save, int margin, int skip, int take)
    {
      var selector = new BaseSelector(CommandSource.Endpoint);

      return await _downloaderService.CreateFragmentsFromDb(selector, new ResultSelector()
      {
        From = DateTime.MinValue,
        To = DateTime.MaxValue,

        Length = length, // before the margin
        Margin = margin, // Before the lost
        Lost = lost, // Percent to loose

        SymbolId = 4,
        TagString = null,
        Replace = false,
        Skip = 0,
        Take = take < 100 ? take : 100,
        Mode = 0 // no overwrite
      });
    }

    #endregion

    [HttpGet("api/[controller]/{symbolId}/collect")]
    public async Task<JsonResult> GetSymbol(int symbolId)
    {
      var selector = new BaseSelector(CommandSource.Endpoint);
      var date = DateTime.UtcNow.Date;
      var symbol = _symbolRepository.All().FirstOrDefault(p => p.SymbolId == symbolId);
      if (symbol != null)
      {
        var result = await _downloaderService.Pool(selector, symbol, date);
        return Json(result);
      }

      return Json(null);
    }

    [HttpPost("download")]
    public async Task<IActionResult> Download(DownloadOptions options)
    {
      var selector = new BaseSelector(CommandSource.Endpoint);
      // var reports = await _downloaderService.Download(selector, options);
      return null; // Content(JsonConvert.SerializeObject(reports), "application/json");
    }

    private ProcessType[] IsMonthly = new ProcessType[] {ProcessType.RF_MONTHLY_SUMMARIES, ProcessType.RF_UPLOAD_MONTHLY};

    [HttpGet(
      "code/{code}/from/{fromDate}/take/{take}/source/{source}/replaceDestination/{replaceDestinationIfExists}/backward/{isBackward}/on/allForMonth/processType/{processType}")]
    public async Task<IActionResult> AllForMonth(string code, DateTime fromDate, int take, DataSourceType source,
      ProcessType processType, bool replaceDestinationIfExists, bool isBackward)
    {
      var selector = new BaseSelector(CommandSource.Endpoint);
      ReportingOptions reportingOptions = new ReportingOptions()
      {
        Source = source,
        FromDate = fromDate,
        Take = take,
        Codes = code.Split(',', StringSplitOptions.RemoveEmptyEntries),
        ReplaceDestinationIfExists = replaceDestinationIfExists,
        GoBackward = isBackward,
        ProcessType = processType,
        SkipDailySummaryCaching = replaceDestinationIfExists,
        SkipMonthlySummaryCaching = replaceDestinationIfExists,
        IsMonthly = IsMonthly.Contains(processType) // Will process first day of each month, even if that is in a weekend, because it will contain all the month data aggregated
      };
      var results = await _downloaderService.BulkProcess(selector, reportingOptions, ActionRange.AllForMonth);
      return Json(results);
    }

    [HttpGet(
      "code/{code}/from/{fromDate}/take/{take}/source/{source}/replaceDestination/{replaceDestinationIfExists}/backward/{isBackward}/on/allForAll/processType/{processType}")]
    public async Task<IActionResult> AllForAll(string code, DateTime fromDate, int take, DataSourceType source,
      ProcessType processType, bool replaceDestinationIfExists, bool isBackward)
    {
      var selector = new BaseSelector(CommandSource.Endpoint);
      ReportingOptions reportingOptions = new ReportingOptions()
      {
        Source = source,
        FromDate = fromDate,
        Take = take,
        Codes = code.Split(',', StringSplitOptions.RemoveEmptyEntries),
        ReplaceDestinationIfExists = replaceDestinationIfExists,
        GoBackward = isBackward,
        ProcessType = processType,
      };
      var results = await _downloaderService.BulkProcess(selector, reportingOptions, ActionRange.AllForAll);
      return Json(results);
    }

    [HttpGet(
      "code/{code}/from/{fromDate}/take/{take}/source/{source}/replaceDestination/{replaceDestinationIfExists}/backward/{isBackward}/on/allForDay/processType/{processType}")]
    public async Task<IActionResult> AllForDay(string code, DateTime fromDate, int take, DataSourceType source,
      ProcessType processType, bool replaceDestinationIfExists, bool isBackward)
    {
      var selector = new BaseSelector(CommandSource.Endpoint);
      ReportingOptions reportingOptions = new ReportingOptions()
      {
        Source = source,
        FromDate = fromDate,
        Take = take,
        ReplaceDestinationIfExists = replaceDestinationIfExists,
        GoBackward = isBackward,
        ProcessType = processType,
      };
      var results = await _downloaderService.BulkProcess(selector, reportingOptions, ActionRange.AllForDay);
      return Json(results);
    }

    [HttpGet(
      "code/{code}/from/{selectedDay}/take/{take}/source/{source}/replaceDestination/{replaceDestinationIfExists}/backward/{isBackward}/on/codeForMonth/processType/{processType}")]
    public async Task<IActionResult> CodeForMonth(string code, DateTime selectedDay, int take, DataSourceType source,
      ProcessType processType, bool replaceDestinationIfExists, bool isBackward)
    {
      var selector = new BaseSelector(CommandSource.Endpoint);
      ReportingOptions reportingOptions = new ReportingOptions()
      {
        Source = source,
        FromDate = selectedDay,
        Take = take,
        Code = code,
        ReplaceDestinationIfExists = replaceDestinationIfExists,
        GoBackward = isBackward,
        ProcessType = processType,
      };
      var results = await _downloaderService.BulkProcess(selector, reportingOptions, ActionRange.CodeForMonth);
      return Json(results);
    }

    [HttpGet(
      "code/{code}/from/{selectedDay}/take/{take}/source/{source}/replaceDestination/{replaceDestinationIfExists}/backward/{isBackward}/on/codeForDay/processType/{processType}")]
    public async Task<IActionResult> CodeForDay(string code, DateTime selectedDay, int take, DataSourceType source,
      ProcessType processType, bool replaceDestinationIfExists, bool isBackward)
    {
      var selector = new BaseSelector(CommandSource.Endpoint);
      ReportingOptions reportingOptions = new ReportingOptions()
      {
        Source = source,
        FromDate = selectedDay,
        Take = take,
        Code = code,
        ReplaceDestinationIfExists = replaceDestinationIfExists,
        GoBackward = isBackward,
        ProcessType = processType,
      };
      var results = await _downloaderService.BulkProcess(selector, reportingOptions, ActionRange.CodeForDay);
      return Json(results);
    }

    [HttpGet(
      "code/{codes}/from/{selectedDay}/take/{take}/source/{source}/replaceDestination/{replaceDestinationIfExists}/backward/{isBackward}/on/codeForMissingDays/processType/{processType}")]
    public async Task<IActionResult> CodeForMissingDays(string code, DateTime selectedDay, int take,
      DataSourceType source,
      ProcessType processType, bool replaceDestinationIfExists, bool isBackward)
    {
      var selector = new BaseSelector(CommandSource.Endpoint);
      ReportingOptions reportingOptions = new ReportingOptions()
      {
        Source = source,
        FromDate = selectedDay,
        Code = code,
        Take = take,
        // ReplaceDestinationIfExists = replaceDestinationIfExists,
        GoBackward = isBackward,
        ProcessType = processType,
      };
      var results = await _downloaderService.BulkProcess(selector, reportingOptions, ActionRange.CodeForDay);
      return Json(results);
    }

    #region REPORTING

    /* ===== --- ===== --- ===== */

    /*static*/
    DateTime _lastDate = DateTime.MinValue;

    /*static*/
    int _lastSymbol;

    [HttpGet("code/{code}/from/{fromDate}/take/{take}/source/{source}/list/files/content")]
    public async Task<object> ListFiles_WithContent_PerCode_OnTheGivenMonth(DateTime fromDate, int take, string code,
      DataSourceType source)
    {
      var selector = new BaseSelector(CommandSource.Endpoint);
      ReportingOptions reportingOptions = new ReportingOptions()
      {
        Source = source,
        FromDate = fromDate,
        Take = take,
        Code = code,
      };
      return await _downloaderService.ListFiles_WithContent_PerCode_OnTheGivenMonth(selector, reportingOptions);
    }

    // "downloader/from/2020-03-01/source/file/avoidReadingFilesContent/true/count/files"
    [HttpGet(
      "source/{source}/useMonthlySummaryCaching/{skipMonthlySummaryCaching}/avoidReadingFilesContent/{avoidReadingFilesContent}/count/files")]
    public async Task<object> CountFiles(DateTime fromDate, DataSourceType source, bool avoidReadingFilesContent,
      bool skipMonthlySummaryCaching)
    {
      var selector = new BaseSelector(CommandSource.Endpoint);
      ReportingOptions reportingOptions = new ReportingOptions()
      {
        Source = source,
        FromDate = fromDate,
        // SkipMonthlySummaryCaching = skipMonthlySummaryCaching, // Will sum from CountHistoricalFiles instead of counting 
        AvoidReadingFilesContent = avoidReadingFilesContent
      };
      // return await _downloaderService.CountFiles(selector, reportingOptions);
      return await _downloaderService.RF_MONTHLY_SUMMARIES_From_CountHistoricalFiles(selector, reportingOptions);
    }

    [HttpGet("from/{fromDate}/source/{source}/count/files")]
    public async Task<object> CountFiles_PerCode_OnTheGivenMonth(DateTime fromDate, DataSourceType source)
    {
      var selector = new BaseSelector(CommandSource.Endpoint);
      ReportingOptions reportingOptions = new ReportingOptions()
      {
        Source = source,
        FromDate = fromDate,
      };
      return await _downloaderService.RF_MONTHLY_SUMMARIES_From_CountHistoricalFiles(selector, reportingOptions);
    }

    [HttpGet("{daysBefore}/database/downloads")]
    public async Task<object> GetCompletion([FromQuery] string action, int daysBefore)
    {
      var selector = new BaseSelector(CommandSource.Endpoint);
      /*
      var intervals = _tickRepository.All().ToList();
      int counter = intervals.Count();
      foreach (var interval in intervals)
      {
        counter--;
        if (interval.Seconds == 0) {
          interval.Seconds = (new DateTimeOffset(interval.Date)).ToUnixTimeSeconds();
          await _tickRepository.UpdateAsync(interval);
        }
      }
      */

      var existentResult = _resultRepository.GetAllByKey("DownloadsReport").FirstOrDefault();

      if (existentResult != null && string.IsNullOrEmpty(action))
      {
        var data = JsonConvert.DeserializeObject<List<DownloadIntradayReport>>(existentResult.TagString);
        return data;
      }
      /*
      List<long> times = new List<long>();
      for(int i=0; i < 365; i++){
        long seconds = (new DateTimeOffset(System.DateTime.UtcNow.Date.AddDays(-i).ToLocalTime())).ToUnixTimeSeconds();
        times.Add(seconds);
      }
      */


      var logEntryBreakdown = new LogEntryBreakdown("DownloadsReport");
      var ticks = _tickRepository
        .GetAll()
        .Where(p => p.Seconds > 0 && p.SymbolId != 0 && (new[] {1, 17, 3, 5}).Contains(p.SymbolId))
        .GroupBy(p => p.SymbolId)
        .OrderByDescending(p => p.Count())
        // .Take(5)
        .Select(p => new DownloadIntradayReport()
          {
            SymbolId = p.Key,
            Code = p.Min(x => x.Symbol),
            From = p.Min(x => x.Date),
            To = p.Max(x => x.Date),
            Count = p.Count(),
            Details = new List<IntradayDetail>()
          }
        );
      var tl = ticks.ToList();
      foreach (var symbol in tl)
      {
        // for (int i = 0; i < 365; i++)
        {
          // var seconds = (new DateTimeOffset(DateTime.Now.Date.AddDays(-i))).ToUnixTimeSeconds();
          // if((new DateTimeOffset(symbol.From)).ToUnixTimeSeconds() <= seconds && (new DateTimeOffset(symbol.To)).ToUnixTimeSeconds() >= seconds)
          {
            var records = _tickRepository
              // .GetByDayAndSymbolId(symbol.SymbolId, seconds)
              .GetByRangeAndSymbolId(symbol.SymbolId, (new DateTimeOffset(symbol.From)).ToUnixTimeSeconds(),
                (new DateTimeOffset(symbol.To)).ToUnixTimeSeconds())
              .GroupBy(p => p.Seconds)
              .Select(p => new IntradayDetail()
              {
                Samples = p.Sum(t => t.Samples),
                Count = p.Count(),
                Seconds = p.Min(t => t.Seconds)
              });
            foreach (var detail in records)
            {
              symbol.Details.Add(detail);
            }
          }
        }
      }

      logEntryBreakdown.EndTime = DateTimeOffset.UtcNow.ToUnixTimeSeconds();
      if (existentResult != null)
      {
        existentResult.StartTime = logEntryBreakdown.StartTime;
        existentResult.EndTime = logEntryBreakdown.EndTime;
        existentResult.Tag = logEntryBreakdown;
        existentResult.TagString = JsonConvert.SerializeObject(tl);
        await _resultRepository.UpdateAsync(existentResult);
      }
      else
      {
        await _resultRepository.AddAsync(new Result(selector)
        {
          StartTime = logEntryBreakdown.StartTime,
          EndTime = logEntryBreakdown.EndTime,
          Tag = logEntryBreakdown,
          TagString = JsonConvert.SerializeObject(tl),
          Key = logEntryBreakdown.Indicator,
          User = null
        });
      }

      return tl;
    }

    #endregion

#if DEBUG

    [HttpGet("aggregate/fromDb/{replace}")]
    public async Task<List<Tick>> AggregateFromDb(bool fromFiles, bool replace)
    {
      var selector = new BaseSelector(CommandSource.Endpoint);
      var reports = await _downloaderService.GetAllHistoricalFromDb(selector, replace);
      return reports;
    }

    [HttpGet("aggregate/fromFiles/{replace}")]
    public async Task<Dictionary<int, object[][]>> AggregateFromFiles(bool fromFiles, bool replace)
    {
      var selector = new BaseSelector(CommandSource.Endpoint);
      var reports = await _downloaderService.GetAllHistoricalFromFiles(selector, replace);
      return reports;
    }

    [HttpGet("{symbolId}/collect/{date}")]
    public async Task<JsonResult> GetSymbolsLookup(int symbolId, DateTime date)
    {
      var selector = new BaseSelector(CommandSource.Endpoint);
      var symbol = _symbolRepository.GetAll().FirstOrDefault(p => p.SymbolId == symbolId);
      StockReport report = null;
      if (symbol != null)
      {
        report = await _downloaderService.FetchDate(selector, symbol, date);
      }

      return Json(report);
    }

    [HttpGet("collect/historical/{skip}/{days}/{fromFiles}")]
    public async Task<List<StockReport>> CollectHistoricalData(int days, int skip, int fromFiles)
    {
      var selector = new BaseSelector(CommandSource.Endpoint);
      var dates = new List<DateTime>();
      for (var i = days; i >= skip; i--)
      {
        dates.Add(DateTime.UtcNow.Date.AddDays(-i));
      }

      var left = days;
      List<StockReport> reports = new List<StockReport>();
      var symbols = _symbolRepository.GetAll().Where(symbol => Static.SP500.Contains(symbol.Code)).ToList();
      int symbolIndex = symbols.Count;
      if (fromFiles < 1)
      {
        foreach (var date in dates)
        {
          if (_lastDate > date)
          {
            continue;
          }

          if (date > _lastDate)
          {
            _lastDate = date;
            _lastSymbol = 0;
          }

          left--;
          if (date.DayOfWeek == DayOfWeek.Saturday || date.DayOfWeek == DayOfWeek.Sunday)
          {
            continue;
          }

          for (int index = 0; index < symbols.Count; index++)
          {
            if (_lastDate > date)
            {
              break;
            }

            if (_lastSymbol >= index)
            {
              continue;
            }

            if (index > _lastSymbol)
            {
              _lastSymbol = index;
            }

            var symbol = symbols[index];
            if (!Static.SP500.Contains(symbol.Code))
            {
              continue;
            }

            symbolIndex--;
            Console.WriteLine("Downloading... " + date.ToShortDateString() + " " + index);
            // StockReport report = null;
            // var stringData = 
            await _downloaderService.DownloadCodeForDay(selector, new DownloadOptions()
            {
              SymbolIds = new[] {symbol.SymbolId},
              Dates = new[] {date},
            });
          }
        }
      }

      symbolIndex = symbols.Count;
      // await _downloaderService.GetAllHistorical(false); // Cascade to next action
      return reports;
    }
#endif

    [HttpPost("fragments/{token}/{mode}")]
    public async Task<IEnumerable<Sample>> GetFragmentsPost([FromBody] ResultSelector resultSelector, string token,
      int mode)
    {
      var selector = new BaseSelector(CommandSource.Endpoint) {User = await _userRepository.GetByTokenAsync(token)};
      return await _downloaderService.CreateFragmentsFromDb(selector, resultSelector);
    }


    [HttpGet("pool")]
    public async Task<IActionResult> Pool()
    {
      var selector = new BaseSelector(CommandSource.Endpoint);
      var symbols = await _downloaderService.Pool(selector);

      var endpoint = new StockEndpoint()
      {
#if DEBUG
        Description = "Debug CTRL",
#else
        Description = "Prod CTRL",
#endif
        Url = "https://localhost:5000/api/symbol/{symbolId:1}/collect/{date:2020-03-26}",
        Updated = DateTimeOffset.UtcNow.ToUnixTimeSeconds(),
        Name = "pool",
        Action = symbols != null ? JsonConvert.SerializeObject(symbols) : null
      };

      return Json(symbols);
    }

    [HttpGet("symbol/{symbolId}/pool/{date}")]
    public async Task<JsonResult> GetSymbolsOnDate(int symbolId, DateTime date)
    {
      var selector = new BaseSelector(CommandSource.Endpoint);
      if (date == DateTime.MinValue)
      {
        return Json(null);
      }

      // var l = _symbolRepository.All().Where(p => p.Name.Contains("oeing")).ToList();
      var symbol = _symbolRepository.All().FirstOrDefault(p => p.SymbolId == symbolId);
      if (symbol != null)
      {
        var result = await _downloaderService.Pool(selector, symbol, date);
        return Json(result);
      }

      return Json(null);
    }

    [HttpGet("collect/{i}")]
    public async Task<IActionResult> Collect(int i)
    {
      var selector = new BaseSelector(CommandSource.Endpoint);
      var symbols = _symbolRepository.GetAllActive().Select(p => p).ToList();
      List<StockReport> reports = new List<StockReport>();
      foreach (var symbol in symbols)
      {
        var report = await _downloaderService.FetchLast(selector, symbol, i);
        if (report != null)
        {
          reports.AddRange(report);
        }
      }

      return Json(reports);
    }

    [HttpGet("collect/{date}")]
    public async Task<JsonResult> GetSymbolsOnDate(string date)
    {
      var selector = new BaseSelector(CommandSource.Endpoint);
      DateTime collectionDate = DateTime.MinValue;
      DateTime.TryParse(date, out collectionDate);
      var symbols = _symbolRepository.GetAllActive().ToList();
      // Tick tick = null;
      var items = 0;
      foreach (var symbol in symbols)
      {
        var result = await _downloaderService.Pool(selector, symbol, collectionDate);
        System.Threading.Thread.Sleep(500);
        items += 1;
      }

      return Json(items);
    }

  }
}