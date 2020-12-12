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
    private readonly IAlertRepository _alertRepository;
    private readonly ITickRepository _tickRepository;
    private readonly IDailyRepository _dailyRepository;
    private readonly IDownloaderService _downloaderService;
    private readonly ILogRepository _logRepository;
    private readonly IResultRepository _resultRepository;

    private string PageToken { get; set; }

    public DownloaderController(
      IUserRepository userRepository,
      ISymbolRepository symbolRepository,
      IAlertRepository alertRepository,
      ITickRepository stockRepository,
      IDailyRepository dailyRepository,
      IDownloaderService downloaderService,
      ILogRepository logRepository,
      IResultRepository resultRepository
      )
    {
      _userRepository = userRepository;
      _symbolRepository = symbolRepository;
      _alertRepository = alertRepository;
      _tickRepository = stockRepository;
      _dailyRepository = dailyRepository;
      _downloaderService = downloaderService;
      _logRepository = logRepository;
      _resultRepository = resultRepository;

      _downloaderService.SetOptions(new DownloaderOptions()
      {
        IEX_Token = "pk_aa5cc122f48d4640b41e25e781347d74",
        DailyGraphsFolder = @"C:\PRO\EasyStockData\DailyGraphs",
        DailySymbolHistoryFolder = @"C:\PRO\EasyStockData\DailySymbolHistory",
        SymbolHistoryFolder = @"C:\PRO\EasyStockData\SymbolHistory",
        LargeResultsFolder = @"C:\PRO\EasyStockData\LargeResults",
        ReleaseConfiguration_Allow_WriteFiles = false,
        DebugConfiguration_Allow_WriteFiles = false,
        ReleaseConfiguration_Allow_OverwriteFiles = false,
        DebugConfiguration_Allow_OverwriteFiles = false,
#if DEBUG
        FallbackToFiles = true
#endif
      });
    }

    [HttpGet("fragments/{symbolId}/{length}/{lost}/{from}/{to}/{save}/{margin}/{skip}/{take}")]
    public async Task<IEnumerable<Sample>> GetFragmentsGet(string from, string to, int symbolId, int length, int lost, double save, int margin, int skip, int take)
    {
      var selector = new BaseSelector(CommandSource.Endpoint) { };

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

    [HttpGet("api/[controller]/{symbolId}/collect")]
    public async Task<JsonResult> GetSymbol(int symbolId)
    {
      var selector = new BaseSelector(CommandSource.Endpoint) { };
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
      var selector = new BaseSelector(CommandSource.Endpoint) { };
      var reports = await _downloaderService.Download(selector, options);
      return Content(JsonConvert.SerializeObject(reports), "application/json");
    }


    /* ===== --- ===== --- ===== */

    /*static*/
    DateTime lastDate = DateTime.MinValue;
    /*static*/
    int lastSymbol = 0;

    [HttpGet("{daysBefore}/file/downloads")]
    public async Task<object> CheckDownloadCompletion(int daysBefore)
    {
      var selector = new BaseSelector(CommandSource.Endpoint) { };
      return await _downloaderService.BuildDownloadSummary(selector, false);
    }

    [HttpGet("{daysBefore}/database/downloads")]
    public async Task<object> GetCompletion([FromQuery] string action, int daysBefore)
    {
      var selector = new BaseSelector(CommandSource.Endpoint) { };
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
        .Where(p => p.Seconds > 0 && p.SymbolId != 0 && (new int[] { 1, 17, 3, 5 }).Contains(p.SymbolId))
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
              .GetByRangeAndSymbolId(symbol.SymbolId, (new DateTimeOffset(symbol.From)).ToUnixTimeSeconds(), (new DateTimeOffset(symbol.To)).ToUnixTimeSeconds())
              .GroupBy(p => p.Seconds)
              .Select(p => new IntradayDetail()
              {
                Samples = p.Sum(p => p.Samples),
                Count = p.Count(),
                Seconds = p.Min(p => p.Seconds)
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


#if DEBUG
    [HttpGet("aggregate/fromDb/{replace}")]
    public async Task<List<Tick>> AggregateFromDb(bool fromFiles, bool replace)
    {
      var selector = new BaseSelector(CommandSource.Endpoint) { };
      var reports = await _downloaderService.GetAllHistoricalFromDb(selector, replace);
      return reports;
    }

    [HttpGet("aggregate/fromFiles/{replace}")]
    public async Task<Dictionary<int, object[][]>> AggregateFromFiles(bool fromFiles, bool replace)
    {
      var selector = new BaseSelector(CommandSource.Endpoint) { };
      var reports = await _downloaderService.GetAllHistoricalFromFiles(selector, replace);
      return reports;
    }

    [HttpGet("{symbolId}/collect/{date}")]
    public async Task<JsonResult> GetSymbolsLookup(int symbolId, DateTime date)
    {
      var selector = new BaseSelector(CommandSource.Endpoint) { };
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
      var selector = new BaseSelector(CommandSource.Endpoint) { };
      var dates = new List<DateTime>() { };
      for (var i = days; i >= skip; i--)
      {
        dates.Add(DateTime.UtcNow.Date.AddDays(-i));
      }

      var left = days;
      List<StockReport> reports = new List<StockReport>();
      var symbols = _symbolRepository.GetAll().Where(p => p.Active).ToList();
      int symbolIndex = symbols.Count;
      if (fromFiles < 1)
      {
        foreach (var date in dates)
        {
          if (lastDate > date)
          {
            continue;
          }
          if (date > lastDate)
          {
            lastDate = date;
            lastSymbol = 0;
          }
          left--;
          if (date.DayOfWeek == DayOfWeek.Saturday || date.DayOfWeek == DayOfWeek.Sunday)
          {
            continue;
          }

          for (int symi = 0; symi < symbols.Count; symi++)
          {
            if (lastDate > date)
            {
              break;
            }
            if (lastSymbol >= symi)
            {
              continue;
            }
            if (symi > lastSymbol)
            {
              lastSymbol = symi;
            }
            var symbol = symbols[symi];
            if (!_downloaderService.GetSP500().Contains(symbol.Code))
            {
              continue;
            }
            symbolIndex--;
            Console.WriteLine("Downloading... " + date.ToShortDateString() + " " + symi);
            StockReport report = null;
            if (symbol != null)
            {
              var stringData = await _downloaderService.FetchString(selector, new DownloadOptions()
              {
                SymbolIds = new int[] { symbol.SymbolId },
                Dates = new DateTime[] { date },
                FromFileIfExists = true
              });
            }
          }
        }
      }
      symbolIndex = symbols.Count;
      // await _downloaderService.GetAllHistorical(false); // Cascade to next action
      return reports;
    }
#endif

    [HttpPost("fragments/{token}/{mode}")]
    public async Task<IEnumerable<Sample>> GetFragmentsPost([FromBody] ResultSelector resultSelector, string token, int mode)
    {
      var selector = new BaseSelector(CommandSource.Endpoint) { };
      selector.User = await _userRepository.GetByTokenAsync(token);
      return await _downloaderService.CreateFragmentsFromDb(selector, resultSelector);
    }


    [HttpGet("pool")]
    public async Task<IActionResult> Pool()
    {
      var selector = new BaseSelector(CommandSource.Endpoint) { };
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
      var selector = new BaseSelector(CommandSource.Endpoint) { };
      if (date == DateTime.MinValue)
      {
        return Json(null);
      }
      var l = _symbolRepository.All().Where(p => p.Name.Contains("oeing")).ToList();
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
      var selector = new BaseSelector(CommandSource.Endpoint) { };
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
      var selector = new BaseSelector(CommandSource.Endpoint) { };
      DateTime collectionDate = DateTime.MinValue;
      DateTime.TryParse(date, out collectionDate);
      var symbols = _symbolRepository.GetAllActive().ToList();
      Tick tick = null;
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