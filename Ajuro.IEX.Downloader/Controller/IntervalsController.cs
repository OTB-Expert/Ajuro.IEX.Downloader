using System.Threading.Tasks;
using Microsoft.Extensions.Configuration;
using System.Collections.Generic;
using System.Linq;
using System;
using System.IO;
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
  public partial class IntervalsController : Controller
  {
    private static IConfiguration Configuration { get; set; }

    private readonly IUserRepository _userRepository;

    private readonly ISymbolRepository _symbolRepository;

    // private readonly IAlertRepository _alertRepository;
    private readonly ITickRepository _tickRepository;

    // private readonly IDailyRepository _dailyRepository;
    private readonly IIntervalService _intervalService;

    // private readonly ILogRepository _logRepository;
    private readonly IResultRepository _resultRepository;

    // private string PageToken { get; set; }

    public IntervalsController(
      IUserRepository userRepository,
      ISymbolRepository symbolRepository,
      // IAlertRepository alertRepository,
      ITickRepository stockRepository,
      // IDailyRepository dailyRepository,
      IIntervalService intervalService,
      // ILogRepository logRepository,
      IResultRepository resultRepository
    )
    {
      _userRepository = userRepository;
      _symbolRepository = symbolRepository;
      // _alertRepository = alertRepository;
      _tickRepository = stockRepository;
      _intervalService = intervalService;
      // _logRepository = logRepository;
      _resultRepository = resultRepository;
    }

    [HttpGet("collect/{ids}/last_month")]
    public async Task<IActionResult> CollectMissingMonths(string ids, Static.LastIntervalType intervalType)
    {
      return await CreateIntervalPreviews(ids, Static.LastIntervalType.Month);
    }

    [HttpGet("collect/{ids}/last_year")]
    public async Task<IActionResult> CollectMissingYears(string ids, Static.LastIntervalType intervalType)
    {
      return await CreateIntervalPreviews(ids, Static.LastIntervalType.Year);
    }
    

    [HttpGet("preview/{ids}/interval/{intervalType}/preview")]
    public async Task<IActionResult> CreateIntervalPreviews(string ids, Static.LastIntervalType intervalType)
    {
      var symbolIds = Static.SplitStringIntoInts(ids);
      var tick = _intervalService.GetIntervalPreview(new BaseSelector(), intervalType, symbolIds).ToList();
      return Json(tick);
    }
    

    [HttpGet("move/{date}")]
    public async Task<IActionResult> Move(string date)
    {
      var destDir = $"/home/florin/PRO/Data/Historical/{date}/IEX";
      if (!Directory.Exists(destDir))
      {
        Directory.CreateDirectory(destDir);
      }
      
      // var files = Directory.GetFiles("/home/florin/PRO/Data/Historical/DailySymbolHistory/", $"Intraday_*_{date}*.json");
      var files = Directory.GetFiles("/media/florin/OTB/PRO/Data/Historical/DailySymbolHistory/", $"Intraday_*_{date}*.json");
      // var files = Directory.GetFiles("/media/florin/25613752-0e97-4104-87cf-2aaee599c1c21/home/florin/PRO/Test3/Data/Historical/DailySymbolHistory/", $"Intraday_*_{date}*.json");
      foreach (var filePath in files)
      {
        var fileName = filePath.Substring(filePath.LastIndexOf("/") + 1);
        if (!System.IO.File.Exists($"{destDir}/{fileName}"))
        {
          System.IO.File.Copy(filePath, destDir + "/" + fileName);
        }
      }
      return Json(files.Length);
    }

    [HttpGet("create/year/interval")]
    public async Task<IActionResult> CreateYearInterval()
    {
      var missingItems = await _intervalService.CreateIntervalPreviews(Static.LastIntervalType.Year);
      return Json(missingItems);
    }

    [HttpGet("create/month/interval")]
    public async Task<IActionResult> CreateMonthInterval()
    {
      var missingItems = await _intervalService.CreateIntervalPreviews(Static.LastIntervalType.Month);
      return Json(missingItems);
    }

    [HttpGet("create/week/interval")]
    public async Task<IActionResult> CreateWeekInterval()
    {
      var missingItems = await _intervalService.CreateIntervalPreviews(Static.LastIntervalType.Week);
      return Json(missingItems);
    }

    [HttpGet("create/day/interval")]
    public async Task<IActionResult> CreateDayInterval()
    {
      // same as  admin/{env}/{token}/create_missing_intervals/{seconds}
      var missingItems = await _intervalService.CreateIntervalPreviews(Static.LastIntervalType.Day);
      return Json(missingItems);
    }

    [HttpPost("upload/{date}/{code}")]
    public async Task<IActionResult> UploadInterval([FromBody] UploadIntervalModel model, DateTime date, string code)
    {
      if (model == null)
      {
        return BadRequest("Unable to deserialize");
      }

      if (model.Ticks == null)
      {
        model.Ticks = JsonConvert.DeserializeObject<List<TickArray>>(model.Serialized);
      }
      
      var selector = new BaseSelector(CommandSource.Endpoint);
      Tick intervalTick = await _intervalService.UploadInterval(model);
      return Json(intervalTick);
    }
  }
}