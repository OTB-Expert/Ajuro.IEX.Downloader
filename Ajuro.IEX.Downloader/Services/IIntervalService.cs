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
using Ajuro.Security.Controllers.v3;
using Microsoft.AspNetCore.Http.Internal;

namespace Ajuro.IEX.Downloader.Services
{
    public interface IIntervalService
    {
        Task<IEnumerable<string>> CreateIntervalPreviews(Static.LastIntervalType interval);
        IQueryable<Tick> GetIntervalPreview(BaseSelector selector, Static.LastIntervalType intervalType, int[] symbolId = null);
        Task<Tick> UploadInterval(UploadIntervalModel model);
    }

}
