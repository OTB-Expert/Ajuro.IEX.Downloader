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
    public partial class DownloaderService : IDownloaderService
    {

        #region FRAGMENTS
        


        public async Task<string> CreateAndSaveSegments(BaseSelector selector, ResultSelector resultSelector)
        {
            var TagString = resultSelector.TagString;
            if (string.IsNullOrEmpty(TagString))
            {
                StringBuilder sb = new StringBuilder();
                var samples = await ListFragments_FromDb(selector, resultSelector);
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
                FilePaths = Directory.GetFiles(downloaderOptions.DailySymbolHistoryFolder, resultSelector.SymbolId > 0 ? symbol.Code + ".json" : "*.json");
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
        public async Task<IEnumerable<Sample>> ListFragments_FromDb(BaseSelector selector, ResultSelector resultSelector)
        {
            resultSelector.SymbolId = 5;
            resultSelector.Take = 100;
            if (resultSelector.Margin == 0)
            {
                resultSelector.Margin = 3;
            }


            bool running = true;
            List<Sample> samples = new List<Sample>();
            var symbol = Static.SymbolsDictionary[resultSelector.SymbolId];
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
                        SymbolId = symbol.SymbolId,
                        Date = Static.ReadableTimespan(values[0].T),
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
}
