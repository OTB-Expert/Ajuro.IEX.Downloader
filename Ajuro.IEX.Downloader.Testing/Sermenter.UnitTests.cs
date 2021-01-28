using System.Collections.Generic;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System.Linq;
using Ajuro.Net.Types.Stock.Models;
using Ajuro.Net.Testing;
using Ajuro.IEX.Downloader.Services;
using System;

namespace Ajuro.Core.Testing
{
    [TestClass]
    public class Segmenter_UnitTest : TestingResources
    {
        public List<UseCase> allUseCases { get; set; }
        private List<UseCase> failedUseCases { get; set; }
        private DownloaderService downloaderService { get; set; }

        public Segmenter_UnitTest()
        {
            var optionsBuilder = new Microsoft.EntityFrameworkCore.DbContextOptionsBuilder();
            base.Initialize();
            Initialize();
        }

        public void Initialize()
        {
            allUseCases = new List<UseCase>();
            downloaderService = new DownloaderService
            (
                UserRepositoryMock.Object,
                SymbolRepositoryMock.Object,
                AlertRepositoryMock.Object,
                DailyRepositoryMock.Object,
                TickRepositoryMock.Object,
                EndpointRepositoryMock.Object,
                LogRepositoryMock.Object,
                ResultRepositoryMock.Object
            );

            downloaderService.SetOptions(new DownloaderOptions()
            {
                DailySymbolHistoryFolder = "TestData\\DailySymbolHistory",
                MonthlyParsedFiles = "TestData\\SymbolHistory",
                LargeResultsFolder = "TestData\\LargeResults"
            });
        }

        [TestMethod]
        public void Can_Generate_Segments()
        {
            BaseSelector selector = new BaseSelector(CommandSource.UnitTesting);
            allUseCases = new List<UseCase>()
            {
                new UseCase()
                {
                    Name = "Create segments, with basic settings",
                    ForcedResults =
                    {
                        // ForcedResult.ThrowsException
                    },
                    ExpectedMessage = string.Empty,
                },
            };

            LoggingObjectsItems.Clear();
            var result = downloaderService.CreateFragmentsFromFiles(selector, new ResultSelector()            
            // var result = downloaderService.CreateFragmentsFromDb(new BaseSelector(CommandSource.UnitTesting), new ResultSelector()
            {
                From = DateTime.MinValue,
                To = DateTime.MaxValue,

                Length = 5, // before the margin
                Margin = 3, // Before the lost
                Lost = 10, // Percent to loose

                SymbolId = 4,
                TagString = null,
                Replace = false,
                Skip = 0,
                Take = 100,
                Mode = 0 // no overwrite
            }).Result;

            Assert.IsTrue(result.Count() == 3, "Unexpected number of ticks for symbol");
            Assert.IsTrue(result.First().Pick.V == 1000, "Unexpected Pick value");
            Assert.IsTrue(result.First().Entry.V == 970, "Unexpected Entry value");
            Assert.IsTrue(result.First().Margin.V == 930, "Unexpected Margin value");
            Assert.IsTrue(result.First().Lost.V == 900, "Unexpected Lost value");
            Assert.IsTrue(result.First().Min.V <= 900, "Unexpected Min value");
        }

        [TestMethod]
        public void Can_Generate_Segments_From_DB()
        {
            BaseSelector selector = new BaseSelector(CommandSource.UnitTesting);
            allUseCases = new List<UseCase>()
            {
                new UseCase()
                {
                    Name = "Create segments, with basic settings",
                    ForcedResults =
                    {
                        // ForcedResult.ThrowsException
                    },
                    ExpectedMessage = string.Empty,
                },
            };

            LoggingObjectsItems.Clear();
            // var result = downloaderService.CreateFragmentsFromFiles(new BaseSelector(CommandSource.UnitTesting), new ResultSelector()            
            var result = downloaderService.CreateFragmentsFromDb(new BaseSelector(CommandSource.UnitTesting), new ResultSelector()
            {
                From = DateTime.MinValue,
                To = DateTime.MaxValue,

                Length = 5, // before the margin
                Margin = 3, // Before the lost
                Lost = 5, // Percent to loose

                SymbolId = 5,
                TagString = null,
                Replace = false,
                Skip = 0,
                Take = 100,
                Mode = 0 // no overwrite
            }).Result;

            Assert.IsTrue(result.Count() == 7, "Unexpected number of ticks for symbol");
            Assert.IsTrue(result.First().Pick.V == 47860, "Unexpected Pick value");
            Assert.IsTrue(result.First().Entry.V == 46450, "Unexpected Entry value");
            Assert.IsTrue(result.First().Margin.V == 47580, "Unexpected Margin value");
            Assert.IsTrue(result.First().Lost.V == 45000, "Unexpected Lost value");
            Assert.IsTrue(result.First().Min.V <= 45000, "Unexpected Min value");
        }


        [TestMethod]
        public void Can_Aggregate_Files()
        {
            BaseSelector selector = new BaseSelector(CommandSource.UnitTesting);
            allUseCases = new List<UseCase>()
            {
                new UseCase()
                {
                    Name = "CurrentPrice going under BuyAnchor will not change BuyAnchor",
                    ForcedResults =
                    {
                        // ForcedResult.ThrowsException
                    },
                    ExpectedMessage = string.Empty,
                },
            };

            LoggingObjectsItems.Clear();
            var result = downloaderService.GetAllHistoricalFromDb(selector, true).Result;

            var aaplSummary = result.FirstOrDefault(p => p.Symbol == "AAPL");
            Assert.IsTrue(aaplSummary.Samples == 9, "Unexpected number of ticks for symbol");
            // [[1577784600000,289.86],[1577784660000,289.809],[1577784720000,290.494],[1577957400000,296.083],[1577957460000,295.587],[1577957520000,295.483],[1578043800000,297.102],[1578043860000,298.307],[1578043920000,298.861]]         
        }
    }
}
