using System.Collections.Generic;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System.Linq;
using Ajuro.Net.Types.Stock.Models;
using Ajuro.Net.Types.UnitTest;
using Ajuro.IEX.Downloader.Services;
using System;

namespace Ajuro.Core.Testing
{
    [TestClass]
    public class Downloader_UnitTest: TestingResources
    {
        public List<UseCase> allUseCases { get; set; }
        private List<UseCase> failedUseCases { get; set; }
        private DownloaderService downloaderService { get; set; }

        public Downloader_UnitTest()
        {
            var optionsBuilder = new Microsoft.EntityFrameworkCore.DbContextOptionsBuilder();
            /*optionsBuilder.UseSqlServer("Server = (localdb)\\mssqllocaldb; Database = TestDb; Trusted_Connection = True; ");
            ApplicationDbContext applicationDbContext = new ApplicationDbContext(optionsBuilder.Options);

            // applicationDbContext.Database.EnsureDeleted();
            applicationDbContext.Database.EnsureCreated();*/
            Initialize();
        }

        public void Initialize()
        {
            allUseCases = new List<UseCase>();
            downloaderService = new DownloaderService
            (
                UserRepositoryMock.Object,
                AlpacaAccountRepositoryMock.Object,
                AlpacaOrderRepositoryMock.Object,
                AlpacaPositionRepositoryMock.Object,
                CountryRepositoryMock.Object,
                CurrencyRepositoryMock.Object,
                SymbolRepositoryMock.Object,
                AlertRepositoryMock.Object,
                PositionRepositoryMock.Object,
                DailyRepositoryMock.Object,
                TickRepositoryMock.Object,
                NewsRepositoryMock.Object,
                EndpointRepositoryMock.Object,
                SubscriptionRepositoryMock.Object,
                LogRepositoryMock.Object,
                ResultRepositoryMock.Object
            );

            downloaderService.SetOptions(new DownloaderOptions()
            {
                DailySymbolHistoryFolder = "TestData\\DailySymbolHistory",
                SymbolHistoryFolder = "TestData\\SymbolHistory",
                LargeResultsFolder = "TestData\\LargeResults"
            });
        }

        [TestMethod]
        public void Can_Generate_Downloads_Summary()
        {
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
            var result = downloaderService.BuildDownloadSummary(false).Result;
            Assert.IsTrue(result.Count == 505, "Unexpected number of symbols");
            var aaplSummary = result.FirstOrDefault(p => p.Code == "AAPL");

            Assert.IsTrue(aaplSummary.Count == 4, "Unexpected number of files");
            Assert.IsTrue(aaplSummary.From == DateTime.Parse("31-12-2019"), "Unexpected starting date");
            Assert.IsTrue(aaplSummary.To == DateTime.Parse("03-01-2020"), "Unexpected starting date");

            // All samples are OK
            Assert.IsTrue((int)aaplSummary.Details[0].Samples == 3, "Unexpected number of valid items");
            Assert.IsTrue(aaplSummary.Details[0].Total == 3, "Unexpected number of total items");
            Assert.IsTrue(aaplSummary.Details[0].Seconds == 1577750400, "Unexpected timestamp");
            Assert.IsTrue((int)aaplSummary.Details[0].Samples == aaplSummary.Details[0].Total, "Unexpected to find some invalid items");

            // Json is valid but is an empty array
            Assert.IsTrue((int)aaplSummary.Details[1].Samples == 0, "Unexpected number of valid items");
            Assert.IsTrue(aaplSummary.Details[1].Total == 0, "Unexpected number of total items");
            Assert.IsTrue(aaplSummary.Details[1].Seconds == 1577836800, "Unexpected timestamp");

            // Some values are null or <= 0            
        }


        [TestMethod]
        public void Can_Aggregate_Files()
        {
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
            var result = downloaderService.GetAllHistoricalFromDb(true).Result;

            var aaplSummary = result.FirstOrDefault(p => p.Symbol == "AAPL");
            Assert.IsTrue(aaplSummary.Samples == 9, "Unexpected number of ticks for symbol");
            // [[1577784600000,289.86],[1577784660000,289.809],[1577784720000,290.494],[1577957400000,296.083],[1577957460000,295.587],[1577957520000,295.483],[1578043800000,297.102],[1578043860000,298.307],[1578043920000,298.861]]         
        }
    }

    public class MyDbContextOptions : Microsoft.EntityFrameworkCore.Infrastructure.IDbContextOptions
    {
       public MyDbContextOptions(Microsoft.EntityFrameworkCore.DbContextOptions builder)
        {

        }

        public IEnumerable<Microsoft.EntityFrameworkCore.Infrastructure.IDbContextOptionsExtension> Extensions => null;

        TExtension Microsoft.EntityFrameworkCore.Infrastructure.IDbContextOptions.FindExtension<TExtension>()
        {
            return null;  // throw new NotImplementedException();
        }
    }
}
