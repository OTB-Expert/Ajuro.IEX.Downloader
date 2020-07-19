[Ajuro.Net.Types](https://otb-expert.github.io/Ajuro.Net.Types/)

# Ajuro.IEX.Downloader
	

[NuGet](https://www.nuget.org/packages/Ajuro.IEX.Downloader) | [GitHub](https://github.com/OTB-Expert/Ajuro.IEX.Downloader) | [Docs](https://profimedica.github.io/Ajuro.IEX.Downloader)

-------


## Quick Start

![Overview diagram](https://otb.expert/projects/ajuro.iex.downloader/overview.png)


**Download Intraday Data**

Historical data can be downloaded day-by-day using this endpoint: 
https://iexcloud.io/docs/api/#intraday-prices


## Developer Guide

There are 3 steps.

- Step 1) Download, 
- Step 2) Join, 
- Step 3) Aggregate

There are 2 save options

- Save to DB
- Save to Files

There are 3 dateRange Options

- { startDate: MinDate, endDate: MinDate, take: ignored } - Process all history except today
- { startDate: GivenDate, endDate: MinDate, take: ignored } - Process all from and including GivenDate
- { startDate: MinDate, endDate: GivenDate, take: ignored } - Process all before and including GivenDate
- { startDate: any, endDate: any, take: value } - Creadte the missing date from the GivenDate by adding value Days. 0 is ignored.

   Composing Date options: 

      { startDate: null, endDate: null, take: 1 } = [Today] 
      { startDate: null, endDate: null, take: -1 } = [(Today-1)] - Last n-days except today
      { startDate: null, endDate: null, take: -5 } = [(Today-5) - (Today-1)] - Last n-days except today
      { startDate: null, endDate: null, take: 0 } = [minDate - (Today-1)] - All history except today
      { startDate: 2020-01-01, endDate: null, take: 1 } = [2020-01-01] 
      { startDate: null, endDate: 2020-01-01, take: 1 } = [2020-01-01] 
      { startDate: 2020-01-01, endDate: null, take: +5 } = [2020-01-01 - 2020-01-05] 
      { startDate: null, endDate: 2020-01-05, take: -5 } = [2020-01-01 - 2020-01-05] 
      { startDate: 2020-01-01, endDate: 2020-01-05, take: 0 } = [2020-01-01 - 2020-01-05] 
      { startDate: 2020-01-01, endDate: null, take: 0 } = [minDate - 2020-01-01]  - Here minDate will be provided in class options
      { startDate: null, endDate: 2020-01-01, take: 0 } = [2020-01-01 - (Today-1)]  - Here maxDate will be provided in class options
      { startDate: 2020-01-01, endDate: null, take: 1 } = [2020-01-01] 

There are 6 source-target-action options (DB options are not for step 3 aggregate):

**ifMissing** = return existing or process and return the new record
 
Step 1) Download options:

- download_toFile // ifMissing
- download_toFile_overwrite
- download_toDb // ifMissing
- download_toDb_overwrite

Step 2) Join options:

- fromFile_toFile // ifMissing
- fromFile_toFile_overwrite
- fromFile_toDb // ifMissing
- fromFile_toDb_overwrite
- fromDb_toDb // ifMissing
- fromDb_toDb_overwrite

Step 3) Aggregate options:

- affregate_fromFile // ifMissing
- affregate_fromFile_overwrite
- affregate_fromDb // ifMissing
- affregate_fromDb_overwrite

  Composing asve options:

	{
		download: fromFile_toFile_ifMissing, 
		join: fromFile_toFile_ifMissing, 
		aggregate: fromFile_overwrite
	}

![Overview diagram](https://otb.expert/projects/ajuro.iex.downloader/flow.png)

**Downloader controller**

OTB Expert downloads page:

http://otb.expert/Ajuro.Web.JS/src/#page-downloads

Endpoint:

https://otb.expert/api/downloader/file/downloads

Daily downloads completness:

![Overview diagram](https://otb.expert/projects/ajuro.iex.downloader/page-downloads.png)

##### DownloadIntradayReport

- SymbolId - local symbolId associated with the IEX symbol.
- Code - symbol code used by all brockers.
- From - the date of the oldest record (hour is omitted).
- To - the date of the most recent record (hour is omitted).
- Count - number of days collected.
- Details - see IntradayDetail.

code:

    public class DownloadIntradayReport
    {
        public int SymbolId { get; set; }
        public string Code { get; set; }
        public DateTime From { get; set; }
        public DateTime To { get; set; }
        public int Count { get; set; }
        public List<IntradayDetail> Details { get; set; }
    }


##### IntradayDetails

- Count - Number of valid ticks in the intraday array (null or zero values are not valide)
- Total - Number ticks including the invalid ones. -1 for file reading exception.
- Seconds - Unix timestamp. See https://www.unixtimestamp.com/index.php
- Samples - not used for reports.

code:

    public class IntradayDetail
    {
        public object Samples { get; set; }
        public int Count { get; set; }
        public int Total { get; set; }
        public long Seconds { get; set; }
    }

**Release Notes:**
v 0.0.1: Prerelease version


## To do list:

- Alert on missing intra-day files
- Show last status per symbol (Code, LastIntraday DB/Files, LastProcessed DB/Files)
- UnitTesting - DateTime intervals, Incremental download, incremental processing
- UnitTesting - Email alert on missing intraday

------------