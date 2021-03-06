﻿using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using CsvHelper;
using CsvHelper.Configuration;
using Microsoft.Data.Sqlite;
using Mono.Options;
using Newtonsoft.Json;
using Nexosis.Api.Client;
using Nexosis.Api.Client.Model;

namespace AirQuality
{
    class Program
    {
        // things to do with this program
        enum Action
        {
            Help, // obvious
            Import, // get the data out of a CSV into a sqlite database, clobbers whatever was there
            Preprocess, // this imputes missing values in the data set creating a different table in the sqlite db
            Upload, // saves the data to Nexosis API for later processing
            Forecast, // predict future values using the saved data
            Impact, // look at the impact of past events on the values
            Results, // query the API and save results to the database in a separate table
            List, // print list of items submitted to the API
        }

        static void Main(string[] args)
        {
            var action = Action.Help;
            var sourceFiles = new List<string>();
            var database = string.Empty;
            var dataSetName = string.Empty;
            var impactName = string.Empty;
            var listType = string.Empty;
            var processType = string.Empty;
            var source = string.Empty;
            var interval = string.Empty;
            DateTimeOffset? startDate = null;
            DateTimeOffset? endDate = null;
            Guid? sessionId = null;

            var options = new OptionSet
            {
                { "h|help", "Show this message and exit", v => { action = Action.Help; } },
                { "d|database=", "Set the database name", v => { database = Path.GetFullPath(v); } },
                { "ds|dataset=", "Set the data set name", v => { dataSetName = v; } },
                { "source=", "Set the type of source column for data upload", v => { source = v;  } },
                { "interval=", "Set the interval used to query the data or used to ask for predictions", v => { interval = v;  } },
                { "import", "", v => { action = Action.Import; } },
                { "files=", "Files to use as part of import", v =>
                    {
                        sourceFiles.AddRange(Directory.GetFiles(Path.GetDirectoryName(v), Path.GetFileName(v),
                            SearchOption.TopDirectoryOnly));
                    }
                },
                { "preprocess=", "Run the pre-processing to eliminate invalid values", v => { action = Action.Preprocess; processType = v; } },
                { "upload", "Save the data to the Nexosis API", v => { action = Action.Upload; } },
                { "forecast", "Run forecasting for the period given by the start and end dates.", v => { action = Action.Forecast; } },
                { "impact", "Run impact analysis for period given by the start and end dates. Also requires a name is given.", v => { action = Action.Impact; } },
                { "name=", "Name for the forecast or impact analysis session", v => { impactName = v; } },
                { "results", "Get results. Requires --sessionid to be given.", v => { action = Action.Results; } },
                { "id|sessionid=", "Id from forecast or impact session. Used when querying results.", v => { sessionId = Guid.Parse(v); } },
                { "list=", "", v => { action = Action.List; listType = v; } },
                { "s|start=", "Date and time (ISO 8601 format) to start prediction/analysis." , v => { startDate = DateTimeOffset.Parse(v); } },
                { "e|end=", "Date and time (ISO 8601 format) to end prediction/analysis.", v => { endDate = DateTimeOffset.Parse(v); } }
            };

            try
            {
                options.Parse(args);
            }
            catch (OptionException)
            {
                action = Action.Help;
            }

            switch (action)
            {
                case Action.Help:
                    ShowHelp(options);
                    break;
                case Action.Import:
                    Console.Out.WriteLine($"Importing data into {database}");
                    RunImport(database, sourceFiles);
                    break;
                case Action.Preprocess:
                    Console.Out.WriteLine("Ensuring continuous data");
                    Preprocess(database, processType);
                    break;
                case Action.Upload:
                    Console.Out.WriteLine("Submitting data to Nexosis API");
                    UploadData(database, dataSetName, startDate, endDate, source, interval).GetAwaiter().GetResult();
                    break;
                case Action.Forecast:
                    if (!startDate.HasValue || !endDate.HasValue)
                    {
                        Console.Error.WriteLine("--start and --end must be provided to run a forecast session");
                        ShowHelp(options);
                        break;
                    }
                    Console.Out.WriteLine($"Initiating forecast for {dataSetName} over dates {startDate:O} to {endDate:O}");
                    Forecast(database, dataSetName, startDate.Value, endDate.Value, interval).GetAwaiter().GetResult();
                    break;
                case Action.Impact:
                    if (!startDate.HasValue || !endDate.HasValue || string.IsNullOrWhiteSpace(impactName))
                    {
                        Console.Error.WriteLine("--start, --end, and --name must be provided to run an impact session");
                        ShowHelp(options);
                        break;
                    }
                    Console.Out.WriteLine($"Analyzing impact of event {impactName} on dates {startDate:O} to {endDate:O}");
                    Impact(database, impactName, dataSetName, startDate.Value, endDate.Value, interval).GetAwaiter().GetResult();
                    break;
                case Action.Results:
                    if (!sessionId.HasValue)
                    {
                        Console.Error.WriteLine("--sessionid must be set to get results");
                        ShowHelp(options);
                        break;
                    }
                    Console.Out.WriteLine($"Getting results for session: {sessionId}...");
                    Results(database, sessionId.Value).GetAwaiter().GetResult();
                    break;
                case Action.List:
                    if (string.IsNullOrEmpty(listType))
                    {
                        Console.Error.WriteLine("The type of object to list must be given.");
                        ShowHelp(options);
                        break;
                    }
                    ListItems(listType, database).GetAwaiter().GetResult();
                    break;
                default:
                    ShowHelp(options);
                    break;
            }
        }

        private static void ShowHelp(OptionSet options)
        {
            var console = Console.Error;

            console.WriteLine($"Usage: {Process.GetCurrentProcess().ProcessName} [options]");
            console.WriteLine();
            console.WriteLine("Program to manage data analysis for air quality data.");
            console.WriteLine();
            console.WriteLine("Options:");

            options.WriteOptionDescriptions(console);
        }

        static SqliteConnection OpenDatabase(string database)
        {
            var conn = new SqliteConnection($"Filename={database}");
            conn.Open();

            return conn;
        }

        // loads the files into the local db
        private static void RunImport(string database, IEnumerable<string> files)
        {
            using (var db = OpenDatabase(database))
            {
                SetupImport(db);

                foreach (var file in files)
                {
                    DoImport(db, file);
                }

                db.Close();
            }
        }
        private static void DoImport(SqliteConnection db, string file)
        {
            Console.Out.WriteLine($"Importing file {file},");

            var cst = TimeZoneInfo.FindSystemTimeZoneById("China Standard Time");
            using (var tran = db.BeginTransaction())
            using (var f = File.OpenText(file))
            using (var csv = new CsvReader(f, new CsvConfiguration { IgnoreBlankLines = true, TrimFields = true }))
            {
                // the file has the following layout:
                // Site, Parameter, Date (LST), Year, Month, Day, Hour, Value, Unit, Duration, QC Name
                // 
                // For this implementation, we are pulling the indivdual values for the date and composing them
                // as a China Standard Time date. We are also including the measured value and if it is valid.
                // the rest are not interesting right now

                while (csv.Read())
                {
                    var date = new DateTimeOffset(csv.GetField<int>("Year"), csv.GetField<int>("Month"),
                        csv.GetField<int>("Day"), csv.GetField<int>("Hour"), 0 /* minute */, 0 /* second */,
                        cst.BaseUtcOffset);
                    var value = csv.GetField<int>("Value");
                    var valid = csv.GetField("QC Name").Equals("Valid", StringComparison.OrdinalIgnoreCase);
                    AddMeasurement(db, date, value, valid);
                }
                tran.Commit();
            }
        }

        // imputes values for missing data saving to db
        private static void Preprocess(string database, string processType)
        {
            using (var db = OpenDatabase(database))
            {
                SetupProcessing(db);

                // insert all the good records.
                using (var tran = db.BeginTransaction())
                {
                    CopyValidData(db);
                    tran.Commit();
                }

                // get bad records and create faked values for them
                using (var tran = db.BeginTransaction())
                {
                    var reader = GetInvalidData(db);

                    // could do this with the reader and save the double iteration and some mem
                    // but in this case both are very small
                    var missing = new List<DateTimeOffset>();
                    while (reader.Read())
                    {
                        missing.Add(DateTimeOffset.Parse(reader.GetString(0)));
                    }

                    int start = 0;
                    for (int i = 0; i < missing.Count - 1;)
                    {
                        var interval = missing[i + 1] - missing[i];
                        if (interval.Hours > 1)
                        {
                            i = i + 1;
                            while (start < i)
                            {
                                // should be figuring out some sort of better imputed value here based on preceding and following good values from data
                                // but for now just adding the average of the entire data set
                                AddImputedValue(db, missing[start], 93);
                                start++;
                            }
                        }
                        else
                        {
                            i = i + 1;
                        }
                    }
                    // take care of last value as we bailied on it above
                    while (start < missing.Count)
                    {
                        AddImputedValue(db, missing[start], 93);
                        start++;
                    }

                    tran.Commit();
                }

                db.Close();
            }
        }

        // pulls data from local db and submits to API
        private static async Task UploadData(string database, string dataSetName, DateTimeOffset? startDate, DateTimeOffset? endDate, string source = null, string interval = "hour")
        {
            var columns = new Dictionary<string, ColumnMetadata>
            {
                { "timestamp", new ColumnMetadata { DataType = ColumnType.Date, Role = ColumnRole.Timestamp } },
                { "value", new ColumnMetadata { DataType = ColumnType.Numeric, Role = ColumnRole.Target } }
            };
            var api = new NexosisClient("249c5810fd58403c9e2bfeae423a72fd"); //Environment.GetEnvironmentVariable("NEXOSIS_PROD_KEY"));

            using (var db = OpenDatabase(database))
            {
                var measurements = LoadMeasurements(db, startDate, endDate, source, interval);
                var batchSize = 5000;

                // there is a limit on request size so we batch the data that is to be uploaded
                for (int i = 0; i < ((measurements.Count / batchSize) + 1); i++)
                {
                    var ds = await api.DataSets.Create(
                        DataSet.From(dataSetName,
                        new DataSetDetail { Columns = columns, Data = measurements.Skip(i * batchSize).Take(batchSize).ToList() }
                    ));
                    Console.Out.WriteLine($"Added to data set named {ds.DataSetName}.");
                }
                db.Close();
            }
        }
        private static List<Dictionary<string, string>> LoadMeasurements(SqliteConnection db, DateTimeOffset? startDate, DateTimeOffset? endDate, string source, string interval)
        {
            var measures = new List<Dictionary<string, string>>();

            // get all the data out
            var reader = GetMeasurements(db, startDate, endDate, source, interval);
            while (reader.Read())
            {
                measures.Add(new Dictionary<string, string>
                {
                    { "timestamp", reader.GetString(0) },
                    { "value", reader.GetString(1) }
                });
            }
            return measures;
        }

        // submits a forecast session
        private static async Task Forecast(string database, string dataSetName, DateTimeOffset startDate, DateTimeOffset endDate, string interval = "Day")
        {
            var api = new NexosisClient(Environment.GetEnvironmentVariable("NEXOSIS_PROD_KEY"));
            if (!Enum.TryParse(interval, /*ignoreCase*/ true, out ResultInterval parsedInterval))
            {
                parsedInterval = ResultInterval.Day;
            }
            // given the name of the dataset, the 'column' of the data to predict on and the date range, it is easy to kick it off.
            var foreacstSession = await api.Sessions.CreateForecast(Sessions.Forecast(dataSetName, startDate, endDate, parsedInterval, "value"));

            using (var db = OpenDatabase(database))
            {
                SetupSessionResults(db);
                AddSessionRecord(db, foreacstSession.SessionId, dataSetName, foreacstSession.RequestedDate);
            }

            Console.Out.WriteLine($"Creating hourly forecast on {dataSetName} data from {startDate:O} to {endDate:O}. Session id: {foreacstSession.SessionId}");
        }

        // creates an impact session 
        private static async Task Impact(string database, string impactName, string dataSetName, DateTimeOffset startDate, DateTimeOffset endDate, string interval = "Day")
        {
            var api = new NexosisClient(Environment.GetEnvironmentVariable("NEXOSIS_PROD_KEY"));
            if (!Enum.TryParse(interval, /*ignoreCase*/ true, out ResultInterval parsedInterval))
            {
                parsedInterval = ResultInterval.Day;
            }

            // given the name of the dataset, the 'column' of the data to predict on and the date range, it is easy to kick it off.
            var impactSession = await api.Sessions.AnalyzeImpact(Sessions.Impact(dataSetName, startDate, endDate, parsedInterval, impactName, "value"));

            using (var db = OpenDatabase(database))
            {
                SetupSessionResults(db);
                AddSessionRecord(db, impactSession.SessionId, $"{dataSetName}.{impactName}", impactSession.RequestedDate);
            }

            Console.Out.WriteLine($"Analyzing {parsedInterval.ToString().ToLower()} impact on {dataSetName} data from {startDate:O} to {endDate:O}. Session id: {impactSession.SessionId}");
        }

        // gets results for a session and saves to local db
        private static async Task Results(string database, Guid sessionId)
        {
            var api = new NexosisClient(Environment.GetEnvironmentVariable("NEXOSIS_PROD_KEY"));
            var results = await api.Sessions.GetResults(sessionId);

            // only save results if we actually have them
            if (results.Status != Status.Completed)
            {
                Console.Out.WriteLine($"Unable to get results from session in {results.Status} state.");
                return;
            }

            // save results
            using (var db = OpenDatabase(database))
            {
                // if there is more to save, then do it
                UpdateSession(sessionId, db, results);

                using (var tran = db.BeginTransaction())
                {
                    CleanResults(sessionId, db);
                    foreach (var item in results.Data)
                    {
                        AddResult(sessionId, db, item);
                    }
                    tran.Commit();
                }

                db.Close();
            }
        }

        private static async Task ListItems(string listType, string database)
        {
            var api = new NexosisClient(Environment.GetEnvironmentVariable("NEXOSIS_PROD_KEY"));
            if (listType.Equals("data", StringComparison.OrdinalIgnoreCase))
            {
                var results = await api.DataSets.List();
                foreach (var r in results.Items)
                {
                    Console.Out.WriteLine(r.DataSetName);
                }
            }
            else if (listType.Equals("remote-sessions", StringComparison.OrdinalIgnoreCase))
            {
                var results = await api.Sessions.List();
                Console.Out.WriteLine("SessionId                           \tRequested Date                   \tStart Date                         \tEnd Date                         \tType      \tStatus");
                foreach (var r in results.Items)
                {
                    Console.Out.WriteLine($"{r.SessionId:D}\t{r.RequestedDate:O}\t{r.StartDate:O}\t{r.EndDate:O}\t{r.Type.ToString("G").PadRight(10)}\t{r.Status}");
                }
            }
            else if (listType.Equals("local-sessions", StringComparison.OrdinalIgnoreCase))
            {
                using (var db = OpenDatabase(database))
                using (var sessions = GetSessions(db))
                {
                    Console.Out.WriteLine("SessionId                           \tRequested Date                   \tName");
                    while (sessions.Read())
                    {
                        Console.WriteLine($"{Guid.Parse(sessions.GetString(0)):D}\t{sessions.GetDateTime(2):O}\t{sessions.GetString(1)}");
                    }
                }
            }
        }


        // table schema definition methods 
        private static void SetupImport(SqliteConnection db)
        {
            db.CreateCommand("CREATE TABLE IF NOT EXISTS import (id integer PRIMARY KEY AUTOINCREMENT, timestamp text NOT NULL, value integer NOT NULL, is_valid INTEGER DEFAULT 0)").ExecuteNonQuery();
        }

        private static void SetupProcessing(SqliteConnection db)
        {
            db.CreateCommand("CREATE TABLE IF NOT EXISTS measurements(id integer PRIMARY KEY AUTOINCREMENT, timestamp TEXT NOT NULL, value INTEGER NOT NULL, source TEXT NOT NULL, interval TEXT NOT NULL default 'd')").ExecuteNonQuery();
        }

        private static void SetupSessionResults(SqliteConnection db)
        {
            db.CreateCommand("CREATE TABLE IF NOT EXISTS sessions(id INTEGER PRIMARY KEY, session_id TEXT NOT NULL, name TEXT NOT NULL, session_date TEXT NOT NULL, meta TEXT)").ExecuteNonQuery();
            db.CreateCommand("CREATE TABLE IF NOT EXISTS session_results(session_id INTEGER NOT NULL, timestamp TEXT NOT NULL, value DOUBLE NOT NULL, FOREIGN KEY(session_id) REFERENCES sessions(id))").ExecuteNonQuery();
        }


        // `import` table data access
        private static void AddMeasurement(SqliteConnection db, DateTimeOffset date, int value, bool valid)
        {
            db.CreateCommand(
                "INSERT INTO import VALUES (NULL, @ts, @value, @valid)",
                new SqliteParameter("@ts", date),
                new SqliteParameter("@value", value),
                new SqliteParameter("@valid", valid)
            ).ExecuteNonQuery();
        }

        private static SqliteDataReader GetInvalidData(SqliteConnection db)
        {
            return db.CreateCommand("SELECT timestamp FROM import WHERE is_valid = 0 ORDER BY timestamp").ExecuteReader();
        }

        private static void CopyValidData(SqliteConnection db)
        {
            db.CreateCommand("INSERT INTO measurements SELECT NULL, timestamp, value, 'sensor', 'h' FROM import WHERE is_valid = 1").ExecuteNonQuery();
        }


        // `measurement` table data access 
        private static SqliteDataReader GetMeasurements(SqliteConnection db, DateTimeOffset? startDate, DateTimeOffset? endDate, string source, string interval)
        {
            return db.CreateCommand(
                "SELECT timestamp, value FROM measurements WHERE interval = @interval AND (@source IS NULL OR source = @source) AND timestamp BETWEEN @start AND @end ORDER BY timestamp",
                new SqliteParameter("@interval", interval.Substring(0, 1).ToLowerInvariant()),
                new SqliteParameter("@source", source),
                new SqliteParameter("@start", startDate ?? DateTimeOffset.MinValue),
                new SqliteParameter("@end", endDate ?? DateTimeOffset.MaxValue)).ExecuteReader();
        }

        private static void AddImputedValue(SqliteConnection db, DateTimeOffset date, int value)
        {
            db.CreateCommand(
                "INSERT INTO measurements VALUES(NULL, @ts, @value, 'imputed', 'h')",
                new SqliteParameter("@ts", date.ToString("O")),
                new SqliteParameter("@value", value)
            ).ExecuteNonQuery();
        }


        // `session` table data access
        private static void AddSessionRecord(SqliteConnection db, Guid sessionId, string sessionName, DateTimeOffset date)
        {
            db.CreateCommand("INSERT INTO sessions VALUES(@id, @name, @date, NULL)",
                new SqliteParameter("@id", sessionId.ToString("N")),
                new SqliteParameter("@name", sessionName),
                new SqliteParameter("@date", date)
            ).ExecuteNonQuery();
        }

        private static SqliteDataReader GetSessions(SqliteConnection db)
        {
            return db.CreateCommand("SELECT * FROM sessions").ExecuteReader();
        }
        
        private static void CleanResults(Guid sessionId, SqliteConnection db)
        {
            db.CreateCommand(
                "DELETE FROM session_results WHERE session_id = @id",
                new SqliteParameter("@id", sessionId.ToString("N"))
            ).ExecuteNonQuery();
        }
        
        private static void AddResult(Guid sessionId, SqliteConnection db, Dictionary<string, string> item)
        {
            db.CreateCommand(
                "INSERT INTO session_results VALUES (@id, @ts, @value)",
                new SqliteParameter("@id", sessionId.ToString("N")),
                new SqliteParameter("@ts", item["timestamp"]),
                new SqliteParameter("@value", item["value"])
            ).ExecuteNonQuery();
        }

        private static void UpdateSession(Guid sessionId, SqliteConnection db, SessionResult results)
        {
            db.CreateCommand("UPDATE sessions SET meta = @meta WHERE session_id = @id",
                new SqliteParameter("@meta", JsonConvert.SerializeObject(results.Metrics)),
                new SqliteParameter("@id", sessionId.ToString("N"))
            ).ExecuteNonQuery();
        }

        //to get impact results for olympics
        // SELECT 
        //  m.timestamp, m.value, p.timestamp, p.value 
        // FROM 
        //  measurements m LEFT JOIN session_results p ON datetime(m.timestamp) = datetime(p.timestamp) 
        // WHERE 
        //  m.timestamp BETWEEN "2008-07-01 00:00 +8:00" AND "2008-08-31 00:00 +8:00" ORDER BY m.timestamp;


        // to do the averages
        // INSERT INTO measurements SELECT NULL, date(timestamp), avg(value), 'computed', 'd' FROM measurements GROUP BY date(timestamp);

        // to do the moving averages
        //INSERT INTO measurements
        //SELECT NULL,
        //       moving.ts,
        //       moving.average,
        //       'avg',
        //       'd'
        //FROM
        //    ( 
        //        WITH m (ts,val) AS
        //        (SELECT 
        //            timestamp,
        //            value
        //        FROM 
        //            measurements
        //        WHERE
        //            source = 'computed'
        //            AND   
        //            INTERVAL = 'd'
        //            AND
        //            timestamp BETWEEN date('2008-05-01') AND date('2008-09-30')
        //        ) 
        //        SELECT 
        //            m.ts,
        //            AVG(past.value) AS average 
        //        FROM 
        //            m, 
        //            measurements AS past 
        //        WHERE 
        //            past.source = 'computed' 
        //            AND 
        //            past.interval = 'd' 
        //            AND 
        //            past.timestamp BETWEEN date(m.ts,'-6 days') AND m.ts
        //        GROUP BY 
        //            m.ts, m.val
        //    ) AS moving;

    }

    public static class SqliteConnectionExtensions
    {
        public static SqliteCommand CreateCommand(this SqliteConnection db, string commandText)
        {
            var cmd = db.CreateCommand();
            cmd.CommandText = commandText;
            return cmd;
        }

        public static SqliteCommand CreateCommand(this SqliteConnection db, string commandText, params SqliteParameter[] parameters)
        {
            var cmd = db.CreateCommand();
            cmd.CommandText = commandText;
            cmd.Parameters.AddRange(parameters);
            return cmd;
        }
    }
    
}