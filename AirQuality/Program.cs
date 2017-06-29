using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using CsvHelper;
using CsvHelper.Configuration;
using Microsoft.Data.Sqlite;
using Mono.Options;
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
            Analyze, // look at the impact of past events on the values
            Results // query the API and save results to the database in a separate table
        }

        static void Main(string[] args)
        {
            var action = Action.Help;
            var sourceFiles = new List<string>();
            var database = string.Empty;
            var dataSetName = string.Empty;
            var impactName = string.Empty;
            var startDate = DateTimeOffset.UtcNow;
            var endDate = DateTimeOffset.UtcNow.AddDays(5);
            var sessionId = Guid.Empty;

            var options = new OptionSet
            {
                { "h|help", "Show this message and exit", v => { action = Action.Help; } },
                { "d|database=", "Set the database name", v => { database = Path.GetFullPath(v); } },
                { "ds|dataset=", "Set the data set name", v => { dataSetName = v; } },
                {
                    "import=", "Import files given as arguments", v =>
                    {
                        action = Action.Import;
                        sourceFiles.AddRange(Directory.GetFiles(Path.GetDirectoryName(v), Path.GetFileName(v),
                            SearchOption.TopDirectoryOnly));
                    }
                },
                { "preprocess", "Run the pre-processing to eliminate invalid values", v => { action = Action.Preprocess; } },
                { "upload", "Save the data to the Nexosis API", v => { action = Action.Upload; } },
                { "forecast", "Run forecasting for the given start and end dates", v => { action = Action.Forecast; } },
                { "analyze", "Run impact analysis for the given start and end dates", v => { action = Action.Analyze; } },
                { "name", "Name for the forecast or impact analysis session", v => { impactName = v; } },
                { "results", "Get results", v => { action = Action.Results; } },
                { "id|sessionid=", "Id from forecast or impact session. Used when querying results.", v => { sessionId = Guid.Parse(v); } },
                { "s|start=", "Date and time (ISO 8601 format) to start prediction/analysis. Default value is current time in UTC.", v => { startDate = DateTimeOffset.Parse(v); } },
                { "e|end=", "Date and time (ISO 8601 format) to end prediction/analysis. Default value is current time in UTC +5 days.", v => { endDate = DateTimeOffset.Parse(v); } }
                
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
                    Console.Out.WriteLine($"Importing data into {database}...");
                    RunImport(database, sourceFiles);
                    Console.Out.WriteLine("...complete.");
                    break;
                case Action.Preprocess:
                    Console.Out.WriteLine("Ensuring continuous data...");
                    Preprocess(database);
                    Console.Out.WriteLine("...complete.");
                    break;
                case Action.Upload:
                    Console.Out.WriteLine("Submitting data to Nexosis API...");
                    UploadData(database, dataSetName).GetAwaiter().GetResult();
                    Console.Out.WriteLine("...complete.");
                    break;
                case Action.Forecast:
                    Console.Out.WriteLine($"Initiating forecast for {dataSetName} over dates {startDate:O} to {endDate:O}");
                    Forecast(dataSetName, startDate, endDate).GetAwaiter().GetResult();
                    break;
                case Action.Analyze:
                    Console.Out.WriteLine($"Analyzing impact of event {impactName} on dates {startDate:O} to {endDate:O}");
                    Impact(impactName, dataSetName, startDate, endDate).GetAwaiter().GetResult();
                    break;
                case Action.Results:
                    Console.Out.WriteLine($"Getting results for session: {sessionId}...");
                    Results(database, sessionId).GetAwaiter().GetResult();
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

        private static void RunImport(string database, IEnumerable<string> files)
        {
            using (var db = OpenDatabase(database))
            {
                SetupImport(db);

                foreach (var file in files) DoImport(db, file);

                db.Close();
            }
        }

        private static void SetupImport(SqliteConnection db)
        {
            db.CreateCommand("DROP TABLE IF EXISTS import").ExecuteNonQuery();
            db.CreateCommand("CREATE TABLE import (id integer PRIMARY KEY AUTOINCREMENT, timestamp text NOT NULL, value integer NOT NULL, is_valid INTEGER DEFAULT 0)").ExecuteNonQuery();
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


                    var cmd = db.CreateCommand(
                        "INSERT INTO import VALUES (NULL, @ts, @value, @valid)",
                        new SqliteParameter("@ts", date),
                        new SqliteParameter("@value", value),
                        new SqliteParameter("@valid", valid));

                    cmd.ExecuteNonQuery();
                }
                tran.Commit();
            }
        }

        private static void Preprocess(string database)
        {
            using (var db = OpenDatabase(database))
            {
                SetupProcessing(db);

                // insert all the good records.
                using (var tran = db.BeginTransaction())
                {
                    db.CreateCommand("INSERT INTO quality_measures SELECT NULL, timestamp, value, 'sensor' FROM import WHERE is_valid = 1").ExecuteNonQuery();
                    tran.Commit();
                }

                // get bad records and create faked values for them
                using (var tran = db.BeginTransaction())
                {
                    var reader = db.CreateCommand("SELECT timestamp FROM import WHERE is_valid = 0 ORDER BY timestamp").ExecuteReader();

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

        private static void SetupProcessing(SqliteConnection db)
        {
            db.CreateCommand("DROP TABLE IF EXISTS quality_measures").ExecuteNonQuery();
            db.CreateCommand("CREATE TABLE quality_measures(id integer PRIMARY KEY AUTOINCREMENT, timestamp text NOT NULL, value integer NOT NULL, source text NOT NULL)").ExecuteNonQuery();
        }

        private static void AddImputedValue(SqliteConnection db, DateTimeOffset date, int value)
        {
            db.CreateCommand(
                "INSERT INTO quality_measures VALUES(NULL, @ts, @value, 'imputed')",
                new SqliteParameter("@ts", date.ToString("O")),
                new SqliteParameter("@value", value)
            ).ExecuteNonQuery();
        }

        private static async Task UploadData(string database, string dataSetName)
        {
            var columns = new Dictionary<string, ColumnMetadata>
            {
                { "timestamp", new ColumnMetadata { DataType = ColumnType.Date, Role = ColumnRole.Timestamp } },
                { "value", new ColumnMetadata { DataType = ColumnType.Numeric, Role = ColumnRole.Target } }
            };
            var api = new NexosisClient(Environment.GetEnvironmentVariable("NEXOSIS_PROD_KEY"));

            using (var db = OpenDatabase(database))
            {
                var measurements = LoadMeasurements(db);

                // breaking it down by year because there is a limit of the request PUT size. utilizing S3 or 
                // another hosted import source would eliminate this hack
                foreach (var year in Enumerable.Range(2008, 10))
                {
                    var ds = await api.DataSets.Create(dataSetName,
                        new DataSetDetail
                        {
                            Columns = columns,
                            Data = measurements.Where(d => DateTimeOffset.Parse(d["timestamp"]).Year == year).ToList()
                        });
                    Console.Out.WriteLine(
                        $"Added to data set named {ds.DataSetName} for {year} costing ${ds.Cost.Amount}.");
                }
                db.Close();
            }
        }

        private static List<Dictionary<string, string>> LoadMeasurements(SqliteConnection db)
        {
            var measures = new List<Dictionary<string, string>>();

            // get all the data out
            var reader = db.CreateCommand("SELECT timestamp, value FROM quality_measures ORDER BY timestamp").ExecuteReader();
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

        private static async Task Forecast(string dataSetName, DateTimeOffset startDate, DateTimeOffset endDate)
        {
            var api = new NexosisClient(Environment.GetEnvironmentVariable("NEXOSIS_PROD_KEY"));

            // given the name of the dataset, the 'column' of the data to predict on and the date range, it is easy to kick it off.
            var foreacstSession = await api.Sessions.CreateForecast(dataSetName, "value", startDate, endDate, ResultInterval.Hour);
            
            Console.Out.WriteLine($"Creating hourly forecast on {dataSetName} data from {startDate:O} to {endDate:O} costing ${foreacstSession.Cost.Amount}. Session id: {foreacstSession.SessionId}");
        }
        
        private static async Task Impact(string impactName, string dataSetName, DateTimeOffset startDate, DateTimeOffset endDate)
        {
            var api = new NexosisClient(Environment.GetEnvironmentVariable("NEXOSIS_PROD_KEY"));

            // given the name of the dataset, the 'column' of the data to predict on and the date range, it is easy to kick it off.
            var foreacstSession = await api.Sessions.AnalyzeImpact(dataSetName, impactName, "value", startDate, endDate, ResultInterval.Hour);
            
            Console.Out.WriteLine($"Analyzing hourly impact on {dataSetName} data from {startDate:O} to {endDate:O} costing ${foreacstSession.Cost.Amount}. Session id: {foreacstSession.SessionId}");
        }


        private static async Task Results(string database, Guid sessionId)
        {
            var api = new NexosisClient(Environment.GetEnvironmentVariable("NEXOSIS_PROD_KEY"));

            // given the name of the dataset, the 'column' of the data to predict on and the date range, it is easy to kick it off.
            var results = await api.Sessions.GetResults(sessionId);

            if (results.Status != SessionStatus.Completed)
            {
                Console.Out.WriteLine($"Unable to get results from session in {results.Status} state.");
                return;
            }

            foreach (var item in results.Data)
            {
                Console.Out.WriteLine(item["timestamp"] + ", " + item["value"]);
            }
            using (var db = OpenDatabase(database))
            {
                SetupForSessionResults(db);
                var tran = db.BeginTransaction();
                foreach (var item in results.Data)
                {
                    var addResults = db.CreateCommand(
                        "INSERT INTO predictions VALUES (@id, @date, @ts, @value)",
                        new SqliteParameter("@id", sessionId.ToString("N")),
                        new SqliteParameter("@date", results.RequestedDate.ToString("O")),
                        new SqliteParameter("@ts", item["timestamp"]),
                        new SqliteParameter("@value", item["value"]));

                    addResults.ExecuteNonQuery();
                }
                tran.Commit();
                
                db.Close(); 
            }
        }

        private static void SetupForSessionResults(SqliteConnection db)
        {
            db.CreateCommand("CREATE TABLE IF NOT EXISTS predictions(session_id text, session_date text, timestamp text, value double)").ExecuteNonQuery();
        }
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