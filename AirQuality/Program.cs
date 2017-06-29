using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using CsvHelper;
using CsvHelper.Configuration;
using Microsoft.Data.Sqlite;
using Mono.Options;

namespace AirQuality
{
    class Program
    {
        // things to do with this program
        enum Action
        {
            Help,         // obvious
            Import,       // get the data out of a CSV into a sqlite database, clobbers whatever was there
            Preprocess,   // this imputes missing values in the data set creating a different table in the sqlite db
            Upload,       // saves the data to Nexosis API for later processing
            Forecast,     // predict future values using the saved data
            Analyze,      // look at the impact of past events on the values
            Results       // query the API and save results to the database in a separate table
        }

        static void Main(string[] args)
        {
            var action = Action.Help;
            var sourceFiles = new List<string>();
            var database = String.Empty;
            var startDate = DateTimeOffset.UtcNow;
            var endDate = DateTimeOffset.UtcNow.AddDays(5);

            var options = new OptionSet
            {
                { "h|help", "Show this message and exit", v => { action = Action.Help; } },
                { "d|database=", "Set the database name", v => { database = Path.GetFullPath(v); } },
                {
                    "import=", "Import files given as arguments", v =>
                    {
                        action = Action.Import;
                        sourceFiles.AddRange(Directory.GetFiles(Path.GetDirectoryName(v), Path.GetFileName(v),
                            SearchOption.TopDirectoryOnly));
                    }
                },
                {
                    "preprocess", "Run the pre-processing to eliminate invalid values",
                    v => { action = Action.Preprocess; }
                },
                { "forecast", "Run forecasting for the given start and end dates", v => { action = Action.Forecast; } },
                {
                    "analyze", "Run impact analysis for the given start and end dates", v => { action = Action.Analyze; }
                },
                {
                    "s|start=", "Date and time (ISO 8601 format) to start prediction/analysis",
                    v => { startDate = DateTimeOffset.Parse(v); }
                },
                {
                    "e|end=", "Date and time (ISO 8601 format) to end prediction/analysis",
                    v => { endDate = DateTimeOffset.Parse(v); }
                },
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
                    Console.Out.WriteLine($"Ensuring continuous data...");
                    Preprocess(database);
                    Console.Out.WriteLine("...complete.");
                    break;
                case Action.Upload:
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
            var drop = db.CreateCommand();
            drop.CommandText = "DROP TABLE IF EXISTS import";
            drop.ExecuteNonQuery();

            var create = db.CreateCommand();
            create.CommandText = "CREATE TABLE import (id integer PRIMARY KEY AUTOINCREMENT, timestamp text NOT NULL, value integer NOT NULL, is_valid INTEGER DEFAULT 0)";
            create.ExecuteNonQuery();
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


                    var cmd = db.CreateCommand();
                    cmd.CommandText = "INSERT INTO import VALUES (NULL, @ts, @value, @valid)";
                    cmd.Parameters.Add(new SqliteParameter("@ts", date));
                    cmd.Parameters.Add(new SqliteParameter("@value", value));
                    cmd.Parameters.Add(new SqliteParameter("@valid", valid));

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
                    var insertValid = db.CreateCommand();
                    insertValid.CommandText =
                        "INSERT INTO quality_measures SELECT NULL, timestamp, value, 'sensor' FROM import WHERE is_valid = 1";
                    insertValid.ExecuteNonQuery();

                    tran.Commit();
                }

                // get bad records and create faked values for them
                using (var tran = db.BeginTransaction())
                {
                    var queryInvalid = db.CreateCommand();
                    queryInvalid.CommandText =
                        "SELECT timestamp FROM import WHERE is_valid = 0 ORDER BY timestamp";

                    var reader = queryInvalid.ExecuteReader();

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
                    // take care of last value as we bailied on it
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
            var drop = db.CreateCommand();
            drop.CommandText = "DROP TABLE IF EXISTS quality_measures";
            drop.ExecuteNonQuery();

            var create = db.CreateCommand();
            create.CommandText =
                "CREATE TABLE quality_measures(id integer PRIMARY KEY AUTOINCREMENT, timestamp text NOT NULL, value integer NOT NULL, source text NOT NULL)";
            create.ExecuteNonQuery();
        }
        
        private static void AddImputedValue(SqliteConnection db, DateTimeOffset date, int value)
        {
            var insertValid = db.CreateCommand();
            insertValid.CommandText = "INSERT INTO quality_measures VALUES(NULL, @ts, @value, 'imputed')";
            insertValid.Parameters.Add(new SqliteParameter("@ts", date.ToString("O")));
            insertValid.Parameters.Add(new SqliteParameter("@value", value));
            insertValid.ExecuteNonQuery();
        }

    }
}