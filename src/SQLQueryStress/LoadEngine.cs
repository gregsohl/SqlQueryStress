using Microsoft.Data.SqlClient;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.ComponentModel;
using System.Threading;

namespace SQLQueryStress
{
    internal class LoadEngine
    {
        private static BlockingCollection<QueryOutput> QueryOutInfo;
        private static CancellationTokenSource _backgroundWorkerCTS;
        private readonly bool _collectIoStats;
        private readonly bool _collectTimeStats;
        private readonly List<SqlCommand> _commandPool = new List<SqlCommand>();
        private readonly int _commandTimeout;

        private readonly string _connectionString;
        private readonly bool _forceDataRetrieval;
        private readonly bool _killQueriesOnCancel;
        private readonly int _iterations;
        private readonly string _paramConnectionString;
        private readonly Dictionary<string, string> _paramMappings;
        //private readonly List<Queue<queryOutput>> queryOutInfoPool = new List<Queue<queryOutput>>();        
        private readonly string _paramQuery;
        private readonly string _query;
        private readonly List<Thread> _threadPool = new List<Thread>();
        private readonly int _threads;
		private static int _finishedThreads;
        private int _queryDelay;

        public LoadEngine(string connectionString, string query, int threads, int iterations, string paramQuery,
            Dictionary<string, string> paramMappings, string paramConnectionString, int commandTimeout,
            bool collectIoStats, bool collectTimeStats, bool forceDataRetrieval, bool killQueriesOnCancel, CancellationTokenSource cts)
        {
            //Set the min pool size so that the pool does not have
            //to get allocated in real-time
            var builder = new SqlConnectionStringBuilder(connectionString)
            {
                MinPoolSize = threads,
                MaxPoolSize = threads,
                CurrentLanguage = "English"
            };
            QueryOutInfo = new BlockingCollection<QueryOutput>();
            _connectionString = builder.ConnectionString;
            _query = query;
            _threads = threads;
            _iterations = iterations;
            _paramQuery = paramQuery;
            _paramMappings = paramMappings;
            _paramConnectionString = paramConnectionString;
            _commandTimeout = commandTimeout;
            _collectIoStats = collectIoStats;
            _collectTimeStats = collectTimeStats;
            _forceDataRetrieval = forceDataRetrieval;
            _killQueriesOnCancel = killQueriesOnCancel;
            _backgroundWorkerCTS = cts;
        }

		public static int FinishThread()
		{
			int resultingValue = Interlocked.Increment(ref _finishedThreads);
			return resultingValue;
		}
		public static bool ExecuteCommand(string connectionString, string sql)
        {
            using var conn = new SqlConnection(connectionString);
            conn.Open();
            using var cmd = new SqlCommand(sql, conn);
            cmd.ExecuteNonQuery();
            return true;
        }

        public void StartLoad(BackgroundWorker worker, int queryDelay)
        {
            _queryDelay = queryDelay;

            StartLoad(worker);
        }

        private void StartLoad(BackgroundWorker worker)
        {
            var useParams = false;

            var badParams = new List<string>();
            foreach (var theKey in _paramMappings.Keys)
            {
                if ((_paramMappings[theKey] == null) || (_paramMappings[theKey].Length == 0))
                {
                    badParams.Add(theKey);
                }
            }

            foreach (var theKey in badParams)
            {
                _paramMappings.Remove(theKey);
            }

            //Need some parameters?
            if (_paramMappings.Count > 0)
            {
                ParamServer.Initialize(_paramQuery, _paramConnectionString, _paramMappings);
                useParams = true;
            }

            //Initialize the connection pool            
            var conn = new SqlConnection(_connectionString);
            //TODO: use this or not??
            SqlConnection.ClearPool(conn);
            conn.Open();
            conn.Dispose();

            //Spin up the load threads
            for (var i = 0; i < _threads; i++)
            {
                conn = new SqlConnection(_connectionString);

                //TODO: Figure out how to make this option work (maybe)
                //conn.FireInfoMessageEventOnUserErrors = true;

                SqlCommand statsComm = null;

                var queryComm = new SqlCommand { CommandTimeout = _commandTimeout, Connection = conn, CommandText = _query };

                if (useParams)
                {
                    queryComm.Parameters.AddRange(ParamServer.GetParams());
                }

                var setStatistics = (_collectIoStats ? @"SET STATISTICS IO ON;" : string.Empty) + (_collectTimeStats ? @"SET STATISTICS TIME ON;" : string.Empty);

                if (setStatistics.Length > 0)
                {
                    statsComm = new SqlCommand { CommandTimeout = _commandTimeout, Connection = conn, CommandText = setStatistics };
                }

                //Queue<queryOutput> queryOutInfo = new Queue<queryOutput>();

                using var input = new QueryInput(statsComm, queryComm,
                    QueryOutInfo,
                    _iterations, _forceDataRetrieval, _queryDelay, worker, _killQueriesOnCancel, _threads);

                var theThread = new Thread(input.StartLoadThread) { Priority = ThreadPriority.BelowNormal, IsBackground = true };
                theThread.Name = "thread: " + i;

                _threadPool.Add(theThread);
                _commandPool.Add(queryComm);
                //queryOutInfoPool.Add(queryOutInfo);
            }
            // create a token source for the workers to be able to listen to a cancel event
            using var workerCTS = new CancellationTokenSource();
            _finishedThreads = 0;
            for (var i = 0; i < _threads; i++)
            {
                _threadPool[i].Start(workerCTS.Token);
            }

            //Start reading the queue...
            var cancelled = false;

            while (!cancelled)
            {
                QueryOutput theOut = null;
                try
                {
                    // wait for OutInfo items in the queue or a user cancel event
                    theOut = QueryOutInfo.Take(_backgroundWorkerCTS.Token);
                }
                catch (Exception ex)
                {
                    // The exception is InvalidOperationException if the threads are done
                    // and OperationCanceledException if the user clicked cancel.
                    // If it's OperationCanceledException, we need to cancel
                    // the worker threads and wait for them to exit
                    if (ex is OperationCanceledException)
                    {
                        workerCTS.Cancel();
                        foreach (var theThread in _threadPool)
                        {
                            // give the thread max 5 seconds to cancel nicely
                            if (!theThread.Join(5000))
                                theThread.Interrupt();
                        }
                    }
                    SqlConnection.ClearAllPools();
                    cancelled = true;
                }

                if (theOut != null)
                {
                    //Report output to the UI
                    int finishedThreads = Interlocked.CompareExchange(ref _finishedThreads, 0, 0);
                    theOut.ActiveThreads = _threads - finishedThreads;
                    worker.ReportProgress((int)(_finishedThreads / (decimal)_threads * 100), theOut);
                }
            }
        }


        //TODO: Monostate pattern to be investigated (class is never instantiated)
	}
}