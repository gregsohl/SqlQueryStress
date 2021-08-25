using System;
using System.Collections;
using System.Collections.Concurrent;
using System.ComponentModel;
using System.Data;
using System.Diagnostics;
using System.Globalization;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;

using Microsoft.Data.SqlClient;

namespace SQLQueryStress
{
	internal class QueryInput : IDisposable
	{
		[ThreadStatic] private static QueryOutput _outInfo;

		//This regex is used to find the number of logical reads
		//in the messages collection returned in the queryOutput class
		private static readonly Regex FindReads = new Regex(@"(?:Table (\'\w{1,}\'|'#\w{1,}\'|'##\w{1,}\'). Scan count \d{1,}, logical reads )(\d{1,})", RegexOptions.Compiled);

		//This regex is used to find the CPU and elapsed time
		//in the messages collection returned in the queryOutput class
		private static readonly Regex FindTimes =
			new Regex(
				@"(?:SQL Server Execution Times:|SQL Server parse and compile time:)(?:\s{1,}CPU time = )(\d{1,})(?: ms,\s{1,}elapsed time = )(\d{1,})",
				RegexOptions.Compiled);

		private readonly SqlCommand _queryComm;
		private readonly BlockingCollection<QueryOutput> _queryOutInfo;

		private readonly SqlCommand _statsComm;

		//private static Dictionary<int, List<string>> theInfoMessages = new Dictionary<int, List<string>>();

		private readonly Stopwatch _sw = new Stopwatch();
		private readonly System.Timers.Timer _killTimer = new System.Timers.Timer();
		private readonly bool _forceDataRetrieval;
		//          private readonly Queue<queryOutput> queryOutInfo;
		private readonly int _iterations;
		private readonly int _queryDelay;
		private readonly int _numWorkerThreads;
		private readonly BackgroundWorker _backgroundWorker;

		public QueryInput(SqlCommand statsComm, SqlCommand queryComm,
			BlockingCollection<QueryOutput> queryOutInfo,
			int iterations, bool forceDataRetrieval, int queryDelay, BackgroundWorker _backgroundWorker, bool killQueriesOnCancel, int numWorkerThreads)
		{
			_statsComm = statsComm;
			_queryComm = queryComm;
			_queryOutInfo = queryOutInfo;
			//                this.queryOutInfo = queryOutInfo;
			_iterations = iterations;
			_forceDataRetrieval = forceDataRetrieval;
			_queryDelay = queryDelay;
			_numWorkerThreads = numWorkerThreads;

			//Prepare the infoMessages collection, if we are collecting statistics
			//if (stats_comm != null)
			//    theInfoMessages.Add(stats_comm.Connection.GetHashCode(), new List<string>());

			this._backgroundWorker = _backgroundWorker;

			if (killQueriesOnCancel)
			{
				_killTimer.Interval = 2000;
				_killTimer.Elapsed += KillTimer_Elapsed;
				_killTimer.Enabled = true;
			}
		}

		private void KillTimer_Elapsed(object sender, System.Timers.ElapsedEventArgs e)
		{
			if (_backgroundWorker.CancellationPending)
			{
				_queryComm.Cancel();
				_killTimer.Enabled = false;
			}
			else if (_queryComm.Connection == null || _queryComm.Connection.State == ConnectionState.Closed)
			{
				_killTimer.Enabled = false;
			}
		}

		private static void GetInfoMessages(object sender, SqlInfoMessageEventArgs args)
		{
			foreach (SqlError err in args.Errors)
			{
				var matches = FindReads.Split(err.Message);

				//we have a read
				if (matches.Length > 1)
				{
					_outInfo.LogicalReads += Convert.ToInt32(matches[2], CultureInfo.InvariantCulture);
					continue;
				}

				matches = FindTimes.Split(err.Message);

				//we have times
				if (matches.Length > 1)
				{
					_outInfo.CpuTime += Convert.ToInt32(matches[1], CultureInfo.InvariantCulture);
					_outInfo.ElapsedTime += Convert.ToInt32(matches[2], CultureInfo.InvariantCulture);
				}
			}
		}

		public void StartLoadThread(Object token)
		{
			bool runCancelled = false;

			CancellationToken ctsToken = (CancellationToken)token;
			try
			{
				ctsToken.Register(() =>
				{
					// Cancellation on the token will interrupt and cancel the thread
					runCancelled = true;
					_statsComm.Cancel();
					_queryComm.Cancel();
				});
				//do the work
				using (var conn = _queryComm.Connection)
				{
					bool statisticsEnabled = conn.StatisticsEnabled;

					conn.StatisticsEnabled = true;
					conn.ResetStatistics();

					SqlInfoMessageEventHandler handler = GetInfoMessages;

					for (var i = 0; i < _iterations && !runCancelled; i++)
					{
						Exception outException = null;

						try
						{
							//initialize the outInfo structure
							_outInfo = new QueryOutput();

							if (conn != null)
							{
								conn.Open();

								//set up the statistics gathering
								if (_statsComm != null)
								{
									_statsComm.ExecuteNonQuery();
									Thread.Sleep(0);
									conn.InfoMessage += handler;
								}
							}

							//Params are assigned only once -- after that, their values are dynamically retrieved
							if (_queryComm.Parameters.Count > 0)
							{
								ParamServer.GetNextRow_Values(_queryComm.Parameters);
							}

							_sw.Start();

							//TODO: This could be made better
							if (_forceDataRetrieval)
							{
								var reader = _queryComm.ExecuteReader();
								Thread.Sleep(0);

								do
								{
									Thread.Sleep(0);

									while (!runCancelled && reader.Read())
									{
										//grab the first column to force the row down the pipe
										// ReSharper disable once UnusedVariable
										var x = reader[0];
										Thread.Sleep(0);
									}
								} while (!runCancelled && reader.NextResult());
							}
							else
							{
								_queryComm.ExecuteNonQuery();
								Thread.Sleep(0);
							}

							_sw.Stop();
						}
						catch (Exception ex)
						{
							if (!runCancelled)
								outException = ex;

							if (_sw.IsRunning)
							{
								_sw.Stop();
							}
						}
						finally
						{
							//Clean up the connection
							if (conn != null)
							{
								if (_statsComm != null)
								{
									conn.InfoMessage -= handler;
								}
								conn.Close();
							}
						}

						var finished = i == _iterations - 1;

						//List<string> infoMessages = null;

						//infoMessages = (stats_comm != null) ? theInfoMessages[connectionHashCode] : null;

						/*
                            queryOutput theout = new queryOutput(
                                outException,
                                sw.Elapsed,
                                finished,
                                (infoMessages == null || infoMessages.Count == 0) ? null : infoMessages.ToArray());
                             */

						_outInfo.E = outException;
						_outInfo.Time = _sw.Elapsed;
						_outInfo.Finished = finished;

						_queryOutInfo.Add(_outInfo);

						//Prep the collection for the next round
						//if (infoMessages != null && infoMessages.Count > 0)
						//    infoMessages.Clear();

						_sw.Reset();

						if (!runCancelled)
						{
							try
							{
								if (_queryDelay > 0)
									Task.Delay(_queryDelay, ctsToken).Wait();
							}
							catch (AggregateException ae)
							{
								ae.Handle((x) =>
								{
									if (x is TaskCanceledException)
									{
										runCancelled = true;
										return true;
									}
									// if we get here, the exception wasn't a cancel
									// so don't swallow it
									return false;
								});
							}
						}
					}

					long bytesReceived = 0L;
					IDictionary retrieveStatistics = conn.RetrieveStatistics();
					if (retrieveStatistics != null)
					{
						bytesReceived = (long)retrieveStatistics["BytesReceived"];
					}

					_outInfo.BytesReceived = bytesReceived;

					conn.StatisticsEnabled = statisticsEnabled;
				}
			}
			catch (Exception)
			{
				if (runCancelled)
				{
					//queryOutput theout = new queryOutput(null, new TimeSpan(0), true, null);
					_outInfo.Time = new TimeSpan(0);
					_outInfo.Finished = true;
					_queryOutInfo.Add(_outInfo);
				}
			}

			int finishedThreads = LoadEngine.FinishThread();
			if (finishedThreads == _numWorkerThreads)
			{
				// once all of the threads have exited, tell the other side that we're done adding items to the collection
				_queryOutInfo.CompleteAdding();
			}
		}

		public void Dispose()
		{
			_killTimer.Dispose();
		}
	}
}
