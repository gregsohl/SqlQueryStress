using System;

namespace SQLQueryStress
{
	internal class QueryOutput
	{
		public int CpuTime;
		public Exception E;
		public int ElapsedTime;
		public bool Finished;
		public int LogicalReads;
		public TimeSpan Time;

		// Remaining active threads for the load
		public int ActiveThreads;

		public long BytesReceived;

		/*
            public queryOutput(
                Exception e, 
                TimeSpan time, 
                bool finished,
                string[] infoMessages)
            {
                this.e = e;
                this.time = time;
                this.finished = finished;
                this.infoMessages = infoMessages;
            }
             */
	}
}
