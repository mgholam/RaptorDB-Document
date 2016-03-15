using System;
using System.Timers;
using System.Threading.Tasks;
using System.Collections.Concurrent;

namespace RaptorDB.Views
{
    internal class TaskQueue
    {
        public TaskQueue()
        {
            _timer = new Timer();
            _timer.Interval = Global.TaskCleanupTimerSeconds * 1000;
            _timer.Elapsed += new ElapsedEventHandler(_timer_Elapsed);
            _timer.Enabled = true;
            _log.Debug("TaskQueue starting");
        }

        private ILog _log = LogManager.GetLogger(typeof(TaskQueue));
        private object _lock = new object();
        private bool _shuttingdown = false;
        private Timer _timer;
        private ConcurrentQueue<Task> _que = new ConcurrentQueue<Task>();

        void _timer_Elapsed(object sender, ElapsedEventArgs e)
        {
            lock (_lock)
            {
                while (_que.Count > 0)
                {
                    //_log.Debug("in queue cleanup, count = " + _que.Count);
                    if (_shuttingdown)
                        break;
                    Task t = null;
                    if (_que.TryPeek(out t))
                    {
                        if (t.IsCompleted || t.IsCanceled || t.IsFaulted)
                            _que.TryDequeue(out t);
                        else
                            break;
                    }
                    else
                        break;
                }
            }
        }

        public void AddTask(Action action)
        {
            if (_shuttingdown == false)
                _que.Enqueue(Task.Factory.StartNew(action));
        }

        public void Shutdown()
        {
            _log.Debug("TaskQueue shutdown");
            // wait for tasks to finish
            _shuttingdown = true;
            _timer.Enabled = false;
            if (_que.Count > 0)
                Task.WaitAll(_que.ToArray());
        }
    }
}
