using System;
using System.Collections.Generic;
using System.Net;
using System.Threading.Tasks;

namespace InfluxDBClient
{
    public abstract class Client : IDisposable
    {
        protected static readonly IReadOnlyDictionary<TimeUnit, string> TimeUnitCode = new Dictionary<TimeUnit, string>
        {
            { TimeUnit.Nanosecond, "n" },
            { TimeUnit.Microsecond, "m" },
            { TimeUnit.Millisecond, "ms" },
            { TimeUnit.Second, "s" },
            { TimeUnit.Minute, "m" },
            { TimeUnit.Hour, "h" }
        };

        private IPAddress _server;
        private int _port;

        private TimeUnit _timestampPrecision;

        protected Client(IPAddress server, int port)
        {
            if (server == null)
            {
                throw new ArgumentNullException(nameof(server));
            }

            if (port < 1 || port > 65535)
            {
                throw new ArgumentOutOfRangeException(nameof(port));
            }

            _server = server;
            _port = port;
        }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        public IPAddress Server
        {
            get { return _server; }
            set
            {
                if (value == null)
                {
                    throw new ArgumentNullException(nameof(value), "Server cannot be null");
                }

                if (!_server.Equals(value))
                {
                    _server = value;
                    OnClientPropertyChanged();
                }
            }
        }

        public int Port
        {
            get { return _port; }
            set
            {
                if (value < 1 || value > 65535)
                {
                    throw new ArgumentOutOfRangeException(nameof(value));
                }

                if (_port != value)
                {
                    _port = value;
                    OnClientPropertyChanged();
                }
            }
        }

        public TimeUnit TimestampPrecision
        {
            get { return _timestampPrecision; }
            set
            {
                if (_timestampPrecision != value)
                {
                    _timestampPrecision = value;
                    OnClientPropertyChanged();
                }
            }
        }

        public Task WriteAsync(params Point[] points)
        {
            return WriteAsync((IEnumerable<Point>)points);
        }

        public abstract Task WriteAsync(IEnumerable<Point> points);

        protected virtual void OnClientPropertyChanged()
        {
        }

        protected virtual void Dispose(bool disposing)
        {
        }
    }
}
