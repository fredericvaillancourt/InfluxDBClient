using System;
using System.Collections.Generic;
using System.Net;
using System.Threading.Tasks;

namespace InfluxDBClient
{
    public abstract class Client : IDisposable
    {
        private IPAddress _server;
        private int _port;

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
                if (_server != value)
                {
                    _server = value;
                    OnServerOrPortChanged();
                }
            }
        }

        public int Port
        {
            get { return _port; }
            set
            {
                if (_port != value)
                {
                    _port = value;
                    OnServerOrPortChanged();
                }
            }
        }

        public Task WriteAsync(params Point[] points)
        {
            return WriteAsync((IEnumerable<Point>)points);
        }

        public abstract Task WriteAsync(IEnumerable<Point> points);

        protected virtual void OnServerOrPortChanged()
        {
        }

        protected virtual void Dispose(bool disposing)
        {
        }
    }
}
