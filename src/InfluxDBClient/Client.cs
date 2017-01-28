using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Text;
using System.Threading.Tasks;

namespace InfluxDBClient
{
    public class Client
    {
        private static readonly Dictionary<TimeUnit, string> TimeUnitCode = new Dictionary<TimeUnit, string>
        {
            { TimeUnit.Nanosecond, "n" },
            { TimeUnit.Microsecond, "m" },
            { TimeUnit.Millisecond, "ms" },
            { TimeUnit.Second, "s" },
            { TimeUnit.Minute, "m" },
            { TimeUnit.Hour, "h" }
        };

        private readonly HttpClient _client = new HttpClient() { Timeout = TimeSpan.FromSeconds(10) };
        private TimeUnit _timestampPrecision = TimeUnit.Millisecond;
        private string _server;
        private int _port;
        private string _database;
        private Uri _writeUri;
        private ICredentials _credentials;

        public Client(string server, int port, string database)
        {
            if (server == null)
            {
                throw new ArgumentNullException(nameof(server));
            }

            if (port < 1 || port > 65535)
            {
                throw new ArgumentOutOfRangeException(nameof(port));
            }

            if (database == null)
            {
                throw new ArgumentNullException(nameof(database));
            }

            _server = server;
            Port = port;
            _database = database;

            UpdateUri();
        }

        public string Server
        {
            get { return _server; }
            set
            {
                if (_server != value)
                {
                    _server = value;
                    UpdateUri();
                    UpdateCredentials();
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
                    UpdateUri();
                    UpdateCredentials();
                }
            }
        }

        public string Database
        {
            get { return _database; }
            set
            {
                if (_database != value)
                {
                    _database = value;
                    UpdateUri();
                    UpdateCredentials();
                }
            }
        }

        public ICredentials Credentials
        {
            get { return _credentials; }
            set
            {
                if (_credentials != value)
                {
                    _credentials = value;
                    UpdateCredentials();
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
                    UpdateUri();
                }
            }
        }

        public TimeSpan Timeout
        {
            get { return _client.Timeout; }
            set { _client.Timeout = value; }
        }

        public Task WriteAsync(params Point[] points)
        {
            return WriteAsync((IEnumerable<Point>)points);
        }

        public async Task WriteAsync(IEnumerable<Point> points)
        {
            LineProtocolWriter writer = new LineProtocolWriter(TimestampPrecision);
            foreach (var point in points)
            {
                writer.Write(point);
            }

            HttpResponseMessage response = await _client.PostAsync(_writeUri, new StringContent(writer.ToString()));

            if (response.StatusCode == HttpStatusCode.NoContent)
            {
                return;
            }

            if (response.StatusCode == HttpStatusCode.BadRequest)
            {
                throw new FormatException(await response.Content.ReadAsStringAsync());
            }

            if (response.StatusCode == HttpStatusCode.NotFound)
            {
                throw new InvalidDataException(await response.Content.ReadAsStringAsync());
            }

            if (response.StatusCode == HttpStatusCode.InternalServerError)
            {
                throw new Exception(await response.Content.ReadAsStringAsync());
            }

            response.EnsureSuccessStatusCode();
        }

        private void UpdateUri()
        {
            var precision = TimestampPrecision != TimeUnit.Nanosecond
                ? $"&precision={TimeUnitCode[TimestampPrecision]}"
                : string.Empty;

            _writeUri = new UriBuilder("http", Server, Port, "write")
            {
                Query = $"db={Database}{precision}"
            }.Uri;
        }

        private void UpdateCredentials()
        {
            if (_credentials != null)
            {
                var credentials = _credentials.GetCredential(_writeUri, "Basic");
                var encoded = Convert.ToBase64String(Encoding.UTF8.GetBytes($"{credentials.UserName}:{credentials.Password}"));
                _client.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Basic", encoded);
            }
            else
            {
                _client.DefaultRequestHeaders.Authorization = null;
            }
        }
    }
}
