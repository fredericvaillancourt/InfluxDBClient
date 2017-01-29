using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Text;
using System.Threading.Tasks;

namespace InfluxDBClient
{
    public class InfluxDBHttpClient : Client
    {
        private readonly HttpClient _client = new HttpClient() { Timeout = TimeSpan.FromSeconds(10) };
        private Uri _writeUri;
        private ICredentials _credentials;

        private string _database;

        public InfluxDBHttpClient(IPAddress server, int port, string database)
            : base(server, port)
        {
            if (database == null)
            {
                throw new ArgumentNullException(nameof(database));
            }

            if (string.IsNullOrWhiteSpace(database))
            {
                throw new ArgumentException("Database cannot be empty.", nameof(database));
            }

            _database = database;

            UpdateUri();
        }

        public string Database
        {
            get { return _database; }
            set
            {
                if (value == null)
                {
                    throw new ArgumentNullException(nameof(value), "Database cannot be null");
                }

                if (string.IsNullOrWhiteSpace(value))
                {
                    throw new ArgumentException("Database cannot be empty.", nameof(value));
                }

                if (_database != value)
                {
                    _database = value;
                    UpdateUri();
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

        public TimeSpan Timeout
        {
            get { return _client.Timeout; }
            set { _client.Timeout = value; }
        }

        public async Task<Version> PingAsync()
        {
            var response = await _client.GetAsync(new UriBuilder("http", Server.ToString(), Port, "ping").Uri);
            var versionHeader = response.Headers.First(h => h.Key == "X-Influxdb-Version");
            return Version.Parse(versionHeader.Value.First());
        }

        public override async Task WriteAsync(IEnumerable<Point> points)
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

        protected override void OnClientPropertyChanged()
        {
            UpdateUri();
            base.OnClientPropertyChanged();
        }

        protected override void Dispose(bool disposing)
        {
            if (disposing)
            {
                _client.Dispose();
            }

            base.Dispose(disposing);
        }

        private void UpdateUri()
        {
            var precision = TimestampPrecision != TimeUnit.Nanosecond
                ? $"&precision={TimeUnitCode[TimestampPrecision]}"
                : string.Empty;

            _writeUri = new UriBuilder("http", Server.ToString(), Port, "write")
            {
                Query = $"db={Database}{precision}"
            }.Uri;

            UpdateCredentials();
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
