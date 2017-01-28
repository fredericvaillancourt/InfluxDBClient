using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Net.Http;
using System.Threading.Tasks;

namespace InfluxDBClient
{
    public class Client
    {
        private readonly Uri _writeUri;

        public Client(string server, int port, string database)
        {
            UriBuilder builder = new UriBuilder("http", server, port, "write");
            builder.Query = "db=" + database + "&precision=ms";
            _writeUri = builder.Uri;
        }

        public Task WriteAsync(params Point[] points)
        {
            return WriteAsync((IEnumerable<Point>)points);
        }

        public async Task WriteAsync(IEnumerable<Point> points)
        {
            LineProtocolWriter writer = new LineProtocolWriter();
            foreach (var point in points)
            {
                writer.Write(point);
            }

            HttpResponseMessage response;
            using (var client = new HttpClient() {Timeout = TimeSpan.FromSeconds(10)})
            {
                response = await client.PostAsync(_writeUri, new StringContent(writer.ToString()));
            }

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
    }
}
