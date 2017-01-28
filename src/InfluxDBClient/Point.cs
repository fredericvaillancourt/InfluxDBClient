using System;
using System.Collections.Generic;

namespace InfluxDBClient
{
    public class Point
    {
        public string Measurement { get; set; }

        public Dictionary<string, string> Tags { get; } = new Dictionary<string, string>();

        public Dictionary<string, object> Fields { get; } = new Dictionary<string, object>();

        public DateTime? Timestamp { get; set; }
    }
}
