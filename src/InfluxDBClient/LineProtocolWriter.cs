using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text;

namespace InfluxDBClient
{
    public class LineProtocolWriter
    {
        private static readonly Dictionary<Type, Action<StringBuilder, object>> TypeHandlers;
        private static readonly DateTime Epoch = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);
        private readonly StringBuilder _builder = new StringBuilder();

        static LineProtocolWriter()
        {
            TypeHandlers = new Dictionary<Type, Action<StringBuilder, object>>
            {
                { typeof(string), AppendString },
                { typeof(char), AppendChar },
                { typeof(bool), AppendBool },
                { typeof(float), AppendFloat },
                { typeof(double), AppendDouble },
                { typeof(sbyte), AppendSByte },
                { typeof(short), AppendShort },
                { typeof(int), AppendInt },
                { typeof(long), AppendLong },
                { typeof(byte), AppendByte },
                { typeof(ushort), AppendUShort },
                { typeof(uint), AppendUInt },
                { typeof(ulong), AppendULong }
            };
        }

        public void Write(Point point)
        {
            if (_builder.Length > 0)
            {
                _builder.Append('\n');
            }

            AppendEscapedMeasurement(_builder, point.Measurement);
            AppendTags(point);
            AppendFields(point);
            AppendTimestamp(point);
        }

        public override string ToString()
        {
            return _builder.ToString();
        }

        private void AppendTags(Point point)
        {
            if (point.Tags.Count > 0)
            {
                _builder.Append(',');
                bool needComa = false;
                foreach (var tag in point.Tags.OrderBy(k => k.Key, StringComparer.Ordinal))
                {
                    if (needComa)
                    {
                        _builder.Append(',');
                    }
                    else
                    {
                        needComa = true;
                    }

                    AppendEscapedKey(_builder, tag.Key);
                    _builder.Append('=');
                    _builder.Append(tag.Value);
                }
            }
        }

        private void AppendFields(Point point)
        {
            if (point.Fields.Count > 0)
            {
                _builder.Append(' ');
                bool needComa = false;
                foreach (var field in point.Fields)
                {
                    if (needComa)
                    {
                        _builder.Append(',');
                    }
                    else
                    {
                        needComa = true;
                    }

                    AppendEscapedKey(_builder, field.Key);
                    _builder.Append('=');

                    Action<StringBuilder, object> handler;
                    if (TypeHandlers.TryGetValue(field.Value.GetType(), out handler))
                    {
                        handler(_builder, field.Value);
                    }
                    else
                    {
                        AppendString(_builder, field.Value.ToString());
                    }
                }
            }
        }

        private void AppendTimestamp(Point point)
        {
            if (point.Timestamp.HasValue)
            {
                _builder.Append(' ');
                DateTime value = point.Timestamp.Value.ToUniversalTime();
                TimeSpan timeSpan = value - Epoch;
                _builder.Append((long)timeSpan.TotalMilliseconds);
            }
        }

        private static void AppendEscapedMeasurement(StringBuilder builder, string measurement)
        {
            int index = builder.Length;
            builder.Append(measurement)
                   .Replace(",", @"\,", index, builder.Length - index)
                   .Replace(" ", @"\ ", index, builder.Length - index);
        }

        private static void AppendEscapedKey(StringBuilder builder, string key)
        {
            int index = builder.Length;
            builder.Append(key)
                   .Replace(",", @"\,", index, builder.Length - index)
                   .Replace("=", @"\=", index, builder.Length - index)
                   .Replace(" ", @"\ ", index, builder.Length - index);
        }

        private static void AppendEscapedString(StringBuilder builder, string str)
        {
            int index = builder.Length;
            builder.Append(str)
                   .Replace("\"", @"\\\", index, builder.Length - index);
        }

        private static void AppendString(StringBuilder builder, object value)
        {
            builder.Append('"');
            AppendEscapedString(builder, (string) value);
            builder.Append('"');
        }

        private static void AppendChar(StringBuilder builder, object value)
        {
            builder.Append('"');
            AppendEscapedString(builder, ((char)value).ToString());
            builder.Append('"');
        }

        private static void AppendBool(StringBuilder builder, object value)
        {
            builder.Append((bool)value ? 't' : 'f');
        }

        private static void AppendFloat(StringBuilder builder, object value)
        {
            builder.Append(((float)value).ToString("R", NumberFormatInfo.InvariantInfo));
        }

        private static void AppendDouble(StringBuilder builder, object value)
        {
            builder.Append(((double)value).ToString("R", NumberFormatInfo.InvariantInfo));
        }

        private static void AppendSByte(StringBuilder builder, object value)
        {
            builder.Append((sbyte)value);
            builder.Append('i');
        }

        private static void AppendShort(StringBuilder builder, object value)
        {
            builder.Append((short)value);
            builder.Append('i');
        }

        private static void AppendInt(StringBuilder builder, object value)
        {
            builder.Append((int)value);
            builder.Append('i');
        }

        private static void AppendLong(StringBuilder builder, object value)
        {
            builder.Append((long)value);
            builder.Append('i');
        }

        private static void AppendByte(StringBuilder builder, object value)
        {
            builder.Append((byte)value);
            builder.Append('i');
        }

        private static void AppendUShort(StringBuilder builder, object value)
        {
            builder.Append((ushort)value);
            builder.Append('i');
        }

        private static void AppendUInt(StringBuilder builder, object value)
        {
            builder.Append((uint)value);
            builder.Append('i');
        }

        private static void AppendULong(StringBuilder builder, object value)
        {
            ulong casted = (ulong) value;
            if (casted > long.MaxValue)
            {
                throw new NotSupportedException("Unsigned long is too big.");
            }

            builder.Append(casted);
            builder.Append('i');
        }
    }
}
