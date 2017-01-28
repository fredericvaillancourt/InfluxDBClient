using System.Collections.Generic;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading.Tasks;

namespace InfluxDBClient
{
    public class InfluxDBUdpClient : Client
    {
        private readonly Socket _socket = new Socket(AddressFamily.InterNetwork, SocketType.Dgram, ProtocolType.IP);
        private EndPoint _endPoint;

        public InfluxDBUdpClient(IPAddress server, int port)
            : base(server, port)
        {
            UpdateEndpoint();
        }
        
        public override Task WriteAsync(IEnumerable<Point> points)
        {
            LineProtocolWriter writer = new LineProtocolWriter(TimeUnit.Nanosecond);
            foreach (var point in points)
            {
                writer.Write(point);
            }

            byte[] buffer = Encoding.UTF8.GetBytes(writer.ToString());
            var completionSource = new TaskCompletionSource<object>();
            var args = new SocketAsyncEventArgs
            {
                RemoteEndPoint = _endPoint,
                UserToken = completionSource
            };
            args.SetBuffer(buffer, 0, buffer.Length);
            args.Completed += Completed;
            _socket.SendToAsync(args);

            return completionSource.Task;
        }

        protected override void OnServerOrPortChanged()
        {
            UpdateEndpoint();
            base.OnServerOrPortChanged();
        }

        protected override void Dispose(bool disposing)
        {
            if (disposing)
            {
                _socket.Dispose();
            }

            base.Dispose(disposing);
        }

        private void Completed(object sender, SocketAsyncEventArgs e)
        {
            var completionSource = e.UserToken as TaskCompletionSource<object>;
            completionSource.TrySetResult(null);
        }

        private void UpdateEndpoint()
        {
            _endPoint = new IPEndPoint(Server, Port);
        }
    }
}
