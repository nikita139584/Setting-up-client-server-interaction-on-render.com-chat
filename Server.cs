using System;
using System.Collections.Concurrent;
using System.Net;
using System.Net.WebSockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

class Program
{
    static readonly ConcurrentDictionary<Guid, WebSocket> Clients = new();

    static async Task Main()
    {
        string port = Environment.GetEnvironmentVariable("PORT") ?? "5000";

        HttpListener listener = new HttpListener();
        listener.Prefixes.Add($"http://+:{port}/");

        listener.Start();

        Console.WriteLine($"Server started on port {port}");

        while (true)
        {
            HttpListenerContext context = await listener.GetContextAsync();

            if (context.Request.IsWebSocketRequest &&
                context.Request.Url.AbsolutePath == "/ws")
            {
                _ = HandleClient(context);
            }
            else
            {
                context.Response.StatusCode = 400;

                byte[] buffer = Encoding.UTF8.GetBytes("WebSocket endpoint: /ws");

                await context.Response.OutputStream.WriteAsync(buffer);
                context.Response.Close();
            }
        }
    }

    static async Task HandleClient(HttpListenerContext context)
    {
        WebSocket socket = null;
        Guid id = Guid.NewGuid();

        try
        {
            var wsContext = await context.AcceptWebSocketAsync(null);
            socket = wsContext.WebSocket;

            Clients.TryAdd(id, socket);

            Console.WriteLine($"Client connected ({Clients.Count})");

            byte[] buffer = new byte[4096];

            while (socket.State == WebSocketState.Open)
            {
                WebSocketReceiveResult result =
                    await socket.ReceiveAsync(
                        new ArraySegment<byte>(buffer),
                        CancellationToken.None);

                if (result.MessageType == WebSocketMessageType.Close)
                    break;

                string message =
                    Encoding.UTF8.GetString(buffer, 0, result.Count);

                Console.WriteLine(message);

                await Broadcast(message);
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine(ex.Message);
        }
        finally
        {
            Clients.TryRemove(id, out _);

            if (socket != null)
            {
                try
                {
                    await socket.CloseAsync(
                        WebSocketCloseStatus.NormalClosure,
                        "",
                        CancellationToken.None);
                }
                catch
                {
                }

                socket.Dispose();
            }

            Console.WriteLine($"Client disconnected ({Clients.Count})");
        }
    }

    static async Task Broadcast(string message)
    {
        byte[] data = Encoding.UTF8.GetBytes(message);

        foreach (var client in Clients)
        {
            if (client.Value.State == WebSocketState.Open)
            {
                try
                {
                    await client.Value.SendAsync(
                        new ArraySegment<byte>(data),
                        WebSocketMessageType.Text,
                        true,
                        CancellationToken.None);
                }
                catch
                {
                }
            }
        }
    }
}