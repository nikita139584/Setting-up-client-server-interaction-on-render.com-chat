using System;
using System.Net.WebSockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

class Client
{
    static async Task Main()
    {
        Console.OutputEncoding = Encoding.UTF8;

        Console.Write("Введіть ваше ім'я: ");
        string username = Console.ReadLine();

        using ClientWebSocket socket = new ClientWebSocket();

        Uri server = new Uri("wss://YOUR-RENDER-APP.onrender.com/ws");

        try
        {
            Console.WriteLine("Підключення до сервера...");
            await socket.ConnectAsync(server, CancellationToken.None);

            Console.WriteLine("Підключено!");
            Console.WriteLine("Введіть повідомлення (/exit для виходу)\n");

            _ = Task.Run(() => ReceiveMessages(socket));

            while (socket.State == WebSocketState.Open)
            {
                string text = Console.ReadLine();

                if (text == "/exit")
                    break;

                string message = $"{username}: {text}";

                byte[] data = Encoding.UTF8.GetBytes(message);

                await socket.SendAsync(
                    new ArraySegment<byte>(data),
                    WebSocketMessageType.Text,
                    true,
                    CancellationToken.None);
            }

            if (socket.State == WebSocketState.Open)
            {
                await socket.CloseAsync(
                    WebSocketCloseStatus.NormalClosure,
                    "Exit",
                    CancellationToken.None);
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Помилка: {ex.Message}");
        }
    }

    static async Task ReceiveMessages(ClientWebSocket socket)
    {
        byte[] buffer = new byte[4096];

        while (socket.State == WebSocketState.Open)
        {
            try
            {
                WebSocketReceiveResult result = await socket.ReceiveAsync(
                    new ArraySegment<byte>(buffer),
                    CancellationToken.None);

                if (result.MessageType == WebSocketMessageType.Close)
                    break;

                string message = Encoding.UTF8.GetString(buffer, 0, result.Count);

                Console.WriteLine(message);
            }
            catch
            {
                break;
            }
        }

        Console.WriteLine("З'єднання закрито.");
    }
}