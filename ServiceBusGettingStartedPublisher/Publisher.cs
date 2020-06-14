using Microsoft.Azure.ServiceBus;
using System;
using System.Text;
using System.Threading.Tasks;

namespace ServiceBusGettingStartedPublisher
{
    internal static class Publisher
    {
        private const string ServiceBusConnectionString = "<Your Connection String goes here>";
        private const string TopicName = "videoreceived.topic";

        public static async Task Main(string[] args)
        {
            var topicClient = new TopicClient(ServiceBusConnectionString, TopicName);

            do
            {
                var videoFileName = Guid.NewGuid().ToString("N");
                string messageBody = $"{{Resolution: \"4K\", Duration: \"00:30:00\" VideoFile: \"{videoFileName}\"}}";
                var message = new Message(Encoding.UTF8.GetBytes(messageBody));
                message.CorrelationId = Guid.NewGuid().ToString("N");
                message.ContentType = "application/json";

                message.UserProperties.Add("UserName", Guid.NewGuid().ToString("N"));
                message.UserProperties.Add("Stage", "A");

                await topicClient.SendAsync(message);
                Console.WriteLine($"Message sent: {messageBody}");

                var consoleKeyInfo = Console.ReadKey();
                if (consoleKeyInfo.KeyChar == 'x')
                {
                    break;
                }

            } while (true);

            Console.WriteLine("Done!");
            await topicClient.CloseAsync();
        }
    }
}
