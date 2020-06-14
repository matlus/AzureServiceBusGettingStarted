using Microsoft.Azure.ServiceBus;
using System;
using System.Text;
using System.Threading.Tasks;

namespace ServiceBusGettingStartedSubscriber
{
    internal static class Subscriber
    {
        private const string ServiceBusConnectionString = "<Your Connection String goes here>";
        private const string TopicName = "videoreceived.topic";
        private const string SubscriptionName = "videoreceived.transcoder.queue";

        public static async Task Main(string[] args)
        {
            var subscriptionClient = new SubscriptionClient(ServiceBusConnectionString, TopicName, SubscriptionName);

            var messageHandlerOptions = new MessageHandlerOptions(ExceptionReceivedHandler);
            messageHandlerOptions.AutoComplete = false;

            subscriptionClient.RegisterMessageHandler((message, cancellationToken) =>
            {
                Console.WriteLine($"UserName: {message.UserProperties["UserName"]}");
                Console.WriteLine($"Stage: {message.UserProperties["Stage"]}");
                Console.WriteLine($"ContentType: {message.ContentType}");
                Console.WriteLine($"CorrelationId: {message.CorrelationId}");

                Console.WriteLine(Encoding.UTF8.GetString(message.Body));
                subscriptionClient.CompleteAsync(message.SystemProperties.LockToken);
                Console.WriteLine();
                return Task.CompletedTask;
            }
            , messageHandlerOptions);

            Console.ReadLine();
            await subscriptionClient.CloseAsync();
        }

        static Task ExceptionReceivedHandler(ExceptionReceivedEventArgs exceptionReceivedEventArgs)
        {
            Console.ForegroundColor = ConsoleColor.Red;
            Console.WriteLine(exceptionReceivedEventArgs.Exception.GetType());
            Console.WriteLine(exceptionReceivedEventArgs.Exception.Message);
            Console.ResetColor();
            return Task.CompletedTask;
        }
    }
}
