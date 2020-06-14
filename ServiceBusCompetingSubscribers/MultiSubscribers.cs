using Microsoft.Azure.ServiceBus;
using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;

namespace ServiceBusCompetingSubscribers
{
    internal static class MultiSubscribers
    {
        private const string ServiceBusConnectionString = "<Your Connection String goes here>";
        private const string TopicName = "videoreceived.topic";
        private const string TranscoderSubscriptionName = "videoreceived.transcoder.queue";
        private const string NotificationSubscriptionName = "videoreceived.notification.queue";

        public static async Task Main(string[] args)
        {
            var subscriptionClient = new SubscriptionClient(ServiceBusConnectionString, TopicName, TranscoderSubscriptionName);
            var subscriptionClient2 = new SubscriptionClient(ServiceBusConnectionString, TopicName, NotificationSubscriptionName);
            var cancellationTokenSource = new CancellationTokenSource();
            var cancellationToken = cancellationTokenSource.Token;

            var transcoderMessageReceiveState = new MessageReceiveState(TranscoderSubscriptionName, subscriptionClient, cancellationToken);
            var notificationMessageReceiveState = new MessageReceiveState(NotificationSubscriptionName, subscriptionClient2, cancellationToken);
            Task[] subscriberTasks =
            {
                new TaskFactory().StartNew(MessageReceiveHandler, transcoderMessageReceiveState, cancellationToken),
                new TaskFactory().StartNew(MessageReceiveHandler, transcoderMessageReceiveState, cancellationToken),
                new TaskFactory().StartNew(MessageReceiveHandler, notificationMessageReceiveState, cancellationToken),
                new TaskFactory().StartNew(MessageReceiveHandler, notificationMessageReceiveState, cancellationToken),
            };

            try
            {
                ConsoleKeyInfo keyInfo;
                do
                {
                    keyInfo = Console.ReadKey();

                } while (keyInfo.KeyChar != 'x');

                cancellationTokenSource.Cancel(false);
                Task.WaitAll(subscriberTasks, cancellationToken);
                Console.WriteLine("All Tasks Completed");                
            }
            catch (OperationCanceledException oce)
            {
                Console.WriteLine(oce.Message);
            }
            catch (AggregateException ae)
            {
                Console.WriteLine(ae.Message);
            }
            finally
            {
                cancellationTokenSource.Dispose();
                await subscriptionClient.CloseAsync();
            }
        }

        private static void MessageReceiveHandler(object state)
        {
            var messageReceiveState = (MessageReceiveState)state;
            var subscriptionClient = messageReceiveState.SubscriptionClient;
            var cancellationToken = messageReceiveState.CancellationToken;
            
            var messageHandlerOptions = new MessageHandlerOptions(ExceptionReceivedHandler);
            messageHandlerOptions.AutoComplete = false;
            subscriptionClient.RegisterMessageHandler((message, cToken) =>
            {
                Console.ForegroundColor = messageReceiveState.SubscriptionName == TranscoderSubscriptionName ? ConsoleColor.Green : ConsoleColor.Yellow;
                Console.WriteLine("Subscription Name: " + messageReceiveState.SubscriptionName);
                subscriptionClient.CompleteAsync(message.SystemProperties.LockToken);
                Console.WriteLine();
                return Task.CompletedTask;
            }
            , messageHandlerOptions);

            while (true)
            {
                Thread.Sleep(100);
                if (cancellationToken.IsCancellationRequested)
                {
                    Debug.WriteLine("Token Cancelled");
                    cancellationToken.ThrowIfCancellationRequested();
                }
            }
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

    internal sealed class MessageReceiveState
    {
        public string SubscriptionName { get; }
        public SubscriptionClient SubscriptionClient { get; }
        public CancellationToken CancellationToken { get; set; }

        public MessageReceiveState(string subscriptionName, SubscriptionClient subscriptionClient, CancellationToken cancellationToken)
        {
            SubscriptionName = subscriptionName;
            SubscriptionClient = subscriptionClient;
            CancellationToken = cancellationToken;
        }
    }
}
