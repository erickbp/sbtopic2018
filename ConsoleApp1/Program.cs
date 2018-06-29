using System;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.ServiceBus;

namespace ConsoleApp1
{
    internal static class Program
    {
        private const string ServiceBusConnectionString = null;
        private const string TopicName = null;
        private const string SubscriptionName = null;

        private static ITopicClient _topicClient;
        private static ISubscriptionClient _subscriptionClient1;
        private static ISubscriptionClient _subscriptionClient2;


        private static void Main()
        {
            MainAsync().GetAwaiter().GetResult();
        }

        private static async Task MainAsync()
        {
            const int numberOfMessages = 10;
            _topicClient = new TopicClient(ServiceBusConnectionString, TopicName);

            await SendMessagesAsync(numberOfMessages);

            _subscriptionClient1 = new SubscriptionClient(ServiceBusConnectionString, TopicName, SubscriptionName);
            _subscriptionClient2 = new SubscriptionClient(ServiceBusConnectionString, TopicName, SubscriptionName);

            Console.WriteLine("======================================================");
            Console.WriteLine("Press any key to exit after receiving all the messages.");
            Console.WriteLine("======================================================");

            RegisterOnMessageHandlerAndReceiveMessages1();
            RegisterOnMessageHandlerAndReceiveMessages2();

            Console.ReadKey();

            await _subscriptionClient1.CloseAsync();
            await _subscriptionClient2.CloseAsync();

            await _topicClient.CloseAsync();
        }

        private static async Task ProcessMessagesAsync1(Message message, CancellationToken token)
        {
            Console.WriteLine($"ProcessMessagesAsync1:::: {Encoding.UTF8.GetString(message.Body)} with LockToken: {message.SystemProperties.LockToken}");
            await Task.Delay(5000, token);
            await _subscriptionClient1.CompleteAsync(message.SystemProperties.LockToken);
        }

        private static async Task ProcessMessagesAsync2(Message message, CancellationToken token)
        {
            Console.WriteLine($"ProcessMessagesAsync2:::: {Encoding.UTF8.GetString(message.Body)} with LockToken: {message.SystemProperties.LockToken}");
            await Task.Delay(5000, token);
            await _subscriptionClient2.CompleteAsync(message.SystemProperties.LockToken);
        }
      
        private static void RegisterOnMessageHandlerAndReceiveMessages1()
        {
            var messageHandlerOptions = new MessageHandlerOptions(ExceptionReceivedHandler)
            {
                MaxConcurrentCalls = 1,
                AutoComplete = false
            };
            _subscriptionClient1.RegisterMessageHandler(ProcessMessagesAsync1, messageHandlerOptions);
        }

        private static void RegisterOnMessageHandlerAndReceiveMessages2()
        {
            var messageHandlerOptions = new MessageHandlerOptions(ExceptionReceivedHandler)
            {
                MaxConcurrentCalls = 1,
                AutoComplete = false
            };
            _subscriptionClient2.RegisterMessageHandler(ProcessMessagesAsync2, messageHandlerOptions);
        }

        private static async Task SendMessagesAsync(int numberOfMessagesToSend)
        {
            for (var i = 0; i < numberOfMessagesToSend; i++)
            {
                try
                {
                    var messageBody = $"Message {i}";
                    var message = new Message(Encoding.UTF8.GetBytes(messageBody));

                    Console.WriteLine($"Sending message: {messageBody}");
                    await _topicClient.SendAsync(message);
                }
                catch (Exception exception)
                {
                    Console.WriteLine($"{DateTime.Now} :: Exception: {exception.Message}");
                }
            }
        }

        private static Task ExceptionReceivedHandler(ExceptionReceivedEventArgs exceptionReceivedEventArgs)
        {
            Console.WriteLine($"Message handler encountered an exception {exceptionReceivedEventArgs.Exception}.");
            return Task.CompletedTask;
        }

    }
}