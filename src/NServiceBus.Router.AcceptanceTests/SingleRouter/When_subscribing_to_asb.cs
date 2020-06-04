#if !NET461
namespace NServiceBus.Router.AcceptanceTests.SingleRouter
{
    using System;
    using System.Text.RegularExpressions;
    using System.Threading.Tasks;
    using AcceptanceTesting;
    using Configuration.AdvancedExtensibility;
    using Events;
    using Features;
    using NServiceBus;
    using NServiceBus.AcceptanceTests;
    using NServiceBus.AcceptanceTests.EndpointTemplates;
    using NUnit.Framework;
    using Serialization;
    using Settings;
    using Conventions = AcceptanceTesting.Customization.Conventions;

    [TestFixture]
    public class When_subscribing_to_asb : NServiceBusAcceptanceTest
    {
        static string PublisherEndpointName => Conventions.EndpointNamingConvention(typeof(PublisherAsAsb));

        public When_subscribing_to_asb()
        {
            Environment.SetEnvironmentVariable("AzureServiceBus.ConnectionString", "Endpoint=sb://nictest.servicebus.windows.net/;SharedAccessKeyName=KCOM.AppointmentManagement.Service;SharedAccessKey=TXHAlL084u/FZ/0btlwpUX0W0YbY/hkcFNYQ4lH7V8Y=");
        }


        [Test]
        public async Task It_should_throw_error_when_subscribe_and_deliver_the_message_even_if_eventname_too_long()
        {
           var result = await Scenario.Define<Context>()
                                       .WithRouter("Router", (c, cfg) =>
                                                             {
                                                                 var leftIface = cfg.AddInterface<TestTransport>("Left", t =>
                                                                                                                         {
                                                                                                                             t.BrokerAlpha();
                                                                                                                             var settings = t.GetSettings();

                                                                                                                             var builder = new ConventionsBuilder(settings);
                                                                                                                             builder.DefiningEventsAs(EventConvention);
                                                                                                                             settings.Set(builder.Conventions);

                                                                                                                         }).InMemorySubscriptions();
                                                                 leftIface.LimitMessageProcessingConcurrencyTo(1); //To ensure when tracer arrives the subscribe request has already been processed.;
                                                                 cfg.AddInterface<AzureServiceBusTransport>("Right", t =>
                                                                                                                     {
                                                                                                                         var connString = Environment.GetEnvironmentVariable("AzureServiceBus.ConnectionString");
                                                                                                                         t.ConnectionString(connString);
                                                                                                                         var settings = t.GetSettings();
                                                                                                                         t.Transactions(TransportTransactionMode.ReceiveOnly);

                                                                                                                         var builder = new ConventionsBuilder(settings);
                                                                                                                         builder.DefiningEventsAs(EventConvention);
                                                                                                                         settings.Set(builder.Conventions);

                                                                                                                         //t.RuleNameShortener(PublisherAsAsb.RuleNameShortener);
                                                                                                                         //t.SubscriptionNameShortener(PublisherAsAsb.RuleNameShortener);

                                                                                                                         var serializer = Tuple.Create(new NewtonsoftSerializer() as SerializationDefinition, new SettingsHolder());
                                                                                                                         settings.Set("MainSerializer", serializer);
                                                                                                                     });
                                                                 cfg.UseStaticRoutingProtocol().AddForwardRoute("Left", "Right");
                                                                 cfg.UseStaticRoutingProtocol().AddForwardRoute("Right", "Left");
                                                             })
                                       .WithEndpoint<PublisherAsAsb>(c => c
                                                                          .When(async (s, context) =>
                                                                                {
                                                                                    await s.ScheduleEvery(TimeSpan.FromSeconds(5), ctx =>
                                                                                                                                   {
                                                                                                                                       context.ShortEventSubscribed = true;
                                                                                                                                       context.LongEventSubscribed = true;
                                                                                                                                       return Task.CompletedTask;
                                                                                                                                   });
                                                                                })

                                                                          .When(x => x.ShortEventSubscribed, async s =>
                                                                                                             {
                                                                                                                 await Console.Out.WriteLineAsync("Publishing MyAsbNotQuiteSoVeryVeryVeryVeryLongEvent").ConfigureAwait(false);
                                                                                                                 await s.Publish(new MyAsbNotQuiteSoVeryVeryVeryVeryLongEvent()).ConfigureAwait(false);
                                                                                                             })
                                                                          .When(x => x.LongEventSubscribed, async s =>
                                                                                                            {
                                                                                                                await Console.Out.WriteLineAsync("Publishing MyAsbVeryVeryVeryVeryVeryVeryVeryVeryVeryVeryVeryVeryLongEvent").ConfigureAwait(false);
                                                                                                                await s.Publish(new MyAsbVeryVeryVeryVeryVeryVeryVeryVeryVeryVeryVeryVeryLongEvent()).ConfigureAwait(false);
                                                                                                            }))

                                       .WithEndpoint<SubscriberToAsb>(c => c.When(async (s, context) =>
                                                                                  {
                                                                                      await Console.Out.WriteLineAsync("Subscribing MyAsbNotQuiteSoVeryVeryVeryVeryLongEvent").ConfigureAwait(false);
                                                                                      await s.Subscribe<MyAsbNotQuiteSoVeryVeryVeryVeryLongEvent>().ConfigureAwait(false);
                                                                                      await Console.Out.WriteLineAsync("Subscribing MyAsbVeryVeryVeryVeryVeryVeryVeryVeryVeryVeryVeryVeryLongEvent").ConfigureAwait(false);
                                                                                      await s.Subscribe<MyAsbVeryVeryVeryVeryVeryVeryVeryVeryVeryVeryVeryVeryLongEvent>().ConfigureAwait(false);
                                                                                  }))
                                       .Done(c => c.ShortEventDelivered && c.LongEventDelivered)
                                       .Run(TimeSpan.FromSeconds(10));

            Assert.IsTrue(result.ShortEventDelivered);
            Assert.IsTrue(result.LongEventDelivered);
        }

        [Test]
        public async Task It_should_subscribe_and_deliver_the_message_even_if_eventname_too_long_with_rule()
        {
            var result = await Scenario.Define<Context>()
                                       .WithRouter("Router", (c, cfg) =>
                                                             {
                                                                 var leftIface = cfg.AddInterface<TestTransport>("Left", t =>
                                                                                                                         {
                                                                                                                             t.BrokerAlpha();
                                                                                                                             var settings = t.GetSettings();

                                                                                                                             var builder = new ConventionsBuilder(settings);
                                                                                                                             builder.DefiningEventsAs(EventConvention);
                                                                                                                             settings.Set(builder.Conventions);

                                                                                                                         }).InMemorySubscriptions();
                                                                 leftIface.LimitMessageProcessingConcurrencyTo(1); //To ensure when tracer arrives the subscribe request has already been processed.;
                                                                 cfg.AddInterface<AzureServiceBusTransport>("Right", t =>
                                                                                                                     {
                                                                                                                         var connString = Environment.GetEnvironmentVariable("AzureServiceBus.ConnectionString");
                                                                                                                         t.ConnectionString(connString);
                                                                                                                         var settings = t.GetSettings();
                                                                                                                         t.Transactions(TransportTransactionMode.ReceiveOnly);

                                                                                                                         var builder = new ConventionsBuilder(settings);
                                                                                                                         builder.DefiningEventsAs(EventConvention);
                                                                                                                         settings.Set(builder.Conventions);

                                                                                                                         t.RuleNameShortener(PublisherAsAsb.RuleNameShortener);
                                                                                                                         t.SubscriptionNameShortener(PublisherAsAsb.RuleNameShortener);

                                                                                                                         var serializer = Tuple.Create(new NewtonsoftSerializer() as SerializationDefinition, new SettingsHolder());
                                                                                                                         settings.Set("MainSerializer", serializer);
                                                                                                                     });
                                                                 cfg.UseStaticRoutingProtocol().AddForwardRoute("Left", "Right");
                                                                 cfg.UseStaticRoutingProtocol().AddForwardRoute("Right", "Left");
                                                             })
                                       .WithEndpoint<PublisherAsAsb>(c => c
                                                                          .When(async (s, context) =>
                                                                                {
                                                                                    await s.ScheduleEvery(TimeSpan.FromSeconds(5), ctx =>
                                                                                                                                   {
                                                                                                                                       context.ShortEventSubscribed = true;
                                                                                                                                       context.LongEventSubscribed = true;
                                                                                                                                       return Task.CompletedTask;
                                                                                                                                   });
                                                                                })

                                                                          .When(x => x.ShortEventSubscribed, async s =>
                                                                                                             {
                                                                                                                 await Console.Out.WriteLineAsync("Publishing MyAsbNotQuiteSoVeryVeryVeryVeryLongEvent").ConfigureAwait(false);
                                                                                                                 await s.Publish(new MyAsbNotQuiteSoVeryVeryVeryVeryLongEvent()).ConfigureAwait(false);
                                                                                                             })
                                                                          .When(x => x.LongEventSubscribed, async s =>
                                                                                                            {
                                                                                                                await Console.Out.WriteLineAsync("Publishing MyAsbVeryVeryVeryVeryVeryVeryVeryVeryVeryVeryVeryVeryLongEvent").ConfigureAwait(false);
                                                                                                                await s.Publish(new MyAsbVeryVeryVeryVeryVeryVeryVeryVeryVeryVeryVeryVeryLongEvent()).ConfigureAwait(false);
                                                                                                            }))

                                       .WithEndpoint<SubscriberToAsb>(c => c.When(async (s, context) =>
                                                                                  {
                                                                                      await Console.Out.WriteLineAsync("Subscribing MyAsbNotQuiteSoVeryVeryVeryVeryLongEvent").ConfigureAwait(false);
                                                                                      await s.Subscribe<MyAsbNotQuiteSoVeryVeryVeryVeryLongEvent>().ConfigureAwait(false);
                                                                                      await Console.Out.WriteLineAsync("Subscribing MyAsbVeryVeryVeryVeryVeryVeryVeryVeryVeryVeryVeryVeryLongEvent").ConfigureAwait(false);
                                                                                      await s.Subscribe<MyAsbVeryVeryVeryVeryVeryVeryVeryVeryVeryVeryVeryVeryLongEvent>().ConfigureAwait(false);
                                                                                  }))
                                       .Done(c => c.ShortEventDelivered && c.LongEventDelivered)
                                       .Run(TimeSpan.FromSeconds(10));

            Assert.IsTrue(result.ShortEventDelivered);
            Assert.IsTrue(result.LongEventDelivered);
        }

        static bool EventConvention(Type x)
        {
            return x.Namespace == "Events";
        }

        class Context : ScenarioContext
        {
            public bool LongEventDelivered { get; set; }
            public bool ShortEventDelivered { get; set; }
            public bool ShortEventSubscribed { get; set; }
            public bool LongEventSubscribed { get; set; }
        }

        class PublisherAsAsb : EndpointConfigurationBuilder
        {
            public PublisherAsAsb()
            {
                EndpointSetup<DefaultServer>(c =>
                                             {
                                                 var connString = Environment.GetEnvironmentVariable("AzureServiceBus.ConnectionString");
                                                 var transport = c.UseTransport<AzureServiceBusTransport>();
                                                 transport.ConnectionString(connString);
                                                 c.Conventions().DefiningEventsAs(EventConvention);
                                                 transport.RuleNameShortener(RuleNameShortener);
                                                 transport.SubscriptionNameShortener(RuleNameShortener);
                                             }); //.CustomEndpointName("PublisherWithAVeryVeryVeryVeryVeryVeryVeryVeryVeryVeryLongName");
            }

            public static string RuleNameShortener(string arg)
            {
                var result = arg;

                var regex = new Regex("Very");

                if (arg.Length > 50)
                {
                    while (result.Length > 50)
                    {
                        result = regex.Replace(result, "", 1);
                    }
                }

                return result;
            }

        }

        class SubscriberToAsb : EndpointConfigurationBuilder
        {
            public SubscriberToAsb()
            {
                EndpointSetup<DefaultServer>(c =>
                                             {
                                                 c.DisableFeature<AutoSubscribe>();

                                                 var transport = c.UseTransport<TestTransport>().BrokerAlpha();
                                                 c.LimitMessageProcessingConcurrencyTo(1);
                                                 c.Conventions().DefiningEventsAs(EventConvention);

                                                 var router = transport.Routing().ConnectToRouter("Router");
                                                 router.RegisterPublisher(typeof(MyAsbVeryVeryVeryVeryVeryVeryVeryVeryVeryVeryVeryVeryLongEvent), PublisherEndpointName);
                                                 router.RegisterPublisher(typeof(MyAsbNotQuiteSoVeryVeryVeryVeryLongEvent), PublisherEndpointName);


                                             }).CustomEndpointName("SubscriberWithAVeryVeryVeryVeryVeryVeryVeryVeryVeryVeryLongName");
            }

            class MyAsbVeryVeryVeryVeryVeryLongNamedEventHandler : IHandleMessages<MyAsbVeryVeryVeryVeryVeryVeryVeryVeryVeryVeryVeryVeryLongEvent>,
                                                                   IHandleMessages<MyAsbNotQuiteSoVeryVeryVeryVeryLongEvent>
            {
                Context scenarioContext;

                public MyAsbVeryVeryVeryVeryVeryLongNamedEventHandler(Context scenarioContext)
                {
                    this.scenarioContext = scenarioContext;
                }

                public Task Handle(MyAsbVeryVeryVeryVeryVeryVeryVeryVeryVeryVeryVeryVeryLongEvent message, IMessageHandlerContext context)
                {
                    Console.Out.WriteLine("MyAsbVeryVeryVeryVeryVeryVeryVeryVeryVeryVeryVeryVeryLongEvent delivered");
                    scenarioContext.LongEventDelivered = true;
                    return Task.CompletedTask;
                }

                public Task Handle(MyAsbNotQuiteSoVeryVeryVeryVeryLongEvent message, IMessageHandlerContext context)
                {
                    Console.Out.WriteLine("MyAsbNotQuiteSoVeryVeryVeryVeryLongEvent delivered");
                    scenarioContext.ShortEventDelivered = true;
                    return Task.CompletedTask;
                }
            }
        }
    }
}
#endif
