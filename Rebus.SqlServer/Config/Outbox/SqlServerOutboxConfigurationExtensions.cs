﻿using System;
using Rebus.Logging;
using Rebus.Pipeline;
using Rebus.Retry.Simple;
using Rebus.SqlServer.Outbox;
using Rebus.Threading;
using Rebus.Transport;

namespace Rebus.Config.Outbox;

/// <summary>
/// Configuration extensions for the experimental outbox support
/// </summary>
public static class SqlServerOutboxConfigurationExtensions
{
    /// <summary>
    /// Configures Rebus to use an outbox with default options.
    /// This will store a (message ID, source queue) tuple for all processed messages, and under this tuple any messages sent/published will
    /// also be stored, thus enabling truly idempotent message processing.
    /// </summary>
    public static RebusConfigurer Outbox(this RebusConfigurer configurer, Action<StandardConfigurer<IOutboxStorage>> configure)
        => Outbox(configurer, configure, new OutboxOptions());
        
    /// <summary>
    /// Configures Rebus to use an outbox with given options.
    /// This will store a (message ID, source queue) tuple for all processed messages, and under this tuple any messages sent/published will
    /// also be stored, thus enabling truly idempotent message processing.
    /// </summary>
    public static RebusConfigurer Outbox(this RebusConfigurer configurer, Action<StandardConfigurer<IOutboxStorage>> configure, OutboxOptions options)
    {
        if (configurer == null) throw new ArgumentNullException(nameof(configurer));
        if (configure == null) throw new ArgumentNullException(nameof(configure));
        if (options == null) throw new ArgumentNullException(nameof(options));

        configurer.Options(o =>
        {
            configure(StandardConfigurer<IOutboxStorage>.GetConfigurerFrom(o));

            // if no outbox storage was registered, no further calls must have been made... that's ok, so we just bail out here
            if (!o.Has<IOutboxStorage>()) return;

            o.Decorate<ITransport>(c => new OutboxClientTransportDecorator(c.Get<ITransport>(), c.Get<IOutboxStorage>()));

            o.Register(c =>
            {
                var asyncTaskFactory = c.Get<IAsyncTaskFactory>();
                var rebusLoggerFactory = c.Get<IRebusLoggerFactory>();
                var outboxStorage = c.Get<IOutboxStorage>();
                var transport = c.Get<ITransport>();
                return new OutboxForwarder(asyncTaskFactory, rebusLoggerFactory, outboxStorage, transport, options.ForwarderIntervalSeconds);
            });

            o.Decorate(c =>
            {
                _ = c.Get<OutboxForwarder>();
                return c.Get<Options>();
            });

            o.Decorate<IPipeline>(c =>
            {
                var pipeline = c.Get<IPipeline>();
                var outboxConnectionProvider = c.Get<IOutboxConnectionProvider>();
                var step = new OutboxIncomingStep(outboxConnectionProvider);
                return new PipelineStepInjector(pipeline)
                    .OnReceive(step, PipelineRelativePosition.After, typeof(DefaultRetryStep));
            });
        });

        return configurer;
    }
}
