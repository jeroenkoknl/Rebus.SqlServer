using System;
using Rebus.SqlServer.Outbox;

namespace Rebus.Config.Outbox;

/// <summary>
/// Options for the outbox
/// </summary>
public class OutboxOptions
{
    private int _forwarderIntervalSeconds = OutboxForwarder.DefaultForwarderIntervalSeconds;

    /// <summary>
    /// Gets or sets the interval in seconds between each outbox message forwarder run
    /// </summary>
    /// <exception cref="ArgumentOutOfRangeException">Thrown when the interval is set to less than 1 second.</exception>
    public int ForwarderIntervalSeconds
    {
        get => _forwarderIntervalSeconds;
        set
        {
            if (value < 1)
            {
                throw new ArgumentOutOfRangeException(nameof(value), "Forwarder interval must be at least 1 second");
            }
            _forwarderIntervalSeconds = value;
        }
    }
}