using SFB.Web.Infrastructure.Logging;
using System;
using System.Collections.Generic;
using System.Diagnostics;

namespace SFB.Web.Infrastructure.Repositories
{
    public abstract class AppInsightsLoggable
    {
        private readonly ILogManager _logManager;

        protected AppInsightsLoggable(ILogManager logManager)
        {
            _logManager = logManager;
        }

        protected void LogException(Exception exception, string errorMessage)
        {
            Debugger.Break();

            _logManager.LogException(exception, errorMessage);
        }

        protected void LogEvent(string eventName, IDictionary<string, string> properties = null, 
            IDictionary<string, double> metrics = null)
        {
            _logManager.LogEvent(eventName, properties, metrics);
        }
    }
}
