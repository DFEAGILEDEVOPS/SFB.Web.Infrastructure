using System;
using System.Collections.Generic;

namespace SFB.Web.Infrastructure.Logging
{
    public interface ILogManager
    {
        void LogException(Exception exception, string errorMessage);

        void LogEvent(string eventName, IDictionary<string, string> properties = null, 
            IDictionary<string, double> metrics = null);
    }
}
