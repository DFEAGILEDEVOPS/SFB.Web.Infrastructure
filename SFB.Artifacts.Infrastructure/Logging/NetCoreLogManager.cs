using System;
using System.Collections.Generic;
using Microsoft.ApplicationInsights;

namespace SFB.Web.Infrastructure.Logging
{
    // ReSharper disable once UnusedType.Global
    public class NetCoreLogManager : ILogManager
    {
        private readonly string _enableAiTelemetry;
        
        public NetCoreLogManager(string enableAiTelemetry)
        {
            _enableAiTelemetry = enableAiTelemetry;
        }

        public void LogException(Exception exception, string errorMessage)
        {            
            //if (exception is Newtonsoft.Json.JsonSerializationException || exception is Newtonsoft.Json.JsonReaderException)
            //{
                if (_enableAiTelemetry != null && bool.Parse(_enableAiTelemetry))
                {
                    #pragma warning disable CS0618 // Type or member is obsolete
                    var ai = new TelemetryClient();
                    #pragma warning restore CS0618 // Type or member is obsolete
                    ai.TrackException(exception);                    
                    //ai.TrackTrace($"URL: {_httpContextAccessor.HttpContext.Request.Path}");
                    ai.TrackTrace($"Data error message: {errorMessage}");
                    //ai.TrackTrace($"FORM VARIABLES: {_httpContextAccessor.HttpContext.Request.Form}");
                    //var schoolBmCookie = _httpContextAccessor.HttpContext.Request.Cookies[CookieNames.COMPARISON_LIST];
                    //if (schoolBmCookie != null)
                    //{
                    //    ai.TrackTrace($"SCHOOL BM COOKIE: {schoolBmCookie}");
                    //}
                    //var matBmCookie = _httpContextAccessor.HttpContext.Request.Cookies[CookieNames.COMPARISON_LIST_MAT];
                    //if (matBmCookie != null)
                    //{
                    //    ai.TrackTrace($"TRUST BM COOKIE: {matBmCookie}");
                    //}
            }
            //}
        }

        public void LogEvent(string eventName, IDictionary<string, string> properties = null,
            IDictionary<string, double> metrics = null)
        {
            if (_enableAiTelemetry == null || !bool.Parse(_enableAiTelemetry))
            {
                return;
            }
            
#pragma warning disable CS0618 // Type or member is obsolete
            var ai = new TelemetryClient();
#pragma warning restore CS0618 // Type or member is obsolete
            ai.TrackEvent(eventName, properties, metrics);
        }
    }
}
