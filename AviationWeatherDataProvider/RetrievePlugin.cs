using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Text.Json;
using AviationWeatherDataProvider.Models;
using Microsoft.Xrm.Sdk;

namespace AviationWeatherDataProvider
{
    public class RetrievePlugin : PluginBase
    {
        public RetrievePlugin()
            : base(typeof(RetrievePlugin)) { }

        protected override void ExecuteCdsPlugin(ILocalPluginContext localPluginContext)
        {
            var context = localPluginContext.PluginExecutionContext;
            var sysService = localPluginContext.SystemUserService;
            var tracer = localPluginContext.TracingService;

            Entity entity = null;

            try
            {
                var guid = context.PrimaryEntityId;

                var stationTime = Helpers.ParseGuidToText(guid, tracer);
                tracer.Trace($"Station Id: {stationTime.StationId}");
                tracer.Trace($"Time: {stationTime.ObservationTime}");

                var dateString = ConvertDateTimeToString(stationTime.ObservationTime);
                var stationId = stationTime.StationId;

                if (
                    !(
                        WebRequest.Create(
                            $"https://aviationweather.gov/api/data/metar?ids={stationId}&format=json&taf=true&date={dateString}"
                        )
                        is HttpWebRequest webRequest
                    )
                )
                {
                    return;
                }
                webRequest.ContentType = "application/json";
                using (var stream = webRequest.GetResponse().GetResponseStream())
                {
                    using (var streamReader = new StreamReader(stream))
                    {
                        var metarAsJson = streamReader.ReadToEnd();
                        var metarModels = JsonSerializer.Deserialize<List<MetarModel.Metar>>(
                            metarAsJson
                        );
                        var metar = metarModels.FirstOrDefault();
                        if (metar != null)
                        {
                            entity = metar.ToEntity(tracer);
                        }
                    }
                }

                var oneHourFuture = stationTime.ObservationTime.AddHours(1);
                dateString = ConvertDateTimeToString(oneHourFuture);

                if (
                    !(
                        WebRequest.Create(
                            $"https://aviationweather.gov/api/data/pirep?id={stationId}&date={dateString}&age=2&distance=50"
                        )
                        is HttpWebRequest pirepWebRequest
                    )
                )
                {
                    return;
                }
                pirepWebRequest.ContentType = "application/json";
                using (var stream = pirepWebRequest.GetResponse().GetResponseStream())
                {
                    using (var streamReader = new StreamReader(stream))
                    {
                        var pireps = streamReader.ReadToEnd();
                        if (!string.IsNullOrEmpty(pireps))
                        {
                            entity["awx_pireps"] = pireps;
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                tracer.Trace(ex.ToString());
                entity = null;
            }

            if (entity != null)
            {
                context.OutputParameters["BusinessEntity"] = entity;
            }
        }

        public static string ConvertDateTimeToString(DateTime dateTime)
        {
            // Format the date into the desired string format
            string formattedDateString = dateTime.ToString("yyyyMMdd_HHmmss") + "Z";

            return formattedDateString;
        }
    }
}
