using System;
using System.Collections.Generic;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using Microsoft.Xrm.Sdk;

namespace AviationWeatherDataProvider.Models
{
    public class MetarModel
    {
        public class Metar
        {
            [JsonPropertyName("metar_id")]
            public long? MetarId { get; set; }

            [JsonPropertyName("icaoId")]
            public string IcaoId { get; set; }

            [JsonPropertyName("receiptTime")]
            public string ReceiptTime { get; set; }

            [JsonPropertyName("obsTime")]
            public long? ObsTime { get; set; }

            [JsonPropertyName("reportTime")]
            public string ReportTime { get; set; }

            [JsonPropertyName("temp")]
            public float? Temp { get; set; }

            [JsonPropertyName("dewp")]
            public float? Dewp { get; set; }

            [JsonPropertyName("wdir")]
            [JsonConverter(typeof(NumberStringConverter))]
            public string Wdir { get; set; }

            [JsonPropertyName("wspd")]
            public int? Wspd { get; set; }

            [JsonPropertyName("wgst")]
            public double? Wgst { get; set; }

            [JsonPropertyName("visib")]
            [JsonConverter(typeof(NumberStringConverter))]
            public string Visib { get; set; }

            [JsonPropertyName("altim")]
            public float? Altim { get; set; }

            [JsonPropertyName("slp")]
            public double? Slp { get; set; }

            [JsonPropertyName("qcField")]
            public int? QcField { get; set; }

            [JsonPropertyName("wxString")]
            public string WxString { get; set; }

            [JsonPropertyName("presTend")]
            public double? PresTend { get; set; }

            [JsonPropertyName("maxT")]
            public double? MaxT { get; set; }

            [JsonPropertyName("minT")]
            public double? MinT { get; set; }

            [JsonPropertyName("maxT24")]
            public double? MaxT24 { get; set; }

            [JsonPropertyName("minT24")]
            public double? MinT24 { get; set; }

            [JsonPropertyName("precip")]
            public double? Precip { get; set; }

            [JsonPropertyName("pcp3hr")]
            public double? Pcp3hr { get; set; }

            [JsonPropertyName("pcp6hr")]
            public double? Pcp6hr { get; set; }

            [JsonPropertyName("pcp24hr")]
            public double? Pcp24hr { get; set; }

            [JsonPropertyName("snow")]
            public double? Snow { get; set; }

            [JsonPropertyName("vertVis")]
            public double? VertVis { get; set; }

            [JsonPropertyName("metarType")]
            public string MetarType { get; set; }

            [JsonPropertyName("rawOb")]
            public string RawOb { get; set; }

            [JsonPropertyName("mostRecent")]
            public int MostRecent { get; set; }

            [JsonPropertyName("lat")]
            public float? Lat { get; set; }

            [JsonPropertyName("lon")]
            public float? Lon { get; set; }

            [JsonPropertyName("elev")]
            public int? Elev { get; set; }

            [JsonPropertyName("prior")]
            public int? Prior { get; set; }

            [JsonPropertyName("name")]
            public string Name { get; set; }

            [JsonPropertyName("clouds")]
            public List<Cloud> Clouds { get; set; }

            [JsonPropertyName("rawTaf")]
            public string RawTaf { get; set; }

            public Entity ToEntity(ITracingService tracer)
            {
                Entity entity = new Entity("awx_metar");
                if (!DateTime.TryParse(ReportTime, out DateTime reportDateTime))
                {
                    tracer.Trace("ReportTime ({0}) is not a valid DateTime.", ReportTime);
                    throw new InvalidCastException("ReportTime is not a valid DateTime.");
                }
                var id = Helpers.GenerateGuidFromText(IcaoId, reportDateTime);
                entity["awx_metarid"] = id;
                entity["awx_rawtext"] = RawOb;
                entity["awx_name"] = IcaoId;
                entity["awx_stationname"] = Name;
                entity["awx_observationtime"] = reportDateTime;
                entity["awx_latitude"] = Lat;
                entity["awx_longitude"] = Lon;
                entity["awx_temp_c"] = Temp;
                entity["awx_dewpoint_c"] = Dewp;
                entity["awx_wind_dir_degrees"] = Wdir;
                entity["awx_wind_speed_kt"] = Wspd;
                entity["awx_visibility_statute_mi"] = Visib;
                entity["awx_altim_in_hg"] = Altim;
                entity["awx_taf"] = RawTaf;
                entity["awx_elevation_m"] = Elev;
                if (Clouds != null && Clouds.Count > 0)
                {
                    var cloudDescriptions = new StringBuilder();
                    foreach (var cloud in Clouds)
                    {
                        cloudDescriptions.AppendLine(
                            $"{cloud.Cover}{(cloud.Base == null ? string.Empty : $" at {cloud.Base}")}"
                        );
                    }
                    entity["awx_clouds"] = cloudDescriptions.ToString().TrimEnd();
                }
                else
                {
                    entity["awx_clouds"] = string.Empty;
                }

                return entity;
            }
        }

        public class Cloud
        {
            [JsonPropertyName("cover")]
            public string Cover { get; set; }

            [JsonPropertyName("base")]
            public int? Base { get; set; }
        }

        public static Guid GenerateGuidFromText(string stationId, DateTime observationTime)
        {
            // Ensure the stationId is 4-5 characters
            if (stationId.Length < 4 || stationId.Length > 5)
            {
                throw new ArgumentException("StationId must be 4 or 5 characters long.");
            }

            // Format the DateTime to a fixed-length string
            string formattedTime = observationTime.ToString("yyyyMMddHHmm");

            // Combine the StationId and formatted DateTime
            string combinedText = stationId + formattedTime;

            // Ensure the combined length is 16 characters or less
            if (combinedText.Length > 16)
            {
                throw new ArgumentException(
                    $"Combined text {combinedText} is too long to convert to a GUID."
                );
            }

            byte[] textBytes = Encoding.UTF8.GetBytes(combinedText);
            byte[] guidBytes = new byte[16];
            Array.Copy(textBytes, guidBytes, textBytes.Length);
            return new Guid(guidBytes);
        }

        public static (string StationId, DateTime ObservationTime) ParseGuidToText(
            Guid guid,
            ITracingService tracer = null
        )
        {
            byte[] guidBytes = guid.ToByteArray();
            string combinedText = Encoding.UTF8.GetString(guidBytes).TrimEnd('\0');

            // Extract the StationId and DateTime parts
            string stationId = combinedText.Substring(0, 4); // Assuming StationId is always 4 characters
            string dateTimePart = combinedText.Substring(4);

            if (tracer != null)
            {
                tracer.Trace("Parsing GUID to text...");
                tracer.Trace(guid.ToString());
                tracer.Trace(combinedText);
                tracer.Trace(stationId);
                tracer.Trace(dateTimePart);
            }

            DateTime observationTime = DateTime.ParseExact(
                dateTimePart + "00",
                "yyyyMMddHHmmss",
                null
            );
            return (stationId, observationTime);
        }
    }

    internal class NumberStringConverter : JsonConverter<string>
    {
        public override string Read(
            ref Utf8JsonReader reader,
            Type typeToConvert,
            JsonSerializerOptions options
        )
        {
            if (reader.TokenType == JsonTokenType.Number)
            {
                try
                {
                    return reader.GetInt32().ToString();
                }
                catch (FormatException)
                {
                    return reader.GetDouble().ToString();
                }
            }
            else if (reader.TokenType == JsonTokenType.String)
            {
                return reader.GetString();
            }
            throw new JsonException("Invalid token type for wind direction");
        }

        public override void Write(
            Utf8JsonWriter writer,
            string value,
            JsonSerializerOptions options
        )
        {
            writer.WriteStringValue(value);
        }
    }

    public static class Helpers
    {
        public static Guid GenerateGuidFromText(string stationId, DateTime observationTime)
        {
            // Ensure the stationId is 4-5 characters
            if (stationId.Length < 4 || stationId.Length > 5)
            {
                throw new ArgumentException("StationId must be 4 or 5 characters long.");
            }

            // Format the DateTime to a fixed-length string
            string formattedTime = observationTime.ToString("yyyyMMddHHmm");

            // Combine the StationId and formatted DateTime
            string combinedText = stationId + formattedTime;

            // Ensure the combined length is 16 characters or less
            if (combinedText.Length > 16)
            {
                throw new ArgumentException(
                    $"Combined text {combinedText} is too long to convert to a GUID."
                );
            }

            byte[] textBytes = Encoding.UTF8.GetBytes(combinedText);
            byte[] guidBytes = new byte[16];
            Array.Copy(textBytes, guidBytes, textBytes.Length);
            return new Guid(guidBytes);
        }

        public static (string StationId, DateTime ObservationTime) ParseGuidToText(
            Guid guid,
            ITracingService tracer = null
        )
        {
            byte[] guidBytes = guid.ToByteArray();
            string combinedText = Encoding.UTF8.GetString(guidBytes).TrimEnd('\0');

            // Extract the StationId and DateTime parts
            string stationId = combinedText.Substring(0, 4); // Assuming StationId is always 4 characters
            string dateTimePart = combinedText.Substring(4);

            if (tracer != null)
            {
                tracer.Trace("Parsing GUID to text...");
                tracer.Trace(guid.ToString());
                tracer.Trace(combinedText);
                tracer.Trace(stationId);
                tracer.Trace(dateTimePart);
            }

            DateTime observationTime = DateTime.ParseExact(
                dateTimePart + "00",
                "yyyyMMddHHmmss",
                null
            );
            return (stationId, observationTime);
        }
    }
}
