namespace AviationWeatherDataProvider
{
    public static class Utils
    {
        public static float? ConvertToCelsius(float? fahrenheit)
        {
            if (fahrenheit == null)
            {
                return null;
            }
            return (fahrenheit - 32) * 5 / 9;
        }

        public static float? ConvertToFahrenheit(float? celsius)
        {
            if (celsius == null)
            {
                return null;
            }
            return celsius * 9 / 5 + 32;
        }

        public static float? MillibarsToInOfMercury(float? millibars)
        {
            if (millibars == null)
            {
                return null;
            }
            return (float)System.Math.Round((double)(millibars * 0.02953f), 2);
        }

        public static float? InOfMercuryToMillibars(float? inOfMercury)
        {
            if (inOfMercury == null)
            {
                return null;
            }
            return inOfMercury / 0.02953f;
        }

        public static int? MetersToFeet(int? meters)
        {
            if (meters == null)
            {
                return null;
            }
            return (int)(meters * 3.28084);
        }
    }
}
