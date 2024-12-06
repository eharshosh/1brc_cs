using System.Diagnostics;
using System.Text;
using System.Text.RegularExpressions;

namespace _1brc;

internal static class Program
{
    public static void Main(string[] args)
    {
        const string dataFilename = "/Users/eitan/development/1brc.data/measurements-1000000000.txt";
        const string compareToFilename = "/Users/eitan/development/1brc.data/measurements-1000000000.out";

// const string dataFilename = "/Users/eitan/development/1brc.data/measurements-10000000.txt";
// const string compareToFilename = "/Users/eitan/development/1brc.data/measurements-10000000.out";

        var sw = Stopwatch.StartNew();
        var map = new Dictionary<string, CityState>();
        var alternateLookup = map.GetAlternateLookup<ReadOnlySpan<char>>();
        var lineCount = 0;

        using var fs = File.OpenRead(dataFilename);
        using var reader = new StreamReader(fs, Encoding.UTF8);

        while (reader.ReadLine() is { } line)
        {
            if (++lineCount % 100_000_000 == 0)
            {
                Console.WriteLine($"{lineCount}@{sw.Elapsed}");
            }

            var lineSpan = line.AsSpan();
            var semiColonIndex = lineSpan.IndexOf(';');
            var city = lineSpan[..semiColonIndex];
            var measurement = lineSpan[(semiColonIndex + 1)..];
            var dotIndex = measurement.IndexOf('.');
            long factor = (int)Math.Pow(10, measurement.Length - dotIndex);
            var signFactor = measurement[0] == '-' ? -1 : 1;
            var value = (int)(int.Parse(measurement[..dotIndex]) * 1000
                                + int.Parse(measurement[(dotIndex + 1)..]) * factor * signFactor);
            if (!alternateLookup.TryGetValue(city, out var state))
            {
                state = new CityState { Min = value, Max = value, Sum = value, Count = 1 };
                alternateLookup[city] = state;
                continue;
            }

            state.Min = Math.Min(state.Min, value);
            state.Max = Math.Max(state.Max, value);
            state.Sum += value;
            state.Count++;
        }

        var outputName = Path.GetFileNameWithoutExtension(dataFilename) + "_ours.out";
        var outFile = File.OpenWrite(outputName);
        var writer = new StreamWriter(outFile);
        var output = new List<OutputLine>(map.Keys.Count);
        foreach (var (key, state) in map.OrderBy(kvp => kvp.Key))
        {
            var avg = Math.Round((double)state.Sum / state.Count / 100) / 10;
            var min = Math.Round((double)state.Min / 100) / 10;
            var max = Math.Round((double)state.Max / 100) / 10;
            writer.WriteLine(key + ";" + min + ";" + avg + ";" + max);
            output.Add(new(key, (float)min, (float)avg, (float)max));
        }

        Console.WriteLine(sw.Elapsed);


        var outputSorted = output.ToDictionary(x => x.City);
        var compareTo = ReadCompareTo(compareToFilename);
        foreach (var (city, item) in compareTo)
        {
            if (!outputSorted.TryGetValue(city, out var outputItem))
            {
                Console.WriteLine(city + " not found");
                continue;
            }

            if (item != outputItem)
            {
                Console.WriteLine($"{city}: {item} != {outputItem}");
            }
        }
    }

    static Dictionary<string, OutputLine> ReadCompareTo(string filename)
    {
        var rawInput = File.ReadAllText(filename)[1..^2];
        return Regex
            .Split(rawInput, @"([\w\s\(\),\.'\-]+)=([\-\.\d]+)/([\-\.\d]+)/([\-\.\d]+)(, )?",
                RegexOptions.CultureInvariant)
            .Chunk(6)
            .Select(chunk =>
                new OutputLine(chunk[1], float.Parse(chunk[2]), float.Parse(chunk[3]), float.Parse(chunk[4])))
            .ToDictionary(x => x.City);
    }

    class CityState
    {
        public int Min;
        public long Sum;
        public int Max;
        public int Count;
    }

    record OutputLine(string City, float Min, float Average, float Max);
}