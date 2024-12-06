using System.Diagnostics;

using var fs = File.OpenRead("/Users/eitan/development/1brc.data/measurements-1000000000.txt");
using var reader = new StreamReader(fs);

var sw = new Stopwatch();
var map = new Dictionary<string, CityState>();
var lineCount = 0;
while (reader.ReadLine() is { } line)
{
    if (++lineCount % 100_000_000 == 0)
    {
        Console.WriteLine(lineCount);
    } 
    var parts = line.Split(';');
    var (city, measurement) = (parts[0], parts[1]);
    var value = float.Parse(measurement);
    if (!map.TryGetValue(city, out var state))
    {
        state = new CityState { min = value, max = value, sum = value, count = 1 };
        map[city] = state;
        continue;
    }

    state.min = Math.Min(state.min, value);
    state.max = Math.Max(state.max, value);
    state.sum += value;
    state.count++;
    
}

Console.WriteLine(sw.Elapsed);

class CityState
{
    public float min;
    public double sum;
    public float max;
    public int count;
}