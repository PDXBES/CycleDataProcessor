using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;

namespace CycleDataProcessor
{
  class Program
  {
    /*  SQL used to generate input file:
        select station.station_name, pump_number, pump_rate_gpm, cycle_change_time, onoff_state
        from station join pump on station.station_id = pump.station_id join cycle_data on pump.pump_id = cycle_data.pump_id
        where station.station_name like '%Tryon%'
        and cycle_change_time > '1999-01-01'
        order by cycle_change_time
     */
    static void Main(string[] args)
    {
      string inputFile = @"C:\temp\tryon_ps_cycle_data.rpt";
      string outputFile = @"C:\temp\tryon_ps_effluent.csv";


      DateTime startTime = new DateTime(1999, 1, 1, 0, 0, 0);
      DateTime endTime = new DateTime(2012, 5, 31, 23, 55, 0);
      TimeSpan timeSpan = new TimeSpan(1, 0, 0);

      DateTime currentTime = startTime;

      TextReader reader = new StreamReader(inputFile);

      List<CycleDataEntry> cycleEntries = new List<CycleDataEntry>();

      reader.ReadLine();
      reader.ReadLine();
      string line = reader.ReadLine();
      while (line != String.Empty)
      {
        string[] tokens = line.Split(new string[1] { " " }, StringSplitOptions.RemoveEmptyEntries);

        string stationName = "";
        int i, pumpNumber;

        for (i = 0; i < tokens.Length; i++)
        {
          if (!Int32.TryParse(tokens[i], out pumpNumber))
            stationName += tokens[i];
          else break;
        }
        pumpNumber = Int32.Parse(tokens[i++]);
        double pumpRate = Double.Parse(tokens[i++]);
        DateTime date = DateTime.Parse(tokens[i++] + " " + tokens[i++]);
        bool onOff = tokens[i] == "1" ? true : false;

        cycleEntries.Add(new CycleDataEntry(date, stationName, pumpNumber, pumpRate, onOff));

        line = reader.ReadLine();
      }

      CycleStateCollection cycleCollection =
        new CycleStateCollection(cycleEntries, startTime, endTime, timeSpan);

      cycleCollection.WriteFile(outputFile);

    }

  }

  internal class CycleDataEntry
  {
    public DateTime cycleChangeTime;
    public string station;
    public int pumpNumber;
    public double pumpRate;
    public bool onOff;

    Dictionary<int, bool> pumpRunning = new Dictionary<int, bool>();

    public CycleDataEntry(DateTime cycleChangeTime, string station, int pumpNumber, double pumpRate, bool onOff)
    {
      this.cycleChangeTime = cycleChangeTime;
      this.station = station;
      this.pumpNumber = pumpNumber;
      this.pumpRate = pumpRate;
      this.onOff = onOff;
    }
  }

  internal class CycleStateCollection
  {
    Dictionary<int, Dictionary<DateTime, PumpState>> allPumpStates;
    DateTime startTime, endTime;
    TimeSpan timeSpan;

    public CycleStateCollection(List<CycleDataEntry> cycleEntries, DateTime startTime, DateTime endTime, TimeSpan timeSpan)
    {
      this.startTime = startTime;
      this.endTime = endTime;
      this.timeSpan = timeSpan;

      DateTime currentTime = startTime;
      DateTime previousTime = currentTime;

      allPumpStates = new Dictionary<int, Dictionary<DateTime, PumpState>>();

      int entryIndex = 0;
      while (currentTime < endTime)
      {

        foreach (Dictionary<DateTime, PumpState> pumpStates in allPumpStates.Values)
        {
          PumpState assumedState = new PumpState();
          assumedState.pumpOn = pumpStates[previousTime].pumpOn;
          assumedState.pumpRate = pumpStates[previousTime].pumpRate;
          assumedState.runTime = assumedState.pumpOn ? timeSpan : new TimeSpan();
          pumpStates.Add(currentTime, assumedState);
        }

        for (int i = entryIndex; i < cycleEntries.Count; i++)
        {
          CycleDataEntry currentCycleEntry = cycleEntries[i];

          if (currentTime < currentCycleEntry.cycleChangeTime)
            break;

          Dictionary<DateTime, PumpState> pumpStates;

          int currentPumpNumber = currentCycleEntry.pumpNumber;

          if (!allPumpStates.ContainsKey(currentPumpNumber))
            allPumpStates.Add(currentPumpNumber, new Dictionary<DateTime, PumpState>());
          pumpStates = allPumpStates[currentPumpNumber];

          PumpState currentState;

          if (!pumpStates.ContainsKey(currentTime))
            pumpStates.Add(currentTime, new PumpState());
          currentState = pumpStates[currentTime];

          bool previousPumpOn = currentState.pumpOn;
          currentState.pumpOn = currentCycleEntry.onOff;
          currentState.pumpRate = currentCycleEntry.pumpRate;

          if (currentState.pumpOn && previousPumpOn)
            currentState.runTime = timeSpan;
          else if (currentState.pumpOn && !previousPumpOn)
            currentState.runTime += (currentTime - currentCycleEntry.cycleChangeTime);
          else if (!currentState.pumpOn && previousPumpOn)
            currentState.runTime -= (currentTime - currentCycleEntry.cycleChangeTime);
          else if (!currentState.pumpOn && !previousPumpOn)
            currentState.runTime = new TimeSpan();
          else
            throw new Exception("Error runTime");

          if (currentState.runTime > timeSpan || currentState.runTime.Ticks < 0)
            throw new Exception("Error runTime");

          entryIndex++;

        }
        previousTime = currentTime;
        currentTime += timeSpan;
      }

    }

    public void WriteFile(string outputFile)
    {
      TextWriter writer = new StreamWriter(outputFile);

      for (DateTime currentTime = startTime; currentTime < endTime; currentTime += timeSpan)
      {
        WriteLine(writer, currentTime);
      }
      writer.Close();
    }

    public void WriteLine(TextWriter writer, DateTime date)
    {
      double flowRate = 0;

      foreach (KeyValuePair<int, Dictionary<DateTime, PumpState>> kvp in allPumpStates)
      {
        int pumpNumber = kvp.Key;
        Dictionary<DateTime, PumpState> pumpStates = kvp.Value;

        if (pumpStates.ContainsKey(date))
        {
          double pumpRate = PumpRateOverride.PumpRate[pumpNumber];
          flowRate += (pumpStates[date].runTime.Ticks / (double)timeSpan.Ticks * pumpRate);
        }
      }
      flowRate = Math.Min(flowRate, PumpRateOverride.UltimateCapacity);
      writer.WriteLine(date.ToString("g") + "," + flowRate.ToString("F2"));
    }

    internal static class PumpRateOverride
    {
      internal static Dictionary<int, double> PumpRate
      {
        get
        {
          Dictionary<int, double> pumpRate = new Dictionary<int, double>();
          pumpRate.Add(1, 660);
          pumpRate.Add(2, 660);
          pumpRate.Add(3, 660);
          return pumpRate;
        }
      }

      internal static double UltimateCapacity = 2000;

    }
  }

  internal class PumpState
  {
    public TimeSpan runTime;
    public bool pumpOn = false;
    public double pumpRate;

    public PumpState()
    {

    }
  }
}
