using System;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Channels;
using Microsoft.VisualStudio.Threading;
using DotNetConfig;

// ======================================================================================

public class Message {  // to value?
    public Message (int _chan, int _gen, double _value) {
        (chan, gen, value) = (_chan, _gen, _value);
    }

    public override string ToString () {
        return $"({chan}, {gen}, {value.ToString ("F2")})";
    }
    
    public int chan;
    public int gen;
    public double value;
}

// ======================================================================================

public class Proxel {
    Random random;
    int row, col;  // ccordinates
    public double Pvalue; // own value
    int[] neighs;  // e.g. 2, 3, 4, for the N boundary
    int[] gens;    // 2:g2, 3:g3, 4:g4
    double[] values; // 2:v2, 3:v3, 4:v4
    bool[] recs;   // 2:r2, 3:r3, 4:r4
    int msgsent;   // # total sent messages
    int msgrec;    // # total received messages
    public int CompletionTime;
    
    Channel <Message> [] pchans;  // multiplexed channels, with logical subchannels for each neighbour
    // AsyncQueue <Message> stash1, stash2;   // for ahead of time messages
    Queue <Message> stash1, stash2;   // for ahead of time messages
    
    public Proxel (int _row, int _col, double _val) { 
        (row, col, Pvalue) = (_row, _col, _val);
        
        random = new Random ();
        msgsent = 0;
        msgrec = 0;
        pchans = new Channel <Message> [1+4];
        stash1 = new Queue <Message> (NMP.Z4);
        stash2 = new Queue <Message> (NMP.Z4);
        
        SortedSet <int> ns = new SortedSet <int> ();
        if (row > 1) { ns.Add (1); pchans[1] = NMP.chans[row-1,col]; }
        if (row < NMP.ROWS) { ns.Add (3); pchans[3] = NMP.chans[row+1,col]; }
        if (col > 1) { ns.Add (4); pchans[4] = NMP.chans[row,col-1]; }
        if (col < NMP.COLS) { ns.Add (2); pchans[2] = NMP.chans[row,col+1]; }
        neighs = ns.ToArray ();
        // if (NMP.TRACE) await Console.Error.WriteLineAsync ($"... {row} {col} {String.Join (",", ns)}");

        gens = new int[1+4];
        values = new double[1+4];
        recs = new bool[1+4];
        
        // local init, not from env
        for (int n = 0; n <= 4; n ++) {
            gens[n] = -1;     // to allow gen 0 messages
            values[n] = 0.0;  // to be filled from gen 0 messages
            recs[n] = false;
        }
        
        var r = RunAsync ();
    }
    
    public override string ToString () {  // Trace
        return $"{row} {col} {Pvalue.ToString("F2")} ({string.Join(",", gens[1..].Select(g => g < 0? "-": g.ToString()))})";
    }

    public async Task RunAsync () {
        try {
            // *** complete init be sending proxel's own value
            
            foreach (int w in neighs) {
                var msg = new Message (NMP.REV[w], 0, Pvalue);
                var s = SendAsync (w, msg);  // fire and forget
            }
            msgsent = 0;  // not counting these, else neighs.Length

            int loops = 0;
            
            for (;;) {
                loops ++;
                
                // *** RECEIVE ***
                
                if (NMP.TRACE) await Console.Error.WriteLineAsync ($"... {this} <=  ?");
                int rs = 0;
                
                switch (NMP.S) {
                case 0:  // global sync
                    for (int n = 0; n < neighs.Length; n ++) {
                        var msg1 = await NMP.chans[row,col].Reader.ReadAsync ();
                        rs += Process (msg1);
                    }
                    Debug.Assert (rs == neighs.Length);
                    msgrec += rs;
                    
                    await NMP.GlobalSyncBarrier.SignalAndWait ();
                    
                    break;

                case 1:  // local sync
                case 2:  // async
                    if (NMP.TRACE && stash1.Count + stash2.Count > 0) await Console.Error.WriteLineAsync ($"... {this} <<  {stash1.Count} << {stash2.Count}");
                    
                    while (stash1.Count > 0) {
                        var msg = stash1.Dequeue ();
                        rs += Process (msg, true);
                    }

                    Func <bool> loop = () => NMP.S == 2 ?  rs == 0 : rs < neighs.Length;
                    while (loop ()) {
                        var msg = await NMP.chans[row,col].Reader.ReadAsync ();
                        rs += Process (msg);
                        
                        while (NMP.chans[row,col].Reader.TryRead (out msg)) {
                            rs += Process (msg);
                        }
                    }

                    var s1 = stash1;   // empty now
                    stash1 = stash2;  // may contain ahead of time messages
                    stash2 = s1;       // empty
                    msgrec += rs;
                    // if (NMP.TRACE) await Console.Error.WriteLineAsync ($"... {this} +=  {rs}");
                    
                    break;
                }
                
                if (NMP.TRACE) await Console.Error.WriteLineAsync ($"... {this} ++  {rs} ({String.Join (", ", values[1..].Select(v => v.ToString("F2")))})");

                // *** SEND ***

                Message[] msgs = new Message[1+4];
            
                switch (NMP.S) {
                case 0:
                case 1:

                    foreach (int w in neighs) {
                        double val = Pvalue;  // proxel's own value
                        foreach (int u in neighs) {
                            if (u != w) {
                                val += values[u];
                            }
                        }

                        if (msgs[w] == null) {
                            msgs[w] = new Message (NMP.REV[w], gens[w]+1,
                                Average (val));
                        }
                    }
                    break;
/*
                    foreach (int[] p in NMP.Perm (row, col)) {
                        // if (NMP.TRACE) await Console.Error.WriteLineAsync ($"... {row} {col} Perm {String.Join (",", p)}");
                        int x = 0, y = 1, z = 2, w = p.Length-1;
                        
                        double val = Pvalue;  // proxel's own value
                        
                        if (recs[p[x]]) {
                            val += values[p[x]];
                            if (y < w) {
                                val += values[p[y]];
                                if (z < w) {
                                        val += values[p[z]];
                                }
                            }
                        }
                                        
                        if (msgs[p[w]] == null) {
                            msgs[p[w]] = new Message (NMP.REV[p[w]], gens[p[x]]+1,
                                Average (val));
                        }
                    }
                    break;
*/
                case 2:
                    foreach (int[] p in NMP.Perm (row, col)) {
                        // if (NMP.TRACE) await Console.Error.WriteLineAsync ($"... {row} {col} Perm {String.Join (",", p)}");
                        int x = 0, y = 1, z = 2, w = p.Length-1;
                        
                        double? val = Pvalue;  // proxel's own value
                        
                        if (recs[p[x]]) {
                            if (gens[p[x]] < NMP.I) {
                                val += values[p[x]];
                                if (y < w) {
                                    if (gens[p[x]] <= gens[p[y]]) {
                                        val += values[p[y]];
                                        if (z < w) {
                                            if (gens[p[x]] <= gens[p[z]]) {
                                                val += values[p[z]];
                                            } else {
                                                val = null;
                                            }
                                        }
                                    } else {
                                        val = null;
                                    }
                                }
                            } else {
                                val = null;
                            }
                                        
                            if (val.HasValue) {
                                if (msgs[p[w]] == null) {
                                    msgs[p[w]] = new Message (NMP.REV[p[w]], gens[p[x]]+1,
                                        Average (val.Value));
                                }
                            }
                        }
                    }
                    break;
                }
                
                double Average (double val) {
                    return val / (double) (neighs.Length);  // -1 +1
                }
                
                Oracle (msgs);
                
                if (NMP.TRACE) await Console.Error.WriteLineAsync ($"... {this} =>  !");
                
                int count = 1;
                double v = Pvalue;
                for (int n = 1; n <= 4; n +=1 ) {
                    if (msgs[n] != null) {
                        count ++;
                        v += msgs[n].value;
                        // await SendAsync (n, msgs[n]); 
                        var s = SendAsync (n, msgs[n]);  // fire and forget
                        msgsent ++;
                    }
                }
                Pvalue = v / count;
                
                foreach (int n in neighs) {
                    recs[n] = false;
                }
                
                // *** BREAK?
                
                var b = true;
                foreach (var n in neighs) {
                    if (gens[n] < NMP.I) b = false;  // goto next round
                }
                if (b) break;
            }
        
            // this proxel has completed
            if (NMP.TRACE || NMP.RESULTS) await Console.Error.WriteLineAsync ($"... {this} $$  {loops} {msgsent} {msgrec} ({string.Join(", ", values[1..].Select(v => v <= 0.0? "-": v.ToString("F2")))})");

            NMP.chans[row, col].Writer.Complete ();
            CompletionTime = (int) NMP.Timer.ElapsedMilliseconds;
            if (Interlocked.Decrement (ref NMP._GridSize) == 0) NMP.Completed.SetResult ();

        } catch (Exception ex) {
            await Console.Error.WriteLineAsync ($"*** {this} Proxel.Run {ex.Message}");
            if (Interlocked.Decrement (ref NMP._GridSize) == 0) NMP.Completed.SetResult ();
        }

    
        void Oracle (Message [] msgs) {
            for (int n = 1; n <= 4; n +=1 ) {
                if (msgs[n] != null) {
                    Thread.SpinWait (NMP.ORACLE * 5000);  // * 0.25 ms ?
                }
            }
        }

        async ValueTask SendAsync (int n, Message msg) {
            if (NMP.TRACE) await Console.Error.WriteLineAsync ($"... {this} =>  {n} {msg}");
            
            var d = NMP.Delay (row, col, msg.chan);
            // switch {
                // 1 => NMP.DELAY1,
                // 2 => NMP.DELAY2,
                // 3 => NMP.DELAY3,
                // _ => NMP.DELAY4,  // 4
            // };
            await Task.Delay (Delay(d));
            
            await pchans[n].Writer.WriteAsync (msg);
        }
        
        int Delay (int d) {
            if (d >= 0) return d;
            d = -d;
            var d2 = d / 2;
            return random.Next (d-d2, d-d2+1);
        }
              
        int Process (Message msg, bool fromstash = false) {
            var c = msg.chan;
            var g = msg.gen;
            var v = msg.value;
            var s = fromstash? "<<": msg.chan.ToString(" 0");
            
            if (! recs[c] && gens[c] + 1 == g) {  // added protection against non fifo
                recs[c] = true;
                gens[c] = g;
                values[c] = v;
                if (NMP.TRACE) {var a = Console.Error.WriteLineAsync ($"... {this} <= {s} {msg}");}
                return 1;
                
            } else {  // stash
                stash2.Enqueue (msg);
                if (NMP.TRACE) {var a = Console.Error.WriteLineAsync ($"... {this} << {s} {msg}");}
                return 0;
            }
        }
    }
}

// ======================================================================================

public class NMP {
    public readonly static int ROWS = 4;    // grid ROWS size, require ROWS > 1
    public readonly static int COLS = 5;    // grid y size, require COLS > 1
    public static int _GridSize = ROWS * COLS;  // grid 2D size
    public readonly static int Z = 4;      // sub-channel buffer size 
    public readonly static int Z4 = 4 * Z; // multiplexed channel buffer size
    public readonly static int I = 1;    // iterations
    public readonly static int S = 1;    // 0 = global sync, 1 = local sync, 2 = async
    public readonly static string Version = S switch { 0 => "sync", 1 => "loc sync", _ => "async" };
    public readonly static bool TRACE = false;  // traces
    public readonly static bool RESULTS = true;  // results as traces $$
    public readonly static int ORACLE = 1;  // cpu spinwait per message, *0.25 ms ?
    public readonly static int DELAY1 = 0;  // delay to N
    public readonly static int DELAY2 = 0;  // delay to E
    public readonly static int DELAY3 = 0;  // delay to S
    public readonly static int DELAY4 = 0;  // delay to W
    
    public readonly static bool COMPLETIONS = true;  // completion times
    public readonly static bool PVALUES = true;      // final Pvalues
    
    public readonly static int PATTERN = 2;  // init Pvalues 
        // 0 : all 0.0
        // 1 : all 1.0
        // 2 : 0.0, 0.5, 0.5, 1.0 - NW, NE, SW, SE; aka SQUARES
        // 3 : custom for most progressive case (?)

    private static IEnumerable <int[]> Perm2NE, Perm2SE, Perm2SW, Perm2NW;
    private static IEnumerable <int[]> Perm3N, Perm3E, Perm3S, Perm3W;    
    private static IEnumerable <int[]> Perm4;
    
    static NMP () {
        try {
            var pcount = Environment.ProcessorCount;
            Console.Error.WriteLine ($"... ProcessorCount = {pcount}");
            ThreadPool.GetMinThreads (out int workers, out int completions);
            Console.Error.WriteLine ($"... workers, completions = {workers}, {completions}");
                
            
            var args = Environment.GetCommandLineArgs ();
            
            if (args.Length > 1 && File.Exists (args[1])) {
                Console.Error.WriteLine ($"... Config file = {args[1]}");
                var config = Config.Build (args[1]);
                
                ROWS = (int?) config .GetNumber ("nmp", "ROWS") ?? ROWS; 
                COLS = (int?) config .GetNumber ("nmp", "COLS") ?? COLS; 
                _GridSize = ROWS * COLS;                
                Z = (int?) config .GetNumber ("nmp", "Z") ?? Z; 
                Z4 = 4 * Z;
                I = (int?) config .GetNumber ("nmp", "I") ?? Z;             
                S = (int?) config .GetNumber ("nmp", "S") ?? S;
                Version = S switch { 0 => "sync", 1 => "loc sync", _ => "async" };
                TRACE = (bool?) config .GetBoolean ("nmp", "TRACE") ?? TRACE;
                RESULTS = (bool?) config .GetBoolean ("nmp", "RESULTS") ?? RESULTS;
                ORACLE = (int?) config .GetNumber ("nmp", "ORACLE") ?? ORACLE;
                DELAY1 = (int?) config .GetNumber ("nmp", "DELAY1") ?? DELAY1;
                DELAY2 = (int?) config .GetNumber ("nmp", "DELAY2") ?? DELAY2;
                DELAY3 = (int?) config .GetNumber ("nmp", "DELAY3") ?? DELAY3;
                DELAY4 = (int?) config .GetNumber ("nmp", "DELAY4") ?? DELAY4;
                PATTERN = (int?) config .GetNumber ("nmp", "PATTERN") ?? PATTERN;
            
            } else {
                Console.Error.WriteLine ($"... No config file");
            }
            
            Console.Error.WriteLine ($"... ROWS = {ROWS}, COLS = {COLS}, Z = {Z}, I = {I}, S = {S}, \r\n" +
                $"    TRACE = {TRACE}, RESULTS = {RESULTS}, \r\n" + 
                $"    ORACLE = {ORACLE}, DELAY1 = {DELAY1}, DELAY2 = {DELAY2}, DELAY3 = {DELAY3}, DELAY4 = {DELAY4}, PATTERN = {PATTERN}");
        
            Perm2NE = Perm (new int[] {3, 4}); // no 1, 2
            Perm2SE = Perm (new int[] {1, 4}); // no 2, 3
            Perm2SW = Perm (new int[] {1, 2}); // no 3, 4
            Perm2NW = Perm (new int[] {2, 3}); // no 1, 4

            Perm3N = Perm (new int[] {2, 3, 4}); // no 1
            Perm3E = Perm (new int[] {1, 3, 4}); // no 2
            Perm3S = Perm (new int[] {1, 2, 4}); // no 3
            Perm3W = Perm (new int[] {1, 2, 3}); // no 4

            Perm4 = Perm (4);

        } catch (Exception ex) {
            Console.Error.WriteLine ($"*** NMP {ex.Message}");
        }
    }
    
    static IEnumerable <IEnumerable <int>> _Perm (List <int> s) {
        if (s.Count == 0) yield return Enumerable .Empty <int> ();
        
        else {
            foreach (var h in s) {
                var z = new List <int> (s);
                z.Remove (h);
                foreach (var t in _Perm (z)) {
                    yield return Enumerable .Repeat (h, 1) .Concat (t);
                }
                // z.Add (h);
            }
        }
    }

    static List <int[]> Perm (List <int> s) {
        return _Perm (s) .Select (p => p.ToArray ()) .ToList ();
    }

    static IEnumerable <int[]> Perm (int n) {
        return Perm (Enumerable.Range (1, n) .ToList ());
    }

    static IEnumerable <int[]> Perm (int[] seq) {
        var s = Perm (new List <int> (seq));
        return s .Select (p => p.ToArray ()) .ToList ();
    }

    static void PrintPerm (int n, IEnumerable <int[]> pn) {
        Console.WriteLine ($"... {n} {pn.Count ()}");
        foreach (var p in pn) {
            Console.WriteLine ($"    {String.Join (", ", p)}");
        }
    }

    public static IEnumerable <int[]> Perm (int row, int col) {
        if (row == 1) {
            if (col == 1) return Perm2NW;
            else if (col == COLS) return Perm2NE;
            else return Perm3N;
        
        } else if (row == ROWS) {
            if (col == 1) return Perm2SW;
            else if (col == COLS) return Perm2SE;
            else return Perm3S;
        
        } else if (col == 1) {
            return Perm3W;
        
        } else if (col == COLS) {
            return Perm3E;
        
        } else {
            return Perm4;
        }
    }
        
    public static Stopwatch Timer;
    public static Proxel [,] proxels;
    public static Channel <Message> [,] chans;
    public static AsyncBarrier GlobalSyncBarrier;
    public static int[] REV = new int[] {0, 3, 4, 1, 2};

    public static TaskCompletionSource Completed = new TaskCompletionSource ();
    
    static int[][] CompletionTimes;
    static double[][] Pvalues;
    
    static double Value (int row, int col) {
        switch (PATTERN) {
        case 0: return 0.0;
        case 1: return 1.0;
        
        case 2: // squares
            if (row < ROWS/2) {
                if (col < COLS/2) return 0.0;
                else return 0.5;
            } else {
                if (col < COLS/2) return 0.5;
                else return 1.0;
            }
        
        case 3: // for illustrating worst case (?)
            return (double) (row - 1) / (double) (ROWS - 1);
        
        default: return 0.5;
        }
    }
    
    public static int Delay (int row, int col, int direction) {
        switch (PATTERN) {
        case 0: case 1: case 2:
            switch (direction) {
            case 1: return DELAY1;
            case 2: return DELAY2;
            case 3: return DELAY3;  
            case 4: return DELAY4;
            default: return 0;
            }

        case 3:
            switch (direction) {
            case 1: return DELAY1;
            case 2: return row < ROWS? DELAY2: DELAY2*10;
            case 3: return DELAY3;  
            case 4: return row < ROWS? DELAY4: DELAY4*10;;
            default: return 0;
            }

        default: return 0;
        }
    }

    public static async Task Main () { 
        try {
                Timer = Stopwatch.StartNew (); {
                GlobalSyncBarrier = new AsyncBarrier (_GridSize);

                proxels = new Proxel [1+ROWS, 1+COLS];
                CompletionTimes = new int[1+ROWS][];
                Pvalues = new double[1+ROWS][];
                for (int row = 1; row <= ROWS; row ++) {
                    CompletionTimes[row] = new int [1+COLS];
                    Pvalues[row] = new double [1+COLS];
                }
                chans = new Channel <Message> [1+ROWS, 1+COLS];
                BoundedChannelOptions o = new BoundedChannelOptions (Z4) 
                    {SingleWriter=false, SingleReader=true,};
                
                IEnumerable <(int, int)> RCS () {
                    for (int row = 1; row <= ROWS; row ++) {
                        for (int col = 1; col <= COLS; col ++) {
                            yield return (row, col);
                        }
                    }
                }
                
                Parallel.ForEach (RCS().ToArray(), 
                    rc => {
                        chans [rc.Item1, rc.Item2] = Channel .CreateBounded <Message> (o);
                    }
                );
                
                Parallel.ForEach (RCS().ToArray(), 
                    rc => {
                        int row = rc.Item1;
                        int col = rc.Item2;
                        double val = Value (row, col);
                        proxels [row, col] = new Proxel (row, col, val);
                    }
                );
                
                // await completion of all proxels
                await Completed.Task;
            }
            
            Timer.Stop ();

            await Console.Error.WriteLineAsync ($"... All proxels have completed {Version}: {Timer.ElapsedMilliseconds} ms");
            
            if (COMPLETIONS) {
                await Console.Error.WriteLineAsync ($"... CompletionTimes {Version}");
                
                for (int row = 1; row <= ROWS; row ++) {
                    for (int col = 1; col <= COLS; col ++) {
                        CompletionTimes[row][col] = proxels[row, col].CompletionTime;
                    }
                }
                
                int[] mins = new int[1+ROWS];
                int[] avgs = new int[1+ROWS];
                int[] maxs = new int[1+ROWS];
                
                for (int row = 1; row <= ROWS; row ++) {
                    int[] cts = CompletionTimes[row][1..];
                    mins[row] = cts.Min();
                    avgs[row] = (int) cts.Average();
                    maxs[row] = cts.Max();
                await Console.Error.WriteLineAsync ($"    {mins[row]} {avgs[row]} {maxs[row]} : {string.Join (" ", cts)}");
                }
                
                int min = mins[1..].Min();
                int avg = (int) avgs[1..].Average();
                int max = maxs[1..].Max();
                await Console.Error.WriteLineAsync ($"    {min} {avg} {max}");
            }

            if (PVALUES) {
                await Console.Error.WriteLineAsync ($"... Pvalues {Version}");
                
                for (int row = 1; row <= ROWS; row ++) {
                    for (int col = 1; col <= COLS; col ++) {
                        Pvalues[row][col] = proxels[row, col].Pvalue;
                    }
                }
                
                for (int row = 1; row <= ROWS; row ++) {
                    await Console.Error.WriteLineAsync ($"    {string.Join (" ", Pvalues[row][1..].Select(v => v.ToString("F2")))}");
                }
            }

            await Console.Error.WriteLineAsync ($"... All proxels have completed {Version}: {Timer.ElapsedMilliseconds} ms");
            
        } catch (Exception ex) {
            await Console.Error.WriteLineAsync ($"*** Main {ex.Message}");
        }
    }    
}

// ======================================================================================

