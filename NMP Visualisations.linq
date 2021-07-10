<Query Kind="FSharpProgram">
  <Namespace>System.Collections.Immutable</Namespace>
</Query>

let gridWidth = 9
let gridHeight = 9

let maxGenerations : byte = 5uy

let noDelay = 0uy
let someDelay = 2uy

let delayNorth = noDelay
let delayEast = noDelay
let delaySouth = noDelay
let delayWest = noDelay

let initialCentreValue = 1.0f

let updateMode = "max"
let isDelays = "no_delay"
let generations = "four_generations"

let outputFilenameBase = sprintf "%s_%s_%s" updateMode isDelays generations

// This is currently unused, since the Tex output's squares are plenty big already
let scalingFactor = 1

let finalOutputBaseDirectory = Path.GetDirectoryName (Util.CurrentQueryPath) // Borrowed from https://stackoverflow.com/questions/3802779/linqpad-script-directory/3811389#3811389
let finalOutputDirectory = Path.Join(finalOutputBaseDirectory, "tex")

//*************************************************************************************

let centreX = gridWidth / 2
let centreY = gridHeight / 2

[<Struct>]
type Coord = { X: int; Y: int }

type NeighboursDU =
    | North
    | East
    | South
    | West

// This assumes that origin is in the top-left corner, and grows out to the right and downwards (which is the same as usually assumed in computer vision)
let getNeighbourCoords x y = function
    | North -> (x, y - 1)
    | East -> (x + 1, y)
    | South -> (x, y + 1)
    | West -> (x - 1, y)    
    
let getReverseNeighbourDir = function
    | North -> South
    | East -> West
    | South -> North
    | West -> East
    
let getDirDelay = function
    | North -> delayNorth
    | East -> delayEast
    | South -> delaySouth
    | West -> delayWest

// This assumes that origin is in the top-left corner, and grows out to the right and downwards (which is the same as usually assumed in computer vision)
let getNeighboursSet x y =
    let mutable neighbours = List.Empty
    if x > 0 then
        neighbours <- NeighboursDU.West :: neighbours
        
    if y > 0 then
        neighbours <- NeighboursDU.North :: neighbours
        
    if x < (gridWidth - 1) then
        neighbours <- NeighboursDU.East :: neighbours
        
    if y < (gridHeight - 1) then
        neighbours <- NeighboursDU.South :: neighbours
        
    Set.ofList neighbours

[<Struct>]
type Message = { Generation: byte; Datum: single; Delay: byte }

[<NoComparison>]
type Proxel = { Adjacency: Set<NeighboursDU>; Messages: Dictionary<NeighboursDU, Message>; mutable ReceiptTokens: Set<NeighboursDU>; mutable pDatum: single }

let initProxel x y = 
    let al = getNeighboursSet x y
    let alCount = Set.count al
    
    let messages = new Dictionary<NeighboursDU, Message>(alCount)
    Seq.iter (fun k -> messages.Add(k, { Generation = 0uy; Datum = 0.0f; Delay = noDelay })) al
    
    let rs = Set(al)
    
    { Adjacency = al; Messages = messages; ReceiptTokens = rs; pDatum = 0.0f }

let isProxelReadyToFinalise proxel = 
    let ms = proxel.Messages
    ms.All(fun entry -> entry.Value.Generation >= maxGenerations) && proxel.ReceiptTokens.IsEmpty

let isAllProxelsReadyToFinalise proxels =
    Seq.init (Array2D.length1 proxels) (fun i -> proxels.[i,*]) |> 
        Seq.forall (fun v -> Array.forall isProxelReadyToFinalise v)        

//*************************************************************************************

//let maxOverProxel proxel = Seq.maxBy (fun m -> m.Datum) proxel.Messages.Values

let messageUpdateComputation _proxel data = 
    List.max data

//let messageUpdateComputation _proxel data = 
//    List.average data

//let messageUpdateComputation proxel data = 
//    Math.Max(proxel.pDatum, List.max data)
    
//*************************************************************************************
// Bits to output the results in some fashion

Directory.CreateDirectory(finalOutputDirectory) |> ignore

let scaleGrid inputGrid = 

    let scaledGrid = Array2D.zeroCreate (gridWidth * scalingFactor) (gridHeight * scalingFactor)
    
    let iterationFunction x y value = 
        for i = (x * scalingFactor) to ((x * scalingFactor) + scalingFactor - 1) do
            for j = (y * scalingFactor) to ((y * scalingFactor) + scalingFactor - 1) do
                scaledGrid.[i,j] <- value
                
    Array2D.iteri iterationFunction inputGrid
    scaledGrid
    
let grayFloatToByte f = 
    let f' = MathF.Round(255.0f * f)
    Convert.ToByte(f')
    
let writeGridToTikz inputGrid fileName =

    let nodeTemplate = sprintf "\\node [fill={gray!%d}] at (%d,%d) {};"

    let bytesGrid = Array2D.map grayFloatToByte inputGrid
    
    bytesGrid.Dump("bytesGrid")
    
    use cout = new System.IO.StreamWriter(Path.Join(finalOutputDirectory, (fileName + ".tex")))
    cout.WriteLine(@"\documentclass[tikz]{standalone}")
    cout.WriteLine(@"\usetikzlibrary{positioning}")
    cout.WriteLine(String.Empty)
    cout.WriteLine(@"\begin{document}")
    // If you DON'T want grid lines on the images, then uncomment the below line, and comment out the line after that
    //cout.WriteLine(@"\begin{tikzpicture}[outer sep=0pt,minimum size=1cm]")
    cout.WriteLine(@"\begin{tikzpicture}[outer sep=0pt,minimum size=1cm,nodes=draw]")
    
    Array2D.iteri (fun x y v -> cout.WriteLine(nodeTemplate v x (-1 * y))) bytesGrid
    
    cout.WriteLine(@"\end{tikzpicture}")
    cout.WriteLine(@"\end{document}")
    
//*************************************************************************************
    
let resetReceiptTokens proxel = 
    proxel.ReceiptTokens <- Set.empty
    
let updateProxelData proxel = 
    let messageData = Seq.map (fun m -> m.Datum) proxel.Messages.Values
    let selectedDatum = Seq.max messageData
    //let updateDatum = Math.Max(proxel.pDatum, selectedDatum)
    let updateDatum = selectedDatum
    
    proxel.pDatum <- updateDatum

let isProxelsDirGenerationLowEnough proxel dir = 
    proxel.Messages.[dir].Generation < maxGenerations

let chooseNeighboursToSendTo proxel dir =
    let otherDirs = Set.remove dir proxel.Adjacency
    let separations = Set.map (fun o -> (o, Set.remove o otherDirs)) otherDirs
    Set.filter (fun (_o,s) -> Set.isEmpty s || Set.forall (fun x -> proxel.Messages.[dir].Generation <= proxel.Messages.[x].Generation) s) separations |>
    Set.map (fun (o,_s) -> o)
    
let computeNewMessage proxel dir =
    let otherNeighbours = proxel.Adjacency.Remove dir |> Set.toList
    let updateData = List.map (fun n -> proxel.Messages.[n].Datum) otherNeighbours
    let lowestGeneration = List.map (fun n -> proxel.Messages.[n].Generation) otherNeighbours |> List.min
    (dir, { Generation = lowestGeneration + 1uy; Datum = messageUpdateComputation proxel updateData; Delay = getDirDelay dir })

let computeOutgoingMessages proxel =
    Set.filter (isProxelsDirGenerationLowEnough proxel) proxel.ReceiptTokens |>
    Set.map (chooseNeighboursToSendTo proxel) |> Set.unionMany |> Set.toList |>
    List.map (computeNewMessage proxel)
    
let distributeMessages (allProxels : Proxel[,]) x y messages =
    
    let distributeMessage (dir, msg) =
        let (cx, cy) = getNeighbourCoords x y dir
        let oppDir = getReverseNeighbourDir dir
        
        allProxels.[cx,cy].Messages.[oppDir] <- msg
        allProxels.[cx,cy].ReceiptTokens <- Set.add oppDir allProxels.[cx,cy].ReceiptTokens
        
    let (sends,keeps) = List.partition (fun (_,m) -> m.Delay = 0uy) messages
    List.iter distributeMessage sends
    List.map (fun (d,m) -> (d, {m with Delay = m.Delay - 1uy})) keeps

//*************************************************************************************

let extractPData proxels = 
    Array2D.map (fun p -> p.pDatum) proxels

let finaliseProxel proxel = 
    Seq.map (fun m -> m.Datum) proxel.Messages.Values |> Seq.max
    
let allMessagesAtZeroDelay messages = 
    let messagesOnly = Array2D.map (List.map snd) messages

    let delayAtZero m = m.Delay = 0uy
    let allDelaysAtZero ml = List.forall delayAtZero ml
    let mutable result = true
    Array2D.iter (fun m -> if allDelaysAtZero m then result <- false) messagesOnly
    result

//*************************************************************************************

(* ^^^^^^^^^^ Initialisation ^^^^^^^^^^ *)
// This fundamentally covers rules 1 and 2 for current purposes    
let proxels = Array2D.init gridWidth gridHeight initProxel
let mutable messages = Array2D.create gridWidth gridHeight List.empty
let mutable roundCounter = 0
    
Seq.iter (fun n -> proxels.[centreX, centreY].Messages.[n] <- { Generation = 0uy; Datum = initialCentreValue; Delay = someDelay }) proxels.[centreX, centreY].Messages.Keys
proxels.[centreX,centreY].pDatum <- 1.0f

let printMessages proxels = 
    printfn "Round %d:" roundCounter
    for y = 0 to (Array2D.length2 proxels) - 1 do
        for x = 0 to (Array2D.length1 proxels) - 1 do
            printf "%1.2f\t" (Math.Max(proxels.[x,y].pDatum, (finaliseProxel proxels.[x,y])))
        printfn ""
    printfn ""
    
printMessages proxels
writeGridToTikz (extractPData proxels) (sprintf "%s_round_%d" outputFilenameBase roundCounter)

(* ^^^^^^^^^^ Oracle Query and Neighbour Messaging ^^^^^^^^^^ *)
while not ((isAllProxelsReadyToFinalise proxels) || (allMessagesAtZeroDelay messages)) do
    let newMessages = Array2D.map computeOutgoingMessages proxels
    Array2D.iteri (fun x y m -> messages.[x,y] <- List.append messages.[x,y] m) newMessages
    Array2D.iter resetReceiptTokens proxels
    messages <- Array2D.mapi (distributeMessages proxels) messages
    Array2D.iter updateProxelData proxels
    roundCounter <- roundCounter + 1
    writeGridToTikz (extractPData proxels) (sprintf "%s_round_%d" outputFilenameBase roundCounter)
    
    //(Array2D.map finaliseProxel proxels).Dump("iteration")
    proxels.[centreX,centreY].Messages.Dump("centre messages")
    printMessages proxels

(* ^^^^^^^^^^ Finalisation ^^^^^^^^^^ *)
let outputs = Array2D.map finaliseProxel proxels
//proxels.Dump()
outputs.Dump("Final output")