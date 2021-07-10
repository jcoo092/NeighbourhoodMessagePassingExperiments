<Query Kind="FSharpProgram" />

// This particular script basically completely ignores the inputs from the environment, since there is no actual message passing taking place

// Only system parameters to change, really
//let rng = System.Random(12171503)
let rng = System.Random()

let maxIterations = 5

// The below two functions were lifted verbatim from https://stackoverflow.com/a/2184129
let rec insertions x = function
    | []             -> [[x]]
    | (y :: ys) as l -> (x::l)::(List.map (fun x -> y::x) (insertions x ys))

let rec permutations = function
    | []      -> seq [ [] ]
    | x :: xs -> Seq.concat (Seq.map (insertions x) (permutations xs))

type NeighbourDirection =
    | One
    | Two
    | Three
    | Four
    
let allNeighbours = [ One; Two; Three; Four ]

let dropNeighbour neighboursList neighbour = 
    List.filter (fun n -> n <> neighbour) neighboursList        
    
// X = One, Y = Two, Z = Three, W = Four
type neighboursCounters = { X: int; Y: int; Z: int; W: int }

let lookupDirectionInNeighbourCounter neighbourCounter = function 
    | One -> neighbourCounter.X
    | Two -> neighbourCounter.Y
    | Three -> neighbourCounter.Z
    | Four -> neighbourCounter.W
    
let incrementNeighboursCounter neighbourCounter = function
    | One -> {neighbourCounter with X = neighbourCounter.X + 1 }
    | Two -> {neighbourCounter with Y = neighbourCounter.Y + 1 }
    | Three -> {neighbourCounter with Z = neighbourCounter.Z + 1 }
    | Four -> {neighbourCounter with W = neighbourCounter.W + 1 }

let isNeighboursCounterEqualsNum neighbourCounter num  = 
    neighbourCounter.X = num && neighbourCounter.Y = num && neighbourCounter.Z = num && neighbourCounter.W = num

let getEligibleNeighbours neighbourCounter max =
    List.filter (fun n -> (lookupDirectionInNeighbourCounter neighbourCounter n) < max) allNeighbours

let receiveRandomly messageReceipts receiptTokens = 
        
    let eligibleNeighbours = getEligibleNeighbours messageReceipts maxIterations
    let numEligibleNeighbours = List.length eligibleNeighbours
    let howManyReceive = rng.Next(1, numEligibleNeighbours + 1)
    let indices = List.init howManyReceive (fun _ -> rng.Next(List.length eligibleNeighbours)) |> List.distinct
    
    let receiveMsg (mr, rt) neighbourIdx =
        (incrementNeighboursCounter mr (List.item neighbourIdx eligibleNeighbours), Set.add (List.item neighbourIdx eligibleNeighbours) rt)
    
    List.fold receiveMsg (messageReceipts, receiptTokens) indices

let countSends messageReceipts sendsSet neighbour =
    let perms = dropNeighbour allNeighbours neighbour |> permutations
    let neighbourIteration = lookupDirectionInNeighbourCounter messageReceipts neighbour
    let validPerms = Seq.filter (fun l -> List.forall (fun x -> neighbourIteration <= (lookupDirectionInNeighbourCounter messageReceipts x)) (List.take 2 l)) perms
    validPerms.Dump("Valid Permutations for rule 3")
    let targetNeighbours = Seq.map (fun l -> List.last l) validPerms |> Set.ofSeq
    Set.union sendsSet targetNeighbours

// Closest equivalent to rules 1 & 2
let mutable receiptTokens = Set.empty
let mutable messageReceipts = { X = 0; Y = 0; Z = 0; W = 0 }
let mutable messageSends = { X = 0; Y = 0; Z = 0; W = 0 }

// A set for 'sends' is used in place of actual w OQ messages here
let mutable sendsSet = Set.empty

let mutable keepLooping = true

while keepLooping do
    // equivalent of rules 3 && 5
    let neighboursWithRTs = Set.toList receiptTokens
    sendsSet <- List.fold (fun s n -> countSends messageReceipts s n) sendsSet neighboursWithRTs
    
    // rule 4
    if not (Set.isEmpty sendsSet) then
        // rules 6 & 7
        messageSends <- List.fold incrementNeighboursCounter messageSends (Set.toList sendsSet)
        sendsSet <- Set.empty
    
    // rule 8
    receiptTokens <- Set.empty
    
    // rule 9
    if isNeighboursCounterEqualsNum messageReceipts maxIterations then
        keepLooping <- false
        
    // rule 10
    // the if check is an ugly hack to get the program to follow the ruleset properly
    if keepLooping then
        let mrAndRt = receiveRandomly messageReceipts receiptTokens
        messageReceipts <- fst mrAndRt
        receiptTokens <- snd mrAndRt
    
        printfn "messageReceipts:\n %A\n\n" messageReceipts
        
messageReceipts.Dump("Final messageReceipts")
messageSends.Dump("Final messageSends")