package main

import (
	"math/rand"
	"os"
	"bufio"
	"fmt"
	"time"
)

func main() {

	N := 500
	rand.Seed(123)
	clampedRWwithNDDist := CWD(ND(0,10.0), 0, 100, rand.Float64() * 100.0)
	clampedRWwithUDDist := CWD(UD(0,10.0), 0, 100, rand.Float64() * 100.0)
	monolythicRWwithNDDist := MWD(ND(0, 1), 0)
	monolythicRWwithUDDist := MWD(UD(0, 1), 0)

	outFile, err := os.Create("generators.csv")
	if err != nil {
		panic(err)
	}
	out := bufio.NewWriterSize(outFile, 4<<20)
	out.WriteString("Clamped Random walk with Normal distribution step,Clamped Random walk with Uniform distribution step,Monolythic Random walk with Normal distribution step,Monolythic Random walk with Uniform distribution step\n")
	fmt.Printf("Generating %d steps ..\n", N)
	start := time.Now()
	for i:=0;i<N;i++ {
		fmt.Fprintf(out,"%.2f,%.2f,%.2f,%.2f\n", clampedRWwithNDDist.Get(), clampedRWwithUDDist.Get(), monolythicRWwithNDDist.Get(), monolythicRWwithUDDist.Get())
		clampedRWwithNDDist.Advance()
		clampedRWwithUDDist.Advance()
		monolythicRWwithNDDist.Advance()
		monolythicRWwithUDDist.Advance()
		out.Flush()
	}
	outFile.Close()
	took := time.Now().Sub(start)
	fmt.Printf("Done. Took %.2fs\n", float64(took.Nanoseconds())/1.0e9)
}
