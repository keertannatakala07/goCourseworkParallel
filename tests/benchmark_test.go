package tests

import (
	"fmt"
	"testing"

	"uk.ac.bris.cs/gameoflife/gol"
)

func BenchmarkGameOfLife(b *testing.B) {
	images := []gol.Params{
		{ImageWidth: 16, ImageHeight: 16},
		{ImageWidth: 64, ImageHeight: 64},
		{ImageWidth: 128, ImageHeight: 128},
		{ImageWidth: 256, ImageHeight: 256},
		{ImageWidth: 512, ImageHeight: 512},
	}

	for _, image := range images {
		for threads := 1; threads <= 16; threads++ {
			params := image
			params.Turns = 100
			params.Threads = threads

			name := fmt.Sprintf("%dx%d/%d_workers", params.ImageWidth, params.ImageHeight, threads)
			b.Run(name, func(b *testing.B) {
				b.StopTimer()
				for i := 0; i < b.N; i++ {
					events := make(chan gol.Event)
					drained := make(chan struct{})
					go func() {
						for range events {
						}
						close(drained)
					}()

					b.StartTimer()
					gol.Run(params, events, nil)
					<-drained
					b.StopTimer()
				}
			})
		}
	}
}

func BenchmarkThreeStrategies(b *testing.B) {
	sizes := []int{16, 64, 128} // Add 2048 if you have images/2048x2048.pgm
	threadCounts := []int{1, 2, 4, 8, 16}

	for _, size := range sizes {
		for _, threads := range threadCounts {
			params := gol.Params{
				ImageWidth:  size,
				ImageHeight: size,
				Turns:       100,
				Threads:     threads,
			}

			name := fmt.Sprintf("size=%d/threads=%d", size, threads)
			b.Run(name, func(b *testing.B) {
				b.ReportAllocs()
				b.StopTimer()
				for i := 0; i < b.N; i++ {
					events := make(chan gol.Event)
					drained := make(chan struct{})
					go func() {
						for range events {
						}
						close(drained)
					}()

					b.StartTimer()
					gol.Run(params, events, nil)
					<-drained
					b.StopTimer()
				}
			})
		}
	}
}
