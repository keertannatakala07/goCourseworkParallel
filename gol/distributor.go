package gol

import (
	"fmt"
	"sync"
	"time"

	"uk.ac.bris.cs/gameoflife/util"
)

type distributorChannels struct {
	events     chan<- Event
	ioCommand  chan<- ioCommand
	ioIdle     <-chan bool
	ioFilename chan<- string
	ioOutput   chan<- uint8
	ioInput    <-chan uint8
	keyPresses <-chan rune
}

func calculateNextState(p Params, startY int, endY int, world [][]byte, newWorld [][]byte, wg *sync.WaitGroup, mu *sync.Mutex, pause *bool, cond *sync.Cond, events chan<- Event, currentTurn int) {

	defer wg.Done()

	for row := startY; row < endY; row++ {

		mu.Lock()
		for *pause {
			cond.Wait()
		}
		mu.Unlock()

		for column := 0; column < p.ImageWidth; column++ {
			liveCells := 0
			for dr := -1; dr <= 1; dr++ {
				for dc := -1; dc <= 1; dc++ {
					if dr == 0 && dc == 0 {
						continue
					}
					nRow := (row + dr + p.ImageHeight) % p.ImageHeight
					nCol := (column + dc + p.ImageWidth) % p.ImageWidth
					if world[nRow][nCol] == 255 {
						liveCells++
					}
				}
			}

			var testChangeWorld byte

			if world[row][column] == 255 {
				if liveCells == 2 || liveCells == 3 {
					testChangeWorld = 255
				} else {
					testChangeWorld = 0
				}
			} else {
				if liveCells == 3 {
					testChangeWorld = 255

				} else {
					testChangeWorld = 0
				}
			}

			newWorld[row][column] = testChangeWorld

			if world[row][column] != testChangeWorld {
				events <- CellFlipped{
					CompletedTurns: currentTurn,
					Cell:           util.Cell{X: column, Y: row},
				}
			}
		}
	}
}

func calculateAliveCells(p Params, world [][]byte) []util.Cell {
	cellCoordsSlice := []util.Cell{}

	for row := 0; row < p.ImageHeight; row++ {
		for column := 0; column < p.ImageWidth; column++ {
			if world[row][column] == 255 {
				cellCoordsSlice = append(cellCoordsSlice, util.Cell{X: column, Y: row})
			}
		}
	}

	return cellCoordsSlice
}

// distributor divides the work between workers and interacts with other goroutines.
func distributor(p Params, c distributorChannels) {
	// TODO: Create a 2D slice to store the world.
	c.ioCommand <- ioInput
	filename := fmt.Sprintf("%dx%d", p.ImageWidth, p.ImageHeight)
	c.ioFilename <- filename

	worldSlice := make([][]byte, p.ImageHeight)
	for i := 0; i < len(worldSlice); i++ {
		worldSlice[i] = make([]byte, p.ImageWidth)
	}

	for row := 0; row < p.ImageHeight; row++ {
		for column := 0; column < p.ImageWidth; column++ {
			worldSlice[row][column] = <-c.ioInput
		}
	}

	var firstAliveCells []util.Cell
	for i := 0; i < p.ImageHeight; i++ {
		for j := 0; j < p.ImageWidth; j++ {
			if worldSlice[i][j] == 255 {
				firstAliveCells = append(firstAliveCells, util.Cell{X: j, Y: i})
			}
		}
	}

	c.events <- CellsFlipped{CompletedTurns: 0, Cells: firstAliveCells}

	c.events <- StateChange{CompletedTurns: 0, NewState: Executing}

	// TODO: Execute all turns of the Game of Life.
	newWorld := make([][]byte, p.ImageHeight)
	for i := range newWorld {
		newWorld[i] = make([]byte, p.ImageWidth)
	}

	sectionSize := p.ImageHeight / p.Threads

	//group for waiting for workers to finish
	var wg sync.WaitGroup
	//group for waiting for ticker and keyboard go routines to finish
	var bg sync.WaitGroup
	var mu sync.Mutex

	//conditional lock to check for pausing for lock and unlocking
	var cond = sync.NewCond(&mu)

	mustQuit := false
	pause := false

	completedTurns := 0
	aliveCells := calculateAliveCells(p, worldSlice)

	ticker := time.NewTicker(2 * time.Second)

	//channel to end the go rountines
	done := make(chan bool)

	//ticker rountine
	bg.Add(1)
	go func() {
		defer bg.Done()
		for {
			//select allows waiting without cpu usage
			select {
			case <-done:
				//ends the rountine
				return
			case <-ticker.C:
				mu.Lock()
				for pause {
					//waits here for the pause var to change
					cond.Wait()
				}
				mu.Unlock()

				mu.Lock()
				safecompletedTurns := completedTurns
				safealiveCells := aliveCells
				mu.Unlock()

				c.events <- AliveCellsCount{CompletedTurns: safecompletedTurns, CellsCount: len(safealiveCells)}
			}
		}
	}()

	//keyboard rountine
	bg.Add(1)
	go func() {
		defer bg.Done()
		for {
			select {
			case k := <-c.keyPresses:
				fmt.Println("key received:", string(k))

				if k == 's' {

					mu.Lock()

					safeTurns := completedTurns

					worldCopy := make([][]byte, p.ImageHeight)
					for r := range worldSlice {
						worldCopy[r] = make([]byte, p.ImageWidth)
						copy(worldCopy[r], worldSlice[r])
					}

					mu.Unlock()

					c.ioCommand <- ioOutput

					outputFilename := fmt.Sprintf(
						"%dx%dx%d",
						p.ImageWidth,
						p.ImageHeight,
						safeTurns,
					)

					c.ioFilename <- outputFilename

					for row := 0; row < p.ImageHeight; row++ {
						for column := 0; column < p.ImageWidth; column++ {
							c.ioOutput <- worldCopy[row][column]
						}
					}

					c.ioCommand <- ioCheckIdle
					<-c.ioIdle

					c.events <- ImageOutputComplete{
						CompletedTurns: safeTurns,
						Filename:       outputFilename,
					}
				} else if k == 'q' {
					mu.Lock()
					// broadcast to the the workers that the condition has been updated so they check the loop condition again
					mustQuit = true
					pause = false
					cond.Broadcast()
					mu.Unlock()
					return
				} else if k == 'p' {
					mu.Lock()
					if pause {
						fmt.Println("Resuming the simulation.")
						pause = false
						cond.Broadcast()
						mu.Unlock()

						mu.Lock()
						turns := completedTurns
						mu.Unlock()

						c.events <- StateChange{
							CompletedTurns: turns,
							NewState:       Executing,
						}

					} else {
						fmt.Println("Pausing the simulation. Press 'p' again to resume.")
						pause = true
						turns := completedTurns
						mu.Unlock()

						c.events <- StateChange{
							CompletedTurns: turns,
							NewState:       Paused,
						}
					}
				}
			case <-done:
				return
			}
		}
	}()

	for i := 0; i < p.Turns; i++ {
		currentTurn := i + 1

		for w := 0; w < p.Threads; w++ {
			startY := w * sectionSize
			endY := (w + 1) * sectionSize

			if w == p.Threads-1 {
				endY = p.ImageHeight
			}

			wg.Add(1)

			go calculateNextState(p, startY, endY, worldSlice, newWorld, &wg, &mu, &pause, cond, c.events, currentTurn)

		}
		// wait for all workers to finish
		wg.Wait()

		mu.Lock()
		worldSlice, newWorld = newWorld, worldSlice
		completedTurns = i + 1
		aliveCells = calculateAliveCells(p, worldSlice)
		mu.Unlock()

		mu.Lock()
		if mustQuit {
			mu.Unlock()
			break
		}
		mu.Unlock()
		c.events <- TurnComplete{CompletedTurns: completedTurns}
	}
	c.ioCommand <- ioOutput
	var outputFilename string

	// if quit add the output file name with only the turns completed so far
	mu.Lock()
	if mustQuit {
		mu.Unlock()
		outputFilename = fmt.Sprintf("%dx%dx%d", p.ImageWidth, p.ImageHeight, completedTurns)
	} else {
		mu.Unlock()
		outputFilename = fmt.Sprintf("%dx%dx%d", p.ImageWidth, p.ImageHeight, p.Turns)
	}
	c.ioFilename <- outputFilename

	for row := 0; row < p.ImageHeight; row++ {
		for column := 0; column < p.ImageWidth; column++ {
			c.ioOutput <- worldSlice[row][column]
		}
	}

	// Make sure that the Io has finished any output before exiting.
	c.ioCommand <- ioCheckIdle
	<-c.ioIdle

	// TODO: Report the final state using FinalTurnCompleteEvent.
	c.events <- FinalTurnComplete{CompletedTurns: completedTurns, Alive: aliveCells}
	c.events <- ImageOutputComplete{CompletedTurns: completedTurns, Filename: outputFilename}
	c.events <- StateChange{completedTurns, Quitting}

	// Close the channel to stop the SDL goroutine gracefully. Removing may cause deadlock.
	ticker.Stop()
	close(done)
	//wait for ticker and keyboard go rountines to finish.
	bg.Wait()

	close(c.events)

}
