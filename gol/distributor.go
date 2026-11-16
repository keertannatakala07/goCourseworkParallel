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

func calculateNextState(p Params, startY int, endY int, world [][]byte, wg *sync.WaitGroup, mu *sync.Mutex, pause *bool, cond *sync.Cond, cellsChange chan<- util.Cell) {
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

			if world[row][column] != testChangeWorld {
				cellsChange <- util.Cell{X: column, Y: row}
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

	sectionSize := p.ImageHeight / p.Threads

	var wg sync.WaitGroup
	var bg sync.WaitGroup
	var mu sync.Mutex
	var cond = sync.NewCond(&mu)

	mustQuit := false
	pause := false

	completedTurns := 0
	aliveCells := calculateAliveCells(p, worldSlice)

	ticker := time.NewTicker(2 * time.Second)
	done := make(chan bool)

	bg.Add(1)
	go func() {
		defer bg.Done()
		for {
			select {
			case <-done:
				return
			case <-ticker.C:
				mu.Lock()
				for pause {
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
		cellsChange := make(chan util.Cell, 64)

		for w := 0; w < p.Threads; w++ {
			startY := w * sectionSize
			endY := (w + 1) * sectionSize

			if w == p.Threads-1 {
				endY = p.ImageHeight
			}

			wg.Add(1)

			go calculateNextState(p, startY, endY, worldSlice, &wg, &mu, &pause, cond, cellsChange)

		}

		//close channel to release blocking loop
		go func() {
			wg.Wait()
			close(cellsChange)
		}()

		//add cells from buffer to a slice (blocking loop until the channel is closed)
		var flippedCells []util.Cell
		for cell := range cellsChange {
			flippedCells = append(flippedCells, cell)
		}

		//go through list and update relevant cells accordingly
		mu.Lock()
		for _, cell := range flippedCells {
			if worldSlice[cell.Y][cell.X] == 255 {
				worldSlice[cell.Y][cell.X] = 0
			} else {
				worldSlice[cell.Y][cell.X] = 255
			}
		}
		completedTurns = currentTurn
		aliveCells = calculateAliveCells(p, worldSlice)
		mu.Unlock()

		// send events for flipped cells
		for _, cell := range flippedCells {
			c.events <- CellFlipped{
				CompletedTurns: currentTurn,
				Cell:           cell,
			}
		}

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
	bg.Wait()

	close(c.events)

}
