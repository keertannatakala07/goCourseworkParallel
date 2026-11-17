package gol

import (
	"fmt"

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

func calculateNextState(p Params, world [][]byte, events chan<- Event, currentTurn int) [][]byte {
	newWorld := make([][]byte, p.ImageHeight)
	for i := range newWorld {
		newWorld[i] = make([]byte, p.ImageWidth)
	}

	for row := 0; row < p.ImageHeight; row++ {
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

	return newWorld
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

// distributor runs the Game of Life serially and interacts with I/O and event channels.
func distributor(p Params, c distributorChannels) {
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

	firstAliveCells := calculateAliveCells(p, worldSlice)
	c.events <- CellsFlipped{CompletedTurns: 0, Cells: firstAliveCells}
	c.events <- StateChange{CompletedTurns: 0, NewState: Executing}

	completedTurns := 0

	for i := 0; i < p.Turns; i++ {
		currentTurn := i + 1

		worldSlice = calculateNextState(p, worldSlice, c.events, currentTurn)
		completedTurns = currentTurn

		c.events <- TurnComplete{CompletedTurns: completedTurns}
	}

	aliveCells := calculateAliveCells(p, worldSlice)

	c.ioCommand <- ioOutput
	outputFilename := fmt.Sprintf("%dx%dx%d", p.ImageWidth, p.ImageHeight, p.Turns)
	c.ioFilename <- outputFilename

	for row := 0; row < p.ImageHeight; row++ {
		for column := 0; column < p.ImageWidth; column++ {
			c.ioOutput <- worldSlice[row][column]
		}
	}

	// Make sure that the IO has finished any output before exiting.
	c.ioCommand <- ioCheckIdle
	<-c.ioIdle

	c.events <- FinalTurnComplete{CompletedTurns: completedTurns, Alive: aliveCells}
	c.events <- ImageOutputComplete{CompletedTurns: completedTurns, Filename: outputFilename}
	c.events <- StateChange{completedTurns, Quitting}

	close(c.events)
}
