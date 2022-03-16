package main

import (
	"context"
	"log"
	"time"
)

const maxChanBufSize = 10 // Set the maximum buffer size of each channel

func main() {
	// We create a context with a deadline to simulate a desired timeout.
	// On the platform, this would normally be passed in from further upstream.
	ctxDeadline, cancel := context.WithDeadline(context.Background(), time.Now().Add(2*time.Second))
	defer cancel()

	nums := genNums(1, 100000000)
	RunPipeline(ctxDeadline, nums, 100, 8)
}

// RunPipeline runs a sequence of pipeline stages
func RunPipeline(ctx context.Context, numbers []int, multi int, div int) {
	// This simply sets up our pipeline stages, passing the channel returned from the current stage
	// to the next stage.
	stream := streamNumbers(ctx, numbers)
	multiplied := multiplyNumberStream(ctx, multi, stream)
	divided := divideNumberStream(ctx, div, multiplied)
	logNumberStream(ctx, divided)
}

// streamNumbers takes a slice of numbers, and puts it into a channel.
func streamNumbers(ctx context.Context, numbers []int) <-chan int {
	numStream := make(chan int, maxChanBufSize) // Create our int stream channel
	go func() {
		defer close(numStream) // Close the channel when we are done sending
		i := 0
	streamLoop:
		for {
			select {
			case <-ctx.Done(): // Check to see if the context is done
				return
			default: // If context isn't done, put the current number into the stream
				numStream <- numbers[i]
				i++
				if i >= len(numbers) {
					break streamLoop
				}
			}
		}
	}()
	return numStream
}

// multiplyNumberStream takes a stream of ints and multiplies each number by a given factor
func multiplyNumberStream(ctx context.Context, multi int, numStream <-chan int) <-chan int {
	multiplied := make(chan int, maxChanBufSize)
	go func() {
		defer close(multiplied)
	multiLoop:
		for {
			select {
			case <-ctx.Done(): // Check to see if the context is done
				return
			case num, open := <-numStream: // If context isn't done, pull from the receive channel
				if !open { // If the channel has been closed (no more values), then break
					break multiLoop
				}
				multiplied <- num * multi // Otherwise, do the work and put it on the send channel
			}

		}
	}()
	return multiplied
}

// divideNumberStream takes a stream of ints and divides each number by a divisor
func divideNumberStream(ctx context.Context, div int, numStream <-chan int) <-chan int {
	divided := make(chan int, maxChanBufSize)
	go func() {
		defer close(divided)
	divLoop:
		for {
			select {
			case <-ctx.Done():
				return
			case num, open := <-numStream:
				if !open {
					break divLoop
				}
				divided <- num / div
			}
		}
	}()
	return divided
}

// logNumberStream takes a stream of ints, and logs each one
func logNumberStream(ctx context.Context, numStream <-chan int) {
logLoop:
	for {
		select {
		case <-ctx.Done():
			return
		case num, open := <-numStream:
			if !open {
				break logLoop
			}
			log.Printf("Number: %d", num)
		}
	}
}

func genNums(start, finish int) []int {
	res := make([]int, finish-start)
	for i, num := 0, start; num < finish; num++ {
		res[i] = num
		i++
	}
	return res
}
