//go:build wasm

package main

import (
	sdk "github.com/conduitio/conduit-processor-sdk"
	aggregate "github.com/devarispbrown/conduit-processor-aggregate"
)

func main() {
	sdk.Run(aggregate.NewProcessor())
}
