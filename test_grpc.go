package main
import (
	"context"
	"fmt"
	"time"
	suigrpc "github.com/open-move/sui-go-sdk/grpc"
	v2 "github.com/open-move/sui-go-sdk/proto/sui/rpc/v2"
)
func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	client, err := suigrpc.NewClient(ctx, "https://fullnode.mainnet.sui.io")
	if err != nil { panic(err) }
	
	reqLatest := &v2.GetCheckpointRequest{}
	respLatest, err := client.LedgerClient().GetCheckpoint(ctx, reqLatest)
	if err != nil { panic(err) }
	latest := *respLatest.Checkpoint.SequenceNumber
	fmt.Printf("%v\n", latest)
}
