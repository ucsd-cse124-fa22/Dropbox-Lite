package surfstore

import (
	context "context"
	emptypb "google.golang.org/protobuf/types/known/emptypb"
	"time"
	grpc "google.golang.org/grpc"

)

type RPCClient struct {
	MetaStoreAddr string
	BaseDir       string
	BlockSize     int
}

func (surfClient *RPCClient) GetBlock(blockHash string, blockStoreAddr string, block *Block) error {
	// connect to the server
	conn, err := grpc.Dial(blockStoreAddr, grpc.WithInsecure())
	if err != nil {
		return err
	}
	c := NewBlockStoreClient(conn)

	// perform the call
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	b, err := c.GetBlock(ctx, &BlockHash{Hash: blockHash})
	if err != nil {
		conn.Close()
		return err
	}
	block.BlockData = b.BlockData
	block.BlockSize = b.BlockSize

	// close the connection
	return conn.Close()
}

func (surfClient *RPCClient) PutBlock(block *Block, blockStoreAddr string, succ *bool) error {
	conn, err := grpc.Dial(blockStoreAddr, grpc.WithInsecure())
    if err != nil {
    	return err
    }
    c := NewBlockStoreClient(conn)
    ctx, cancel := context.WithTimeout(context.Background(), time.Second)
    defer cancel()

    success,_ := c.PutBlock(ctx, block)
    *succ = success.GetFlag()

    return conn.Close()
}

func (surfClient *RPCClient) HasBlocks(blockHashesIn []string, blockStoreAddr string, blockHashesOut *[]string) error {
	conn, err := grpc.Dial(blockStoreAddr, grpc.WithInsecure())
    if err != nil {
        return err
    }
    c := NewBlockStoreClient(conn)
    ctx, cancel := context.WithTimeout(context.Background(), time.Second)
    defer cancel()

    bh,_ := c.HasBlocks(ctx,&BlockHashes{Hashes: blockHashesIn})
    *blockHashesOut = bh.GetHashes()

    return conn.Close()
}

func (surfClient *RPCClient) GetFileInfoMap(serverFileInfoMap *map[string]*FileMetaData) error {
	conn, err := grpc.Dial(surfClient.MetaStoreAddr, grpc.WithInsecure())
    if err != nil {
        return err
    }
    m := NewMetaStoreClient(conn)
    ctx, cancel := context.WithTimeout(context.Background(), time.Second)
    in := new(emptypb.Empty)
    defer cancel()

    fim,_ := m.GetFileInfoMap(ctx,in)
    *serverFileInfoMap = fim.GetFileInfoMap()

    return conn.Close()
}

func (surfClient *RPCClient) UpdateFile(fileMetaData *FileMetaData, latestVersion *int32) error {

    conn, err := grpc.Dial(surfClient.MetaStoreAddr, grpc.WithInsecure())
    if err != nil {
        return err
    }
    m := NewMetaStoreClient(conn)
    ctx, cancel := context.WithTimeout(context.Background(), time.Second)
    defer cancel()

    ver,_ := m.UpdateFile(ctx,fileMetaData)
    *latestVersion = ver.GetVersion()
    //fmt.Println("RPC", ver)
    return conn.Close()
}

func (surfClient *RPCClient) GetBlockStoreAddr(blockStoreAddr *string) error {
	conn, err := grpc.Dial(surfClient.MetaStoreAddr, grpc.WithInsecure())
    if err != nil {
        return err
    }
    m := NewMetaStoreClient(conn)
    ctx, cancel := context.WithTimeout(context.Background(), time.Second)
    in := new(emptypb.Empty)
    defer cancel()

    bsa,_ := m.GetBlockStoreAddr(ctx,in)
    *blockStoreAddr = bsa.GetAddr()

    return conn.Close()
}

// This line guarantees all method for RPCClient are implemented
var _ ClientInterface = new(RPCClient)

// Create an Surfstore RPC client
func NewSurfstoreRPCClient(hostPort, baseDir string, blockSize int) RPCClient {

	return RPCClient{
		MetaStoreAddr: hostPort,
		BaseDir:       baseDir,
		BlockSize:     blockSize,
	}
}
