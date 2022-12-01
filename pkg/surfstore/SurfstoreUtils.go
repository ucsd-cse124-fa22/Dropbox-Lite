package surfstore

import (
	"log"
	"os"
	"strings"
	"bufio"
	"errors"
	"fmt"
)

func reconstitute (client RPCClient, file *os.File, blockHashList []string, blockStoreAddr *string, fileName string) {
    fileWriter := bufio.NewWriter(file)
    for _, blockHash := range blockHashList {
        fmt.Println(fileName,"XD")
        block := &Block{}
        client.GetBlock(blockHash,*blockStoreAddr,block)
        fmt.Println(block.BlockSize)
        fileWriter.Write(block.GetBlockData())
    }
    fileWriter.Flush()
    file.Close()
}

// Implement the logic for a client syncing with the server here.
func ClientSync(client RPCClient) {
    //fmt.Println("Client Syncing")
    //if index.txt doesn't exist
    _,err := os.Stat(ConcatPath(client.BaseDir,DEFAULT_META_FILENAME))
    if errors.Is(err, os.ErrNotExist){
        os.Create(ConcatPath(client.BaseDir,DEFAULT_META_FILENAME))
    }

    //create empty hash list
    var emptyHashList []string
    emptyHashList = append(emptyHashList,"0")

    //get remote index
    temp := make(map[string]*FileMetaData)
    serverFileMetaMap := &temp
    client.GetFileInfoMap(serverFileMetaMap)


    //get local index
	indexFileMetaMap,err := LoadMetaFromMetaFile(client.BaseDir)
	if err != nil {
	    log.Printf("Error with loading local index")
	}

	//get block store address
	var temp2 string = ""
	blockStoreAddr := &temp2
	client.GetBlockStoreAddr(blockStoreAddr)

	//base directory
	dirEntries, err := os.ReadDir(client.BaseDir)
	if err != nil {
	    log.Printf("Error with ReadDir")
	}

    //get list of file names in base directory
    fileNameMap := make(map[string]int)

    for _, entry := range dirEntries {
        fileNameMap[entry.Name()] = 1
    }


    for serverFileName, serverFileMetaData := range *serverFileMetaMap {
        _, b_ok := fileNameMap[serverFileName]
        _, i_ok := indexFileMetaMap[serverFileName]

        //if remote and local index has file record, but base directory doesn't (tombstone)
        if !b_ok && i_ok {
            indexFileMetaData := indexFileMetaMap[serverFileName]
            if indexFileMetaData.Version == serverFileMetaData.Version{
                indexFileMetaData.Version += 1
                indexFileMetaData.BlockHashList = emptyHashList
                var latestVersion int32
                client.UpdateFile(indexFileMetaData,&latestVersion)
            } else if indexFileMetaData.Version < serverFileMetaData.Version {
                indexFileMetaMap[serverFileName] = serverFileMetaData
                path := ConcatPath(client.BaseDir,serverFileName)
                file,err := os.Create(path)
                if err != nil {
                    continue
                }
                reconstitute(client,file,serverFileMetaData.BlockHashList,blockStoreAddr,serverFileName)
            }
        } else if !b_ok && !i_ok {
            indexFileMetaMap[serverFileName] = serverFileMetaData
            path := ConcatPath(client.BaseDir,serverFileName)
            file,err := os.Create(path)

            if err != nil {
                continue
            }
            reconstitute(client,file,serverFileMetaData.BlockHashList,blockStoreAddr,serverFileName)
        }
    }

	for _, entry := range dirEntries {
	    fileName := entry.Name()
    	if fileName == "index.txt" || strings.Contains(fileName, ",") || strings.Contains(fileName, "/") {
    	    continue
    	}

    	_, err := entry.Info()
    	if err != nil {
    	    log.Printf("File no longer exists")
    	    continue
    	}

        path := ConcatPath(client.BaseDir,fileName)
        file, err := os.Open(path)
        if err != nil {
            log.Printf("Error opening file")
            continue
        }
        defer file.Close()
        fileReader := bufio.NewReader(file)


        var blockHashList []string
        var blockList []*Block
        for {
            buf := make([]byte, client.BlockSize)
            n,err := fileReader.Read(buf)
            if err != nil {
                break
            }
            hashString := GetBlockHashString(buf[:n])
            blockHashList = append(blockHashList,hashString)
            blockList = append(blockList,&Block{BlockData: buf[:n], BlockSize: int32(n)})
        }

        //file in base dir, but not in local index / has tombstone / has different hash list
        indexFileMetaData,ok := indexFileMetaMap[fileName]

        if !ok {
            indexFileMetaMap[fileName] = &FileMetaData{Filename: fileName, Version: 1, BlockHashList: blockHashList}
        } else if ok  && indexFileMetaData.BlockHashList[0] == "0" {
            indexFileMetaData.Version += 1
            indexFileMetaData.BlockHashList = blockHashList
        } else {
            if len(blockHashList) != len(indexFileMetaData.BlockHashList) {
                indexFileMetaData.BlockHashList = blockHashList
                indexFileMetaData.Version += 1
            } else {
                for i := 0; i < len(blockHashList); i++ {
                    if blockHashList[i] != indexFileMetaData.BlockHashList[i] {
                        indexFileMetaData.BlockHashList = blockHashList
                        indexFileMetaData.Version += 1
                        break;
                    }
                }
            }
        }

        //if file in local, but not in remote index
        indexFileMetaData = indexFileMetaMap[fileName]
        //fmt.Println("indexFileMetaData:", indexFileMetaData)

        var latestVersion int32 = -1

        client.UpdateFile(indexFileMetaData,&latestVersion)
        //fmt.Println("latest ver:", latestVersion)

        if latestVersion == -1 {
            //fmt.Println("hi")
            client.GetFileInfoMap(serverFileMetaMap)
            indexFileMetaMap[fileName] = (*serverFileMetaMap)[fileName]


            file, err := os.Create(path)
            if err != nil {
                log.Printf("Error opening file")
                continue
            }
            serverFileMetaData := (*serverFileMetaMap)[fileName]
            reconstitute(client,file,serverFileMetaData.BlockHashList,blockStoreAddr,fileName)

//             for i:=0; i < len(serverFileMetaData.BlockHashList); i++ {
//                 serverBH := serverFileMetaData.BlockHashList[i]
//                 if (serverBH != blockHashList[i]){
//                     offset := client.BlockSize * i
//                     file.Seek(int64(offset),0)
//                     block := &Block{}
//                     client.GetBlock(serverBH,*blockStoreAddr,block)
//                     file.Write(block.GetBlockData())
//                 }
//             }
//             file.Close()


        } else {

            //fmt.Println("xd")
            for _, block := range blockList {
                var temp bool = true
                succ := &temp
                client.PutBlock(block,*blockStoreAddr,succ)
            }
        }
    }

    WriteMetaFile(indexFileMetaMap,client.BaseDir)
}
