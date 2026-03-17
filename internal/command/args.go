package command

import "github.com/TZJ-BYTE/RediGo/internal/protocol"

func argString(args [][]byte, i int) string {
	return protocol.BytesToString(args[i])
}

func argStringCopy(args [][]byte, i int) string {
	return protocol.BytesToStringCopy(args[i])
}

