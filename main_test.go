package main

import (
	"errors"
	"fmt"
	"io"
	"log"
	"math/rand"
	"os"
	"os/exec"
	"sync"
	"testing"
	"time"
)

func copyAndCapture(w io.Writer, r io.Reader) ([]byte, error) {
	var out []byte
	buf := make([]byte, 1024, 1024)
	for {
		n, err := r.Read(buf[:])
		if n > 0 {
			d := buf[:n]
			out = append(out, d...)
			_, err := w.Write(d)
			if err != nil {
				return out, err
			}
		}
		if err != nil {
			// Read returns io.EOF at the end of file, which is not an error for us
			if err == io.EOF {
				err = nil
			}
			return out, err
		}
	}
}

func saveStream(r io.Reader) ([]byte, error) {
	var out []byte
	buf := make([]byte, 1024, 1024)
	for {
		n, err := r.Read(buf[:])
		if n > 0 {
			d := buf[:n]
			out = append(out, d...)
		}
		if err != nil {
			// Read returns io.EOF at the end of file, which is not an error for us
			if err == io.EOF {
				err = nil
			}
			return out, err
		}
	}
}

var cartridgeDir string = "./cartridge"
var debug bool = false

func execCmd(command string, arg ...string) {
	cmd := exec.Command(command, arg...)
	cmd.Dir = cartridgeDir
	var err error
	var stdout []byte
	if debug {
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
		err = cmd.Start()
	} else {
		stdout, err = cmd.CombinedOutput()
	}
	if err != nil {
		log.Panicf("%v", err)
	}
	log.Println(string(stdout))
}

func startCmdAsync(command string, arg ...string) {
	cmd := exec.Command(command, arg...)
	cmd.Dir = cartridgeDir
	var stdout, stderr []byte
	var errStdout, errStderr error

	stdoutIn, _ := cmd.StdoutPipe()
	stderrIn, _ := cmd.StderrPipe()
	err := cmd.Start()
	if err != nil {
		log.Panicf("%v", err)
	}

	//var wg sync.WaitGroup
	//wg.Add(1)
	go func() {
		stdout, errStdout = copyAndCapture(os.Stdout, stdoutIn)
		//wg.Done()
	}()
	go func() {
		stderr, errStderr = copyAndCapture(os.Stderr, stderrIn)
		//wg.Wait()
	}()
	//err = cmd.Wait()
	//if err != nil {
	//	log.Fatalf("cmd.Run() failed with %s\n", err)
	//}
	//time.Sleep(2 * time.Second)
	if errStdout != nil || errStderr != nil {
		log.Fatal("failed to capture stdout or stderr\n")
	}
	//time.Sleep(3 * time.Second)
	outStr, errStr := string(stdout), string(stderr)
	//outStr = "1"
	//errStr = "2"
	log.Printf("\nout:\n%s\nerr:\n%s\n", outStr, errStr)
}

type CartridgeHelper struct {
	Dir                  string
	stdout, stderr       []byte
	errStdout, errStderr error
	wg                   sync.WaitGroup
	startCmd             *exec.Cmd
}

func captureOutput(r io.Reader) ([]byte, error) {
	var out []byte
	buf := make([]byte, 1024, 1024)
	for {
		n, err := r.Read(buf[:])
		if n > 0 {
			d := buf[:n]
			out = append(out, d...)
		}
		if err != nil {
			// Read returns io.EOF at the end of file, which is not an error for us
			if err == io.EOF {
				err = nil
			}
			return out, err
		}
	}
}

func (helper *CartridgeHelper) Start() {
	helper.startCmd = exec.Command("cartridge", "start")
	helper.startCmd.Dir = helper.Dir

	stdoutIn, _ := helper.startCmd.StdoutPipe()
	stderrIn, _ := helper.startCmd.StderrPipe()
	err := helper.startCmd.Start()
	if err != nil {
		log.Panicf("%v", err)
	}

	helper.wg.Add(1)
	go func() {
		helper.stdout, helper.errStdout = captureOutput(stdoutIn)
		if helper.errStdout != nil {
			log.Fatal("failed to capture stdout\n")
		}
		helper.wg.Done()
	}()
	helper.wg.Add(1)
	go func() {
		helper.stderr, helper.errStderr = captureOutput(stderrIn)
		if helper.errStderr != nil {
			log.Fatal("failed to capture stdout\n")
		}
		helper.wg.Done()
	}()
}

func (helper *CartridgeHelper) Stop() {
	stopCmd := exec.Command("cartridge", "stop")
	stopCmd.Dir = helper.Dir

	stopStdout, errStopStdout := stopCmd.CombinedOutput()
	if errStopStdout != nil {
		log.Panicf("%v", errStopStdout)
	}

	helper.wg.Wait()
	if err := helper.startCmd.Wait(); err != nil {
		log.Printf("waiting failed with %s\n", err)
	}

	if helper.errStdout != nil || helper.errStderr != nil {
		log.Fatal("failed to capture stdout or stderr\n")
	}

	log.Printf("cartrigde stdout:\n%s\n", string(helper.stdout))
	log.Printf("cartrigde stderr:\n%s\n", string(helper.stderr))
	log.Printf("cartrigde stop stdout:\n%s\n", string(stopStdout))
}

func (helper *CartridgeHelper) ReplicasetsSetup() {
	//subCmd := "replicasets"
	subCmd := "replicasets setup"
	//subCmd := "status"
	cmd := exec.Command("cartridge", subCmd)
	cmd.Dir = helper.Dir

	stdout, errStdout := cmd.CombinedOutput()
	if errStdout != nil {
		log.Panicf("%v", errStdout)
	}

	log.Printf("cartrigde %s stdout:\n%s\n", subCmd, string(stdout))
}

func TestBootstrap(t *testing.T) {
	//configFile := "/tmp/vshard_cfg.yaml"

	var helper CartridgeHelper
	helper.Dir = cartridgeDir

	helper.Start()
	time.Sleep(1 * time.Second)

	helper.ReplicasetsSetup()
	time.Sleep(3 * time.Second)
	/*
		r := router.Router{
			Replicasets: make(map[string]router.MasterInstance),
		}
		if err := r.ReadConfigFile(configFile); err != nil {
			log.Fatalf("error reading '%s' vshard config\n%v", configFile, err)
		}
		if err := r.ConnectMasterInstancies(); err != nil {
			log.Fatalf("connection error\n%v", err)
		}
		defer r.CloseConnections()

		if err := r.Bootstrap(); err != nil {
			log.Fatalf("bootstap error\n%v", err)
		}

		r.CreateRoutesTable()

		r.Routes.Range(func(key, value interface{}) bool {
			bucketId := key.(uint64)
			conn := value.(*tarantool.Connection)
			log.Printf("%d %p", bucketId, conn)
			return true
		})
	*/
	time.Sleep(2 * time.Second)
	helper.Stop()
}

// Hello returns a greeting for the named person.
func Hello(name string) (string, error) {
	// If no name was given, return an error with a message.
	if name == "" {
		return name, errors.New("empty name")
	}
	// Create a message using a random format.
	// message := fmt.Sprintf(randomFormat(), name)
	message := fmt.Sprint(randomFormat())
	return message, nil
}

// randomFormat returns one of a set of greeting messages. The returned
// message is selected at random.
func randomFormat() string {
	// A slice of message formats.
	formats := []string{
		"Hi, %v. Welcome!",
		"Great to see you, %v!",
		"Hail, %v! Well met!",
	}

	// Return one of the message formats selected at random.
	return formats[rand.Intn(len(formats))]
}
