/*
* Open-Falcon
*
* Copyright (c) 2014-2018 Xiaomi, Inc. All Rights Reserved.
*
* This product is licensed to you under the Apache License, Version 2.0 (the "License").
* You may not use this product except in compliance with the License.
*
* This product may include a number of subcomponents with separate copyright notices
* and license terms. Your use of these subcomponents is subject to the terms and
* conditions of the subcomponent's license, as noted in the LICENSE file.
 */

package main

import (
	"flag"
	"fmt"
	"github.com/nxsre/mymon-n9e/common"
	"github.com/panjf2000/ants/v2"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"time"

	"github.com/astaxie/beego/logs"
	"github.com/robfig/cron/v3"
	"github.com/ziutek/mymysql/mysql"
	_ "github.com/ziutek/mymysql/native"
	_ "net/http/pprof"
)

// Global tag var
var (
	IsSlave    int
	IsReadOnly int
	Tag        string
	step       int64
)

//Log logger of project
var Log *logs.BeeLogger

func main() {

	// init log and other necessary
	Log = logs.NewLogger(0)
	Log.EnableFuncCallDepth(true)

	// parse config file
	var confFile string
	var confDir string
	var threads int
	var pprofAddr string
	var logLevel int
	var logFile string
	flag.StringVar(&confFile, "c", "", "myMon configure file")
	flag.StringVar(&confDir, "d", "etc", "myMon configure directory")
	flag.IntVar(&threads, "t", runtime.NumCPU(), "the number of threads for multiple instance")
	flag.IntVar(&logLevel, "log-level", 5, "set log level")
	flag.Int64Var(&step, "step", 60, "set step")
	flag.StringVar(&logFile, "log-file", "mymon.log", "set log file")
	version := flag.Bool("v", false, "show version")
	pprof := flag.Bool("pprof", false, "enable pprof")
	flag.StringVar(&pprofAddr, "pprof-addr", "localhost:6060", "customize the pprof listening address")
	flag.Parse()

	if *version {
		fmt.Println(fmt.Sprintf("%10s: %s", "Version", Version))
		fmt.Println(fmt.Sprintf("%10s: %s", "Compile", Compile))
		fmt.Println(fmt.Sprintf("%10s: %s", "Branch", Branch))
		fmt.Println(fmt.Sprintf("%10s: %d", "GitDirty", GitDirty))
		os.Exit(0)
	}

	if *pprof {
		go func() {
			http.ListenAndServe(pprofAddr, nil)
		}()
	}

	Log.SetLevel(logLevel)
	//_ = Log.SetLogger("console")
	_ = Log.SetLogger(
		"file", fmt.Sprintf(
			`{"filename":"%s", "level":%d, "maxlines":0,
					"maxsize":0, "daily":false, "maxdays":0}`,
			logFile, logLevel))

	c := cron.New(cron.WithParser(
		cron.NewParser(cron.SecondOptional|cron.Minute|cron.Hour|cron.Dom|cron.Month|cron.Dow|cron.Descriptor)),
		cron.WithLogger(cron.VerbosePrintfLogger(log.New(os.Stderr, "", log.LstdFlags))))
	spec := fmt.Sprintf("*/%d * * * * *", step)

	defer ants.Release()
	p, _ := ants.NewPoolWithFunc(threads, func(i interface{}) {
		monitor(i.(string))
	})
	defer p.Release()

	go func() {
		for {
			Log.Debug("Cap: %d, Free: %d, Running: %d", p.Cap(), p.Free(), p.Running())
			time.Sleep(1 * time.Second)
		}
	}()

	c.AddFunc(spec, func() {
		if confFile != "" {
			monitor(confFile)
		} else if confDir != "" {
			err := filepath.Walk(confDir, func(confFile string, f os.FileInfo, err error) error {
				if f == nil {
					return err
				}
				if f.IsDir() {
					return nil
				}

				_ = p.Invoke(confFile)
				//go monitor(confFile)
				return nil
			})
			if err != nil {
				log.Fatalln(err)
			}
		} else {
			log.Fatalln("The configuration file does not exist!")
		}
	})
	c.Start()
	log.Println("run")

	select {}
}

func monitor(confFile string) {
	conf, err := common.NewConfig(confFile)
	if err != nil {
		fmt.Printf("NewConfig Error: %s\n", err.Error())
		return
	}
	if conf.Base.LogDir != "" {
		err = os.MkdirAll(conf.Base.LogDir, 0755)
		if err != nil {
			fmt.Printf("MkdirAll Error: %s\n", err.Error())
			return
		}
	}
	if conf.Base.SnapshotDir != "" {
		err = os.MkdirAll(conf.Base.SnapshotDir, 0755)
		if err != nil {
			fmt.Printf("MkdirAll Error: %s\n", err.Error())
			return
		}
	}

	db, err := common.NewMySQLConnection(conf)
	if err != nil {
		fmt.Printf("NewMySQLConnection %s:%d Error: %s\n", conf.DataBase.Host, conf.DataBase.Port, err.Error())
		return
	}
	defer func() {
		_ = db.Close()
	}()

	// start...
	Log.Notice("MySQL Monitor for falcon %s starting", db.NetConn().RemoteAddr())
	start := time.Now()
	defer func() {
		Log.Notice("MySQL Monitor for falcon %s finished, Elapsed time:%s", db.NetConn().RemoteAddr(), time.Now().Sub(start))
	}()
	t := time.NewTicker(time.Second * TimeOut)
	defer t.Stop()
	select {
	case <-t.C:
		Log.Error("Timeout")
		return
	default:
		err = fetchData(conf, db)
		if err != nil && err != io.EOF {
			Log.Error("Error: %s", err.Error())
		}
	}

}

func fetchData(conf *common.Config, db mysql.Conn) (err error) {
	defer func() {
		MySQLAlive(conf, err == nil)
	}()

	// Get GLOBAL variables
	IsReadOnly, err = GetIsReadOnly(db)
	if err != nil {
		return
	}

	// SHOW XXX Metric
	var data []*MetaData

	// Get slave status and set IsSlave global var
	slaveState, err := ShowSlaveStatus(conf, db)
	if err != nil {
		return
	}

	Tag = GetTag(conf)

	globalStatus, err := ShowGlobalStatus(conf, db)
	if err != nil {
		return
	}
	data = append(data, globalStatus...)

	globalVars, err := ShowGlobalVariables(conf, db)
	if err != nil {
		return
	}
	data = append(data, globalVars...)

	innodbState, err := ShowInnodbStatus(conf, db)
	if err != nil {
		return
	}
	data = append(data, innodbState...)

	data = append(data, slaveState...)

	binaryLogStatus, err := ShowBinaryLogs(conf, db)
	if err != nil {
		return
	}
	data = append(data, binaryLogStatus...)

	// Send Data to falcon-agent
	msg, err := SendData(conf, data)
	if err != nil {
		Log.Error("Send response %s:%d - %s", conf.DataBase.Host, conf.DataBase.Port, string(msg))
	} else {
		Log.Info("Send response %s:%d - %s", conf.DataBase.Host, conf.DataBase.Port, string(msg))
	}

	err = ShowProcesslist(conf, db)
	if err != nil {
		return
	}
	return
}
