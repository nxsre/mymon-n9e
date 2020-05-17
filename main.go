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
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
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
)

//Log logger of project
var Log *logs.BeeLogger

func main() {
	// parse config file
	var confFile string
	var confDir string
	flag.StringVar(&confFile, "c", "", "myMon configure file")
	flag.StringVar(&confDir, "d", "etc", "myMon configure directory")
	version := flag.Bool("v", false, "show version")
	pprof := flag.Bool("pprof", false, "")
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
			log.Println(http.ListenAndServe("localhost:6060", nil))
		}()
	}

	c := cron.New(cron.WithParser(
		cron.NewParser(cron.SecondOptional|cron.Minute|cron.Hour|cron.Dom|cron.Month|cron.Dow|cron.Descriptor)),
		cron.WithLogger(cron.VerbosePrintfLogger(log.New(os.Stderr, "", log.LstdFlags))))
	spec := "*/30 * * * * *"
	c.AddFunc(spec, func() {
		if confFile != "" {
			go monitor(confFile)
		} else if confDir != "" {
			err := filepath.Walk(confDir, func(confFile string, f os.FileInfo, err error) error {
				if f == nil {
					return err
				}
				if f.IsDir() {
					return nil
				}
				go monitor(confFile)
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

	// init log and other necessary
	Log = common.MyNewLogger(conf, common.CompatibleLog(conf))

	db, err := common.NewMySQLConnection(conf)
	if err != nil {
		fmt.Printf("NewMySQLConnection Error: %s\n", err.Error())
		return
	}
	defer func() { _ = db.Close() }()

	// start...
	Log.Notice("MySQL Monitor for falcon")
	t := time.NewTicker(time.Second * TimeOut)
	defer t.Stop()
	select {
	case <-t.C:
		Log.Error("Timeout")
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
