package main

import (
	"errors"
	"fmt"
	"os"
	"strconv"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	log "github.com/sirupsen/logrus"
	"sync"
)

var cfgFile string

var RootCmd = &cobra.Command{
	Use: "loki",
	PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
		return readConfig()
	},
	RunE: func(cmd *cobra.Command, args []string) error {
		psqlconn := fmt.Sprintf(
			"host=%s port=%d user=%s password=%s dbname=%s",
			viper.GetString("database_host"),
			viper.GetInt("database_port"),
			viper.GetString("database_username"),
			viper.GetString("database_password"),
			viper.GetString("database_dbname"),
		)
		l := &Loki{psqlconn, nil, &sync.Mutex{}}
		return l.ListenAndServe(viper.GetString("listen_addr"))
	},
}

func main() {
	if err := RootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func init() {
	RootCmd.PersistentFlags().StringVar(&cfgFile, "config", "", "config file (default is /etc/marlin/loki/config.yml)")
}

func readConfig() error {
	if cfgFile != "" {
		viper.SetConfigFile(cfgFile)
	} else {
		viper.SetConfigFile("/etc/marlin/loki/config.yml")
	}

	viper.AutomaticEnv() // read in environment variables that match

	// If a config file is found, read it in.
	err := viper.ReadInConfig()
	if err == nil {
		var cfgVersionOnDisk = viper.GetInt("config_version")
		if cfgVersionOnDisk != 1 {
			return errors.New("Cannot use the given config file as it does not match persistentlog's cfgversion. Wanted " + strconv.Itoa(1) + " but found " + strconv.Itoa(cfgVersionOnDisk))
		}
	} else {
		log.Error("No config file available on local machine. Exiting")
		os.Exit(1)
	}
	return nil
}
