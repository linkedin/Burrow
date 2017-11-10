/* Copyright 2017 LinkedIn Corp. Licensed under the Apache License, Version
 * 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */

package consumer

import (
	"errors"

	"github.com/spf13/viper"
	"go.uber.org/zap"

	"github.com/linkedin/Burrow/core/internal/helpers"
	"github.com/linkedin/Burrow/core/protocol"
)

type Coordinator struct {
	App     *protocol.ApplicationContext
	Log     *zap.Logger
	modules map[string]protocol.Module
}

func GetModuleForClass(app *protocol.ApplicationContext, className string) protocol.Module {
	switch className {
	case "kafka":
		return &KafkaClient{
			App: app,
			Log: app.Logger.With(
				zap.String("type", "module"),
				zap.String("coordinator", "consumer"),
				zap.String("name", "kafka"),
			),
		}
	case "kafkazk":
		return &KafkaZkClient{
			App: app,
			Log: app.Logger.With(
				zap.String("type", "module"),
				zap.String("coordinator", "consumer"),
				zap.String("name", "kafka_zk"),
			),
		}
	default:
		panic("Unknown consumer className provided: " + className)
	}
}

func (cc *Coordinator) Configure() {
	cc.Log.Info("configuring")

	cc.modules = make(map[string]protocol.Module)

	// Create all configured cluster modules, add to list of clusters
	modules := viper.GetStringMap("consumer")
	if len(modules) == 0 {
		panic("At least one consumer module must be configured")
	}
	for name := range modules {
		configRoot := "consumer." + name
		if !viper.IsSet("cluster." + viper.GetString(configRoot+".cluster")) {
			panic("Consumer '" + name + "' references an unknown cluster '" + viper.GetString(configRoot+".cluster") + "'")
		}
		module := GetModuleForClass(cc.App, viper.GetString(configRoot+".class-name"))
		module.Configure(name, configRoot)
		cc.modules[name] = module
	}
}

func (cc *Coordinator) Start() error {
	cc.Log.Info("starting")

	// Start Consumer modules
	err := helpers.StartCoordinatorModules(cc.modules)
	if err != nil {
		return errors.New("Error starting consumer module: " + err.Error())
	}
	return nil
}

func (cc *Coordinator) Stop() error {
	cc.Log.Info("stopping")

	// The individual consumer modules can choose whether or not to implement a wait in the Stop routine
	helpers.StopCoordinatorModules(cc.modules)
	return nil
}
