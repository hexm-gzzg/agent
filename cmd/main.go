package main

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	kitprometheus "github.com/go-kit/kit/metrics/prometheus"
	"github.com/mainflux/agent/pkg/agent"
	"github.com/mainflux/agent/pkg/agent/api"
	"github.com/mainflux/agent/pkg/bootstrap"
	"github.com/mainflux/agent/pkg/conn"
	"github.com/mainflux/agent/pkg/edgex"
	export "github.com/mainflux/export/pkg/config"
	"github.com/mainflux/mainflux"
	"github.com/mainflux/mainflux/errors"
	"github.com/mainflux/mainflux/logger"
	sdk "github.com/mainflux/mainflux/sdk/go"
	nats "github.com/nats-io/nats.go"
	stdprometheus "github.com/prometheus/client_golang/prometheus"
)

const (
	defHTTPPort                   = "9000"
	defBootstrapURL               = "http://localhost:8202/things/bootstrap"
	defBootstrapID                = ""
	defBootstrapKey               = ""
	defBootstrapRetries           = "5"
	defBootstrapSkipTLS           = "false"
	defBootstrapRetryDelaySeconds = "10"
	defLogLevel                   = "info"
	defEdgexURL                   = "http://localhost:48090/api/v1/"
	defMqttURL                    = "localhost:1883"
	defCtrlChan                   = ""
	defDataChan                   = ""
	defEncryption                 = "false"
	defMqttUsername               = ""
	defMqttPassword               = ""
	defMqttChannel                = ""
	defMqttSkipTLSVer             = "true"
	defMqttMTLS                   = "false"
	defMqttCA                     = "ca.crt"
	defMqttQoS                    = "0"
	defMqttRetain                 = "false"
	defMqttCert                   = "thing.cert"
	defMqttPrivKey                = "thing.key"
	defConfigFile                 = "config.toml"
	defNatsURL                    = nats.DefaultURL
	defHeartbeatInterval          = "10s"
	defTermSessionTimeout         = "60s"
	defBoardName                  = "edge"
	defToken                      = "123456"
	envConfigFile                 = "MF_AGENT_CONFIG_FILE"
	envLogLevel                   = "MF_AGENT_LOG_LEVEL"
	envEdgexURL                   = "MF_AGENT_EDGEX_URL"
	envMqttURL                    = "MF_AGENT_MQTT_URL"
	envHTTPPort                   = "MF_AGENT_HTTP_PORT"
	envBootstrapURL               = "MF_AGENT_BOOTSTRAP_URL"
	envBootstrapID                = "MF_AGENT_BOOTSTRAP_ID"
	envBootstrapKey               = "MF_AGENT_BOOTSTRAP_KEY"
	envBootstrapRetries           = "MF_AGENT_BOOTSTRAP_RETRIES"
	envBootstrapSkipTLS           = "MF_AGENT_BOOTSTRAP_SKIP_TLS"
	envBootstrapRetryDelaySeconds = "MF_AGENT_BOOTSTRAP_RETRY_DELAY_SECONDS"
	envCtrlChan                   = "MF_AGENT_CONTROL_CHANNEL"
	envDataChan                   = "MF_AGENT_DATA_CHANNEL"
	envEncryption                 = "MF_AGENT_ENCRYPTION"
	envNatsURL                    = "MF_AGENT_NATS_URL"
	envBoardName                  = "MF_AGENT_BOARD_NAME"
	envToken                      = "MF_AGENT_TOKEN"
	envBoardCfgFile               = "MF_AGENT_BOARD_CFG_FILE"

	envMqttUsername       = "MF_AGENT_MQTT_USERNAME"
	envMqttPassword       = "MF_AGENT_MQTT_PASSWORD"
	envMqttSkipTLSVer     = "MF_AGENT_MQTT_SKIP_TLS"
	envMqttMTLS           = "MF_AGENT_MQTT_MTLS"
	envMqttCA             = "MF_AGENT_MQTT_CA"
	envMqttQoS            = "MF_AGENT_MQTT_QOS"
	envMqttRetain         = "MF_AGENT_MQTT_RETAIN"
	envMqttCert           = "MF_AGENT_MQTT_CLIENT_CERT"
	envMqttPrivKey        = "MF_AGENT_MQTT_CLIENT_PK"
	envHeartbeatInterval  = "MF_AGENT_HEARTBEAT_INTERVAL"
	envTermSessionTimeout = "MF_AGENT_TERMINAL_SESSION_TIMEOUT"
)

const contentType = "application/senml+json"

var (
	errFailedToSetupMTLS       = errors.New("Failed to set up mtls certs")
	errFetchingBootstrapFailed = errors.New("Fetching bootstrap failed with error")
	errFailedToReadConfig      = errors.New("Failed to read config")
	errFailedToConfigHeartbeat = errors.New("Failed to configure heartbeat")
)

func main() {
	cfg, err := loadEnvConfig()
	if err != nil {
		log.Fatalf(fmt.Sprintf("Failed to load config: %s", err))
	}

	logger, err := logger.New(os.Stdout, cfg.Log.Level)
	if err != nil {
		log.Fatalf(fmt.Sprintf("Failed to create logger: %s", err))
	}

	cfg, err = loadBootConfig(cfg, logger)
	if err != nil {
		logger.Error(fmt.Sprintf("Failed to load config: %s", err))
	}

	nc, err := nats.Connect(cfg.Server.NatsURL)
	if err != nil {
		logger.Error(fmt.Sprintf("Failed to connect to NATS: %s %s", err, cfg.Server.NatsURL))
		os.Exit(1)
	}
	defer nc.Close()

	mqttClient, err := connectToMQTTBroker(cfg.MQTT, logger)
	if err != nil {
		logger.Error(err.Error())
		os.Exit(1)
	}
	edgexClient := edgex.NewClient(cfg.Edgex.URL, logger)

	var mfxdevice *agent.Device

	mfxdevice, err = genLocalThingAndChannels(cfg.BoardCfg.BoardName, cfg.BoardCfg.Token)
	if err != nil {
		logger.Error(fmt.Sprintf("Error in genLocalThingAndChannels(): %s", err))
		os.Exit(1)
	}
	err = updateExportFile(cfg.ExportFile, mfxdevice)
	if err != nil {
		logger.Error(fmt.Sprintf("Error in updateExportFile(): %s", err))
		os.Exit(1)
	}
	svc, err := agent.New(mqttClient, &cfg, edgexClient, nc, logger, *mfxdevice)
	if err != nil {
		logger.Error(fmt.Sprintf("Error in agent service: %s", err))
		os.Exit(1)
	}
	svc = api.MetricsMiddleware(
		svc,
		kitprometheus.NewCounterFrom(stdprometheus.CounterOpts{
			Namespace: "agent",
			Subsystem: "api",
			Name:      "request_count",
			Help:      "Number of requests received.",
		}, []string{"method"}),
		kitprometheus.NewSummaryFrom(stdprometheus.SummaryOpts{
			Namespace: "agent",
			Subsystem: "api",
			Name:      "request_latency_microseconds",
			Help:      "Total duration of requests in microseconds.",
		}, []string{"method"}),
	)
	b := conn.NewBroker(svc, mqttClient, cfg.Channels.Control, nc, logger, mfxdevice)
	go b.Subscribe()

	errs := make(chan error, 3)

	go func() {
		p := fmt.Sprintf(":%s", cfg.Server.Port)
		logger.Info(fmt.Sprintf("Agent service started, exposed port %s", cfg.Server.Port))
		errs <- http.ListenAndServe(p, api.MakeHandler(svc))
	}()

	go func() {
		c := make(chan os.Signal)
		signal.Notify(c, syscall.SIGINT)
		errs <- fmt.Errorf("%s", <-c)
	}()

	err = <-errs
	logger.Error(fmt.Sprintf("Agent terminated: %s", err))
}
func updateExportFile(exportFile string, mfxDevice *agent.Device) error {
	c, err := export.ReadFile(exportFile)
	if err != nil {
		log.Fatalf(fmt.Sprintf("updateExportFile failed: %s", err))
	}
	for idx := range c.Routes {
		route := &c.Routes[idx]
		if strings.Contains(route.NatsTopic, mfxDevice.ExportChannel.ID) == false {
			route.NatsTopic = route.NatsTopic + "." + mfxDevice.ExportChannel.ID
		} 
	}
	c.BoardCfg.ThingID = mfxDevice.Thing.ID
	c.BoardCfg.ThingKey = mfxDevice.Thing.Key
	c.BoardCfg.ControlChannelID = mfxDevice.ControlChannel.ID
	c.BoardCfg.ExportChannelID = mfxDevice.ExportChannel.ID

	export.Save(c)
	return nil
}
func genLocalThingAndChannels(deviceName string, mfToken string) (*agent.Device, error) {
	sdkConf := sdk.Config{
		BaseURL:           "http://127.0.0.1:8182",
		UsersPrefix:       "",
		ThingsPrefix:      "",
		HTTPAdapterPrefix: "",
		MsgContentType:    contentType,
		TLSVerification:   false,
	}
	var page sdk.ThingsPage
	var err error
	var chanPage sdk.ChannelsPage
	var mfThing sdk.Thing
	var controlChannel sdk.Channel
	var exportChannel sdk.Channel

	mainfluxSDK := sdk.NewSDK(sdkConf)
	thingName := deviceName + "_thing"
	page, err = mainfluxSDK.Things(mfToken, 0, 5, thingName)
	if err != nil {
		// log.Fatalf(err.Error())
		fmt.Println(err.Error())
		return nil, err
	}
	if page.Total > 1 {
		for _, th := range page.Things {
			fmt.Printf("th.ID %s,th.Key %s\n", th.ID, mfToken)
			err := mainfluxSDK.DeleteThing(th.ID, mfToken)
			if err != nil {
				// log.Fatalf(err.Error())
				fmt.Println(err.Error())
				return nil, err
			}
		}
	}
	if (page.Total == 0) || (page.Total > 1) {
		var thing = sdk.Thing{
			ID:   "1",
			Name: thingName,
		}
		_, err = mainfluxSDK.CreateThing(thing, mfToken)
		if err != nil {
			log.Fatalf(err.Error())
			fmt.Println(err)
			return nil, err
		}
		page, err = mainfluxSDK.Things(mfToken, 0, 5, thing.Name)
		if err != nil {
			// log.Fatalf(err.Error())
			fmt.Println(err.Error())
		}
	}
	if page.Total == 1 {
		mfThing = page.Things[0]
	} else {
		log.Fatalf("create things failed\n")
	}
	controlChanmelName := deviceName + "controlChannel"
	chanPage, err = mainfluxSDK.Channels(mfToken, 0, 5, controlChanmelName)
	if err != nil {
		// log.Fatalf(err.Error())
		fmt.Println(err.Error())
	}
	if chanPage.Total > 1 {
		for _, chann := range chanPage.Channels {
			err := mainfluxSDK.DeleteChannel(chann.ID, mfToken)
			if err != nil {
				// log.Fatalf(err.Error())
				fmt.Println(err.Error())
				return nil, err
			}
		}
	}
	if (chanPage.Total == 0) || (chanPage.Total > 1) {
		var channel = sdk.Channel{
			ID:   "1",
			Name: controlChanmelName,
		}
		_, err = mainfluxSDK.CreateChannel(channel, mfToken)
		if err != nil {
			log.Fatalf(err.Error())
			fmt.Println(err)
			return nil, err
		}
		chanPage, err = mainfluxSDK.Channels(mfToken, 0, 5, channel.Name)
		if err != nil {
			// log.Fatalf(err.Error())
			fmt.Println(err.Error())
			return nil, err
		}
	}
	if chanPage.Total == 1 {
		controlChannel = chanPage.Channels[0]
	} else {
		log.Fatalf("create channel failed\n")
		return nil, err
	}

	exportChanmelName := deviceName + "exportChannel"
	chanPage, err = mainfluxSDK.Channels(mfToken, 0, 5, exportChanmelName)
	if err != nil {
		// log.Fatalf(err.Error())
		fmt.Println(err.Error())
		return nil, err
	}
	if chanPage.Total > 1 {
		for _, chann := range chanPage.Channels {
			err := mainfluxSDK.DeleteChannel(chann.ID, mfToken)
			if err != nil {
				// log.Fatalf(err.Error())
				fmt.Println(err.Error())
				return nil, err
			}
		}
	}
	if (chanPage.Total == 0) || (chanPage.Total > 1) {
		var channel = sdk.Channel{
			ID:   "1",
			Name: exportChanmelName,
		}
		_, err = mainfluxSDK.CreateChannel(channel, mfToken)
		if err != nil {
			log.Fatalf(err.Error())
			return nil, err
		}
		chanPage, err = mainfluxSDK.Channels(mfToken, 0, 5, channel.Name)
		if err != nil {
			// log.Fatalf(err.Error())
			fmt.Println(err.Error())
			return nil, err
		}
	}
	if chanPage.Total == 1 {
		exportChannel = chanPage.Channels[0]
	} else {
		log.Fatalf("create channel failed\n")
	}
	conIDs := sdk.ConnectionIDs{
		ChannelIDs: []string{exportChannel.ID, controlChannel.ID},
		ThingIDs:   []string{mfThing.ID},
	}
	chanPage, err = mainfluxSDK.ChannelsByThing(mfToken, mfThing.ID, 0, uint64(len(conIDs.ChannelIDs)))
	if err != nil {
		// log.Fatalf(err.Error())
		fmt.Println(err.Error())
		return nil, err
	}
	if chanPage.Total == 0 {
		err = mainfluxSDK.Connect(conIDs, mfToken)
		if err != nil {
			fmt.Println(err.Error())

		}
	} else {
		conIDsTmp := sdk.ConnectionIDs{
			ThingIDs: []string{mfThing.ID},
		}
		flag := false
		for _, codID := range conIDs.ChannelIDs {
			flag = false
			for _, chann := range chanPage.Channels {
				if chann.ID == codID {
					flag = true
					break
				}
			}
			if flag == false {
				conIDsTmp.ChannelIDs = append(conIDsTmp.ChannelIDs, codID)
			}
		}

		for _, chann := range chanPage.Channels {
			flag = false
			for _, codID := range conIDs.ChannelIDs {
				if chann.ID == codID {
					flag = true
					break
				}
			}
			if flag == false {
				fmt.Printf("DisconnectThing:")
				fmt.Printf("thing.ID %s,chan_id %s\n", mfThing.ID, chann.ID)
				mainfluxSDK.DisconnectThing(mfThing.ID, chann.ID, mfToken)
			}
		}
		fmt.Println(conIDsTmp)
		if len(conIDsTmp.ChannelIDs) != 0 {
			err = mainfluxSDK.Connect(conIDsTmp, mfToken)
			if err != nil {
				fmt.Println(err.Error())
				return nil, err
			}
		}
	}
	return &agent.Device{
		MfSdk:          mainfluxSDK,
		Thing:          mfThing,
		ControlChannel: controlChannel,
		ExportChannel:  exportChannel,
		MfToken:        mfToken,
	}, nil
}
func loadEnvConfig() (agent.Config, error) {
	sc := agent.ServerConfig{
		NatsURL: mainflux.Env(envNatsURL, defNatsURL),
		Port:    mainflux.Env(envHTTPPort, defHTTPPort),
	}
	cc := agent.ChanConfig{
		Control: mainflux.Env(envCtrlChan, defCtrlChan),
		Data:    mainflux.Env(envDataChan, defDataChan),
	}
	interval, err := time.ParseDuration(mainflux.Env(envHeartbeatInterval, defHeartbeatInterval))
	if err != nil {
		return agent.Config{}, errors.Wrap(errFailedToConfigHeartbeat, err)
	}

	ch := agent.HeartbeatConfig{
		Interval: interval,
	}
	termSessionTimeout, err := time.ParseDuration(mainflux.Env(envTermSessionTimeout, defTermSessionTimeout))
	if err != nil {
		return agent.Config{}, err
	}
	ct := agent.TerminalConfig{
		SessionTimeout: termSessionTimeout,
	}
	ec := agent.EdgexConfig{URL: mainflux.Env(envEdgexURL, defEdgexURL)}
	lc := agent.LogConfig{Level: mainflux.Env(envLogLevel, defLogLevel)}

	mtls, err := strconv.ParseBool(mainflux.Env(envMqttMTLS, defMqttMTLS))
	if err != nil {
		mtls = false
	}

	skipTLSVer, err := strconv.ParseBool(mainflux.Env(defMqttSkipTLSVer, envMqttSkipTLSVer))
	if err != nil {
		skipTLSVer = true
	}

	qos, err := strconv.Atoi(mainflux.Env(envMqttQoS, defMqttQoS))
	if err != nil {
		qos = 0
	}

	retain, err := strconv.ParseBool(mainflux.Env(envMqttRetain, defMqttRetain))
	if err != nil {
		retain = false
	}

	mc := agent.MQTTConfig{
		URL:         mainflux.Env(envMqttURL, defMqttURL),
		Username:    mainflux.Env(envMqttUsername, defMqttUsername),
		Password:    mainflux.Env(envMqttPassword, defMqttPassword),
		MTLS:        mtls,
		CAPath:      mainflux.Env(envMqttCA, defMqttCA),
		CertPath:    mainflux.Env(envMqttCert, defMqttCert),
		PrivKeyPath: mainflux.Env(envMqttPrivKey, defMqttPrivKey),
		SkipTLSVer:  skipTLSVer,
		QoS:         byte(qos),
		Retain:      retain,
	}

	file := mainflux.Env(envConfigFile, defConfigFile)
	bc := export.BoardConfig{
		BoardName: mainflux.Env(envBoardName, defBoardName),
		Token:     mainflux.Env(envToken, defToken),
	}
	c := agent.NewConfig(sc, cc, ec, lc, mc, ch, ct, file, bc)
	mc, err = loadCertificate(c.MQTT)
	if err != nil {
		return c, errors.Wrap(errFailedToSetupMTLS, err)
	}

	c.MQTT = mc
	agent.SaveConfig(c)
	return c, nil
}

func loadBootConfig(c agent.Config, logger logger.Logger) (bsc agent.Config, err error) {
	file := mainflux.Env(envConfigFile, defConfigFile)
	skipTLS, err := strconv.ParseBool(mainflux.Env(envBootstrapSkipTLS, defBootstrapSkipTLS))
	bsConfig := bootstrap.Config{
		URL:           mainflux.Env(envBootstrapURL, defBootstrapURL),
		ID:            mainflux.Env(envBootstrapID, defBootstrapID),
		Key:           mainflux.Env(envBootstrapKey, defBootstrapKey),
		Retries:       mainflux.Env(envBootstrapRetries, defBootstrapRetries),
		RetryDelaySec: mainflux.Env(envBootstrapRetryDelaySeconds, defBootstrapRetryDelaySeconds),
		Encrypt:       mainflux.Env(envEncryption, defEncryption),
		SkipTLS:       skipTLS,
	}

	if err := bootstrap.Bootstrap(bsConfig, logger, file, c); err != nil {
		return c, errors.Wrap(errFetchingBootstrapFailed, err)
	}

	if bsc, err = agent.ReadConfig(file); err != nil {
		return c, errors.Wrap(errFailedToReadConfig, err)
	}

	mc, err := loadCertificate(bsc.MQTT)
	if err != nil {
		return bsc, errors.Wrap(errFailedToSetupMTLS, err)
	}

	if bsc.Heartbeat.Interval <= 0 {
		bsc.Heartbeat.Interval = c.Heartbeat.Interval
	}

	if bsc.Terminal.SessionTimeout <= 0 {
		bsc.Terminal.SessionTimeout = c.Terminal.SessionTimeout
	}

	bsc.MQTT = mc
	return bsc, nil
}

func connectToMQTTBroker(conf agent.MQTTConfig, logger logger.Logger) (mqtt.Client, error) {
	name := fmt.Sprintf("agent-%s", conf.Username)
	conn := func(client mqtt.Client) {
		logger.Info(fmt.Sprintf("Client %s connected", name))
	}

	lost := func(client mqtt.Client, err error) {
		logger.Info(fmt.Sprintf("Client %s disconnected", name))
	}

	opts := mqtt.NewClientOptions().
		AddBroker(conf.URL).
		SetClientID(name).
		SetCleanSession(true).
		SetAutoReconnect(true).
		SetOnConnectHandler(conn).
		SetConnectionLostHandler(lost)

	if conf.Username != "" && conf.Password != "" {
		opts.SetUsername(conf.Username)
		opts.SetPassword(conf.Password)
	}

	if conf.MTLS {
		cfg := &tls.Config{
			InsecureSkipVerify: conf.SkipTLSVer,
		}

		if conf.CA != nil {
			cfg.RootCAs = x509.NewCertPool()
			cfg.RootCAs.AppendCertsFromPEM(conf.CA)
		}
		if conf.Cert.Certificate != nil {
			cfg.Certificates = []tls.Certificate{conf.Cert}
		}

		cfg.BuildNameToCertificate()
		opts.SetTLSConfig(cfg)
		opts.SetProtocolVersion(4)
	}
	client := mqtt.NewClient(opts)
	token := client.Connect()
	token.Wait()

	if token.Error() != nil {
		return nil, token.Error()
	}
	return client, nil
}

func loadCertificate(cnfg agent.MQTTConfig) (c agent.MQTTConfig, err error) {
	var caByte []byte
	var cc []byte
	var pk []byte
	c = cnfg

	cert := tls.Certificate{}
	if !c.MTLS {
		return c, nil
	}
	// Load CA cert from file
	if c.CAPath != "" {
		caFile, err := os.Open(c.CAPath)
		defer caFile.Close()
		if err != nil {
			return c, err
		}
		caByte, err = ioutil.ReadAll(caFile)
		if err != nil {
			return c, err
		}
	}
	// Load CA cert from string if file not present
	if len(caByte) == 0 && c.CaCert != "" {
		caByte, err = ioutil.ReadAll(strings.NewReader(c.CaCert))
		if err != nil {
			return c, err
		}
	}
	// Load client certificate from file if present
	if c.CertPath != "" {
		clientCert, err := os.Open(c.CertPath)
		defer clientCert.Close()
		if err != nil {
			return c, err
		}
		cc, err = ioutil.ReadAll(clientCert)
		if err != nil {
			return c, err
		}
	}
	// Load client certificate from string if file not present
	if len(cc) == 0 && c.ClientCert != "" {
		cc, err = ioutil.ReadAll(strings.NewReader(c.ClientCert))
		if err != nil {
			return c, err
		}
	}
	// Load private key of client certificate from file
	if c.PrivKeyPath != "" {
		privKey, err := os.Open(c.PrivKeyPath)
		defer privKey.Close()
		if err != nil {
			return c, err
		}
		pk, err = ioutil.ReadAll((privKey))
		if err != nil {
			return c, err
		}
	}
	// Load private key of client certificate from string
	if len(pk) == 0 && c.ClientKey != "" {
		pk, err = ioutil.ReadAll(strings.NewReader(c.ClientKey))
		if err != nil {
			return c, err
		}
	}

	cert, err = tls.X509KeyPair([]byte(c.ClientCert), []byte(c.ClientKey))
	if err != nil {
		return c, err
	}
	c.Cert = cert
	c.CA = caByte
	return c, nil
}
