// Copyright IBM Corp. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package localconfig

import (
	"encoding/json"
	"fmt"
	"path/filepath"
	"sync"
	"time"

	"github.com/Shopify/sarama"
	bccsp "github.com/hyperledger/fabric/bccsp/factory"
	"github.com/hyperledger/fabric/common/flogging"
	"github.com/hyperledger/fabric/common/viperutil"
	coreconfig "github.com/hyperledger/fabric/core/config"
)

var logger = flogging.MustGetLogger("localconfig")

// TopLevel directly corresponds to the orderer config YAML.
// TopLevel orderer config yaml的配置对象
type TopLevel struct {
	// 通过用配置
	General General
	// 文件账本
	FileLedger FileLedger
	// kafka的配置信息，应该是不用了吧
	Kafka Kafka
	// Debug配置信息
	Debug Debug
	// 共识
	Consensus interface{}
	// 操作服务器
	Operations Operations
	// 指标
	Metrics Metrics
	//https://hyperledger-fabric.readthedocs.io/fr/latest/deployorderer/ordererchecklist.html#channelparticipation
	// 初始化的一些设置,系统区块,区块迁移
	ChannelParticipation ChannelParticipation
	// Admin地址
	Admin Admin
}

// General contains config which should be common among all orderer types.
// General包含了通用配置在所有orderer类型
type General struct {
	// 监听地址
	ListenAddress string
	// 监听端口
	ListenPort uint16
	// TLS配置
	TLS TLS
	// 集群
	Cluster Cluster
	// 保持时间
	Keepalive Keepalive
	// 连接超时
	ConnectionTimeout time.Duration
	// 为了兼容性已经提花为BootstrapMethod,
	// 应该是为了语意吧
	GenesisMethod string // For compatibility only, will be replaced by BootstrapMethod
	// 为了兼容性，已经替换为BootstrapFile
	GenesisFile string // For compatibility only, will be replaced by BootstrapFile
	// 创世区块 函数
	BootstrapMethod string
	// 创世区块 文件
	BootstrapFile string
	// 性能测试
	Profile Profile
	// 本地MSPDir
	LocalMSPDir string
	// 本地MSP id
	LocalMSPID string
	// 加密接口
	BCCSP *bccsp.FactoryOpts
	// 认证服务
	Authentication Authentication
}

type Cluster struct {
	ListenAddress                        string
	ListenPort                           uint16
	ServerCertificate                    string
	ServerPrivateKey                     string
	ClientCertificate                    string
	ClientPrivateKey                     string
	RootCAs                              []string
	DialTimeout                          time.Duration
	RPCTimeout                           time.Duration
	ReplicationBufferSize                int
	ReplicationPullTimeout               time.Duration
	ReplicationRetryTimeout              time.Duration
	ReplicationBackgroundRefreshInterval time.Duration
	ReplicationMaxRetries                int
	SendBufferSize                       int
	CertExpirationWarningThreshold       time.Duration
	TLSHandshakeTimeShift                time.Duration
}

// Keepalive contains configuration for gRPC servers.
type Keepalive struct {
	ServerMinInterval time.Duration
	ServerInterval    time.Duration
	ServerTimeout     time.Duration
}

// TLS contains configuration for TLS connections.
// TLS 配置
type TLS struct {
	// 不知道是什么
	Enabled bool
	// 私钥
	PrivateKey string
	// 证书
	Certificate string
	// 根证书
	RootCAs []string
	// client认证是否需要
	ClientAuthRequired bool
	// client 根证书
	ClientRootCAs []string
	// TLS握手时间
	TLSHandshakeTimeShift time.Duration
}

// SASLPlain contains configuration for SASL/PLAIN authentication
type SASLPlain struct {
	Enabled  bool
	User     string
	Password string
}

// Authentication contains configuration parameters related to authenticating
// client messages.
// 认证服务
type Authentication struct {
	TimeWindow         time.Duration
	NoExpirationChecks bool
}

// Profile contains configuration for Go pprof profiling.
// 好像是性能测试
type Profile struct {
	Enabled bool
	Address string
}

// FileLedger contains configuration for the file-based ledger.
// Fileledger包含配置基于文件的账本
type FileLedger struct {
	// 位置
	Location string
	// 前缀
	Prefix string // For compatibility only. This setting is no longer supported.
}

// Kafka contains configuration for the Kafka-based orderer.

type Kafka struct {
	Retry     Retry
	Verbose   bool
	Version   sarama.KafkaVersion // TODO Move this to global config
	TLS       TLS
	SASLPlain SASLPlain
	Topic     Topic
}

// Retry contains configuration related to retries and timeouts when the
// connection to the Kafka cluster cannot be established, or when Metadata
// requests needs to be repeated (because the cluster is in the middle of a
// leader election).
type Retry struct {
	ShortInterval   time.Duration
	ShortTotal      time.Duration
	LongInterval    time.Duration
	LongTotal       time.Duration
	NetworkTimeouts NetworkTimeouts
	Metadata        Metadata
	Producer        Producer
	Consumer        Consumer
}

// NetworkTimeouts contains the socket timeouts for network requests to the
// Kafka cluster.
type NetworkTimeouts struct {
	DialTimeout  time.Duration
	ReadTimeout  time.Duration
	WriteTimeout time.Duration
}

// Metadata contains configuration for the metadata requests to the Kafka
// cluster.
type Metadata struct {
	RetryMax     int
	RetryBackoff time.Duration
}

// Producer contains configuration for the producer's retries when failing to
// post a message to a Kafka partition.
type Producer struct {
	RetryMax     int
	RetryBackoff time.Duration
}

// Consumer contains configuration for the consumer's retries when failing to
// read from a Kafa partition.
type Consumer struct {
	RetryBackoff time.Duration
}

// Topic contains the settings to use when creating Kafka topics
type Topic struct {
	ReplicationFactor int16
}

// Debug contains configuration for the orderer's debug parameters.
// Debug配置了orderer‘s debug 参数
type Debug struct {
	BroadcastTraceDir string
	DeliverTraceDir   string
}

// Operations configures the operations endpoint for the orderer.
// 操作配置Operations
// 1. 日志level操作（不知道干啥。。）
// 2. 健康检查
// 3. endpoint 打印版本信息
type Operations struct {
	ListenAddress string
	TLS           TLS
}

// Metrics configures the metrics provider for the orderer.
// 指标
type Metrics struct {
	Provider string
	Statsd   Statsd
}

// Statsd provides the configuration required to emit statsd metrics from the orderer.
// 统计指标
type Statsd struct {
	Network       string
	Address       string
	WriteInterval time.Duration
	Prefix        string
}

// Admin configures the admin endpoint for the orderer.
type Admin struct {
	ListenAddress string
	TLS           TLS
}

// ChannelParticipation provides the channel participation API configuration for the orderer.
// Channel participation uses the same ListenAddress and TLS settings of the Operations service.
// ChannelParticipation提供channel参与api配置为orderer
// channel参与提供相同的listenAddress和TLS

type ChannelParticipation struct {
	Enabled            bool
	MaxRequestBodySize uint32
}

// Defaults carries the default orderer configuration values.
// Defaults 默认配置
var Defaults = TopLevel{
	General: General{
		ListenAddress:   "127.0.0.1",
		ListenPort:      7050,
		BootstrapMethod: "file",
		BootstrapFile:   "genesisblock",
		Profile: Profile{
			Enabled: false,
			Address: "0.0.0.0:6060",
		},
		Cluster: Cluster{
			ReplicationMaxRetries:                12,
			RPCTimeout:                           time.Second * 7,
			DialTimeout:                          time.Second * 5,
			ReplicationBufferSize:                20971520,
			SendBufferSize:                       10,
			ReplicationBackgroundRefreshInterval: time.Minute * 5,
			ReplicationRetryTimeout:              time.Second * 5,
			ReplicationPullTimeout:               time.Second * 5,
			CertExpirationWarningThreshold:       time.Hour * 24 * 7,
		},
		LocalMSPDir: "msp",
		LocalMSPID:  "SampleOrg",
		BCCSP:       bccsp.GetDefaultOpts(),
		Authentication: Authentication{
			TimeWindow: time.Duration(15 * time.Minute),
		},
	},
	FileLedger: FileLedger{
		Location: "/var/hyperledger/production/orderer",
	},
	Kafka: Kafka{
		Retry: Retry{
			ShortInterval: 1 * time.Minute,
			ShortTotal:    10 * time.Minute,
			LongInterval:  10 * time.Minute,
			LongTotal:     12 * time.Hour,
			NetworkTimeouts: NetworkTimeouts{
				DialTimeout:  30 * time.Second,
				ReadTimeout:  30 * time.Second,
				WriteTimeout: 30 * time.Second,
			},
			Metadata: Metadata{
				RetryBackoff: 250 * time.Millisecond,
				RetryMax:     3,
			},
			Producer: Producer{
				RetryBackoff: 100 * time.Millisecond,
				RetryMax:     3,
			},
			Consumer: Consumer{
				RetryBackoff: 2 * time.Second,
			},
		},
		Verbose: false,
		Version: sarama.V0_10_2_0,
		TLS: TLS{
			Enabled: false,
		},
		Topic: Topic{
			ReplicationFactor: 3,
		},
	},
	Debug: Debug{
		BroadcastTraceDir: "",
		DeliverTraceDir:   "",
	},
	Operations: Operations{
		ListenAddress: "127.0.0.1:0",
	},
	Metrics: Metrics{
		Provider: "disabled",
	},
	ChannelParticipation: ChannelParticipation{
		Enabled:            false,
		MaxRequestBodySize: 1024 * 1024,
	},
	Admin: Admin{
		ListenAddress: "127.0.0.1:0",
	},
}

// Load parses the orderer YAML file and environment, producing
// a struct suitable for config use, returning error on failure.
func Load() (*TopLevel, error) {
	return cache.load()
}

// configCache stores marshalled bytes of config structures that produced from
// EnhancedExactUnmarshal. Cache key is the path of the configuration file that was used.
// 存储byte配置文件结构.
// cache的key是配置文件路径
type configCache struct {
	mutex sync.Mutex
	cache map[string][]byte
}

var cache = &configCache{}

// Load will load the configuration and cache it on the first call; subsequent
// calls will return a clone of the configuration that was previously loaded.
// load将加载配置文件并且cache他，之后的调用将返回一个先前加载的克隆的配置文件
func (c *configCache) load() (*TopLevel, error) {
	var uconf TopLevel
	// 毒蛇配置...为啥叫这个名字呢

	// ConfigParser holds the configuration file locations.
	// It keeps the config file directory locations and env variables.
	// From the file the config is unmarshalled and stored.
	// Currently "yaml" is supported.
	// configParser维护配置文件位置
	// 它保存配置文件路径和环境变量
	// 从文件配置被加载和存储
	// 目前yaml被支持
	// type ConfigParser struct {
	// 	// configuration file to process
	// 	configPaths []string
	// 	configName  string
	// 	configFile  string

	// 	// parsed config
	// 	config map[string]interface{}
	// }
	// 关于viper的文章
	// https://blog.biezhi.me/2018/10/load-config-with-viper.html
	// https://github.com/spf13/viper
	config := viperutil.New()
	// 设置configName为orderer
	config.SetConfigName("orderer")

	// 这里感觉加载配置文件到map[string]interface{}
	// 配置加载为一个词典
	if err := config.ReadInConfig(); err != nil {
		return nil, fmt.Errorf("Error reading configuration: %s", err)
	}

	// 添加锁可能有异步操作吧，后面再看
	c.mutex.Lock()
	defer c.mutex.Unlock()
	// 放进去c.cache
	// 刚开始没有
	// config.ConfigFileUsed()配置文件路径，毕竟cache是根据文件路径和配置存储的
	serializedConf, ok := c.cache[config.ConfigFileUsed()]
	if !ok {
		// 来了， TopLevel来了
		//type ConfigParser struct {
		// configuration file to process
		// 	configPaths []string
		// 	configName  string
		// 	configFile  string

		// 	// parsed config
		// 	config map[string]interface{}
		// }
		// uconf topLevel
		err := config.EnhancedExactUnmarshal(&uconf)
		if err != nil {
			return nil, fmt.Errorf("Error unmarshalling config into struct: %s", err)
		}

		serializedConf, err = json.Marshal(uconf)
		if err != nil {
			return nil, err
		}

		if c.cache == nil {
			c.cache = map[string][]byte{}
		}
		c.cache[config.ConfigFileUsed()] = serializedConf
	}

	err := json.Unmarshal(serializedConf, &uconf)
	if err != nil {
		return nil, err
	}
	uconf.completeInitialization(filepath.Dir(config.ConfigFileUsed()))

	return &uconf, nil
}

func (c *TopLevel) completeInitialization(configDir string) {
	defer func() {
		// Translate any paths for cluster TLS configuration if applicable
		if c.General.Cluster.ClientPrivateKey != "" {
			coreconfig.TranslatePathInPlace(configDir, &c.General.Cluster.ClientPrivateKey)
		}
		if c.General.Cluster.ClientCertificate != "" {
			coreconfig.TranslatePathInPlace(configDir, &c.General.Cluster.ClientCertificate)
		}
		c.General.Cluster.RootCAs = translateCAs(configDir, c.General.Cluster.RootCAs)
		// Translate any paths for general TLS configuration
		c.General.TLS.RootCAs = translateCAs(configDir, c.General.TLS.RootCAs)
		c.General.TLS.ClientRootCAs = translateCAs(configDir, c.General.TLS.ClientRootCAs)
		coreconfig.TranslatePathInPlace(configDir, &c.General.TLS.PrivateKey)
		coreconfig.TranslatePathInPlace(configDir, &c.General.TLS.Certificate)
		coreconfig.TranslatePathInPlace(configDir, &c.General.BootstrapFile)
		coreconfig.TranslatePathInPlace(configDir, &c.General.LocalMSPDir)
		// Translate file ledger location
		coreconfig.TranslatePathInPlace(configDir, &c.FileLedger.Location)
	}()

	for {
		switch {
		case c.General.ListenAddress == "":
			logger.Infof("General.ListenAddress unset, setting to %s", Defaults.General.ListenAddress)
			c.General.ListenAddress = Defaults.General.ListenAddress
		case c.General.ListenPort == 0:
			logger.Infof("General.ListenPort unset, setting to %v", Defaults.General.ListenPort)
			c.General.ListenPort = Defaults.General.ListenPort
		case c.General.BootstrapMethod == "":
			if c.General.GenesisMethod != "" {
				// This is to keep the compatibility with old config file that uses genesismethod
				logger.Warn("General.GenesisMethod should be replaced by General.BootstrapMethod")
				c.General.BootstrapMethod = c.General.GenesisMethod
			} else {
				c.General.BootstrapMethod = Defaults.General.BootstrapMethod
			}
		case c.General.BootstrapFile == "":
			if c.General.GenesisFile != "" {
				// This is to keep the compatibility with old config file that uses genesisfile
				logger.Warn("General.GenesisFile should be replaced by General.BootstrapFile")
				c.General.BootstrapFile = c.General.GenesisFile
			} else {
				c.General.BootstrapFile = Defaults.General.BootstrapFile
			}
		case c.General.Cluster.RPCTimeout == 0:
			c.General.Cluster.RPCTimeout = Defaults.General.Cluster.RPCTimeout
		case c.General.Cluster.DialTimeout == 0:
			c.General.Cluster.DialTimeout = Defaults.General.Cluster.DialTimeout
		case c.General.Cluster.ReplicationMaxRetries == 0:
			c.General.Cluster.ReplicationMaxRetries = Defaults.General.Cluster.ReplicationMaxRetries
		case c.General.Cluster.SendBufferSize == 0:
			c.General.Cluster.SendBufferSize = Defaults.General.Cluster.SendBufferSize
		case c.General.Cluster.ReplicationBufferSize == 0:
			c.General.Cluster.ReplicationBufferSize = Defaults.General.Cluster.ReplicationBufferSize
		case c.General.Cluster.ReplicationPullTimeout == 0:
			c.General.Cluster.ReplicationPullTimeout = Defaults.General.Cluster.ReplicationPullTimeout
		case c.General.Cluster.ReplicationRetryTimeout == 0:
			c.General.Cluster.ReplicationRetryTimeout = Defaults.General.Cluster.ReplicationRetryTimeout
		case c.General.Cluster.ReplicationBackgroundRefreshInterval == 0:
			c.General.Cluster.ReplicationBackgroundRefreshInterval = Defaults.General.Cluster.ReplicationBackgroundRefreshInterval
		case c.General.Cluster.CertExpirationWarningThreshold == 0:
			c.General.Cluster.CertExpirationWarningThreshold = Defaults.General.Cluster.CertExpirationWarningThreshold
		case c.Kafka.TLS.Enabled && c.Kafka.TLS.Certificate == "":
			logger.Panicf("General.Kafka.TLS.Certificate must be set if General.Kafka.TLS.Enabled is set to true.")
		case c.Kafka.TLS.Enabled && c.Kafka.TLS.PrivateKey == "":
			logger.Panicf("General.Kafka.TLS.PrivateKey must be set if General.Kafka.TLS.Enabled is set to true.")
		case c.Kafka.TLS.Enabled && c.Kafka.TLS.RootCAs == nil:
			logger.Panicf("General.Kafka.TLS.CertificatePool must be set if General.Kafka.TLS.Enabled is set to true.")

		case c.Kafka.SASLPlain.Enabled && c.Kafka.SASLPlain.User == "":
			logger.Panic("General.Kafka.SASLPlain.User must be set if General.Kafka.SASLPlain.Enabled is set to true.")
		case c.Kafka.SASLPlain.Enabled && c.Kafka.SASLPlain.Password == "":
			logger.Panic("General.Kafka.SASLPlain.Password must be set if General.Kafka.SASLPlain.Enabled is set to true.")

		case c.General.Profile.Enabled && c.General.Profile.Address == "":
			logger.Infof("Profiling enabled and General.Profile.Address unset, setting to %s", Defaults.General.Profile.Address)
			c.General.Profile.Address = Defaults.General.Profile.Address

		case c.General.LocalMSPDir == "":
			logger.Infof("General.LocalMSPDir unset, setting to %s", Defaults.General.LocalMSPDir)
			c.General.LocalMSPDir = Defaults.General.LocalMSPDir
		case c.General.LocalMSPID == "":
			logger.Infof("General.LocalMSPID unset, setting to %s", Defaults.General.LocalMSPID)
			c.General.LocalMSPID = Defaults.General.LocalMSPID

		case c.General.Authentication.TimeWindow == 0:
			logger.Infof("General.Authentication.TimeWindow unset, setting to %s", Defaults.General.Authentication.TimeWindow)
			c.General.Authentication.TimeWindow = Defaults.General.Authentication.TimeWindow

		case c.Kafka.Retry.ShortInterval == 0:
			logger.Infof("Kafka.Retry.ShortInterval unset, setting to %v", Defaults.Kafka.Retry.ShortInterval)
			c.Kafka.Retry.ShortInterval = Defaults.Kafka.Retry.ShortInterval
		case c.Kafka.Retry.ShortTotal == 0:
			logger.Infof("Kafka.Retry.ShortTotal unset, setting to %v", Defaults.Kafka.Retry.ShortTotal)
			c.Kafka.Retry.ShortTotal = Defaults.Kafka.Retry.ShortTotal
		case c.Kafka.Retry.LongInterval == 0:
			logger.Infof("Kafka.Retry.LongInterval unset, setting to %v", Defaults.Kafka.Retry.LongInterval)
			c.Kafka.Retry.LongInterval = Defaults.Kafka.Retry.LongInterval
		case c.Kafka.Retry.LongTotal == 0:
			logger.Infof("Kafka.Retry.LongTotal unset, setting to %v", Defaults.Kafka.Retry.LongTotal)
			c.Kafka.Retry.LongTotal = Defaults.Kafka.Retry.LongTotal

		case c.Kafka.Retry.NetworkTimeouts.DialTimeout == 0:
			logger.Infof("Kafka.Retry.NetworkTimeouts.DialTimeout unset, setting to %v", Defaults.Kafka.Retry.NetworkTimeouts.DialTimeout)
			c.Kafka.Retry.NetworkTimeouts.DialTimeout = Defaults.Kafka.Retry.NetworkTimeouts.DialTimeout
		case c.Kafka.Retry.NetworkTimeouts.ReadTimeout == 0:
			logger.Infof("Kafka.Retry.NetworkTimeouts.ReadTimeout unset, setting to %v", Defaults.Kafka.Retry.NetworkTimeouts.ReadTimeout)
			c.Kafka.Retry.NetworkTimeouts.ReadTimeout = Defaults.Kafka.Retry.NetworkTimeouts.ReadTimeout
		case c.Kafka.Retry.NetworkTimeouts.WriteTimeout == 0:
			logger.Infof("Kafka.Retry.NetworkTimeouts.WriteTimeout unset, setting to %v", Defaults.Kafka.Retry.NetworkTimeouts.WriteTimeout)
			c.Kafka.Retry.NetworkTimeouts.WriteTimeout = Defaults.Kafka.Retry.NetworkTimeouts.WriteTimeout

		case c.Kafka.Retry.Metadata.RetryBackoff == 0:
			logger.Infof("Kafka.Retry.Metadata.RetryBackoff unset, setting to %v", Defaults.Kafka.Retry.Metadata.RetryBackoff)
			c.Kafka.Retry.Metadata.RetryBackoff = Defaults.Kafka.Retry.Metadata.RetryBackoff
		case c.Kafka.Retry.Metadata.RetryMax == 0:
			logger.Infof("Kafka.Retry.Metadata.RetryMax unset, setting to %v", Defaults.Kafka.Retry.Metadata.RetryMax)
			c.Kafka.Retry.Metadata.RetryMax = Defaults.Kafka.Retry.Metadata.RetryMax

		case c.Kafka.Retry.Producer.RetryBackoff == 0:
			logger.Infof("Kafka.Retry.Producer.RetryBackoff unset, setting to %v", Defaults.Kafka.Retry.Producer.RetryBackoff)
			c.Kafka.Retry.Producer.RetryBackoff = Defaults.Kafka.Retry.Producer.RetryBackoff
		case c.Kafka.Retry.Producer.RetryMax == 0:
			logger.Infof("Kafka.Retry.Producer.RetryMax unset, setting to %v", Defaults.Kafka.Retry.Producer.RetryMax)
			c.Kafka.Retry.Producer.RetryMax = Defaults.Kafka.Retry.Producer.RetryMax

		case c.Kafka.Retry.Consumer.RetryBackoff == 0:
			logger.Infof("Kafka.Retry.Consumer.RetryBackoff unset, setting to %v", Defaults.Kafka.Retry.Consumer.RetryBackoff)
			c.Kafka.Retry.Consumer.RetryBackoff = Defaults.Kafka.Retry.Consumer.RetryBackoff

		case c.Kafka.Version == sarama.KafkaVersion{}:
			logger.Infof("Kafka.Version unset, setting to %v", Defaults.Kafka.Version)
			c.Kafka.Version = Defaults.Kafka.Version

		case c.Admin.TLS.Enabled && !c.Admin.TLS.ClientAuthRequired:
			logger.Panic("Admin.TLS.ClientAuthRequired must be set to true if Admin.TLS.Enabled is set to true")

		default:
			return
		}
	}
}

func translateCAs(configDir string, certificateAuthorities []string) []string {
	var results []string
	for _, ca := range certificateAuthorities {
		result := coreconfig.TranslatePath(configDir, ca)
		results = append(results, result)
	}
	return results
}
