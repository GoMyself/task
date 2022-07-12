package common

import (
	"task/contrib/apollo"
)

type Conf struct {
	Lang         string `json:"lang"`
	Prefix       string `json:"prefix"`
	PullPrefix   string `json:"pull_prefix"`
	IsDev        bool   `json:"is_dev"`
	Sock5        string `json:"sock5"`
	RPC          string `json:"rpc"`
	Fcallback    string `json:"fcallback"`
	AutoPayLimit string `json:"autoPayLimit"`
	Nats         struct {
		Servers  []string `json:"servers"`
		Username string   `json:"username"`
		Password string   `json:"password"`
	} `json:"nats"`
	Rocketmq   []string `json:"rocketmq"`
	Beanstalkd struct {
		Addr    string `json:"addr"`
		MaxIdle int    `json:"maxIdle"`
		MaxCap  int    `json:"maxCap"`
	} `json:"beanstalkd"`
	Db struct {
		Master struct {
			Addr        string `json:"addr"`
			MaxIdleConn int    `json:"max_idle_conn"`
			MaxOpenConn int    `json:"max_open_conn"`
		} `json:"master"`
		Report struct {
			Addr        string `json:"addr"`
			MaxIdleConn int    `json:"max_idle_conn"`
			MaxOpenConn int    `json:"max_open_conn"`
		} `json:"report"`
		Bet struct {
			Addr        string `json:"addr"`
			MaxIdleConn int    `json:"max_idle_conn"`
			MaxOpenConn int    `json:"max_open_conn"`
		} `json:"bet"`
	} `json:"db"`
	Td struct {
		Addr        string `json:"addr"`
		MaxIdleConn int    `json:"max_idle_conn"`
		MaxOpenConn int    `json:"max_open_conn"`
	} `json:"td"`
	Redis struct {
		Addr     []string `json:"addr"`
		Password string   `json:"password"`
	} `json:"redis"`
	Minio struct {
		ImagesBucket    string `json:"images_bucket"`
		JSONBucket      string `json:"json_bucket"`
		Endpoint        string `json:"endpoint"`
		AccessKeyID     string `json:"accessKeyID"`
		SecretAccessKey string `json:"secretAccessKey"`
		UseSSL          bool   `json:"useSSL"`
		UploadURL       string `json:"uploadUrl"`
	} `json:"minio"`
}

func ConfParse(endpoints []string, path string) Conf {

	cfg := Conf{}

	apollo.New(endpoints)
	apollo.Parse(path, &cfg)
	apollo.Close()

	return cfg
}

func ConfPlatParse(endpoints []string, path string) (Conf, map[string]map[string]interface{}, error) {

	cfg := Conf{}
	platCfg := map[string]map[string]interface{}{}

	apollo.New(endpoints)
	apollo.Parse(path, &cfg)
	platformCfg := "/common/platform.toml"

	platCfg, err := apollo.ParseToml(platformCfg, true)
	if err != nil {
		return cfg, platCfg, err
	}

	apollo.Close()

	return cfg, platCfg, nil
}
