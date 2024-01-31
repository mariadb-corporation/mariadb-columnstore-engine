package main

import (
	"database/sql"
	"log"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/spf13/afero"
	"github.com/yandex/pandora/cli"
	"github.com/yandex/pandora/core"
	"github.com/yandex/pandora/core/aggregator/netsample"
	coreimport "github.com/yandex/pandora/core/import"
	"github.com/yandex/pandora/core/register"
)

type Ammo struct {
	Request string
	Tag     string
}

func customAmmoProvider() core.Ammo {
	return &Ammo{}
}

type GunConfig struct {
	Target string `validate:"required"`
}

type Gun struct {
	db   *sql.DB
	conf GunConfig
	aggr core.Aggregator
}

func NewGun(conf GunConfig) *Gun {
	return &Gun{
		conf: conf,
	}
}

func (g *Gun) Bind(aggr core.Aggregator, deps core.GunDeps) error {
	db, err := sql.Open("mysql", g.conf.Target)

	if err != nil {
		log.Fatalf("Error: %s", err)
	}
	g.db = db
	g.aggr = aggr

	return nil
}

func makeArgs(args ...interface{}) []interface{} {
	if len(args) == 0 {
		return []interface{}{}
	}
	return args
}

func (g *Gun) queueCall(request string) (string, error) {
	var version string
	err := g.db.QueryRow(request).Scan(&version)
	return version, err

}

func (g *Gun) Shoot(coreAmmo core.Ammo) {
	ammo := coreAmmo.(*Ammo)
	sample := netsample.Acquire(ammo.Tag)

	code := 200
	var err error
	startTime := time.Now()
	g.queueCall(ammo.Request)
	sample.SetLatency(time.Since(startTime))
	if err != nil {
		log.Printf("Error %s task: %s", ammo.Tag, err)
		code = 500
	}

	defer func() {
		sample.SetProtoCode(code)
		sample.AddTag("SQL")
		g.aggr.Report(sample)
	}()
}

func main() {
	fs := afero.NewOsFs()
	coreimport.Import(fs)

	coreimport.RegisterCustomJSONProvider("mariadb_ammo", customAmmoProvider)
	register.Gun("mariadb_gun", NewGun, func() GunConfig {
		return GunConfig{
			Target: "root:@unix(/run/mysqld/mysqld.sock)/test",
		}
	})
	cli.Run()
}
