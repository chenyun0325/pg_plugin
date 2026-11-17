package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	if e := entry(); e != nil && !errors.Is(e, context.Canceled) {
		log.Print(e)
		os.Exit(1)
	}
}
func entry() error {
	if len(os.Args) < 2 {
		return fmt.Errorf("usage: pgcat-yaml check|init|run --config file.yaml [--once] [--recreate-slot]; convert-wms --input flink.yaml --output pgcat.yaml")
	}
	cmd := os.Args[1]
	fs := flag.NewFlagSet(cmd, flag.ContinueOnError)
	path := fs.String("config", "", "YAML configuration")
	once := fs.Bool("once", false, "apply one decoding batch")
	reset := fs.Bool("recreate-slot", false, "explicitly recreate an uninitialized existing slot")
	input := fs.String("input", "", "Flink YAML")
	output := fs.String("output", "", "generated pgcat YAML")
	if e := fs.Parse(os.Args[2:]); e != nil {
		return e
	}
	if cmd == "convert-wms" {
		return convertWMS(*input, *output)
	}
	c, e := loadConfig(*path)
	if e != nil {
		return e
	}
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	r, e := openRuntime(ctx, c)
	if e != nil {
		return e
	}
	defer r.close()
	if cmd == "check" {
		log.Printf("configuration valid job=%s source_tables=%d hash=%s", c.Job, len(r.tables), c.digest)
		return nil
	}
	if cmd != "init" && cmd != "run" {
		return fmt.Errorf("unknown command %s", cmd)
	}
	if e = r.prepare(); e != nil {
		return e
	}
	if cmd == "init" {
		return r.init(*reset)
	}
	return r.run(*once)
}
