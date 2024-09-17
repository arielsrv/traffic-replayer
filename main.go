package main

import (
	"fmt"
	"github.com/alitto/pond"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/samber/lo"
	"github.com/spf13/viper"
	"io"
	"net/http"
	"os"
	"slices"
	"strings"
)

var (
	c        = 10
	n        = 100
	filePath string
	baseURL  string
	pool     *pond.WorkerPool
)

func init() {
	loadCfg()
	loadPool()
}

func main() {
	bytes, err := os.ReadFile(filePath)
	if err != nil {
		panic(err)
	}

	http.Handle("/metrics", promhttp.Handler())

	go func(pool *pond.WorkerPool) {
		items := slices.
			Chunk(lo.
				Shuffle(strings.
					Split(string(bytes), "\n")), n)

		for chunks := range items {
			for _, id := range chunks {
				apiURL := fmt.Sprintf(baseURL, id)
				fmt.Printf("fetching: %s\n", apiURL)
				pool.Submit(func() {
					request, rErr := http.NewRequest(http.MethodGet, apiURL, http.NoBody)
					if rErr != nil {
						fmt.Printf("error creating request: %v\n", rErr)
						return
					}
					response, rErr := http.DefaultClient.Do(request)
					if rErr != nil {
						fmt.Printf("error making request: %v\n", rErr)
						return
					}
					defer func(Body io.ReadCloser) {
						cErr := Body.Close()
						if cErr != nil {
							fmt.Printf("error closing response body: %v\n", cErr)
						}
					}(response.Body)
				})
			}
		}
	}(pool)

	if err = http.ListenAndServe(":8080", nil); err != nil {
		panic(err)
	}
}

func loadCfg() {
	conf := viper.New()
	conf.SetConfigFile("config.yaml")
	err := conf.ReadInConfig() // Find and read the config file
	if err != nil {            // Handle errors reading the config file
		panic(fmt.Errorf("fatal error config file: %w", err))
	}

	c = conf.GetInt("c")
	n = conf.GetInt("n")
	filePath = conf.GetString("filePath")
	baseURL = conf.GetString("baseUrl")
}

func loadPool() {
	pool = pond.New(c, n)

	// Register pool metrics collectors

	// Worker pool metrics
	prometheus.MustRegister(prometheus.NewGaugeFunc(
		prometheus.GaugeOpts{
			Name: "pool_workers_running",
			Help: "Number of running worker goroutines",
		},
		func() float64 {
			return float64(pool.RunningWorkers())
		}))
	prometheus.MustRegister(prometheus.NewGaugeFunc(
		prometheus.GaugeOpts{
			Name: "pool_workers_idle",
			Help: "Number of idle worker goroutines",
		},
		func() float64 {
			return float64(pool.IdleWorkers())
		}))

	// Task metrics
	prometheus.MustRegister(prometheus.NewCounterFunc(
		prometheus.CounterOpts{
			Name: "pool_tasks_submitted_total",
			Help: "Number of tasks submitted",
		},
		func() float64 {
			return float64(pool.SubmittedTasks())
		}))
	prometheus.MustRegister(prometheus.NewGaugeFunc(
		prometheus.GaugeOpts{
			Name: "pool_tasks_waiting_total",
			Help: "Number of tasks waiting in the queue",
		},
		func() float64 {
			return float64(pool.WaitingTasks())
		}))
	prometheus.MustRegister(prometheus.NewCounterFunc(
		prometheus.CounterOpts{
			Name: "pool_tasks_successful_total",
			Help: "Number of tasks that completed successfully",
		},
		func() float64 {
			return float64(pool.SuccessfulTasks())
		}))
	prometheus.MustRegister(prometheus.NewCounterFunc(
		prometheus.CounterOpts{
			Name: "pool_tasks_failed_total",
			Help: "Number of tasks that completed with panic",
		},
		func() float64 {
			return float64(pool.FailedTasks())
		}))
	prometheus.MustRegister(prometheus.NewCounterFunc(
		prometheus.CounterOpts{
			Name: "pool_tasks_completed_total",
			Help: "Number of tasks that completed either successfully or with panic",
		},
		func() float64 {
			return float64(pool.CompletedTasks())
		}))
}
